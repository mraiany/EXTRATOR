# filepath: extrator-de-rodadas/src/extrator.py

import os
import json
import asyncio
import hashlib
import signal
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Deque, Dict, Any, List, Optional

from playwright.async_api import async_playwright
import aiohttp

CONFIG = {
    "USE_ENV": True,
    "GAME_URL": "https://blaze.bet.br/pt/games/double",
    "SUPABASE_ROUNDS_URL": "https://dudpqqsguexasxbrvykn.supabase.co/rest/v1/historico",
    "SUPABASE_TOKEN": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR1ZHBxcXNndWV4YXN4YnJ2eWtuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTc4OTUwOTYsImV4cCI6MjA3MzQ3MTA5Nn0.YTA9O0CUfrUpqXbRTQOb3IpcX0Fk3HyWjOFK23zM3zs",
    "AWS_SIGNAL_URL": "",
    "AWS_TOKEN": "",
    "HISTORY_MAXLEN": 2000,
    "HEARTBEAT_SECS": 60,
    "USER_AGENT": "",
    "PROXY": "",
    "DEBUG": False,
}

def _boolenv(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "on")

if CONFIG["USE_ENV"]:
    GAME_URL = os.getenv("GAME_URL", CONFIG["GAME_URL"])
    SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", CONFIG["SUPABASE_ROUNDS_URL"])
    SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", CONFIG["SUPABASE_TOKEN"])
    AWS_SIGNAL_URL = os.getenv("AWS_SIGNAL_URL", CONFIG["AWS_SIGNAL_URL"])
    AWS_TOKEN = os.getenv("AWS_TOKEN", CONFIG["AWS_TOKEN"])
    HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", str(CONFIG["HISTORY_MAXLEN"])))
    HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", str(CONFIG["HEARTBEAT_SECS"])))
    USER_AGENT = os.getenv("USER_AGENT", CONFIG["USER_AGENT"]).strip()
    PROXY = os.getenv("PROXY", CONFIG["PROXY"]).strip()
    DEBUG = _boolenv(os.getenv("DEBUG"), CONFIG["DEBUG"])
else:
    GAME_URL = CONFIG["GAME_URL"]
    SUPABASE_ROUNDS_URL = CONFIG["SUPABASE_ROUNDS_URL"]
    SUPABASE_TOKEN = CONFIG["SUPABASE_TOKEN"]
    AWS_SIGNAL_URL = CONFIG["AWS_SIGNAL_URL"]
    AWS_TOKEN = CONFIG["AWS_TOKEN"]
    HISTORY_MAXLEN = int(CONFIG["HISTORY_MAXLEN"])
    HEARTBEAT_SECS = int(CONFIG["HEARTBEAT_SECS"])
    USER_AGENT = CONFIG["USER_AGENT"].strip()
    PROXY = CONFIG["PROXY"].strip()
    DEBUG = CONFIG["DEBUG"]

_DEDUPE: set = set()
_DEDUPE_MAX = 4000

HOOK_JS = r"""
(() => {
  const COLORS = { 1: "red", 0: "white", 2: "black" };
  window.__emittedResults = window.__emittedResults || new Set();

  function tryEmitResult(obj) {
    if (!obj || !obj.id || !obj.payload) return false;

    const isResult =
      obj.id === "double.result" ||
      obj.id === "double:result" ||
      obj.id === "new:game_result" ||
      (obj.id === "double.tick" &&
        obj.payload &&
        obj.payload.color != null &&
        obj.payload.roll != null);

    if (!isResult) return false;

    const rid   = obj.payload?.id || null;
    const color = COLORS[obj.payload?.color] ?? String(obj.payload?.color);
    const roll  = obj.payload?.roll ?? null;
    const at    = obj.payload?.created_at || obj.payload?.updated_at || null;

    const key = `${rid}|${roll}|${at || ""}`;
    if (window.__emittedResults.has(key)) return true;
    window.__emittedResults.add(key);

    console.log("__RESULT__" + JSON.stringify({ roundId: rid, color, roll, at }));
    return true;
  }

  function parseSocketIoPacket(str) {
    if (typeof str !== "string") return;
    if (!str.startsWith("42")) return;
    try {
      const arr = JSON.parse(str.slice(2));
      const eventName = arr?.[0];
      const body      = arr?.[1];
      if (eventName === "data") tryEmitResult(body);
    } catch {}
  }

  const _fetch = window.fetch;
  window.fetch = async function (...args) {
    const res = await _fetch.apply(this, args);
    try {
      const url = (typeof args[0] === "string" ? args[0] : args[0]?.url) || "";
      if (url.includes("/socket.io")) {
        const clone = res.clone();
        clone.text().then(text => {
          const parts = String(text || "").split("42[");
          for (let i = 1; i < parts.length; i++) parseSocketIoPacket("42[" + parts[i]);
        }).catch(() => {});
      }
    } catch {}
    return res;
  };

  const _open = XMLHttpRequest.prototype.open;
  const _send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    this.__isSock = url && url.includes && url.includes("/socket.io");
    return _open.call(this, method, url, ...rest);
  };
  XMLHttpRequest.prototype.send = function (body) {
    if (this.__isSock) {
      this.addEventListener("load", function () {
        try {
          const text = (this.responseType === "" || this.responseType === "text") ? (this.responseText || "") : "";
          const parts = String(text || "").split("42[");
          for (let i = 1; i < parts.length; i++) parseSocketIoPacket("42[" + parts[i]);
        } catch {}
      });
    }
    return _send.call(this, body);
  };

  const OriginalWS = window.WebSocket;
  window.WebSocket = function (url, protocols) {
    const ws = new OriginalWS(url, protocols);
    ws.addEventListener("message", (evt) => {
      if (typeof evt.data === "string") parseSocketIoPacket(evt.data);
    });
    return ws;
  };
})();
"""

def _evt_key(evt: Dict[str, Any]) -> str:
    rid = str(evt.get("roundId") or "")
    roll = str(evt.get("roll") or "")
    at = str(evt.get("at") or "")
    base = f"{rid}|{roll}|{at}"
    return base if rid else hashlib.sha1(base.encode()).hexdigest()

def _parse_occurred_at(at: Optional[str]) -> str:
    if not at:
        return datetime.now(timezone.utc).isoformat()
    try:
        if at.endswith("Z"):
            dt = datetime.fromisoformat(at.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(at)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]) -> None:
    if not SUPABASE_ROUNDS_URL:
        if DEBUG:
            print("[SUPABASE] URL não definido — pulando")
        return
    payload = {
        "round_id": evt.get("roundId"),
        "color": evt.get("color"),
        "roll": evt.get("roll"),
        "occurred_at": _parse_occurred_at(evt.get("at")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    headers = {"Content-Type": "application/json"}
    if SUPABASE_TOKEN:
        headers["apikey"] = SUPABASE_TOKEN
        headers["Authorization"] = f"Bearer {SUPABASE_TOKEN}"
        headers["Prefer"] = "resolution=merge-duplicates"
    async with session.post(SUPABASE_ROUNDS_URL, headers=headers, json=payload) as resp:
        body = await resp.text()
        if resp.status >= 400:
            print(f"[SUPABASE] {resp.status}: {body[:240]}")
        elif DEBUG:
            print(f"[SUPABASE][OK] {resp.status}: {body[:180]}")

def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    N = 12
    last_white_idx = None
    for idx, r in reversed(list(enumerate(history))):
        if r.get("color") == "white":
            last_white_idx = idx
            break
    if last_white_idx is not None:
        gap = len(history) - 1 - last_white_idx
        if gap > N:
            signals.append({
                "action": "bet",
                "target": "white",
                "stake": 1,
                "reason": f"anti-streak-white>{N}",
                "ts": datetime.now(timezone.utc).isoformat(),
            })
    return signals

async def send_signals_to_aws(session: aiohttp.ClientSession, signals: List[Dict[str, Any]]) -> None:
    if not AWS_SIGNAL_URL or not signals:
        return
    headers = {"Content-Type": "application/json"}
    if AWS_TOKEN:
        headers["Authorization"] = f"Bearer {AWS_TOKEN}"
    payload = {"signals": signals}
    async with session.post(AWS_SIGNAL_URL, headers=headers, json=payload) as resp:
        if resp.status >= 400:
            body = await resp.text()
            print(f"[AWS] {resp.status}: {body[:240]}")

async def heartbeat(history: Deque[Dict[str, Any]], stop_evt: asyncio.Event, interval: int):
    if interval <= 0:
        return
    while not stop_evt.is_set():
        await asyncio.sleep(interval)
        c = Counter([r.get("color") for r in history])
        print(f"[HEARTBEAT] rounds={len(history)} red={c.get('red',0)} black={c.get('black',0)} white={c.get('white',0)}")

async def run_once():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with async_playwright() as p:
            launch_kwargs = dict(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                ],
            )
            if PROXY:
                launch_kwargs["proxy"] = {"server": PROXY}

            browser = await p.chromium.launch(**launch_kwargs)

            context_args = {}
            if USER_AGENT:
                context_args["user_agent"] = USER_AGENT
            ctx = await browser.new_context(**context_args)
            page = await ctx.new_page()

            async def on_console(msg):
                try:
                    text = msg.text
                    if not isinstance(text, str) or not text.startswith("__RESULT__"):
                        return
                    evt = json.loads(text[len("__RESULT__"):])

                    key = _evt_key(evt)
                    if key in _DEDUPE:
                        return
                    _DEDUPE.add(key)
                    if len(_DEDUPE) > _DEDUPE_MAX:
                        _DEDUPE.clear(); _DEDUPE.add(key)

                    print(f"[RESULT] {evt.get('roundId')} -> {str(evt.get('color')).upper()} ({evt.get('roll')}) @ {evt.get('at') or ''}")

                    history.append({
                        "roundId": evt.get("roundId"),
                        "color": evt.get("color"),
                        "roll": evt.get("roll"),
                        "at": _parse_occurred_at(evt.get("at")),
                    })

                    await send_to_supabase(session, evt)
                    signals = apply_strategies(history)
                    if signals:
                        await send_signals_to_aws(session, signals)

                except Exception as e:
                    print(f"[CONSOLE] erro: {e}")

            page.on("console", lambda m: asyncio.create_task(on_console(m)))

            await page.add_init_script(HOOK_JS)
            for attempt in range(5):
                try:
                    await page.goto(GAME_URL, wait_until="domcontentloaded", timeout=60_000)
                    break
                except Exception as e:
                    print(f"[NAV] tentativa {attempt+1} falhou: {e}")
                    await asyncio.sleep(3)
                    if attempt == 4:
                        raise

            hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))
            print("Coletando RESULTADOS… (SIGTERM para sair)")

            await asyncio.Event().wait()

            stop_evt.set()
            hb_task.cancel()

def _install_sigterm_handler(loop: asyncio.AbstractEventLoop):
    stop = asyncio.Event()
    def _graceful(*_):
        print("[SIGNAL] SIGTERM recebido — encerrando...")
        for t in asyncio.all_tasks(loop):
            if t is not asyncio.current_task(loop):
                t.cancel()
        loop.call_soon_threadsafe(stop.set)
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _graceful)
        except NotImplementedError:
            pass
    return stop

async def main():
    loop = asyncio.get_running_loop()
    stop_evt = _install_sigterm_handler(loop)

    backoff = 3
    while True:
        try:
            await run_once()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[MAIN] erro: {e}. Reiniciando em {backoff}s…")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())