#!/usr/bin/env python3
"""
extrator.py - Extrator de rodadas (Playwright + hooks JS) com suporte a PROXY

Variáveis suportadas:
  # alvo
  GAME_URL                      (default: https://blaze.bet.br/pt/games/double)

  # Supabase
  SUPABASE_ROUNDS_URL
  SUPABASE_TOKEN

  # Sinais externos (opcional)
  AWS_SIGNAL_URL
  AWS_TOKEN

  # Ajustes
  HISTORY_MAXLEN                (default: 2000)
  HEARTBEAT_SECS                (default: 60)
  USER_AGENT                    (opcional)
  DEBUG                         (true/false)

  # Proxy (escolha um modelo)
  PROXY_URL                     Ex.: http://user:pass@62.192.172.251:12345
  -- ou --
  PROXY_HOST, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD
"""

import os
import json
import asyncio
import hashlib
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Deque, Dict, Any, List, Optional

from playwright.async_api import async_playwright
import aiohttp

# ---------------- CONFIG ----------------
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")

SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")

AWS_SIGNAL_URL = os.getenv("AWS_SIGNAL_URL", "")
AWS_TOKEN = os.getenv("AWS_TOKEN", "")

HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
USER_AGENT = os.getenv("USER_AGENT", "").strip()
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

DEDUPE_MAX = int(os.getenv("DEDUPE_MAX", "4000"))

# Proxy – aceita URL completa ou campos separados
PROXY_URL = os.getenv("PROXY_URL", "").strip()
PROXY_HOST = os.getenv("PROXY_HOST", "").strip()
PROXY_PORT = os.getenv("PROXY_PORT", "").strip()
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "").strip()

# -------------- utils --------------
def _mask_secret(s: str, keep=2) -> str:
    """Mascarar segredos em logs (exibe só prefixo)."""
    if not s:
        return s
    return s[:keep] + "***"

def _build_playwright_proxy() -> Optional[Dict[str, str]]:
    """
    Retorna o dict 'proxy' aceito por chromium.launch() ou None se não definido.
      {'server': 'http://host:port', 'username': '...', 'password': '...'}
    """
    # PRIORIDADE: PROXY_URL (com ou sem credenciais)
    if PROXY_URL:
        # Playwright aceita URL com user:pass@host:port
        # Mas vamos quebrar para poder mascarar no log
        from urllib.parse import urlparse
        u = urlparse(PROXY_URL)
        if not u.scheme or not u.hostname or not u.port:
            print("[PROXY] PROXY_URL inválida; ignorando.")
            return None
        data = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
        if u.username:
            data["username"] = u.username
        if u.password:
            data["password"] = u.password

        if DEBUG:
            masked_user = _mask_secret(data.get("username", ""))
            masked_pass = _mask_secret(data.get("password", ""))
            print(f"[PROXY] via URL -> server={data['server']} user={masked_user} pass={masked_pass}")
        return data

    # OU: campos separados
    if PROXY_HOST and PROXY_PORT:
        server = f"http://{PROXY_HOST}:{PROXY_PORT}"
        data = {"server": server}
        if PROXY_USERNAME:
            data["username"] = PROXY_USERNAME
        if PROXY_PASSWORD:
            data["password"] = PROXY_PASSWORD
        if DEBUG:
            masked_user = _mask_secret(data.get("username", ""))
            masked_pass = _mask_secret(data.get("password", ""))
            print(f"[PROXY] via campos -> server={server} user={masked_user} pass={masked_pass}")
        return data

    # Sem proxy
    return None

# dedupe em nível de Python
_DEDUPE: set = set()

# ---------------- Hook JS (injeta dentro da página) ----------------
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
      const arr = JSON.parse(str.slice(2)); // ["data", {...}]
      const eventName = arr?.[0];
      const body      = arr?.[1];
      if (eventName === "data") tryEmitResult(body);
    } catch (e) {}
  }

  // Hook fetch
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
    } catch (e) {}
    return res;
  };

  // Hook XHR
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
        } catch (e) {}
      });
    }
    return _send.call(this, body);
  };

  // Hook WebSocket
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

# ---------------- helpers ----------------
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

# ---------------- envio supabase ----------------
async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]) -> None:
    if not SUPABASE_ROUNDS_URL:
        if DEBUG:
            print("[SUPABASE] url vazia, pulando envio")
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
    try:
        async with session.post(SUPABASE_ROUNDS_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status >= 400:
                print(f"[SUPABASE] {resp.status}: {body[:240]}")
            elif DEBUG:
                print(f"[SUPABASE][ok] {resp.status} body={body[:240]}")
    except Exception as e:
        print(f"[SUPABASE] exception: {e}")

# ---------------- estratégia ----------------
def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    # Exemplo simples
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
                "ts": datetime.now(timezone.utc).isoformat()
            })
    return signals

# ---------------- envio AWS (sinais) ----------------
async def send_signals_to_aws(session: aiohttp.ClientSession, signals: List[Dict[str, Any]]) -> None:
    if not AWS_SIGNAL_URL or not signals:
        if DEBUG and signals:
            print("[AWS] url vazia, pulando envio")
        return
    headers = {"Content-Type": "application/json"}
    if AWS_TOKEN:
        headers["Authorization"] = f"Bearer {AWS_TOKEN}"
    payload = {"signals": signals}
    try:
        async with session.post(AWS_SIGNAL_URL, headers=headers, json=payload) as resp:
            if resp.status >= 400:
                body = await resp.text()
                print(f"[AWS] {resp.status}: {body[:240]}")
            elif DEBUG:
                print(f"[AWS] ok {resp.status}")
    except Exception as e:
        print(f"[AWS] exception: {e}")

# ---------------- heartbeat ----------------
async def heartbeat(history: Deque[Dict[str, Any]], stop_evt: asyncio.Event, interval: int):
    if interval <= 0:
        return
    last = -1
    while not stop_evt.is_set():
        await asyncio.sleep(interval)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last or DEBUG:
            last = total
            print(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} black={counts.get('black',0)} white={counts.get('white',0)}")

# ---------------- main ----------------
async def main():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with async_playwright() as p:
            # Flags de estabilidade para containers
            launch_kwargs: Dict[str, Any] = dict(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-software-rasterizer",
                    "--mute-audio",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--disable-ipc-flooding-protection",
                ],
            )

            # PROXY (se houver)
            proxy_cfg = _build_playwright_proxy()
            if proxy_cfg:
                launch_kwargs["proxy"] = proxy_cfg

            if DEBUG:
                dbg = dict(launch_kwargs)
                if "proxy" in dbg:
                    pr = dbg["proxy"].copy()
                    if "username" in pr:
                        pr["username"] = _mask_secret(pr["username"])
                    if "password" in pr:
                        pr["password"] = _mask_secret(pr["password"])
                    dbg["proxy"] = pr
                print(f"[BOOT] launching chromium with: {dbg}")

            browser = await p.chromium.launch(**launch_kwargs)

            context_args: Dict[str, Any] = {}
            if USER_AGENT:
                context_args["user_agent"] = USER_AGENT
            ctx = await browser.new_context(**context_args)
            page = await ctx.new_page()

            # ---------- console handler ----------
            async def on_console_msg(msg):
                try:
                    text = msg.text() if callable(getattr(msg, "text", None)) else msg.text
                except Exception:
                    try:
                        text = msg.text()
                    except Exception:
                        text = "<unreadable-console-message>"

                # Log raw console para debug (não imprime secrets)
                if DEBUG:
                    print(f"[CONSOLE-RAW] {text}")

                if not isinstance(text, str) or not text.startswith("__RESULT__"):
                    return

                try:
                    evt = json.loads(text[len("__RESULT__"):])
                except Exception:
                    print("[CONSOLE] __RESULT__ JSON parse failed")
                    return

                key = _evt_key(evt)
                if key in _DEDUPE:
                    return
                _DEDUPE.add(key)
                if len(_DEDUPE) > DEDUPE_MAX:
                    _DEDUPE.clear()
                    _DEDUPE.add(key)

                print(f"[RESULT] {evt.get('roundId')} -> {str(evt.get('color')).upper()} ({evt.get('roll')}) @ {evt.get('at') or ''}")

                history.append({
                    "roundId": evt.get("roundId"),
                    "color": evt.get("color"),
                    "roll": evt.get("roll"),
                    "at": _parse_occurred_at(evt.get("at")),
                })

                try:
                    await send_to_supabase(session, evt)
                except Exception as e:
                    print(f"[SUPABASE] erro: {e}")

                try:
                    signals = apply_strategies(history)
                    if signals:
                        await send_signals_to_aws(session, signals)
                except Exception as e:
                    print(f"[AWS] erro sinais: {e}")

            def console_listener(m):
                asyncio.create_task(on_console_msg(m))
            page.on("console", console_listener)
            page.on("close", lambda *a, **k: None)

            await page.add_init_script(HOOK_JS)

            for attempt in range(3):
                try:
                    print(f"[NAV] indo para {GAME_URL} (tentativa {attempt+1})")
                    await page.goto(GAME_URL, wait_until="domcontentloaded", timeout=45_000)
                    break
                except Exception as e:
                    print(f"[NAV] tentativa {attempt+1} falhou: {e}")
                    if attempt == 2:
                        raise

            hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))

            print("Coletando RESULTADOS… (Ctrl+C para sair)")
            try:
                await asyncio.Event().wait()
            except (KeyboardInterrupt, SystemExit):
                pass
            finally:
                stop_evt.set()
                hb_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
