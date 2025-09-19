#!/usr/bin/env python3
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
PROXY = os.getenv("PROXY", "").strip()
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")
_DEDUPE: set = set()
_DEDUPE_MAX = int(os.getenv("DEDUPE_MAX", "4000"))

# Salas a assinar no socket (vírgula-separado)
SUB_ROOMS = os.getenv(
    "SUB_ROOMS",
    # tentamos vários nomes/aliases comuns
    "double,double_v2,roulette,roulette_v2,double:public,double.tick,double.result"
)

# ---------------- HOOK JS (parsing passivo) ----------------
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
    } catch {}
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
        }).catch(()=>{});
      }
    } catch {}
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
          const isText = (this.responseType === "" || this.responseType === "text");
        const text = isText ? (this.responseText || "") : "";
          const parts = String(text || "").split("42[");
          for (let i = 1; i < parts.length; i++) parseSocketIoPacket("42[" + parts[i]);
        } catch {}
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

# ---------------- utils ----------------
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

# ---------------- supabase ----------------
async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]) -> None:
    if not SUPABASE_ROUNDS_URL:
        if DEBUG: print("[SUPABASE] url vazia, pulando envio")
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

# ---------------- estratégia (exemplo) ----------------
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
                "ts": datetime.now(timezone.utc).isoformat()
            })
    return signals

# ---------------- envio AWS (opcional) ----------------
async def send_signals_to_aws(session: aiohttp.ClientSession, signals: List[Dict[str, Any]]) -> None:
    if not AWS_SIGNAL_URL or not signals:
        if DEBUG and signals: print("[AWS] url vazia, pulando envio")
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
    if interval <= 0: return
    last = -1
    while not stop_evt.is_set():
        await asyncio.sleep(interval)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last or DEBUG:
            last = total
            print(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} black={counts.get('black',0)} white={counts.get('white',0)}")

# ---------------- helpers ----------------
async def bootstrap_page(page):
    """Tenta aceitar cookies/garantir que o app inicializou antes do hook pegar eventos."""
    selectors = [
        'button:has-text("Aceitar")',
        'button:has-text("Accept")',
        '[data-testid*="accept"]',
        '[id*="accept"]',
        '[class*="accept"]',
    ]
    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click(force=True)
                console = f"[BOOTSTRAP] cliquei no consentimento via {sel}"
                print(console)
                break
        except:
            pass
    try:
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except:
        pass

# **novo**: cria um cliente socket.io próprio e assina salas
SUBSCRIBE_CLIENT = r"""
(async () => {
  try {
    const rooms = (window.__SUB_ROOMS || "").split(",").map(s => s.trim()).filter(Boolean);
    if (!rooms.length) { console.log("[SUB] nenhuma sala definida"); return; }

    // usa o socket.io exposto pelo bundle da página
    const target = "https://api-gaming.blaze.bet.br/replication";
    const io = window.io || (window.require && window.require("socket.io-client"));
    if (!io) { console.log("[SUB-ERR] window.io ausente"); return; }

    const sock = io(target, { transports: ["websocket"], forceNew: true });
    window.__SUB_SOCK = sock;

    sock.on("connect", () => {
      console.log("[SUB] conectado ao replication, emitindo subscribe...", rooms);
      try {
        sock.emit("subscribe", { rooms });
      } catch (e) {
        console.log("[SUB-ERR] erro ao emit subscribe", String(e));
      }
    });

    sock.on("error", (e) => console.log("[SUB-ERR] sock error", String(e)));
    sock.on("disconnect", (r) => console.log("[SUB] disconnect", String(r)));

    // evento "data" é onde vêm os payloads
    sock.on("data", (body) => {
      console.log("[SUB-EVT]", JSON.stringify(body).slice(0, 400));
      try {
        // reaproveita o mesmo tryEmitResult do HOOK (embed reduzido):
        const COLORS = { 1: "red", 0: "white", 2: "black" };
        const obj = body;
        if (obj && obj.id && obj.payload) {
          const isResult = obj.id === "double.result" || obj.id === "double:result" ||
                           obj.id === "new:game_result" ||
                           (obj.id === "double.tick" && obj.payload &&
                            obj.payload.color != null && obj.payload.roll != null);
          if (isResult) {
            const rid   = obj.payload?.id || null;
            const color = COLORS[obj.payload?.color] ?? String(obj.payload?.color);
            const roll  = obj.payload?.roll ?? null;
            const at    = obj.payload?.created_at || obj.payload?.updated_at || null;
            console.log("__RESULT__" + JSON.stringify({ roundId: rid, color, roll, at }));
          }
        }
      } catch (e) {}
    });
  } catch (e) {
    console.log("[SUB-ERR] fatal", String(e));
  }
})();
"""

# ---------------- main ----------------
async def main():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with async_playwright() as p:
            launch_kwargs = dict(
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
            if PROXY:
                launch_kwargs["proxy"] = {"server": PROXY}
            if DEBUG:
                print(f"[BOOT] launching chromium with: {launch_kwargs}")

            browser = await p.chromium.launch(**launch_kwargs)

            context_args = {
                "timezone_id": "America/Fortaleza",
                "locale": "pt-BR",
            }
            if USER_AGENT:
                context_args["user_agent"] = USER_AGENT

            ctx = await browser.new_context(**context_args)
            page = await ctx.new_page()

            # ---- logs de rede/socket (debug) ----
            if DEBUG:
                page.on("request", lambda r: "/socket.io" in r.url and print(f"[NET] socket.io req -> {r.method} {r.url}"))
                page.on("response", lambda r: "/socket.io" in r.url and print(f"[NET] socket.io resp -> {r.status} {r.url}"))
                def ws_open(ws):
                    print(f"[WS] open {ws.url}")
                    ws.on("framereceived", lambda data: print(f"[WS-IN] {str(data)[:200]}"))
                page.on("websocket", ws_open)

            # ---- console handler (__RESULT__) ----
            async def on_console_msg(msg):
                try:
                    text = msg.text() if callable(getattr(msg, "text", None)) else msg.text
                except Exception:
                    try: text = msg.text()
                    except Exception: text = "<unreadable-console-message>"

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
                    if DEBUG: print(f"[DEDUPE] skip {key}")
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

                try: await send_to_supabase(session, evt)
                except Exception as e: print(f"[SUPABASE] erro: {e}")

                try:
                    signals = apply_strategies(history)
                    if signals: await send_signals_to_aws(session, signals)
                except Exception as e:
                    print(f"[AWS] erro sinais: {e}")

            page.on("console", lambda m: asyncio.create_task(on_console_msg(m)))

            # injeta hook passivo antes de navegar
            await page.add_init_script(HOOK_JS)

            # navega
            last_err = None
            for attempt in range(1, 3+1):
                try:
                    if DEBUG: print(f"[NAV] indo para {GAME_URL} (tentativa {attempt})")
                    await page.goto(GAME_URL, wait_until="domcontentloaded", timeout=45_000)
                    # re-injeta hook e faz bootstrap
                    await page.evaluate(HOOK_JS)
                    await bootstrap_page(page)

                    # define as salas p/ o cliente ativo
                    await page.add_init_script(f'window.__SUB_ROOMS = {json.dumps(SUB_ROOMS)};')
                    await page.evaluate(f'window.__SUB_ROOMS = {json.dumps(SUB_ROOMS)};')

                    # cria cliente socket.io e assina
                    await page.evaluate(SUBSCRIBE_CLIENT)
                    break
                except Exception as e:
                    last_err = e
                    print(f"[NAV] tentativa {attempt} falhou: {e}")
                    if attempt == 3:
                        raise last_err

            # re-injeta hooks quando frames navegarem (apps SPA)
            page.on("framenavigated", lambda *_: asyncio.create_task(page.add_init_script(HOOK_JS)))

            # heartbeat
            hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))

            print("Coletando RESULTADOS… (Ctrl+C para sair)")
            try:
                await asyncio.Event().wait()
            except (KeyboardInterrupt, SystemExit):
                pass
            finally:
                stop_evt.set()
                hb_task.cancel()
                try: await ctx.close()
                except: pass
                try: await browser.close()
                except: pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
