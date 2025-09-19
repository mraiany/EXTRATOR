#!/usr/bin/env python3
"""
extrator.py - Extrator de rodadas usando Selenium + selenium-wire (suporte a SOCKS5)

Variáveis de ambiente necessárias:
    GAME_URL: URL do jogo Double na Blaze
    SUPABASE_ROUNDS_URL: endpoint REST do Supabase para inserir resultados
    SUPABASE_TOKEN: token do Supabase (apikey)
    USER_AGENT: UA a ser usado (opcional)
    PROXY_URL: endereço do proxy (ex: socks5://user:pass@ip:porta)
    DEBUG: true/false
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from collections import deque, Counter
from typing import Deque, Dict, Any

import aiohttp
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options

# Config via env
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")
USER_AGENT = os.getenv("USER_AGENT", "")
PROXY_URL = os.getenv("PROXY_URL", "")
HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# Dedupe para não repetir
_DEDUPE: set = set()
_DEDUPE_MAX = 4000


def parse_proxy(url: str) -> dict:
    """Converte URL de proxy em dict compatível com selenium-wire"""
    if not url:
        return {}
    return {
        "http": url,
        "https": url,
        "no_proxy": "localhost,127.0.0.1"
    }


def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--mute-audio")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-software-rasterizer")
    opts.headless = True

    if USER_AGENT:
        opts.add_argument(f"user-agent={USER_AGENT}")

    # Proxy via selenium-wire
    sw_opts = {}
    if PROXY_URL:
        sw_opts["proxy"] = parse_proxy(PROXY_URL)

    driver = webdriver.Chrome(
        options=opts,
        seleniumwire_options=sw_opts or None
    )
    return driver


async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]):
    """Envia dados para o Supabase"""
    if not SUPABASE_ROUNDS_URL:
        return
    payload = {
        "round_id": evt.get("roundId"),
        "color": evt.get("color"),
        "roll": evt.get("roll"),
        "occurred_at": evt.get("at"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    headers = {"Content-Type": "application/json"}
    if SUPABASE_TOKEN:
        headers["apikey"] = SUPABASE_TOKEN
        headers["Authorization"] = f"Bearer {SUPABASE_TOKEN}"
        headers["Prefer"] = "resolution=merge-duplicates"
    try:
        async with session.post(SUPABASE_ROUNDS_URL, headers=headers, json=payload) as resp:
            if DEBUG:
                body = await resp.text()
                print(f"[SUPABASE] {resp.status} {body[:200]}")
    except Exception as e:
        print(f"[SUPABASE] erro ao enviar: {e}")


async def heartbeat(history: Deque[Dict[str, Any]], stop_evt: asyncio.Event):
    """Log periódico do estado"""
    last_total = -1
    while not stop_evt.is_set():
        await asyncio.sleep(HEARTBEAT_SECS)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last_total or DEBUG:
            last_total = total
            print(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} "
                  f"black={counts.get('black',0)} white={counts.get('white',0)}")


async def run_loop():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        driver = build_driver()
        driver.get("about:blank")

        # hook_js inline (pode separar em arquivo se preferir)
        hook_js = """
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
            } catch (e) {}
          }

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
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": hook_js})

        print(f"[NAV] abrindo {GAME_URL}")
        driver.get(GAME_URL)

        async def poll_console():
            while not stop_evt.is_set():
                for entry in driver.get_log("browser"):
                    msg = entry.get("message", "").split(" ", 2)[-1]
                    if not msg.startswith("__RESULT__"):
                        continue
                    payload = msg[len("__RESULT__") :]
                    try:
                        evt = json.loads(payload)
                    except Exception:
                        continue
                    key = f"{evt.get('roundId')}|{evt.get('roll')}|{evt.get('at')}"
                    if key in _DEDUPE:
                        continue
                    _DEDUPE.add(key)
                    if len(_DEDUPE) > _DEDUPE_MAX:
                        _DEDUPE.clear()
                        _DEDUPE.add(key)
                    if not evt.get("at"):
                        evt["at"] = datetime.now(timezone.utc).isoformat()
                    print(f"[RESULT] {evt['roundId']} -> {evt['color'].upper()} "
                          f"({evt['roll']}) @ {evt['at']}")
                    history.append(evt)
                    await send_to_supabase(session, evt)
                await asyncio.sleep(1.0)

        hb = asyncio.create_task(heartbeat(history, stop_evt))
        poll = asyncio.create_task(poll_console())
        print("Coletando RESULTADOS… (Ctrl+C para sair)")

        try:
            await asyncio.Event().wait()
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            stop_evt.set()
            hb.cancel()
            poll.cancel()
            driver.quit()


if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
