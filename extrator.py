#!/usr/bin/env python3
"""
extrator.py - Extrator de rodadas usando Selenium

Variáveis de ambiente requeridas:
    GAME_URL: URL do jogo Double na Blaze
    SUPABASE_ROUNDS_URL: endpoint REST do Supabase para inserir resultados
    SUPABASE_TOKEN: token do Supabase (apikey)
    USER_AGENT: UA a ser usado (opcional)
    PROXY_URL: endereço do proxy opcional (p.ex., 'socks5://user:pass@ip:porta' ou 'http://user:pass@ip:porta')
    DEBUG: define modo verboso
"""
import os
import json
import time
import asyncio
from datetime import datetime, timezone
from collections import deque, Counter
from typing import Deque, Dict, List, Optional

import aiohttp
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# config via env
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")
USER_AGENT = os.getenv("USER_AGENT", "")
PROXY_URL = os.getenv("PROXY_URL", "")
HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# dedupe
_DEDUPE: set = set()
_DEDUPE_MAX = 4000

def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.headless = True
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--mute-audio")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-ipc-flooding-protection")
    if USER_AGENT:
        opts.add_argument(f"user-agent={USER_AGENT}")
    # aplica proxy se existir
    proxy = None
    if PROXY_URL:
        # PROXY_URL no formato esquema://user:pass@host:porta
        proxy = PROXY_URL
        # para HTTP ou SOCKS5, Selenium usa argumento geral --proxy-server
        opts.add_argument(f"--proxy-server={proxy}")
    # cria instância
    driver = webdriver.Chrome(options=opts)
    return driver

async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, any]):
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

def apply_strategies(history: Deque[Dict[str, any]]) -> List[Dict[str, any]]:
    # placeholder de estratégia; substitua conforme necessário
    return []

async def heartbeat(history: Deque[Dict[str, any]], stop_evt: asyncio.Event):
    last_total = -1
    while not stop_evt.is_set():
        await asyncio.sleep(HEARTBEAT_SECS)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last_total or DEBUG:
            last_total = total
            print(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} black={counts.get('black',0)} white={counts.get('white',0)}")

async def run_loop():
    history: Deque[Dict[str, any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        driver = build_driver()
        driver.get("about:blank")

        # injeta JS para hook socket.io/fetch similar ao Playwright
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

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": hook_js})

        print(f"[NAV] abrindo {GAME_URL}")
        driver.get(GAME_URL)

        # inicia thread de leitura do console (polling simples)
        async def poll_console():
            last_log = ""
            while not stop_evt.is_set():
                for entry in driver.get_log("browser"):
                    text = entry.get("message", "")
                    if not text:
                        continue
                    # extrai mensagem após " ")"
                    msg = text.split(" ", 2)[-1]
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
                    # normaliza horário
                    if not evt.get("at"):
                        evt["at"] = datetime.now(timezone.utc).isoformat()
                    print(f"[RESULT] {evt['roundId']} -> {evt['color'].upper()} ({evt['roll']}) @ {evt['at']}")
                    history.append(evt)
                    # envia ao Supabase
                    await send_to_supabase(session, evt)
                    # aplica estratégia (opcional)
                    signals = apply_strategies(history)
                    # envia sinais se houver (omiti aqui)
                await asyncio.sleep(1.5)

        hb = asyncio.create_task(heartbeat(history, stop_evt))
        poll = asyncio.create_task(poll_console())
        print("Coletando RESULTADOS… (Ctrl+C para sair)")

        try:
            # espera indefinidamente até interrupção
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
