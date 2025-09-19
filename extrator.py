#!/usr/bin/env python3
"""
extrator.py - Extrator de rodadas usando Selenium + SeleniumWire
Usa proxy HTTP ou SOCKS5 (via selenium-wire), gera histórico de rodadas,
envia para Supabase e realiza heartbeat.
"""

import os
import json
import time
import asyncio
import tempfile
import shutil
from datetime import datetime, timezone
from collections import deque, Counter
from typing import Deque, Dict, List, Optional, Any

import aiohttp
from seleniumwire import webdriver  # proxy via SeleniumWire
from selenium.webdriver.chrome.options import Options

# ==================== Configs via env ====================
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")
USER_AGENT = os.getenv("USER_AGENT", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()
HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

_DEDUPE: set = set()
_DEDUPE_MAX = 4000

def build_driver() -> webdriver.Chrome:
    """
    Cria uma instância do Chrome headless usando selenium-wire.
    Configura user-agent, proxy HTTP ou SOCKS5, diretório temporário
    para profile (evita erro de diretório em uso) e flags de desempenho.
    """
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

    # Define user-agent se houver
    if USER_AGENT:
        opts.add_argument(f"user-agent={USER_AGENT}")

    # Cria um diretório temporário para profile do Chrome
    user_data_dir = tempfile.mkdtemp(prefix="seluser-")
    opts.add_argument(f"--user-data-dir={user_data_dir}")

    # Configura proxy via selenium-wire
    seleniumwire_options = {}
    if PROXY_URL:
        # Espera-se formato: schema://user:pass@host:porta ou host:porta
        # Converte para dict para selenium-wire
        seleniumwire_options['proxy'] = {'http': PROXY_URL, 'https': PROXY_URL}

    driver = webdriver.Chrome(options=opts, seleniumwire_options=seleniumwire_options)

    # Anexa diretório para limpeza ao atributo para remoção ao finalizar
    driver._user_data_dir = user_data_dir
    return driver

async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]) -> None:
    """
    Envia dados de uma rodada para supabase se configurado.
    """
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

def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Lógica de sinal opcional
    return []

async def heartbeat(history: Deque[Dict[str, Any]], stop_evt: asyncio.Event) -> None:
    """
    Imprime heartbeat periódico de número de rodadas e contagem de cores.
    """
    last_total = -1
    while not stop_evt.is_set():
        await asyncio.sleep(HEARTBEAT_SECS)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last_total or DEBUG:
            last_total = total
            print(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} black={counts.get('black',0)} white={counts.get('white',0)}")

async def run_loop() -> None:
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        driver = build_driver()

        # injeta script para capturar pacotes socket.io e logs
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

        # injeta script antes de qualquer navegação
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": hook_js})

        try:
            print(f"[NAV] indo para {GAME_URL}")
            driver.get(GAME_URL)
        except Exception as e:
            print(f"[NAV] erro ao navegar: {e}")
            driver.quit()
            return

        async def poll_console() -> None:
            """
            Varre mensagens do console e processa os resultados __RESULT__.
            """
            while not stop_evt.is_set():
                for entry in driver.get_log("browser"):
                    text = entry.get("message", "")
                    if not text:
                        continue
                    # mensagem bruta do Chrome; pega JSON depois de "__RESULT__"
                    if "__RESULT__" not in text:
                        continue
                    try:
                        payload_str = text.split("__RESULT__", 1)[1]
                        evt = json.loads(payload_str)
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

                    # mostra e salva
                    print(f"[RESULT] {evt['roundId']} -> {evt['color'].upper()} ({evt['roll']}) @ {evt['at']}")
                    history.append(evt)

                    # envia ao Supabase
                    await send_to_supabase(session, evt)

                    # aplica estratégias (opcional)
                    signals = apply_strategies(history)
                    # se houver, enviar sinais via outro endpoint

                await asyncio.sleep(1.5)

        hb_task = asyncio.create_task(heartbeat(history, stop_evt))
        poll_task = asyncio.create_task(poll_console())
        print("Coletando RESULTADOS… (Ctrl+C para sair)")

        try:
            await asyncio.Event().wait()
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            stop_evt.set()
            hb_task.cancel()
            poll_task.cancel()
            driver.quit()
            # Remove profile temporário
            if hasattr(driver, "_user_data_dir"):
                try:
                    shutil.rmtree(driver._user_data_dir)
                except Exception:
                    pass

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
