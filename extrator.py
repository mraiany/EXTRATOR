#!/usr/bin/env python3
"""
Extrator de rodadas – Selenium + Selenium-Wire
- Headless ajustável por variável de ambiente (HEADLESS)
- Logs estratégicos para diagnóstico
- Compatibilidade PROXY_URL / PROXY
- Amostra de console e estado do DOM
- Hook JS para interceptar socket.io (fetch/XHR/WebSocket)
"""

import os
import json
import asyncio
import tempfile
import shutil
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Deque, Dict, Any, List

import aiohttp
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException

# ---------- ENV ----------
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double").strip()
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "").strip()
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "").strip()
USER_AGENT = os.getenv("USER_AGENT", "").strip()

# HEADLESS: "chrome" (recomendado), "new", "off"
HEADLESS = os.getenv("HEADLESS", "chrome").strip().lower()

# Proxy compat: aceita PROXY_URL (novo) ou PROXY (legado)
PROXY_URL = (os.getenv("PROXY_URL") or os.getenv("PROXY") or "").strip()

HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# Inspeção de rede (apenas logs) – cuidado com volume
DEBUG_SOCKET = os.getenv("DEBUG_SOCKET", "false").lower() in ("1", "true", "yes")

_DEDUPE: set = set()
_DEDUPE_MAX = 4000


# ---------- util de log ----------
def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ---------- Navegador ----------
def build_driver() -> webdriver.Chrome:
    chrome_opts = Options()

    # Headless
    if HEADLESS in ("1", "true", "yes", "chrome", "on", ""):
        # modo headless "clássico" do Chrome (mais compatível para console logs)
        chrome_opts.add_argument("--headless")
    elif HEADLESS == "new":
        chrome_opts.add_argument("--headless=new")
    # HEADLESS "off": não adiciona headless (para testes locais)

    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-software-rasterizer")
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--mute-audio")
    chrome_opts.add_argument("--disable-background-networking")
    chrome_opts.add_argument("--disable-background-timer-throttling")
    chrome_opts.add_argument("--disable-renderer-backgrounding")
    chrome_opts.add_argument("--disable-ipc-flooding-protection")
    chrome_opts.add_argument("--no-first-run")
    chrome_opts.add_argument("--no-default-browser-check")
    chrome_opts.add_argument("--window-size=1366,768")
    # bloquear imagens pode destravar renderer em redes mais restritivas
    chrome_opts.add_argument("--blink-settings=imagesEnabled=false")

    # Estratégia de carregamento: não esperar "complete"
    chrome_opts.set_capability("pageLoadStrategy", "eager")

    # Coleta de logs do console
    chrome_opts.set_capability("goog:loggingPrefs", {"browser": "ALL"})

    if USER_AGENT:
        chrome_opts.add_argument(f"user-agent={USER_AGENT}")

    # Perfil temporário único (evita "user data dir already in use")
    user_data_dir = tempfile.mkdtemp(prefix="sel_profile_")
    chrome_opts.add_argument(f"--user-data-dir={user_data_dir}")

    # Proxy via selenium-wire (suporta socks5 e http)
    sw_opts: Dict[str, Dict[str, str]] = {}
    if PROXY_URL:
        sw_opts["proxy"] = {"http": PROXY_URL, "https": PROXY_URL}

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_opts)
    driver._user_data_dir = user_data_dir

    # Timeout de carregamento menor – não ficar preso esperando terceiros
    driver.set_page_load_timeout(45)

    # Desligar cache
    try:
        driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    if DEBUG_SOCKET:
        # Log básico de requisições socket.io (XHR/polling)
        def _request_interceptor(request):
            if "/socket.io" in request.path:
                log(f"[SOCK-REQ] {request.method} {request.path}")

        driver.request_interceptor = _request_interceptor

        def _response_interceptor(request, response):
            if "/socket.io" in request.path:
                log(f"[SOCK-RES] {response.status_code} {request.path}")

        driver.response_interceptor = _response_interceptor

    return driver


# ---------- Supabase ----------
async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]) -> None:
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
                log(f"[SUPABASE] {resp.status} {body[:200]}")
    except Exception as e:
        log(f"[SUPABASE] erro ao enviar: {e}")


# ---------- Estratégia (placeholder) ----------
def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return []


# ---------- Heartbeat ----------
async def heartbeat(history: Deque[Dict[str, Any]], stop_evt: asyncio.Event) -> None:
    last = -1
    while not stop_evt.is_set():
        await asyncio.sleep(HEARTBEAT_SECS)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last or DEBUG:
            last = total
            log(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} "
                f"black={counts.get('black',0)} white={counts.get('white',0)}")


# ---------- Navegação robusta ----------
def dom_ready_state(driver) -> str:
    try:
        return driver.execute_script("return document.readyState || ''") or ""
    except WebDriverException:
        return ""


def page_min_ready(driver) -> bool:
    # consideramos ok se já chegou a 'interactive' (não precisa 'complete')
    state = dom_ready_state(driver)
    return state in ("interactive", "complete")


async def run_loop() -> None:
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        driver = build_driver()

        hook_js = r"""
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
               obj.payload && obj.payload.color != null && obj.payload.roll != null);
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
              const ev = arr?.[0];
              const body = arr?.[1];
              if (ev === "data") tryEmitResult(body);
            } catch(e){}
          }
          const _fetch = window.fetch;
          window.fetch = async function(...args){
            const res = await _fetch.apply(this, args);
            try {
              const url = (typeof args[0] === "string" ? args[0] : args[0]?.url) || "";
              if (url.includes("/socket.io")){
                const clone = res.clone();
                clone.text().then(t=>{
                  const parts = String(t||"").split("42[");
                  for(let i=1;i<parts.length;i++) parseSocketIoPacket("42["+parts[i]);
                }).catch(()=>{});
              }
            } catch(e){}
            return res;
          };
          const _open = XMLHttpRequest.prototype.open;
          const _send = XMLHttpRequest.prototype.send;
          XMLHttpRequest.prototype.open = function(method, url, ...rest){
            this.__isSock = url && url.includes && url.includes("/socket.io");
            return _open.call(this, method, url, ...rest);
          };
          XMLHttpRequest.prototype.send = function(body){
            if (this.__isSock){
              this.addEventListener("load", function(){
                try{
                  const text = (this.responseType===""||this.responseType==="text") ? (this.responseText||"") : "";
                  const parts = String(text||"").split("42[");
                  for(let i=1;i<parts.length;i++) parseSocketIoPacket("42["+parts[i]);
                }catch(e){}
              });
            }
            return _send.call(this, body);
          };
          const OriginalWS = window.WebSocket;
          window.WebSocket = function(url, protocols){
            const ws = new OriginalWS(url, protocols);
            ws.addEventListener("message", (evt) => {
              if (typeof evt.data === "string") parseSocketIoPacket(evt.data);
            });
            return ws;
          };
        })();
        """
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": hook_js})

        # Navegação com tolerância a timeout do renderer
        log(f"[NAV] indo para {GAME_URL}")
        try:
            driver.get(GAME_URL)
        except TimeoutException:
            log("[NAV] timeout do renderer (ignorado); verificando estado do DOM…")
        except Exception as e:
            log(f"[NAV] erro ao navegar: {e}")
            try:
                driver.quit()
            finally:
                return

        # Estado do DOM e título/URL após navegação
        try:
            log(f"[NAV] readyState={dom_ready_state(driver)}")
            log(f"[NAV] title={driver.title!r}")
            log(f"[NAV] url={driver.current_url}")
        except Exception:
            pass

        # Confere se ao menos chegou em 'interactive'; senão tenta recarga
        try_count = 0
        while not page_min_ready(driver) and try_count < 2:
            try_count += 1
            try:
                driver.refresh()
            except Exception:
                pass
            await asyncio.sleep(2)
            log(f"[NAV] retry {try_count} - readyState={dom_ready_state(driver)}")

        async def poll_console() -> None:
            first_dump = True
            while not stop_evt.is_set():
                try:
                    logs = driver.get_log("browser")
                    # Dump inicial de algumas mensagens para diagnóstico
                    if first_dump and logs:
                        first_dump = False
                        log("[CONSOLE] amostra inicial:")
                        for entry in logs[:5]:
                            log(f"    {entry}")
                    for entry in logs:
                        msg = entry.get("message", "")
                        if "__RESULT__" not in msg:
                            continue
                        try:
                            payload = msg.split("__RESULT__", 1)[1]
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
                        log(f"[RESULT] {evt['roundId']} -> {evt['color'].upper()} "
                            f"({evt['roll']}) @ {evt['at']}")
                        history.append(evt)
                        await send_to_supabase(session, evt)
                        _ = apply_strategies(history)
                except Exception as e:
                    # Em headless + proxy, leitura de log pode falhar esporadicamente
                    if DEBUG:
                        log(f"[CONSOLE] erro ao ler logs: {e}")
                await asyncio.sleep(1.2)

        hb_task = asyncio.create_task(heartbeat(history, stop_evt))
        poll_task = asyncio.create_task(poll_console())
        log("Coletando RESULTADOS… (Ctrl+C para sair)")
        try:
            await asyncio.Event().wait()
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            stop_evt.set()
            hb_task.cancel()
            poll_task.cancel()
            try:
                driver.quit()
            finally:
                if hasattr(driver, "_user_data_dir"):
                    try:
                        shutil.rmtree(driver._user_data_dir)
                    except Exception:
                        pass


if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        log("Interrompido pelo usuário")
