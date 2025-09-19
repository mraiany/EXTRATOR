#!/usr/bin/env python3
"""
extrator.py - Extrator de rodadas usando Selenium + selenium-wire (suporte SOCKS5)

Variáveis de ambiente:
  GAME_URL, SUPABASE_ROUNDS_URL, SUPABASE_TOKEN,
  PROXY_URL (ex: 'socks5://user:pass@host:port' ou 'http://user:pass@host:port'),
  USER_AGENT, HISTORY_MAXLEN, HEARTBEAT_SECS, DEBUG

Requisitos pip:
  selenium, selenium-wire, aiohttp
(Instale via requirements.txt/ Dockerfile)
"""
import os
import json
import asyncio
import pathlib
import tempfile
import uuid
from datetime import datetime, timezone
from collections import deque, Counter
from typing import Deque, Dict, Any, List, Optional

import aiohttp

# selenium / selenium-wire
from seleniumwire import webdriver  # selenium-wire fornece proxy auth
from selenium.webdriver.chrome.options import Options

# ---------------- CONFIG (env / defaults) ----------------
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")
USER_AGENT = os.getenv("USER_AGENT", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()  # ex: "socks5://user:pass@ip:port"
HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# dedupe em nível de python
_DEDUPE: set = set()
_DEDUPE_MAX = int(os.getenv("DEDUPE_MAX", "4000"))

# ---------------- Hook JS ----------------
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
    if (!str.startsWith("42")) return; // socket.io event
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

# ---------------- utils ----------------
def _evt_key(evt: Dict[str, Any]) -> str:
    rid = str(evt.get("roundId") or "")
    roll = str(evt.get("roll") or "")
    at = str(evt.get("at") or "")
    base = f"{rid}|{roll}|{at}"
    return base if rid else uuid.uuid5(uuid.NAMESPACE_DNS, base).hex

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
            else:
                if DEBUG:
                    print(f"[SUPABASE][ok] {resp.status} body={body[:240]}")
    except Exception as e:
        print(f"[SUPABASE] exception: {e}")

# ---------------- estratégia (ponto de customização) ----------------
def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    # exemplo simples - substitua pela sua lógica real
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

# ---------------- proxy test util ----------------
async def test_proxy(session: aiohttp.ClientSession, proxy_url: str) -> bool:
    """
    Testa um proxy fazendo request para httpbin.org/ip (só valida que o proxy está ativo).
    Usa aiohttp com argumento proxy.
    """
    try:
        # httpbin em http pode ser bloqueado por TLS/HTTP; usamos https://httpbin.org/ip
        async with session.get("https://httpbin.org/ip", proxy=proxy_url, timeout=10) as resp:
            text = await resp.text()
            if resp.status == 200 and "origin" in text or "ip" in text:
                if DEBUG:
                    print(f"[PROXY-TEST] status: {resp.status} body: {text[:200]}")
                return True
            if DEBUG:
                print(f"[PROXY-TEST] status: {resp.status} body: {text[:200]}")
            return False
    except Exception as e:
        if DEBUG:
            print(f"[PROXY-TEST] exception: {e}")
        return False

# ---------------- selenium builder ----------------
def build_driver(seleniumwire_options: Optional[Dict[str, Any]] = None) -> webdriver.Chrome:
    """
    Cria e retorna um webdriver.Chrome (selenium-wire) com:
      - perfil único (--user-data-dir)
      - cache isolado
      - logging de console habilitado
      - proxy via selenium-wire (se seleniumwire_options passado)
    """
    opts = Options()

    # Chrome flags recomendadas para container
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-setuid-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--mute-audio")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    # 'headless=new' dá mais compatibilidade em versões recentes do Chrome/Chromium
    try:
        opts.add_argument("--headless=new")
    except Exception:
        opts.add_argument("--headless")

    # USER AGENT
    if USER_AGENT:
        opts.add_argument(f"user-agent={USER_AGENT}")

    # habilita logs do console (browser)
    opts.set_capability("goog:loggingPrefs", {"browser": "ALL"})

    # perfil único por execução (evita erro "user data directory already in use")
    base = os.getenv("CHROME_USER_DATA_BASE", "/tmp/chrome-profiles")
    unique = f"profile-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    user_data_dir = pathlib.Path(base) / unique
    user_data_dir.mkdir(parents=True, exist_ok=True)
    opts.add_argument(f"--user-data-dir={str(user_data_dir)}")

    # cache separado
    cache_dir = pathlib.Path("/tmp/chrome-cache") / unique
    cache_dir.mkdir(parents=True, exist_ok=True)
    opts.add_argument(f"--disk-cache-dir={str(cache_dir)}")

    # selenium-wire: passar seleniumwire_options para suportar proxy com auth
    # NOTE: não passar options duas vezes (usar seleniumwire_options kwarg).
    sw_opts = seleniumwire_options or {}

    # Criar driver
    driver = webdriver.Chrome(options=opts, seleniumwire_options=sw_opts)
    # Aumenta timeout de script se precisar
    driver.set_page_load_timeout(60)
    return driver

# ---------------- main loop ----------------
async def main():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=None)

    # Prepara selenium-wire proxy dict se houver PROXY_URL
    sw_proxy = None
    if PROXY_URL:
        # selenium-wire espera dicionário com chaves http/https e valor 'schema://user:pass@host:port'
        # exemplo: {'proxy': {'http': 'socks5://user:pass@ip:port', 'https': 'socks5://user:pass@ip:port'}}
        sw_proxy = {"proxy": {"http": PROXY_URL, "https": PROXY_URL, "no_proxy": "localhost,127.0.0.1"}}
        if DEBUG:
            print(f"[PROXY] will use selenium-wire proxy -> {sw_proxy}")

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Se houver proxy, testar antes
        if PROXY_URL:
            ok = await test_proxy(session, PROXY_URL)
            if not ok:
                print("[PROXY-TEST] proxy indicado falhou no teste HTTP; ainda assim tentarei iniciar o navegador (talvez seja SOCKS-only).")
            else:
                print("[PROXY-TEST] proxy respondeu OK.")

        # cria webdriver
        try:
            driver = build_driver(seleniumwire_options=sw_proxy)
        except Exception as e:
            print(f"[FATAL] erro ao iniciar webdriver: {e}")
            raise

        # Injetar script antes de navegar (via DevTools)
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_JS})
        except Exception as e:
            # em alguns ambientes esse CDP pode falhar; apenas logamos
            if DEBUG:
                print(f"[WARN] addScriptToEvaluateOnNewDocument falhou: {e}")

        # abre a página
        try:
            if DEBUG:
                print(f"[NAV] indo para {GAME_URL}")
            driver.get(GAME_URL)
        except Exception as e:
            print(f"[NAV] erro ao navegar: {e}")

        # task: read browser console logs periodicamente
        async def poll_console():
            # Selenium get_log("browser") devolve lista de entradas
            while not stop_evt.is_set():
                try:
                    # recuperar logs do browser
                    entries = []
                    try:
                        entries = driver.get_log("browser")
                    except Exception:
                        # alguns drivers/versões podem lançar se log não disponível
                        entries = []

                    for entry in entries:
                        text = entry.get("message") or ""
                        if not text:
                            continue
                        # a mensagem do Chrome costuma vir com prefixos; buscamos a substring JSON
                        # tentamos extrair "__RESULT__{...}"
                        idx = text.find("__RESULT__")
                        if idx < 0:
                            continue
                        payload = text[idx + len("__RESULT__") :]
                        try:
                            evt = json.loads(payload)
                        except Exception:
                            # às vezes o console formata com escapes — tentamos limpar
                            try:
                                payload = payload.strip().strip('"').encode('utf-8').decode('unicode_escape')
                                evt = json.loads(payload)
                            except Exception:
                                if DEBUG:
                                    print("[CONSOLE] falha ao decodificar __RESULT__", payload[:200])
                                continue

                        # dedupe python
                        key = _evt_key(evt)
                        if key in _DEDUPE:
                            if DEBUG:
                                print(f"[DEDUPE] skip {key}")
                            continue
                        _DEDUPE.add(key)
                        if len(_DEDUPE) > _DEDUPE_MAX:
                            _DEDUPE.clear()
                            _DEDUPE.add(key)

                        # normaliza horário
                        if not evt.get("at"):
                            evt["at"] = datetime.now(timezone.utc).isoformat()

                        print(f"[RESULT] {evt.get('roundId')} -> {str(evt.get('color')).upper()} ({evt.get('roll')}) @ {evt.get('at')}")
                        history.append({
                            "roundId": evt.get("roundId"),
                            "color": evt.get("color"),
                            "roll": evt.get("roll"),
                            "at": _parse_occurred_at(evt.get("at"))
                        })

                        # envio ao supabase
                        try:
                            await send_to_supabase(session, evt)
                        except Exception as e:
                            print(f"[SUPABASE] erro: {e}")

                        # aplicar estratégia e (opcional) enviar sinais
                        try:
                            signals = apply_strategies(history)
                            # enviar sinais se necessário (função opcional)
                        except Exception as e:
                            if DEBUG:
                                print(f"[STRAT] erro: {e}")

                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if DEBUG:
                        print(f"[poll_console] exception: {e}")
                    await asyncio.sleep(1.0)

        hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))
        poll_task = asyncio.create_task(poll_console())

        print("Coletando RESULTADOS… (Ctrl+C para sair)")
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            stop_evt.set()
            hb_task.cancel()
            poll_task.cancel()
            try:
                driver.quit()
            except Exception:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
