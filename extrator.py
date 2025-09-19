#!/usr/bin/env python3
"""
extrator_selenium.py — Extrator de rodadas (Selenium + selenium-wire + hooks JS)

Como usar local:
  pip install selenium selenium-wire webdriver-manager aiohttp python-dotenv
  # Chrome:
  #  - Windows/macOS: ter Google Chrome instalado
  #  - Linux: instalar chromium/chromedriver (ou deixar webdriver-manager baixar)
  # Rodar:
  DEBUG=true python extrator_selenium.py

Variáveis de ambiente:
  GAME_URL, SUPABASE_ROUNDS_URL, SUPABASE_TOKEN,
  AWS_SIGNAL_URL, AWS_TOKEN,
  HISTORY_MAXLEN, HEARTBEAT_SECS, DEBUG,
  PROXY_URL  (ex.: http://user:pass@ip:porta  ou  socks5://user:pass@ip:porta)
  GOTO_TIMEOUT_MS (padrão 45000)
  WATCHDOG_SILENCE_SECS (padrão 120)
  PAGE_LIFETIME_MIN (padrão 30)
"""

import os
import re
import json
import time
import asyncio
import hashlib
import traceback
from urllib.parse import urlparse
from collections import deque, Counter
from datetime import datetime, timezone, timedelta
from typing import Deque, Dict, Any, List, Optional, Tuple

import aiohttp

# Selenium + selenium-wire (para proxy com auth)
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import WebDriverException, TimeoutException, JavascriptException
from webdriver_manager.chrome import ChromeDriverManager  # baixa driver automaticamente

# ---------------- CONFIG ----------------
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")
AWS_SIGNAL_URL = os.getenv("AWS_SIGNAL_URL", "")
AWS_TOKEN = os.getenv("AWS_TOKEN", "")
HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

PROXY_URL = os.getenv("PROXY_URL", "").strip()  # ex.: http://user:pass@ip:port  ou  socks5://user:pass@ip:port
GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "45000"))
WATCHDOG_SILENCE_SECS = int(os.getenv("WATCHDOG_SILENCE_SECS", "120"))
PAGE_LIFETIME_MIN = int(os.getenv("PAGE_LIFETIME_MIN", "30"))

# dedupe
_DEDUPE: set = set()
_DEDUPE_MAX = int(os.getenv("DEDUPE_MAX", "4000"))

# ---------------- HOOK JS ----------------
HOOK_JS = r"""
(() => {
  const COLORS = { 1: "red", 0: "white", 2: "black" };

  // buffer onde vamos empilhar os resultados para o Python buscar
  window.__resultBuffer = window.__resultBuffer || [];
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

    // empilha no buffer
    window.__resultBuffer.push({ roundId: rid, color, roll, at });
    // também loga, se quiser inspecionar
    try { console.log("__RESULT__" + JSON.stringify({ roundId: rid, color, roll, at })); } catch(e){}

    return true;
  }

  function parseSocketIoPacket(str) {
    if (typeof str !== "string") return;
    if (!str.startsWith("42")) return; // socket.io evento
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

  // Função para o Python obter e limpar o buffer em lote
  window.__drainResults = function() {
    const out = window.__resultBuffer.slice();
    window.__resultBuffer.length = 0;
    return out;
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

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ---------------- supabase ----------------
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
        "ingested_at": _now_utc().isoformat(),
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

# ---------------- estratégias ----------------
def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    # Exemplo didático (substitua pela sua regra real)
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
                "ts": _now_utc().isoformat()
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
    if interval <= 0: return
    last = -1
    while not stop_evt.is_set():
        await asyncio.sleep(interval)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        if total != last or DEBUG:
            last = total
            print(f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} black={counts.get('black',0)} white={counts.get('white',0)}")

# ---------------- webdriver (Chrome + selenium-wire) ----------------
def parse_proxy_env(url: str) -> Tuple[dict, str]:
    """
    Retorna (seleniumwire_options.proxy, info_string_para_log)
    Suporta http(s)://user:pass@ip:port e socks5://user:pass@ip:port (auth ok no selenium-wire)
    """
    if not url:
        return ({}, "[PROXY] disabled"), ""
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    host = parsed.hostname
    port = parsed.port
    user = parsed.username
    pwd = parsed.password
    if not (scheme and host and port):
        return ({}, f"[PROXY] inválido: {url}"), ""

    auth = f"{user}:{pwd}@" if user and pwd else ""
    clean = f"{scheme}://{auth}{host}:{port}"

    # selenium-wire espera dict separado
    proxy_url = f"{scheme}://{auth}{host}:{port}"
    proxy_conf = {
        "http": proxy_url,
        "https": proxy_url,
        "no_proxy": "localhost,127.0.0.1"
    }
    return (proxy_conf, f"[PROXY] {clean}")

def build_driver() -> webdriver.Chrome:
    proxy_conf, logline = parse_proxy_env(PROXY_URL)
    if logline:
        print(logline)

    sw_options = {"verify_ssl": False}
    if proxy_conf:
        sw_options["proxy"] = proxy_conf

    chrome_opts = ChromeOptions()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-setuid-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--disable-software-rasterizer")
    chrome_opts.add_argument("--mute-audio")
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-background-networking")
    chrome_opts.add_argument("--disable-background-timer-throttling")
    chrome_opts.add_argument("--disable-renderer-backgrounding")
    chrome_opts.add_argument("--disable-ipc-flooding-protection")
    chrome_opts.add_argument("--blink-settings=imagesEnabled=false")  # menos banda
    chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_opts.add_experimental_option("useAutomationExtension", False)

    # driver via webdriver-manager (baixa binário compatível)
    driver = webdriver.Chrome(
        ChromeDriverManager().install(),
        seleniumwire_options=sw_options,
        options=chrome_opts,
    )
    driver.set_page_load_timeout(GOTO_TIMEOUT_MS / 1000.0)
    return driver

def safe_exec_script(driver, js: str):
    try:
        return driver.execute_script(js)
    except JavascriptException:
        return None

def inject_hook(driver):
    safe_exec_script(driver, HOOK_JS)

def drain_results(driver) -> List[Dict[str, Any]]:
    res = safe_exec_script(driver, "return (window.__drainResults ? window.__drainResults() : []);")
    return res or []

def click_consent(driver):
    # tenta aceitar consentimento se existir (variações de texto)
    selectors = [
        "button:has-text('Aceitar')",  # só funciona no Playwright; então tentamos por XPath/texto no Selenium
    ]
    # Selenium: tentaremos por XPath com contains(text(),'Aceitar')
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        # tenta um texto provável
        for txt in ["Aceitar", "Accept", "Concordar", "OK"]:
            try:
                btn = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{txt}')]"))
                )
                btn.click()
                print(f"[BOOTSTRAP] cliquei no consentimento '{txt}'")
                return
            except Exception:
                continue
    except Exception:
        pass

# ---------------- NAV/Watchdog ----------------
async def run_loop():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    last_result_at = _now_utc()
    page_started_at = _now_utc()

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        driver = None

        def restart_driver():
            nonlocal driver, page_started_at
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            driver = build_driver()
            page_started_at = _now_utc()
            return driver

        # start
        driver = restart_driver()

        # navegação com retries
        def goto_with_retry(url: str, retries=5, base_sleep=2.0):
            for i in range(retries):
                try:
                    driver.get(url)
                    return True
                except Exception as e:
                    print(f"[NAV] tentativa {i+1} falhou: {e}")
                    time.sleep(base_sleep * (2 ** i))
            return False

        ok = goto_with_retry(GAME_URL, retries=5)
        if not ok:
            raise RuntimeError("Falha ao carregar a página inicial.")

        inject_hook(driver)
        click_consent(driver)

        # heartbeat paralelo
        hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))
        print("Coletando RESULTADOS… (Ctrl+C para sair)")

        try:
            while not stop_evt.is_set():
                # coleta lote de resultados do buffer JS
                batch = drain_results(driver)
                if batch:
                    last_result_at = _now_utc()

                for evt in batch:
                    # dedupe python
                    key = _evt_key(evt)
                    if key in _DEDUPE:
                        continue
                    _DEDUPE.add(key)
                    if len(_DEDUPE) > _DEDUPE_MAX:
                        _DEDUPE.clear()
                        _DEDUPE.add(key)

                    print(f"[RESULT] {evt.get('roundId')} -> {str(evt.get('color')).upper()} ({evt.get('roll')}) @ {evt.get('at') or ''}")

                    # histórico
                    history.append({
                        "roundId": evt.get("roundId"),
                        "color": evt.get("color"),
                        "roll": evt.get("roll"),
                        "at": _parse_occurred_at(evt.get("at")),
                    })

                    # 1) supabase
                    try:
                        await send_to_supabase(session, evt)
                    except Exception as e:
                        print(f"[SUPABASE] erro: {e}")

                    # 2) estratégias + AWS
                    try:
                        signals = apply_strategies(history)
                        if signals:
                            await send_signals_to_aws(session, signals)
                    except Exception as e:
                        print(f"[AWS] erro sinais: {e}")

                # watchdog — sem resultados por muito tempo?
                silence = (_now_utc() - last_result_at).total_seconds()
                if silence > WATCHDOG_SILENCE_SECS:
                    print(f"[WATCHDOG] sem __RESULT__ há {int(silence)}s → recarregando página…")
                    try:
                        driver.refresh()
                        inject_hook(driver)
                        click_consent(driver)
                        last_result_at = _now_utc()  # dá um fôlego
                    except Exception as e:
                        print(f"[WATCHDOG] refresh falhou: {e}. Reiniciando navegador…")
                        driver = restart_driver()
                        goto_with_retry(GAME_URL, retries=5)
                        inject_hook(driver)
                        click_consent(driver)
                        last_result_at = _now_utc()

                # reload preventivo por tempo de vida da página
                if (_now_utc() - page_started_at) > timedelta(minutes=PAGE_LIFETIME_MIN):
                    print("[MAINT] reload preventivo (idade da página)")
                    try:
                        driver.refresh()
                        inject_hook(driver)
                        click_consent(driver)
                        page_started_at = _now_utc()
                    except Exception as e:
                        print(f"[MAINT] refresh falhou: {e}. Reiniciando navegador…")
                        driver = restart_driver()
                        goto_with_retry(GAME_URL, retries=5)
                        inject_hook(driver)
                        click_consent(driver)

                await asyncio.sleep(0.5)

        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            stop_evt.set()
            hb_task.cancel()
            try:
                driver.quit()
            except Exception:
                pass

# ---------------- main ----------------
if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        raise
