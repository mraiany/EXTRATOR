#!/usr/bin/env python3
"""
extrator.py - Extrator de rodadas (Playwright + hooks JS) com teste de proxy.

Como usar (locally / container):
  - pip install playwright aiohttp python-dotenv requests
  - python -m playwright install chromium
  - export DEBUG=true (opcional)
  - export PROXY_URL="http://user:pass@host:port"   # opcional
  - python extrator.py

Variáveis de ambiente:
  GAME_URL, SUPABASE_ROUNDS_URL, SUPABASE_TOKEN,
  AWS_SIGNAL_URL, AWS_TOKEN,
  HISTORY_MAXLEN, HEARTBEAT_SECS, USER_AGENT, PROXY_URL, DEBUG
"""
import os
import json
import asyncio
import hashlib
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Deque, Dict, Any, List, Optional
from urllib.parse import urlparse, unquote

# pip packages
from playwright.async_api import async_playwright
import aiohttp

# optional requests for proxy test
try:
    import requests
except Exception:
    requests = None  # we'll handle absence gracefully

# ---------------- CONFIG (use env ou valores aqui) ----------------
GAME_URL = os.getenv("GAME_URL", "https://blaze.bet.br/pt/games/double")
SUPABASE_ROUNDS_URL = os.getenv("SUPABASE_ROUNDS_URL", "")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN", "")
AWS_SIGNAL_URL = os.getenv("AWS_SIGNAL_URL", "")
AWS_TOKEN = os.getenv("AWS_TOKEN", "")
HISTORY_MAXLEN = int(os.getenv("HISTORY_MAXLEN", "2000"))
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS", "60"))
USER_AGENT = os.getenv("USER_AGENT", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()  # nova variável de ambiente
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# dedupe em nível de Python
_DEDUPE: set = set()
_DEDUPE_MAX = int(os.getenv("DEDUPE_MAX", "4000"))

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

    // envia para console (capturado pelo Playwright)
    console.log("__RESULT__" + JSON.stringify({ roundId: rid, color, roll, at }));
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
    # Exemplo simples (você substitui pela sua lógica)
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
            else:
                if DEBUG:
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


# ---------------- proxy helpers ----------------
def parse_proxy_url(proxy_url: str) -> Optional[Dict[str, Any]]:
    """
    Recebe PROXY_URL como 'http://user:pass@host:port' ou 'socks5://user:pass@host:port'
    Retorna dicionário: {'server': 'http://host:port', 'username': 'user', 'password': 'pass'}
    Ou None se PROXY_URL vazia.
    """
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    if not parsed.scheme or not parsed.hostname:
        return None
    username = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    host = parsed.hostname
    port = parsed.port
    server = f"{parsed.scheme}://{host}:{port}" if port else f"{parsed.scheme}://{host}"
    out = {"server": server}
    if username:
        out["username"] = username
    if password:
        out["password"] = password
    return out


def test_proxy_connectivity(proxy_url: str, timeout: int = 15) -> bool:
    """
    Testa o proxy com requests (sincrono). Retorna True se o teste passar.
    Usa https://api.ipify.org para verificar o IP.
    """
    if not proxy_url:
        return True
    if not requests:
        # requests não instalado: apenas logue e permita continuar (Playwright
        # pode ainda funcionar). Mas recomendamos instalar requests.
        print("[PROXY-TEST] requests library not available, pulando teste direto.")
        return True
    try:
        print(f"[PROXY-TEST] testando proxy -> {proxy_url}")
        r = requests.get("https://api.ipify.org?format=json",
                         proxies={"http": proxy_url, "https": proxy_url},
                         timeout=timeout)
        print("[PROXY-TEST] status:", r.status_code, "body:", r.text[:200])
        return r.status_code == 200
    except Exception as e:
        print("[PROXY-TEST] erro:", e)
        return False


# ---------------- main ----------------
async def main():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()

    # parse proxy
    proxy_obj = parse_proxy_url(PROXY_URL)
    if proxy_obj:
        if DEBUG:
            print(f"[PROXY] via URL -> {PROXY_URL}")
            print(f"[PROXY] parsed -> {proxy_obj}")
    else:
        if DEBUG:
            print("[PROXY] nenhum proxy configurado.")

    # test proxy (sincrono) antes de criar Playwright
    if PROXY_URL:
        ok = test_proxy_connectivity(PROXY_URL)
        if not ok:
            print("[FATAL] Falha no teste do proxy. Verifique PROXY_URL, usuário/senha/porta.")
            raise SystemExit(1)
        else:
            print("[PROXY-TEST] proxy respondeu OK.")

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with async_playwright() as p:
            # Launch options - flags para maximizar compatibilidade em containers/headless
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
            # se proxy existe, converte para playwright
            if proxy_obj:
                # playwright aceita 'server' + optional username/password
                launch_kwargs["proxy"] = {
                    "server": proxy_obj["server"]
                }
                if "username" in proxy_obj:
                    launch_kwargs["proxy"]["username"] = proxy_obj["username"]
                if "password" in proxy_obj:
                    launch_kwargs["proxy"]["password"] = proxy_obj["password"]

            if DEBUG:
                print(f"[BOOT] launching chromium with: { {k: launch_kwargs[k] for k in ('headless','args','proxy') if k in launch_kwargs} }")
            browser = await p.chromium.launch(**launch_kwargs)

            context_args = {}
            if USER_AGENT:
                context_args["user_agent"] = USER_AGENT
            ctx = await browser.new_context(**context_args)
            page = await ctx.new_page()

            # ---------- console handler: logs RAW + __RESULT__ processing ----------
            async def on_console_msg(msg):
                # msg.text may be property or callable depending on version; handle ambos
                try:
                    text = msg.text() if callable(getattr(msg, "text", None)) else msg.text
                except Exception:
                    try:
                        text = msg.text()
                    except Exception:
                        text = "<unreadable-console-message>"

                # raw console for debugging
                print(f"[CONSOLE-RAW] {text}")

                if not isinstance(text, str):
                    return

                if not text.startswith("__RESULT__"):
                    return

                try:
                    evt = json.loads(text[len("__RESULT__"):])
                except Exception:
                    print("[CONSOLE] __RESULT__ JSON parse failed")
                    return

                # dedupe python
                key = _evt_key(evt)
                if key in _DEDUPE:
                    if DEBUG:
                        print(f"[DEDUPE] skip {key}")
                    return
                _DEDUPE.add(key)
                if len(_DEDUPE) > _DEDUPE_MAX:
                    _DEDUPE.clear()
                    _DEDUPE.add(key)

                # log e armazena histórico
                print(f"[RESULT] {evt.get('roundId')} -> {str(evt.get('color')).upper()} ({evt.get('roll')}) @ {evt.get('at') or ''}")

                history.append({
                    "roundId": evt.get("roundId"),
                    "color": evt.get("color"),
                    "roll": evt.get("roll"),
                    "at": _parse_occurred_at(evt.get("at")),
                })

                # 1) envia ao Supabase
                try:
                    await send_to_supabase(session, evt)
                except Exception as e:
                    print(f"[SUPABASE] erro: {e}")

                # 2) gera sinais e envia para AWS (se houver)
                try:
                    signals = apply_strategies(history)
                    if signals:
                        await send_signals_to_aws(session, signals)
                except Exception as e:
                    print(f"[AWS] erro sinais: {e}")

            # listener wrapper para não bloquear
            def console_listener(m):
                asyncio.create_task(on_console_msg(m))
            page.on("console", console_listener)

            # evita logs ruidosos de fechamento
            page.on("close", lambda *a, **k: None)

            # injeta hook ANTES de navegar
            await page.add_init_script(HOOK_JS)

            # tenta navegar com retries
            for attempt in range(3):
                try:
                    if DEBUG:
                        print(f"[NAV] indo para {GAME_URL} (tentativa {attempt+1})")
                    await page.goto(GAME_URL, wait_until="domcontentloaded", timeout=45_000)
                    break
                except Exception as e:
                    print(f"[NAV] tentativa {attempt+1} falhou: {e}")
                    if attempt == 2:
                        raise

            # start heartbeat
            hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))

            print("Coletando RESULTADOS… (Ctrl+C para sair)")
            try:
                await asyncio.Event().wait()
            except (KeyboardInterrupt, SystemExit):
                pass
            finally:
                stop_evt.set()
                hb_task.cancel()
                # o context manager fecha o browser com segurança


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
