# extrator.py
# -----------------------------------------------------------------------------
# Coletor de resultados do Blaze Double usando Playwright (Chromium headless)
#
# Pronto para rodar em container (Render, Fly, Railway, etc.)
#  - Chromium com flags: --no-sandbox, --disable-dev-shm-usage
#  - Logs sem buffer (use python -u ou PYTHONUNBUFFERED=1)
#
# Requisitos:
#   pip install playwright aiohttp python-dotenv (opcional)
#   playwright install --with-deps chromium   # (no Dockerfile já fazemos isso)
#
# Variáveis de ambiente (exemplos):
#   GAME_URL=https://blaze.bet.br/pt/games/double
#   SUPABASE_ROUNDS_URL=https://<proj>.supabase.co/rest/v1/historico
#   SUPABASE_TOKEN=eyJ...  (anon/service key se sua rota exigir)
#   AWS_SIGNAL_URL=...     (opcional)
#   AWS_TOKEN=...          (opcional)
#   HISTORY_MAXLEN=2000
#   HEARTBEAT_SECS=60
#   USER_AGENT="Mozilla/5.0 ..."
#   PROXY=http://user:pass@host:port (opcional)
#   DEBUG=true
# -----------------------------------------------------------------------------

import os
import json
import asyncio
import hashlib
import signal
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Deque, Dict, Any, List, Optional

import aiohttp
from playwright.async_api import async_playwright, Page, ConsoleMessage

# ========================== CONFIG ==========================================

CONFIG = {
    # Use variáveis de ambiente por padrão em produção
    "USE_ENV": True,

    # Defaults (caso a env não esteja definida)
    "GAME_URL": "https://blaze.bet.br/pt/games/double",
    "SUPABASE_ROUNDS_URL": "",
    "SUPABASE_TOKEN": "",
    "AWS_SIGNAL_URL": "",
    "AWS_TOKEN": "",
    "HISTORY_MAXLEN": 2000,
    "HEARTBEAT_SECS": 60,
    "USER_AGENT": "",
    "PROXY": "",
    "DEBUG": False,
}

def _get_cfg():
    if CONFIG["USE_ENV"]:
        return {
            "GAME_URL": os.getenv("GAME_URL", CONFIG["GAME_URL"]),
            "SUPABASE_ROUNDS_URL": os.getenv("SUPABASE_ROUNDS_URL", CONFIG["SUPABASE_ROUNDS_URL"]),
            "SUPABASE_TOKEN": os.getenv("SUPABASE_TOKEN", CONFIG["SUPABASE_TOKEN"]),
            "AWS_SIGNAL_URL": os.getenv("AWS_SIGNAL_URL", CONFIG["AWS_SIGNAL_URL"]),
            "AWS_TOKEN": os.getenv("AWS_TOKEN", CONFIG["AWS_TOKEN"]),
            "HISTORY_MAXLEN": int(os.getenv("HISTORY_MAXLEN", str(CONFIG["HISTORY_MAXLEN"]))),
            "HEARTBEAT_SECS": int(os.getenv("HEARTBEAT_SECS", str(CONFIG["HEARTBEAT_SECS"]))),
            "USER_AGENT": os.getenv("USER_AGENT", CONFIG["USER_AGENT"]).strip(),
            "PROXY": os.getenv("PROXY", CONFIG["PROXY"]).strip(),
            "DEBUG": os.getenv("DEBUG", str(CONFIG["DEBUG"])).lower() in ("1", "true", "yes"),
        }
    return {
        k: (v.strip() if isinstance(v, str) else v)
        for k, v in CONFIG.items()
        if k != "USE_ENV"
    }

CFG = _get_cfg()
GAME_URL = CFG["GAME_URL"]
SUPABASE_ROUNDS_URL = CFG["SUPABASE_ROUNDS_URL"]
SUPABASE_TOKEN = CFG["SUPABASE_TOKEN"]
AWS_SIGNAL_URL = CFG["AWS_SIGNAL_URL"]
AWS_TOKEN = CFG["AWS_TOKEN"]
HISTORY_MAXLEN = CFG["HISTORY_MAXLEN"]
HEARTBEAT_SECS = CFG["HEARTBEAT_SECS"]
USER_AGENT = CFG["USER_AGENT"]
PROXY = CFG["PROXY"]
DEBUG = CFG["DEBUG"]

# Dedupe em nível de Python
_DEDUPE: set = set()
_DEDUPE_MAX = 4000

# ========================== HOOK (JS) =======================================

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
    if (!str.startsWith("42")) return; // socket.io
    try {
      const arr = JSON.parse(str.slice(2)); // ["data", {...}]
      const eventName = arr?.[0];
      const body      = arr?.[1];
      if (eventName === "data") tryEmitResult(body);
    } catch {}
  }

  // fetch
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

  // XHR
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

  // WebSocket
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

# ========================== UTILS ===========================================

def _evt_key(evt: Dict[str, Any]) -> str:
    rid  = str(evt.get("roundId") or "")
    roll = str(evt.get("roll") or "")
    at   = str(evt.get("at") or "")
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

# ========================== ENVIO: Supabase =================================

async def send_to_supabase(session: aiohttp.ClientSession, evt: Dict[str, Any]) -> None:
    if not SUPABASE_ROUNDS_URL:
        if DEBUG:
            print("[SUPABASE] URL não definida — pulando envio", flush=True)
        return

    payload = {
        "round_id":   evt.get("roundId"),
        "color":      evt.get("color"),
        "roll":       evt.get("roll"),
        "occurred_at": _parse_occurred_at(evt.get("at")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    headers = {"Content-Type": "application/json"}
    if SUPABASE_TOKEN:
        headers["apikey"] = SUPABASE_TOKEN
        headers["Authorization"] = f"Bearer {SUPABASE_TOKEN}"
        headers["Prefer"] = "resolution=merge-duplicates"

    if DEBUG:
        try:
            print(f"[SUPABASE][DEBUG] POST {SUPABASE_ROUNDS_URL} payload={json.dumps(payload)}", flush=True)
        except Exception:
            print("[SUPABASE][DEBUG] payload não serializável", flush=True)

    async with session.post(SUPABASE_ROUNDS_URL, headers=headers, json=payload) as resp:
        body = await resp.text()
        if resp.status >= 400:
            print(f"[SUPABASE] {resp.status}: {body[:240]}", flush=True)
        elif DEBUG:
            print(f"[SUPABASE][DEBUG] {resp.status}: {body[:240]}", flush=True)

# ========================== REGRAS / SINAIS =================================

def apply_strategies(history: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    PONTO ÚNICO para suas REGRAS DE NEGÓCIO
    Retorne sinais no formato sugerido abaixo, se quiser.
    """
    signals: List[Dict[str, Any]] = []

    # Exemplo: se "white" não aparece há mais de N rodadas → sinal
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

# ========================== ENVIO: AWS (opcional) ===========================

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
            print(f"[AWS] {resp.status}: {body[:240]}", flush=True)

# ========================== HEARTBEAT =======================================

async def heartbeat(history: Deque[Dict[str, Any]], stop_evt: asyncio.Event, interval: int):
    if interval <= 0:
        return
    while not stop_evt.is_set():
        await asyncio.sleep(interval)
        counts = Counter([r.get("color") for r in history])
        total = len(history)
        print(
            f"[HEARTBEAT] rounds={total} red={counts.get('red',0)} "
            f"black={counts.get('black',0)} white={counts.get('white',0)}",
            flush=True
        )

# ========================== MAIN ============================================

async def _run() -> None:
    print("[BOOT] extrator starting...", flush=True)
    print(f"[BOOT] GAME_URL={GAME_URL}", flush=True)

    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()

    # Respeitar SIGTERM/SIGINT (plataformas de nuvem)
    def _graceful(*_a):
        try:
            stop_evt.set()
        except Exception:
            pass
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _graceful)
        except NotImplementedError:
            # Windows ou ambientes sem suporte
            pass

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with async_playwright() as p:
            launch_kwargs = {
                "headless": True,
                "args": [
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            }
            if PROXY:
                launch_kwargs["proxy"] = {"server": PROXY}

            print("[BOOT] launching chromium with:", launch_kwargs, flush=True)
            browser = await p.chromium.launch(**launch_kwargs)

            context_args = {}
            if USER_AGENT:
                context_args["user_agent"] = USER_AGENT
            ctx = await browser.new_context(**context_args)
            page: Page = await ctx.new_page()

            # Handler de console (usando .text como PROPRIEDADE)
            async def on_console(msg: ConsoleMessage):
                try:
                    text = msg.text
                    if not isinstance(text, str) or not text.startswith("__RESULT__"):
                        return

                    try:
                        evt = json.loads(text[len("__RESULT__"):])
                    except Exception:
                        return

                    # Dedupe
                    key = _evt_key(evt)
                    if key in _DEDUPE:
                        return
                    _DEDUPE.add(key)
                    if len(_DEDUPE) > _DEDUPE_MAX:
                        _DEDUPE.clear()
                        _DEDUPE.add(key)

                    print(
                        f"[RESULT] {evt.get('roundId')} -> {str(evt.get('color')).upper()} "
                        f"({evt.get('roll')}) @ {evt.get('at') or ''}",
                        flush=True
                    )

                    # Atualiza histórico
                    history.append({
                        "roundId": evt.get("roundId"),
                        "color":   evt.get("color"),
                        "roll":    evt.get("roll"),
                        "at":      _parse_occurred_at(evt.get("at")),
                    })

                    # Envio Supabase
                    try:
                        await send_to_supabase(session, evt)
                    except Exception as e:
                        print(f"[SUPABASE] erro: {e}", flush=True)

                    # Estratégias + AWS
                    try:
                        signals = apply_strategies(history)
                        if signals:
                            await send_signals_to_aws(session, signals)
                    except Exception as e:
                        print(f"[AWS] erro sinais: {e}", flush=True)

                except Exception as e:
                    print(f"[CONSOLE] handler error: {e}", flush=True)

            # Listener não bloqueante
            def console_listener(m: ConsoleMessage):
                asyncio.create_task(on_console(m))

            page.on("console", console_listener)
            page.on("close", lambda *a, **k: None)

            # Injeta hook antes de navegar
            await page.add_init_script(HOOK_JS)

            # Navegação com retry simples
            for attempt in range(3):
                try:
                    print(f"[NAV] indo para {GAME_URL} (tentativa {attempt+1})", flush=True)
                    await page.goto(GAME_URL, wait_until="domcontentloaded", timeout=45_000)
                    break
                except Exception as e:
                    print(f"[NAV] tentativa {attempt+1} falhou: {e}", flush=True)
                    if attempt == 2:
                        raise

            # Heartbeat paralelo
            hb_task = asyncio.create_task(heartbeat(history, stop_evt, HEARTBEAT_SECS))

            print("Coletando RESULTADOS… (Ctrl+C para sair)", flush=True)
            try:
                # Espera até sinal de término
                await stop_evt.wait()
            finally:
                hb_task.cancel()
                try:
                    await ctx.close()
                except Exception:
                    pass
                try:
                    await browser.close()
                except Exception:
                    pass
                print("[SHUTDOWN] finalizado.", flush=True)

def main():
    asyncio.run(_run())

if __name__ == "__main__":
    main()
