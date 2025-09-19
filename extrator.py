import os
import json
import asyncio
import tempfile
import shutil
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Deque, Dict, Any, List

import aiohttp
import undetected_chromedriver as uc
from seleniumwire import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException

# ... [demais imports e configurações] ...

def build_driver():
    chrome_opts = uc.ChromeOptions()

    # Lógica headless
    if HEADLESS in ("1", "true", "yes", "chrome", ""):
        chrome_opts.add_argument("--headless")
    elif HEADLESS == "new":
        chrome_opts.add_argument("--headless=new")
    # HEADLESS off: não adiciona flag

    # Outras opções do navegador
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--window-size=1366,768")
    chrome_opts.add_argument("--blink-settings=imagesEnabled=false")
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-background-networking")
    chrome_opts.add_argument("--disable-features=VizDisplayCompositor")

    if USER_AGENT:
        chrome_opts.add_argument(f"--user-agent={USER_AGENT}")

    # Diretório temporário para perfil
    user_data_dir = tempfile.mkdtemp(prefix="uc_profile_")
    chrome_opts.add_argument(f"--user-data-dir={user_data_dir}")

    # Configuração de proxy para selenium-wire
    sw_opts: Dict[str, Any] = {}
    if PROXY_URL:
        sw_opts["proxy"] = {
            "http": PROXY_URL,
            "https": PROXY_URL,
        }

    # lê caminhos de chrome e chromedriver definidos no Dockerfile
    browser_path = os.getenv("CHROME_BINARY_PATH")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    driver_kwargs: Dict[str, Any] = {}
    if browser_path:
        driver_kwargs["browser_executable_path"] = browser_path
    if driver_path:
        driver_kwargs["driver_executable_path"] = driver_path

    # Cria instância do driver passando apenas argumentos válidos
    driver = uc.Chrome(options=chrome_opts, seleniumwire_options=sw_opts, **driver_kwargs)
    driver._user_data_dir = user_data_dir

    # Ajusta timeouts e desativa cache
    driver.set_page_load_timeout(45)
    try:
        driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    return driver

# ... [restante do código do extrator permanece inalterado] ...


# -----------------------
# Supabase sender
# -----------------------
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
                log("[SUPABASE]", resp.status, body[:200])
    except Exception as e:
        log("[SUPABASE] erro ao enviar:", e)

# -----------------------
# socket.io parser helper
# -----------------------
def parse_socketio_frames(text: str):
    """
    Recebe uma string (conteúdo WS/socket.io payload) e tenta extrair objetos
    do tipo 42[ ... ] (socket.io event frames) e devolve lista de objetos.
    """
    results = []
    if not text:
        return results
    # split on 42[ occurrences (socket.io)
    try:
        parts = text.split("42[")
        # parts[0] is prefix, others are JSON arrays like '"data", {...}]...'
        for i in range(1, len(parts)):
            chunk = "42[" + parts[i]  # restore beginning
            try:
                # remove leading '42' then parse JSON
                raw = chunk[2:]
                # raw may end with extra data; find matching bracket slice
                # attempt to parse until closing bracket
                # a simple approach: find last ']' in raw (works normally)
                idx = raw.rfind("]")
                if idx != -1:
                    json_text = raw[:idx+1]
                else:
                    json_text = raw
                arr = json.loads(json_text)
                # arr[0] is event name, arr[1] body
                results.append(arr)
            except Exception:
                # fallback: try parse whole chunk
                try:
                    arr = json.loads(parts[i])
                    results.append(arr)
                except Exception:
                    continue
    except Exception:
        pass
    return results

# -----------------------
# main loop
# -----------------------
async def run_loop():
    history: Deque[Dict[str, Any]] = deque(maxlen=HISTORY_MAXLEN)
    stop_evt = asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        driver = build_driver()

        log("[NAV] indo para", GAME_URL)
        try:
            driver.get(GAME_URL)
        except TimeoutException:
            log("[NAV] timeout do renderer (ignorado), tentando continuar")
        except Exception as e:
            log("[NAV] erro ao navegar:", e)
            driver.quit()
            return

        # logs básicos
        try:
            ready = driver.execute_script("return document.readyState")
            log("[NAV] readyState=", ready)
            log("[NAV] title=", driver.title)
            log("[NAV] url=", driver.current_url)
        except Exception:
            pass

        # heartbeat
        async def heartbeat():
            last_total = -1
            while not stop_evt.is_set():
                await asyncio.sleep(HEARTBEAT_SECS)
                counts = Counter([r.get("color") for r in history])
                total = len(history)
                if total != last_total or DEBUG:
                    last_total = total
                    log("[HEARTBEAT] rounds=", total,
                        "red=", counts.get("red", 0),
                        "black=", counts.get("black", 0),
                        "white=", counts.get("white", 0))

        # Poll websocket messages via selenium-wire (recommended)
        async def poll_ws_requests():
            # keep scanning driver.requests for websocket handshake requests that have ws_messages
            seen_req_ids = set()
            while not stop_evt.is_set():
                try:
                    # driver.requests is a snapshot-like collection; iterate recent
                    for req in list(driver.requests):
                        # only consider websocket handshake or socket.io endpoints
                        url = getattr(req, "url", "") or ""
                        if "/socket.io" not in url and not url.startswith("wss://"):
                            continue
                        # skip if already processed
                        rid = getattr(req, "id", None) or url
                        if rid in seen_req_ids:
                            # still can have new ws_messages on same req; we won't skip
                            pass
                        # ws_messages attribute holds messages for websocket handshake requests
                        ws_msgs = getattr(req, "ws_messages", None)
                        if not ws_msgs:
                            continue
                        # iterate messages
                        for m in ws_msgs:
                            # m.content may be bytes or str
                            content = m.content
                            if isinstance(content, bytes):
                                try:
                                    content = content.decode("utf-8", errors="ignore")
                                except Exception:
                                    content = str(content)
                            # parse socket.io frames inside content
                            frames = parse_socketio_frames(str(content))
                            for frame in frames:
                                try:
                                    ev_name = frame[0]
                                    body = frame[1] if len(frame) > 1 else None
                                    # if body structure matches payload => extract
                                    if isinstance(body, dict):
                                        payload = body
                                        # try to map fields
                                        rid = payload.get("id") or payload.get("roundId") or None
                                        color_val = payload.get("color")
                                        roll = payload.get("roll")
                                        # normalize color mapping if numeric
                                        color = None
                                        if isinstance(color_val, int):
                                            color = {1: "red", 2: "black", 0: "white"}.get(color_val, str(color_val))
                                        else:
                                            color = str(color_val) if color_val is not None else None
                                        at = payload.get("created_at") or payload.get("updated_at") or payload.get("at") or datetime.now(timezone.utc).isoformat()
                                        if rid is None and roll is None:
                                            # not the payload we want
                                            continue
                                        evt = {"roundId": rid, "color": color, "roll": roll, "at": at}
                                        key = f"{evt.get('roundId')}|{evt.get('roll')}|{evt.get('at')}"
                                        if key in _DEDUPE:
                                            continue
                                        _DEDUPE.add(key)
                                        if len(_DEDUPE) > _DEDUPE_MAX:
                                            _DEDUPE.clear()
                                            _DEDUPE.add(key)
                                        log("[WS-RESULT]", evt)
                                        history.append(evt)
                                        # async send to supabase
                                        asyncio.create_task(send_to_supabase(session, evt))
                                except Exception as e:
                                    if DEBUG:
                                        log("[WS-PARSE-ERR]", e)
                        # mark seen
                        seen_req_ids.add(rid)
                    # small sleep to avoid busy loop
                    await asyncio.sleep(0.8)
                except Exception as e:
                    if DEBUG:
                        log("[POLL_WS ERR]", e)
                    await asyncio.sleep(1.5)

        # fallback: poll browser console for __RESULT__ messages
        async def poll_console():
            first = True
            while not stop_evt.is_set():
                try:
                    logs = []
                    try:
                        logs = driver.get_log("browser")
                    except Exception:
                        # headless new may not present console logs in the same way
                        pass
                    if first and logs:
                        first = False
                        log("[CONSOLE] sample:")
                        for entry in logs[:5]:
                            log("  ", entry)
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
                        log("[CONSOLE-RESULT]", evt)
                        history.append(evt)
                        asyncio.create_task(send_to_supabase(session, evt))
                except Exception as e:
                    if DEBUG:
                        log("[CONSOLE POLL ERR]", e)
                await asyncio.sleep(1.2)

        # start tasks
        hb = asyncio.create_task(heartbeat())
        ws_task = asyncio.create_task(poll_ws_requests())
        console_task = asyncio.create_task(poll_console())

        log("Coletando RESULTADOS... (rodando headless)")

        try:
            # rodar indefinidamente; Render container mantem isso vivo
            await asyncio.Event().wait()
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            stop_evt.set()
            for t in (hb, ws_task, console_task):
                t.cancel()
            try:
                driver.quit()
            except Exception:
                pass
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
