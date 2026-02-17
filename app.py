import argparse
import atexit
import io
import ipaddress
import json
import math
import os
import socket
import sys
import threading
import time
import traceback
import wave
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID
from uuid import uuid4

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from wakeonlan import send_magic_packet

import pychromecast
from pychromecast.controllers.dashcast import APP_DASHCAST, APP_NAMESPACE, DashCastController


DISCOVERY_BROWSERS: List[Any] = []
ACTIVE_CAST: Optional[Any] = None
ACTIVE_CAST_LOCK = threading.Lock()
CAST_MONITOR_LOCK = threading.Lock()
CAST_MONITOR_STOP_EVENT: Optional[threading.Event] = None
CAST_MONITOR_THREAD: Optional[threading.Thread] = None
CAST_SESSION_COUNTER = 0
CAST_LAST_END_REASON: Optional[str] = None
CAST_LAST_END_TS_MS: Optional[int] = None
CAST_LAST_APP_ID: Optional[str] = None
CAST_LAST_NAMESPACES: List[str] = []
ACTIVE_CAST_TOKEN: Optional[str] = None
ACTIVE_CAST_TOKEN_LOCK = threading.Lock()
ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS: Optional[int] = None
ACTIVE_CAST_PAGE_LAST_EVENT: Optional[str] = None
ACTIVE_CAST_PAGE_LAST_VISIBLE: Optional[bool] = None
ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR: Optional[str] = None
ACTIVE_CAST_PAGE_HEARTBEAT_COUNT: int = 0
DEBUG_EVENTS: List[Dict[str, Any]] = []
DEBUG_EVENTS_LOCK = threading.Lock()
DEBUG_EVENT_COUNTER = 0
DEBUG_LOG_PATH: Optional[Path] = None
DEBUG_LOG_LOCK = threading.Lock()
PROCESS_EXIT_REASON: Optional[str] = None
PROCESS_EXIT_REASON_LOCK = threading.Lock()
CLEANUP_DONE = False
CLEANUP_LOCK = threading.Lock()


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def set_active_cast(cast: Any) -> None:
    global ACTIVE_CAST
    with ACTIVE_CAST_LOCK:
        ACTIVE_CAST = cast
    with CAST_MONITOR_LOCK:
        global CAST_LAST_END_REASON, CAST_LAST_END_TS_MS, CAST_LAST_APP_ID, CAST_LAST_NAMESPACES
        CAST_LAST_END_REASON = None
        CAST_LAST_END_TS_MS = None
        CAST_LAST_APP_ID = None
        CAST_LAST_NAMESPACES = []


def set_active_cast_token(token: str) -> None:
    global ACTIVE_CAST_TOKEN
    global ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS
    global ACTIVE_CAST_PAGE_LAST_EVENT
    global ACTIVE_CAST_PAGE_LAST_VISIBLE
    global ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR
    global ACTIVE_CAST_PAGE_HEARTBEAT_COUNT
    with ACTIVE_CAST_TOKEN_LOCK:
        ACTIVE_CAST_TOKEN = token
        ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS = None
        ACTIVE_CAST_PAGE_LAST_EVENT = None
        ACTIVE_CAST_PAGE_LAST_VISIBLE = None
        ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR = None
        ACTIVE_CAST_PAGE_HEARTBEAT_COUNT = 0
    debug_event("cast_page_token_set", token=token)


def clear_active_cast_token() -> None:
    global ACTIVE_CAST_TOKEN
    global ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS
    global ACTIVE_CAST_PAGE_LAST_EVENT
    global ACTIVE_CAST_PAGE_LAST_VISIBLE
    global ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR
    global ACTIVE_CAST_PAGE_HEARTBEAT_COUNT
    with ACTIVE_CAST_TOKEN_LOCK:
        ACTIVE_CAST_TOKEN = None
        ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS = None
        ACTIVE_CAST_PAGE_LAST_EVENT = None
        ACTIVE_CAST_PAGE_LAST_VISIBLE = None
        ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR = None
        ACTIVE_CAST_PAGE_HEARTBEAT_COUNT = 0


def get_active_cast_token() -> Optional[str]:
    with ACTIVE_CAST_TOKEN_LOCK:
        return ACTIVE_CAST_TOKEN


def update_cast_page_heartbeat(
    token: str, remote_addr: Optional[str], event_name: str, visible: Optional[bool]
) -> bool:
    global ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS
    global ACTIVE_CAST_PAGE_LAST_EVENT
    global ACTIVE_CAST_PAGE_LAST_VISIBLE
    global ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR
    global ACTIVE_CAST_PAGE_HEARTBEAT_COUNT

    with ACTIVE_CAST_TOKEN_LOCK:
        if ACTIVE_CAST_TOKEN is None or token != ACTIVE_CAST_TOKEN:
            return False
        ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS = now_ms()
        ACTIVE_CAST_PAGE_LAST_EVENT = event_name
        ACTIVE_CAST_PAGE_LAST_VISIBLE = visible
        ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR = remote_addr
        ACTIVE_CAST_PAGE_HEARTBEAT_COUNT += 1
        heartbeat_count = ACTIVE_CAST_PAGE_HEARTBEAT_COUNT

    if heartbeat_count <= 3 or heartbeat_count % 20 == 0:
        debug_event(
            "cast_page_heartbeat",
            count=heartbeat_count,
            heartbeat_event=event_name,
            visible=visible,
            remote_addr=remote_addr,
        )
    return True


def get_cast_page_heartbeat_snapshot() -> Dict[str, Any]:
    with ACTIVE_CAST_TOKEN_LOCK:
        return {
            "cast_token_present": ACTIVE_CAST_TOKEN is not None,
            "cast_token": ACTIVE_CAST_TOKEN,
            "page_last_heartbeat_ms": ACTIVE_CAST_PAGE_LAST_HEARTBEAT_MS,
            "page_last_event": ACTIVE_CAST_PAGE_LAST_EVENT,
            "page_last_visible": ACTIVE_CAST_PAGE_LAST_VISIBLE,
            "page_last_remote_addr": ACTIVE_CAST_PAGE_LAST_REMOTE_ADDR,
            "page_heartbeat_count": ACTIVE_CAST_PAGE_HEARTBEAT_COUNT,
        }


def add_query_param(url: str, key: str, value: str) -> str:
    split = urlsplit(url)
    q = dict(parse_qsl(split.query, keep_blank_values=True))
    q[key] = value
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(q), split.fragment))


def get_configured_public_base_url(config: Dict[str, Any]) -> Optional[str]:
    server_cfg = config.get("server", {})
    raw = server_cfg.get("public_base_url")
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if value.lower() == "auto":
        return None
    if "YOUR_COMPUTER_IP" in value:
        return None
    return value.rstrip("/")


def is_usable_ipv4(ip: Optional[str]) -> bool:
    if not ip:
        return False
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return bool(
        addr.version == 4
        and not addr.is_loopback
        and not addr.is_unspecified
        and not addr.is_multicast
    )


def detect_local_ipv4_for_target(target_host: Optional[str]) -> Optional[str]:
    probe_hosts: List[str] = []
    if target_host:
        probe_hosts.append(target_host)
    probe_hosts.extend(["8.8.8.8", "1.1.1.1"])

    for probe_host in probe_hosts:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((probe_host, 80))
                ip = sock.getsockname()[0]
            if is_usable_ipv4(ip):
                return ip
        except OSError:
            continue

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_DGRAM):
            ip = info[4][0]
            if is_usable_ipv4(ip):
                return ip
    except OSError:
        pass

    return None


def get_cast_host(cast: Any) -> Optional[str]:
    host = getattr(cast, "host", None)
    if isinstance(host, str) and host.strip():
        return host.strip()

    socket_client = getattr(cast, "socket_client", None)
    socket_host = getattr(socket_client, "host", None)
    if isinstance(socket_host, str) and socket_host.strip():
        return socket_host.strip()

    return None


def resolve_public_base_url(config: Dict[str, Any], cast: Any) -> str:
    configured = get_configured_public_base_url(config)
    if configured:
        debug_event("public_base_url_resolved", source="config", url=configured)
        return configured

    server_cfg = config.get("server", {})
    port = int(server_cfg.get("port", 8765))
    cast_host = get_cast_host(cast)
    local_ip = detect_local_ipv4_for_target(cast_host)

    if not local_ip:
        host_cfg = str(server_cfg.get("host", "")).strip()
        if is_usable_ipv4(host_cfg):
            local_ip = host_cfg

    if not local_ip:
        raise RuntimeError(
            "Unable to auto-detect host IPv4 for server.public_base_url. "
            "Set server.public_base_url explicitly, e.g. http://192.168.1.50:8765"
        )

    resolved = f"http://{local_ip}:{port}"
    debug_event("public_base_url_resolved", source="auto", url=resolved, cast_host=cast_host)
    return resolved


def get_active_cast() -> Optional[Any]:
    with ACTIVE_CAST_LOCK:
        return ACTIVE_CAST


def clear_active_cast() -> None:
    global ACTIVE_CAST
    with ACTIVE_CAST_LOCK:
        ACTIVE_CAST = None


def now_ms() -> int:
    return int(time.time() * 1000)


def debug_event(event: str, **fields: Any) -> None:
    global DEBUG_EVENT_COUNTER
    entry = {
        "ts_ms": now_ms(),
        "event": event,
        "thread": threading.current_thread().name,
    }
    entry.update(fields)

    with DEBUG_EVENTS_LOCK:
        DEBUG_EVENT_COUNTER += 1
        entry["seq"] = DEBUG_EVENT_COUNTER
        DEBUG_EVENTS.append(entry)
        if len(DEBUG_EVENTS) > 800:
            del DEBUG_EVENTS[:-800]

    parts = [f"{k}={entry[k]!r}" for k in sorted(entry.keys()) if k not in {"seq", "ts_ms", "event"}]
    line = f"DBG #{entry['seq']} {entry['event']} " + " ".join(parts)
    print(line, flush=True)

    with DEBUG_LOG_LOCK:
        if DEBUG_LOG_PATH is not None:
            try:
                with DEBUG_LOG_PATH.open("a", encoding="utf-8") as fp:
                    fp.write(line + "\n")
            except Exception:
                pass


def get_debug_events_snapshot(limit: int = 200) -> Dict[str, Any]:
    with DEBUG_EVENTS_LOCK:
        events = list(DEBUG_EVENTS[-limit:])
        total = DEBUG_EVENT_COUNTER
    return {"total": total, "events": events}


def set_debug_log_path(path: Path) -> None:
    global DEBUG_LOG_PATH
    with DEBUG_LOG_LOCK:
        DEBUG_LOG_PATH = path


def set_process_exit_reason(reason: str) -> None:
    global PROCESS_EXIT_REASON
    with PROCESS_EXIT_REASON_LOCK:
        PROCESS_EXIT_REASON = reason
    debug_event("process_exit_reason_set", reason=reason)


def get_process_exit_reason() -> Optional[str]:
    with PROCESS_EXIT_REASON_LOCK:
        return PROCESS_EXIT_REASON


def set_process_exit_reason_if_unset(reason: str) -> None:
    global PROCESS_EXIT_REASON
    changed = False
    with PROCESS_EXIT_REASON_LOCK:
        if PROCESS_EXIT_REASON is None:
            PROCESS_EXIT_REASON = reason
            changed = True
    if changed:
        debug_event("process_exit_reason_set", reason=reason)


def install_global_exception_hooks() -> None:
    def handle_sys_exception(exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        debug_event(
            "sys_excepthook",
            exc_type=getattr(exc_type, "__name__", str(exc_type)),
            message=str(exc_value),
            traceback="".join(traceback.format_exception(exc_type, exc_value, exc_tb))[-4000:],
        )
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    def handle_thread_exception(args: Any) -> None:
        debug_event(
            "thread_excepthook",
            thread_name=getattr(args, "thread", None).name if getattr(args, "thread", None) else None,
            exc_type=getattr(getattr(args, "exc_type", None), "__name__", str(getattr(args, "exc_type", None))),
            message=str(getattr(args, "exc_value", None)),
            traceback="".join(
                traceback.format_exception(
                    getattr(args, "exc_type", None),
                    getattr(args, "exc_value", None),
                    getattr(args, "exc_traceback", None),
                )
            )[-4000:],
        )
        if hasattr(threading, "__excepthook__"):
            threading.__excepthook__(args)

    sys.excepthook = handle_sys_exception
    if hasattr(threading, "excepthook"):
        threading.excepthook = handle_thread_exception


def record_cast_end(reason: str, app_id: Optional[str], namespaces: Optional[List[str]]) -> None:
    global CAST_LAST_END_REASON, CAST_LAST_END_TS_MS, CAST_LAST_APP_ID, CAST_LAST_NAMESPACES
    with CAST_MONITOR_LOCK:
        CAST_LAST_END_REASON = reason
        CAST_LAST_END_TS_MS = now_ms()
        CAST_LAST_APP_ID = app_id
        CAST_LAST_NAMESPACES = list(namespaces or [])
    debug_event("cast_end", reason=reason, app_id=app_id, namespaces=list(namespaces or []))


def get_cast_last_end_snapshot() -> Dict[str, Any]:
    with CAST_MONITOR_LOCK:
        monitor_running = (
            CAST_MONITOR_THREAD is not None
            and CAST_MONITOR_THREAD.is_alive()
            and CAST_MONITOR_STOP_EVENT is not None
            and not CAST_MONITOR_STOP_EVENT.is_set()
        )
        return {
            "monitor_running": monitor_running,
            "last_end_reason": CAST_LAST_END_REASON,
            "last_end_ts_ms": CAST_LAST_END_TS_MS,
            "last_app_id": CAST_LAST_APP_ID,
            "last_namespaces": list(CAST_LAST_NAMESPACES),
        }


def stop_cast_monitor() -> None:
    with CAST_MONITOR_LOCK:
        stop_event = CAST_MONITOR_STOP_EVENT
    if stop_event is not None:
        stop_event.set()
        debug_event("cast_monitor_stop_requested")


def start_cast_monitor(cast: Any, config: Dict[str, Any]) -> None:
    global CAST_MONITOR_STOP_EVENT, CAST_MONITOR_THREAD, CAST_SESSION_COUNTER
    stop_cast_monitor()
    with CAST_MONITOR_LOCK:
        CAST_SESSION_COUNTER += 1
        session_id = CAST_SESSION_COUNTER
        stop_event = threading.Event()
        CAST_MONITOR_STOP_EVENT = stop_event
        CAST_MONITOR_THREAD = threading.Thread(
            target=monitor_active_cast_session,
            args=(cast, session_id, stop_event, config),
            daemon=True,
        )
        thread = CAST_MONITOR_THREAD
    debug_event("cast_monitor_start", session_id=session_id, poll_seconds=config.get("cast", {}).get("status_poll_seconds", 2.0))
    thread.start()


def clear_active_cast_if_same(cast: Any) -> bool:
    global ACTIVE_CAST
    with ACTIVE_CAST_LOCK:
        if ACTIVE_CAST is not cast:
            return False
        ACTIVE_CAST = None
        return True


def monitor_active_cast_session(
    cast: Any, session_id: int, stop_event: threading.Event, config: Dict[str, Any]
) -> None:
    cast_cfg = config.get("cast", {})
    poll_seconds = float(cast_cfg.get("status_poll_seconds", 2.0))
    unknown_limit = int(cast_cfg.get("status_miss_limit", 3))
    switch_confirmations = int(cast_cfg.get("status_switch_confirmations", 2))
    startup_grace_seconds = float(cast_cfg.get("status_startup_grace_seconds", 10.0))
    end_on_unavailable = bool(cast_cfg.get("status_end_on_unavailable", False))
    unavailable_log_every = int(cast_cfg.get("status_unavailable_log_every", 5))
    heartbeat_required = bool(cast_cfg.get("page_heartbeat_required", True))
    heartbeat_timeout_seconds = float(cast_cfg.get("page_heartbeat_timeout_seconds", 20.0))
    heartbeat_start_grace_seconds = float(cast_cfg.get("page_heartbeat_start_grace_seconds", 20.0))
    grace_until = time.time() + max(0.0, startup_grace_seconds)
    heartbeat_grace_until = time.time() + max(0.0, heartbeat_start_grace_seconds)

    unknown_count = 0
    non_dashcast_count = 0
    heartbeat_missing_count = 0

    while not stop_event.wait(max(0.5, poll_seconds)):
        with CAST_MONITOR_LOCK:
            if CAST_SESSION_COUNTER != session_id:
                return

        if get_active_cast() is not cast:
            return

        heartbeat_fresh = False
        if heartbeat_required and get_active_cast_token():
            heartbeat = get_cast_page_heartbeat_snapshot()
            last_hb_ms = heartbeat.get("page_last_heartbeat_ms")
            if isinstance(last_hb_ms, int):
                hb_age_seconds = max(0.0, (now_ms() - last_hb_ms) / 1000.0)
                if hb_age_seconds <= heartbeat_timeout_seconds:
                    heartbeat_fresh = True
                    heartbeat_missing_count = 0
                elif time.time() >= heartbeat_grace_until:
                    heartbeat_missing_count += 1
                    if heartbeat_missing_count >= 1:
                        if clear_active_cast_if_same(cast):
                            clear_active_cast_token()
                            record_cast_end("page-heartbeat-timeout", None, None)
                            try:
                                cast.disconnect(timeout=2)
                            except Exception:  # noqa: BLE001
                                pass
                            debug_event(
                                "cast_monitor_end",
                                reason="page-heartbeat-timeout",
                                heartbeat_age_seconds=hb_age_seconds,
                                last_event=heartbeat.get("page_last_event"),
                                last_visible=heartbeat.get("page_last_visible"),
                                last_remote_addr=heartbeat.get("page_last_remote_addr"),
                            )
                        return
            else:
                if time.time() >= heartbeat_grace_until:
                    heartbeat_missing_count += 1
                    if unavailable_log_every > 0 and heartbeat_missing_count % unavailable_log_every == 0:
                        debug_event(
                            "cast_monitor_warning",
                            reason="page-heartbeat-not-started",
                            heartbeat_missing_count=heartbeat_missing_count,
                            grace_seconds=heartbeat_start_grace_seconds,
                        )

        try:
            receiver = cast.socket_client.receiver_controller
            try:
                receiver.update_status()
            except Exception:  # noqa: BLE001
                pass
            status = receiver.status

            if status is None:
                if heartbeat_fresh:
                    unknown_count = 0
                    non_dashcast_count = 0
                    continue
                unknown_count += 1
                non_dashcast_count = 0
                if time.time() < grace_until:
                    continue
                if unknown_count >= unknown_limit:
                    if end_on_unavailable:
                        if clear_active_cast_if_same(cast):
                            clear_active_cast_token()
                            record_cast_end("status-unavailable", None, None)
                            try:
                                cast.disconnect(timeout=2)
                            except Exception:  # noqa: BLE001
                                pass
                            debug_event("cast_monitor_end", reason="status-unavailable")
                        return
                    if unavailable_log_every > 0 and unknown_count % unavailable_log_every == 0:
                        debug_event(
                            "cast_monitor_warning",
                            reason="status-unavailable",
                            unknown_count=unknown_count,
                            unknown_limit=unknown_limit,
                        )
                continue

            app_id = status.app_id
            namespaces = list(status.namespaces or [])

            # Some TVs briefly report no active app/namespaces during transitions.
            if app_id is None and not namespaces:
                if heartbeat_fresh:
                    unknown_count = 0
                    non_dashcast_count = 0
                    continue
                unknown_count += 1
                non_dashcast_count = 0
                if time.time() < grace_until:
                    continue
                if unknown_count >= unknown_limit:
                    if end_on_unavailable:
                        if clear_active_cast_if_same(cast):
                            clear_active_cast_token()
                            record_cast_end("status-unavailable", None, None)
                            try:
                                cast.disconnect(timeout=2)
                            except Exception:  # noqa: BLE001
                                pass
                            debug_event("cast_monitor_end", reason="status-unavailable-empty-status")
                        return
                    if unavailable_log_every > 0 and unknown_count % unavailable_log_every == 0:
                        debug_event(
                            "cast_monitor_warning",
                            reason="status-unavailable-empty-status",
                            unknown_count=unknown_count,
                            unknown_limit=unknown_limit,
                        )
                continue

            unknown_count = 0
            dashcast_active = app_id == APP_DASHCAST and APP_NAMESPACE in namespaces
            if dashcast_active:
                non_dashcast_count = 0
                continue

            if time.time() < grace_until:
                continue

            non_dashcast_count += 1
            if non_dashcast_count >= switch_confirmations:
                if clear_active_cast_if_same(cast):
                    clear_active_cast_token()
                    reason = f"app-switched:{app_id}"
                    record_cast_end(reason, app_id, namespaces)
                    try:
                        cast.disconnect(timeout=2)
                    except Exception:  # noqa: BLE001
                        pass
                    debug_event(
                        "cast_monitor_end",
                        reason="app-switched",
                        app_id=app_id,
                        display_name=status.display_name,
                        namespaces=namespaces,
                    )
                return
        except Exception as exc:  # noqa: BLE001
            if heartbeat_fresh and not end_on_unavailable:
                unknown_count = 0
                non_dashcast_count = 0
                continue
            unknown_count += 1
            non_dashcast_count = 0
            if time.time() < grace_until:
                continue
            if unknown_count >= unknown_limit:
                if end_on_unavailable:
                    if clear_active_cast_if_same(cast):
                        clear_active_cast_token()
                        reason = f"monitor-error:{exc.__class__.__name__}"
                        record_cast_end(reason, None, None)
                        try:
                            cast.disconnect(timeout=2)
                        except Exception:  # noqa: BLE001
                            pass
                        debug_event(
                            "cast_monitor_end",
                            reason="monitor-error",
                            error_type=exc.__class__.__name__,
                            message=str(exc),
                        )
                    return
                if unavailable_log_every > 0 and unknown_count % unavailable_log_every == 0:
                    debug_event(
                        "cast_monitor_warning",
                        reason="monitor-error-ignored",
                        error_type=exc.__class__.__name__,
                        message=str(exc),
                        unknown_count=unknown_count,
                        unknown_limit=unknown_limit,
                    )


def build_app(config: Dict[str, Any]) -> Flask:
    app = Flask(__name__)
    chime_state = {"seq": 0}
    chime_lock = threading.Lock()
    chime_condition = threading.Condition(chime_lock)
    key_debug_state = {"events": [], "count": 0}
    key_debug_lock = threading.Lock()

    @app.route("/health")
    def health():
        return jsonify({"ok": True})

    @app.route("/clock")
    def clock():
        page = config.get("page", {})
        rules = page.get("rules", [])
        cast_token = request.args.get("cast_token", "")
        payload = {
            "title": page.get("title", "Alarm Clock"),
            "time_format_24h": bool(page.get("time_format_24h", False)),
            "sound": page.get("sound", {}),
            "rules": normalize_rules(rules),
            "cast_token": cast_token,
            "key_debug_enabled": bool(page.get("key_debug_enabled", True)),
        }
        return render_template("clock.html", page_config_json=json.dumps(payload))

    @app.route("/chime.wav")
    def chime_wav():
        freq = clamp_float(request.args.get("f"), 880.0, 80.0, 2200.0)
        duration_ms = clamp_float(request.args.get("d"), 300.0, 50.0, 3000.0)
        volume = clamp_float(request.args.get("v"), 0.2, 0.0, 1.0)
        wav_data = generate_tone_wav(freq, duration_ms, volume)
        return Response(wav_data, mimetype="audio/wav")

    @app.route("/api/chime", methods=["GET"])
    def get_chime_state():
        with chime_lock:
            return jsonify({"seq": chime_state["seq"]})

    @app.route("/api/chime", methods=["POST"])
    def trigger_chime():
        with chime_condition:
            chime_state["seq"] += 1
            seq = chime_state["seq"]
            chime_condition.notify_all()
        return jsonify({"ok": True, "seq": seq})

    @app.route("/api/chime/stream", methods=["GET"])
    def stream_chime():
        def event_stream():
            with chime_lock:
                last_seq = chime_state["seq"]
            yield f"event: chime\ndata: {json.dumps({'seq': last_seq})}\n\n"

            while True:
                with chime_condition:
                    notified = chime_condition.wait(timeout=25.0)
                    seq = chime_state["seq"]
                if notified and seq != last_seq:
                    last_seq = seq
                    yield f"event: chime\ndata: {json.dumps({'seq': seq})}\n\n"
                else:
                    yield ": keepalive\n\n"

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(stream_with_context(event_stream()), mimetype="text/event-stream", headers=headers)

    @app.route("/api/page/heartbeat", methods=["POST"])
    def page_heartbeat():
        payload = request.get_json(silent=True) or {}
        token = str(payload.get("token", "")).strip()
        event_name = str(payload.get("event", "alive"))[:64]
        visible_raw = payload.get("visible")
        visible = bool(visible_raw) if isinstance(visible_raw, bool) else None
        accepted = update_cast_page_heartbeat(token, request.remote_addr, event_name, visible)
        if not accepted:
            return jsonify({"ok": False, "accepted": False}), 202
        return jsonify({"ok": True, "accepted": True})

    @app.route("/api/cast/status", methods=["GET"])
    def cast_status():
        runtime = get_cast_last_end_snapshot()
        heartbeat = get_cast_page_heartbeat_snapshot()
        cast = get_active_cast()
        if cast is None:
            return jsonify(
                {
                    "connected": False,
                    "dashcast_active": False,
                    **runtime,
                    **heartbeat,
                }
            )

        status = cast.socket_client.receiver_controller.status
        app_id = status.app_id if status else None
        namespaces = status.namespaces if status else []
        return jsonify(
            {
                "connected": True,
                "friendly_name": getattr(cast, "name", None),
                "app_id": app_id,
                "display_name": status.display_name if status else None,
                "namespaces": namespaces,
                "dashcast_active": app_id == APP_DASHCAST and APP_NAMESPACE in namespaces,
                **runtime,
                **heartbeat,
            }
        )

    @app.route("/api/cast/stop", methods=["POST"])
    def cast_stop():
        stopped = stop_active_cast(stop_app=True, disconnect=True, reason="api-stop")
        return jsonify({"ok": True, "stopped": stopped})

    @app.route("/api/cast/recast", methods=["POST"])
    def cast_recast():
        threading.Thread(target=start_cast_with_retries, args=(config,), daemon=True).start()
        return jsonify({"ok": True, "started": True})

    @app.route("/api/keys", methods=["GET"])
    def key_debug_get():
        with key_debug_lock:
            events = list(key_debug_state["events"])
            count = int(key_debug_state["count"])
        return jsonify({"count": count, "buffered": len(events), "events": events})

    @app.route("/api/keys", methods=["POST"])
    def key_debug_post():
        payload = request.get_json(silent=True) or {}
        event = {
            "ts": int(payload.get("ts", int(time.time() * 1000))),
            "type": str(payload.get("type", ""))[:24],
            "key": str(payload.get("key", ""))[:64],
            "code": str(payload.get("code", ""))[:64],
            "keyCode": int(payload.get("keyCode", 0) or 0),
            "which": int(payload.get("which", 0) or 0),
            "repeat": bool(payload.get("repeat", False)),
            "handled": bool(payload.get("handled", False)),
            "action": str(payload.get("action", ""))[:64],
            "defaultPrevented": bool(payload.get("defaultPrevented", False)),
        }
        with key_debug_lock:
            key_debug_state["count"] += 1
            event["seq"] = int(key_debug_state["count"])
            event["remote_addr"] = request.remote_addr
            key_debug_state["events"].append(event)
            if len(key_debug_state["events"]) > 200:
                key_debug_state["events"] = key_debug_state["events"][-200:]
        debug_event(
            "key_input",
            key_seq=event["seq"],
            remote_addr=event["remote_addr"],
            input_type=event["type"],
            key=event["key"],
            code=event["code"],
            key_code=event["keyCode"],
            which=event["which"],
            handled=event["handled"],
            action=event["action"],
        )
        return ("", 204)

    @app.route("/api/debug/events", methods=["GET"])
    def debug_events_get():
        raw_limit = request.args.get("limit", "200")
        try:
            limit = int(raw_limit)
        except ValueError:
            limit = 200
        limit = max(1, min(1000, limit))
        snapshot = get_debug_events_snapshot(limit=limit)
        snapshot["process_exit_reason"] = get_process_exit_reason()
        return jsonify(snapshot)

    return app


def clamp_float(raw: Optional[str], default: float, low: float, high: float) -> float:
    try:
        val = float(raw) if raw is not None else default
    except ValueError:
        val = default
    return max(low, min(high, val))


def generate_tone_wav(
    frequency_hz: float, duration_ms: float, volume: float, sample_rate: int = 44100
) -> bytes:
    sample_count = int(sample_rate * (duration_ms / 1000.0))
    amplitude = int(32767 * volume)
    fade_samples = max(1, int(sample_rate * 0.01))

    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)

        frames = bytearray()
        for i in range(sample_count):
            env = 1.0
            if i < fade_samples:
                env = i / fade_samples
            elif i > sample_count - fade_samples:
                env = max(0.0, (sample_count - i) / fade_samples)
            sample = int(amplitude * env * math.sin(2.0 * math.pi * frequency_hz * i / sample_rate))
            frames.extend(sample.to_bytes(2, byteorder="little", signed=True))
        wav.writeframes(frames)
    return buf.getvalue()


def normalize_rules(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rule in rules:
        start = rule.get("start")
        end = rule.get("end")
        class_name = rule.get("class_name")
        if not isinstance(start, str) or not class_name:
            continue
        out.append(
            {
                "name": str(rule.get("name", class_name)),
                "start": start,
                "end": end if isinstance(end, str) or end is None else None,
                "class_name": str(class_name),
            }
        )
    return out


def maybe_send_wol(config: Dict[str, Any]) -> None:
    mac = config.get("cast", {}).get("wake_on_lan_mac")
    if mac:
        send_magic_packet(mac)
        delay = float(config.get("cast", {}).get("wake_delay_seconds", 8))
        time.sleep(max(0, delay))


def parse_device_uuid(raw: Any) -> Optional[UUID]:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return UUID(str(raw).strip())
    except ValueError as exc:
        raise RuntimeError(f"Invalid cast.device_uuid value: {raw}") from exc


def find_chromecast(config: Dict[str, Any]) -> Tuple[Any, Optional[Any]]:
    cast_cfg = config.get("cast", {})
    friendly_name = str(cast_cfg.get("friendly_name", "")).strip() or None
    ip = str(cast_cfg.get("ip", "")).strip() or None
    device_uuid = parse_device_uuid(cast_cfg.get("device_uuid"))

    known_hosts = [ip] if ip else None
    if friendly_name or device_uuid:
        chromecasts, browser = pychromecast.get_listed_chromecasts(
            friendly_names=[friendly_name] if friendly_name else None,
            uuids=[device_uuid] if device_uuid else None,
            discovery_timeout=15,
            known_hosts=known_hosts,
        )
        if chromecasts:
            return chromecasts[0], browser

    if known_hosts:
        chromecasts, browser = pychromecast.get_chromecasts(known_hosts=known_hosts, timeout=10)
        if chromecasts:
            return chromecasts[0], browser

    raise RuntimeError(
        "No Chromecast/Google TV found. Set cast.friendly_name (recommended) or "
        "cast.device_uuid. cast.ip is optional as a fallback hint."
    )


def cast_clock_page(config: Dict[str, Any]) -> None:
    maybe_send_wol(config)

    cast, browser = find_chromecast(config)
    if browser is not None:
        # Keep discovery alive for the lifetime of the process; shutting it down
        # early can crash the pychromecast socket thread on some setups.
        DISCOVERY_BROWSERS.append(browser)
    cast.wait()
    set_active_cast(cast)

    base_url = resolve_public_base_url(config, cast)
    cast_token = uuid4().hex
    set_active_cast_token(cast_token)
    target_url = add_query_param(f"{base_url}/clock", "cast_token", cast_token)
    debug_event("clock_target_url", url=target_url)

    volume = config.get("cast", {}).get("volume")
    if isinstance(volume, (int, float)):
        cast.set_volume(float(volume))

    stop_before_cast = bool(config.get("cast", {}).get("stop_app_before_cast", True))
    if stop_before_cast:
        stop_current_app(cast)

    dash = DashCastController()
    cast.register_handler(dash)
    ensure_dashcast_ready(cast)
    send_dashcast_load_command(dash, target_url)
    start_cast_monitor(cast, config)


def launch_dashcast_app(cast: Any, timeout_seconds: float = 20.0) -> None:
    done = threading.Event()
    result: Dict[str, Any] = {"ok": False, "response": None}

    def callback(ok: bool, response: Any) -> None:
        result["ok"] = ok
        result["response"] = response
        done.set()

    cast.socket_client.receiver_controller.launch_app(
        APP_DASHCAST,
        force_launch=True,
        callback_function=callback,
    )

    if not done.wait(timeout_seconds):
        raise RuntimeError("Timed out waiting to launch DashCast app.")
    if not result["ok"]:
        raise RuntimeError(f"DashCast launch failed: {result['response']}")
    time.sleep(0.5)


def stop_current_app(cast: Any, timeout_seconds: float = 10.0) -> None:
    done = threading.Event()

    def callback(_ok: bool, _response: Any) -> None:
        done.set()

    try:
        cast.socket_client.receiver_controller.stop_app(callback_function=callback)
        done.wait(timeout_seconds)
        time.sleep(0.5)
    except Exception:  # noqa: BLE001
        return


def wait_for_dashcast_namespace(cast: Any, timeout_seconds: float = 10.0) -> bool:
    receiver = cast.socket_client.receiver_controller
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        status = receiver.status
        if status and status.app_id == APP_DASHCAST and APP_NAMESPACE in status.namespaces:
            return True
        try:
            receiver.update_status()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)
    return False


def ensure_dashcast_ready(cast: Any) -> None:
    # Two-phase recovery:
    # 1) Force-launch DashCast and verify namespace is active.
    # 2) If verification fails, stop the current app and retry launch once.
    launch_dashcast_app(cast)
    if wait_for_dashcast_namespace(cast):
        return

    stop_current_app(cast)
    launch_dashcast_app(cast)
    if wait_for_dashcast_namespace(cast):
        return

    status = cast.socket_client.receiver_controller.status
    app_id = status.app_id if status else None
    namespaces = status.namespaces if status else []
    raise RuntimeError(
        "DashCast did not become active after relaunch. "
        f"Current app_id={app_id}, namespaces={namespaces}"
    )


def send_dashcast_load_command(dash: DashCastController, url: str) -> None:
    should_reload = False
    reload_milliseconds = 0
    msg = {
        "url": url,
        "force": True,
        "reload": should_reload,
        "reload_time": reload_milliseconds,
    }
    dash.send_message(msg, inc_session_id=True)


def stop_active_cast(stop_app: bool, disconnect: bool, reason: str = "manual-stop") -> bool:
    cast = get_active_cast()
    if cast is None:
        clear_active_cast_token()
        debug_event("stop_active_cast_noop", reason=reason, stop_app=stop_app, disconnect=disconnect)
        return False

    status = None
    try:
        status = cast.socket_client.receiver_controller.status
    except Exception:  # noqa: BLE001
        status = None

    stop_cast_monitor()
    debug_event("stop_active_cast_begin", reason=reason, stop_app=stop_app, disconnect=disconnect)

    if stop_app:
        try:
            cast.quit_app(timeout=8)
        except Exception:  # noqa: BLE001
            pass

    if disconnect:
        try:
            cast.disconnect(timeout=2)
        except Exception:  # noqa: BLE001
            pass

    clear_active_cast_if_same(cast)
    clear_active_cast_token()
    app_id = status.app_id if status else None
    namespaces = status.namespaces if status else None
    record_cast_end(reason, app_id, namespaces)
    debug_event("stop_active_cast_done", reason=reason, app_id=app_id, namespaces=list(namespaces or []))
    return True


def stop_discovery_browsers() -> None:
    while DISCOVERY_BROWSERS:
        browser = DISCOVERY_BROWSERS.pop()
        try:
            browser.stop_discovery()
        except Exception:  # noqa: BLE001
            pass


def cleanup_resources(config: Dict[str, Any], reason: str = "process-exit") -> None:
    global CLEANUP_DONE
    with CLEANUP_LOCK:
        if CLEANUP_DONE:
            debug_event("cleanup_skipped_already_done", reason=reason)
            return
        CLEANUP_DONE = True

    stop_cast_monitor()
    stop_on_exit = bool(config.get("cast", {}).get("stop_on_exit", True))
    debug_event("cleanup_run", reason=reason, stop_on_exit=stop_on_exit)
    stop_active_cast(stop_app=stop_on_exit, disconnect=True, reason=reason)
    clear_active_cast()
    stop_discovery_browsers()


def start_cast_with_retries(config: Dict[str, Any], retries: int = 3) -> None:
    last_error: Optional[Exception] = None
    configured_base_url = get_configured_public_base_url(config)
    clock_url = (
        f"{configured_base_url}/clock"
        if configured_base_url
        else "auto-detected://<host-ip>/clock"
    )
    debug_event("cast_start_begin", retries=retries, clock_url=clock_url)
    for attempt in range(1, retries + 1):
        try:
            debug_event("cast_start_attempt", attempt=attempt)
            cast_clock_page(config)
            debug_event("cast_start_success", attempt=attempt)
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            debug_event(
                "cast_start_attempt_failed",
                attempt=attempt,
                error_type=exc.__class__.__name__,
                message=str(exc),
            )
            if is_dashcast_namespace_error(exc):
                debug_event("cast_start_dashcast_unsupported", clock_url=clock_url)
                record_cast_end("dashcast-unsupported", None, None)
                return
            time.sleep(3)

    record_cast_end("cast-start-failed", None, None)
    debug_event(
        "cast_start_failed",
        retries=retries,
        error_type=last_error.__class__.__name__ if last_error else None,
        message=str(last_error) if last_error else None,
        clock_url=clock_url,
    )


def is_dashcast_namespace_error(exc: Exception) -> bool:
    msg = str(exc)
    return "Namespace urn:x-cast:com.madmod.dashcast is not supported" in msg


def main() -> None:
    install_global_exception_hooks()
    debug_event("app_boot", pid=os.getpid(), cwd=str(Path.cwd()))

    parser = argparse.ArgumentParser(description="Cast a customizable HTML alarm clock to Google TV.")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to JSON config file (default: config.json).",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. Copy config.example.json to config.json and edit it."
        )

    config = load_config(config_path)
    debug_event("config_loaded", path=str(config_path))
    debug_cfg = config.get("debug", {})
    log_file = str(debug_cfg.get("log_file", "runtime_debug.log")).strip() or "runtime_debug.log"
    log_path = Path(log_file)
    if not log_path.is_absolute():
        log_path = Path.cwd() / log_path
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    set_debug_log_path(log_path)
    debug_event("debug_log_path_set", path=str(log_path))
    app = build_app(config)
    atexit.register(lambda: cleanup_resources(config, reason=get_process_exit_reason() or "atexit"))

    server_cfg = config.get("server", {})
    host = str(server_cfg.get("host", "0.0.0.0"))
    port = int(server_cfg.get("port", 8765))
    debug_event("flask_config", host=host, port=port)

    cast_thread = threading.Thread(
        target=start_cast_with_retries, args=(config,), daemon=True, name="cast-start-thread"
    )
    cast_thread.start()
    debug_event("cast_thread_started", thread_name=cast_thread.name)

    try:
        debug_event("flask_run_start")
        app.run(host=host, port=port, debug=False, use_reloader=False)
        set_process_exit_reason_if_unset("flask-run-returned")
        debug_event("flask_run_returned")
    except KeyboardInterrupt as exc:
        set_process_exit_reason_if_unset("keyboard-interrupt")
        debug_event("flask_run_keyboard_interrupt", message=str(exc))
        raise
    except BaseException as exc:  # noqa: BLE001
        set_process_exit_reason_if_unset(f"flask-run-exception:{exc.__class__.__name__}")
        debug_event(
            "flask_run_exception",
            error_type=exc.__class__.__name__,
            message=str(exc),
            traceback=traceback.format_exc()[-4000:],
        )
        raise
    finally:
        reason = get_process_exit_reason() or "process-exit"
        debug_event("cleanup_begin", reason=reason)
        cleanup_resources(config, reason=reason)


if __name__ == "__main__":
    main()
