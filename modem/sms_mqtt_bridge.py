#!/usr/bin/env python3
"""SMS <-> MQTT bridge for the openstick.

TOPICS (configurable via OPENSTICK_ID env — defaults to the wlan0 MAC suffix):
  openstick/<id>/status       birth "online" (retained) / LWT "offline"
  openstick/<id>/sms/inbound  publishes every new JSON in /mnt/sms_ram/wms_inbox/
                              QoS 1, retained (so HA sees the last one on startup)
                              payload = the JSON already produced by wms_sms_daemon
  openstick/<id>/sms/send     subscribed. HA publishes:
                                {"to": "+79...", "text": "hi"}   # or "message"
                                {"to": "+79...", "text": "hi", "req_id": "abc"}
                              QoS 1 — the bridge shells out to send_sms.py.
  openstick/<id>/sms/sent     publishes result of each send command:
                                {"req_id": "...", "to": "...", "ok": true/false,
                                 "chars": N, "segments": N, "detail": "..."}

Design notes:
- Watches the inbox by polling every 2 s. wms_sms_daemon already wrote files
  atomically (.tmp -> rename), so a partial JSON is never observed.
- Publishes with QoS 1 + retain, then deletes the file on the paho publish
  ack. If broker is down we stay connected via reconnect and keep files.
- HA send-command runs `send_sms.py` in a subprocess; parses its stdout for
  the "seg N/M OK/FAIL" lines to build the result payload.
- Single-shot connection with auto-reconnect from paho — no threads of our own.
"""
import json
import os
import pathlib
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

import paho.mqtt.client as mqtt

# --------- config ---------
BROKER_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
BROKER_PORT = int(os.environ.get("MQTT_PORT", "1883"))
BROKER_USER = os.environ.get("MQTT_USER", "openstick")
BROKER_PASS = os.environ.get("MQTT_PASS", "")
INBOX_DIR   = pathlib.Path(os.environ.get("WMS_INBOX_DIR", "/mnt/sms_ram/wms_inbox"))
SEND_SMS    = os.environ.get("SEND_SMS_BIN", "/usr/local/bin/send_sms.py")
POLL_S      = float(os.environ.get("BRIDGE_POLL_S", "2"))

def _parse_whitelist(raw: str) -> set:
    """Normalise numbers so '+79...', '79...', ' +7 (000) 000-00-00 ' all match.
    Keep only leading + and digits."""
    out = set()
    for item in raw.split(","):
        s = "".join(ch for ch in item if ch.isdigit() or ch == "+")
        if not s:
            continue
        # tolerate 8-prefixed Russian numbers by folding to +7
        if s.startswith("8") and len(s) == 11:
            s = "+7" + s[1:]
        elif not s.startswith("+"):
            s = "+" + s
        out.add(s)
    return out

WHITELIST = _parse_whitelist(os.environ.get("SMS_TRUSTED_NUMBERS", ""))

# Команда, пролежавшая в очереди дольше срока, уже бесполезна: абонент
# получит "рейс вылетел" через несколько часов после посадки. Отправитель
# ставит expires_at (epoch, секунды); без него поведение прежнее.
SMS_MAX_PER_HOUR = int(os.environ.get("SMS_MAX_PER_HOUR", "30"))
_SENT_AT: list = []          # отметки отправок для скользящего часа
_SANE_EPOCH = 1_700_000_000  # раньше этого часы модема не синхронизированы


def _expired(data: dict) -> str:
    """Пустая строка - можно слать, иначе причина отказа."""
    exp = data.get("expires_at")
    if exp is None:
        return ""
    now = time.time()
    if now < _SANE_EPOCH:
        # Часы не синхронизированы - молча выбросить всё было бы хуже,
        # чем доставить с опозданием.
        log("expiry check skipped: clock not synced")
        return ""
    try:
        exp = float(exp)
    except (TypeError, ValueError):
        return ""
    if now > exp:
        return f"expired {int(now - exp)}s ago"
    return ""


def _rate_limited() -> str:
    now = time.time()
    _SENT_AT[:] = [t for t in _SENT_AT if now - t < 3600]
    if len(_SENT_AT) >= SMS_MAX_PER_HOUR:
        return f"rate limit {SMS_MAX_PER_HOUR}/hour"
    _SENT_AT.append(now)
    return ""


# --------- transliteration (Cyrillic → Latin, GOST-style) ---------

_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}

def transliterate(s: str) -> str:
    """Cyrillic → Latin. Preserves case, non-Cyrillic chars pass through."""
    out = []
    for ch in s:
        low = ch.lower()
        mapped = _TRANSLIT.get(low)
        if mapped is None:
            out.append(ch)
            continue
        if ch.isupper():
            out.append(mapped.capitalize() if mapped else "")
        else:
            out.append(mapped)
    return "".join(out)


def _device_id() -> str:
    if os.environ.get("OPENSTICK_ID"):
        return os.environ["OPENSTICK_ID"]
    try:
        with open("/sys/class/net/wlan0/address") as f:
            mac = f.read().strip().replace(":", "")
        return "openstick_" + mac[-6:]
    except Exception:
        return "openstick_" + socket.gethostname()


DEVICE_ID = _device_id()
BASE = f"openstick/{DEVICE_ID}"
T_STATUS   = f"{BASE}/status"
T_INBOUND  = f"{BASE}/sms/inbound"
T_TRUSTED  = f"{BASE}/sms/inbound_trusted"
T_SEND     = f"{BASE}/sms/send"
T_SENT     = f"{BASE}/sms/sent"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# --------- outbound: watch INBOX_DIR, publish new files ---------

def _iter_new_files(pub_queue: "queue.Queue"):
    """Poll inbox and enqueue new .json files for publishing."""
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            for p in sorted(INBOX_DIR.glob("*.json")):
                pub_queue.put(p)
        except Exception as exc:
            log("watcher error:", exc)
        time.sleep(POLL_S)


def _sender_thread(client: mqtt.Client, pub_queue: "queue.Queue"):
    while True:
        p: pathlib.Path = pub_queue.get()
        try:
            payload = p.read_bytes()
            data = json.loads(payload)         # validate before publishing garbage
            info = client.publish(T_INBOUND, payload=payload, qos=1, retain=True)
            info.wait_for_publish(timeout=10)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                log(f"publish rc={info.rc}, keeping {p.name}")
                continue
            # 2nd publish: only if sender is trusted — HA automations subscribe
            # to inbound_trusted to run commands / call the LLM without
            # exposing themselves to arbitrary SMS spammers
            sender = str(data.get("from", ""))
            if WHITELIST and sender in WHITELIST:
                info2 = client.publish(T_TRUSTED, payload=payload, qos=1, retain=False)
                info2.wait_for_publish(timeout=10)
                log(f"published trusted+deleted {p.name} (from={sender})")
            elif WHITELIST:
                log(f"published (untrusted from={sender}) — dropped {p.name}")
            else:
                log(f"published (no whitelist) — deleted {p.name}")
            p.unlink()
        except Exception as exc:
            log(f"publish failed {p}: {exc!r}")
            time.sleep(2)


# --------- inbound: HA -> command -> subprocess send_sms.py ---------

SEG_OK_RE   = re.compile(r"seg (\d+)/(\d+) OK\s+msg_id=(\d+)")
SEG_FAIL_RE = re.compile(r"seg (\d+)/(\d+) FAIL\s+(.*)")
SEGMENTS_RE = re.compile(r"segments: (\d+)\s+text_chars: (\d+)")


def _run_send(to: str, text: str, req_id: str) -> dict:
    """Shell out to send_sms.py, capture and parse its output."""
    try:
        cp = subprocess.run(
            [SEND_SMS, "--to", to, "--text", text],
            capture_output=True, text=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        return {"req_id": req_id, "to": to, "ok": False, "detail": "timeout"}

    out = cp.stdout + cp.stderr
    ok_count = len(SEG_OK_RE.findall(out))
    fail_count = len(SEG_FAIL_RE.findall(out))
    seg_hdr = SEGMENTS_RE.search(out)
    total = int(seg_hdr.group(1)) if seg_hdr else 0
    chars = int(seg_hdr.group(2)) if seg_hdr else len(text)

    result = {
        "req_id": req_id,
        "to": to,
        "ok": cp.returncode == 0 and ok_count == total > 0,
        "chars": chars,
        "segments": total,
        "segments_ok": ok_count,
        "segments_failed": fail_count,
    }
    if not result["ok"]:
        result["detail"] = out[-800:].strip()
    return result


# ---------- Home Assistant MQTT Discovery ----------

DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")

def _publish_discovery(client: mqtt.Client):
    """Advertise this stick to HA via MQTT Discovery. HA auto-creates entities;
    no YAML editing needed on the HA side."""
    device = {
        "identifiers": [DEVICE_ID],
        "name": f"OpenStick {DEVICE_ID}",
        "manufacturer": "OpenStick",
        "model": "UFI MSM8916",
    }
    availability = [{"topic": T_STATUS, "payload_available": "online",
                     "payload_not_available": "offline"}]

    configs = [
        # notify service — HA users call: service notify.openstick_<id>_sms
        # with data: {target: "+79...", message: "..."}
        (f"{DISCOVERY_PREFIX}/notify/{DEVICE_ID}/sms/config", {
            "platform": "notify",
            "unique_id": f"{DEVICE_ID}_sms_send",
            "name": "SMS",
            "command_topic": T_SEND,
            "command_template": '{"to":"{{ target[0] }}","text":"{{ message }}"}',
            "qos": 1,
            "device": device,
            "availability": availability,
        }),
        # sensor: last incoming SMS — state = sender, attrs = full payload
        (f"{DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/last_sms/config", {
            "unique_id": f"{DEVICE_ID}_last_sms",
            "name": "Last incoming SMS",
            "state_topic": T_INBOUND,
            "value_template": "{{ value_json['from'] }}",
            "json_attributes_topic": T_INBOUND,
            "device": device,
            "availability": availability,
        }),
        # sensor: last incoming SMS from a WHITELISTED sender (this is the one
        # HA automations should trigger on for command routing / LLM prompt)
        (f"{DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/last_trusted_sms/config", {
            "unique_id": f"{DEVICE_ID}_last_trusted_sms",
            "name": "Last trusted SMS",
            "state_topic": T_TRUSTED,
            "value_template": "{{ value_json['from'] }}",
            "json_attributes_topic": T_TRUSTED,
            "device": device,
            "availability": availability,
        }),
        # sensor: last send result — state = ok/fail, attrs = full response
        (f"{DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/last_sms_send/config", {
            "unique_id": f"{DEVICE_ID}_last_sms_send",
            "name": "Last SMS send result",
            "state_topic": T_SENT,
            "value_template": "{{ 'ok' if value_json.ok else 'fail' }}",
            "json_attributes_topic": T_SENT,
            "device": device,
            "availability": availability,
        }),
        # binary_sensor: online/offline
        (f"{DISCOVERY_PREFIX}/binary_sensor/{DEVICE_ID}/online/config", {
            "unique_id": f"{DEVICE_ID}_online",
            "name": "Online",
            "state_topic": T_STATUS,
            "payload_on": "online",
            "payload_off": "offline",
            "device_class": "connectivity",
            "device": device,
        }),
    ]

    for topic, cfg in configs:
        client.publish(topic, json.dumps(cfg, ensure_ascii=False), qos=1, retain=True)
    log(f"published {len(configs)} HA Discovery configs")


def _on_connect(client, userdata, flags, rc):
    log(f"MQTT connect rc={rc}")
    if rc == 0:
        client.publish(T_STATUS, "online", qos=1, retain=True)
        client.subscribe(T_SEND, qos=1)
        _publish_discovery(client)
        log(f"subscribed to {T_SEND}")


def _on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        log("send command: bad JSON, ignoring")
        return
    to   = data.get("to") or data.get("number")
    text = data.get("text") or data.get("message") or data.get("body")
    req  = str(data.get("req_id") or uuid.uuid4())
    # tolerate HA templating quirks: `+79...` in a JSON dict template can end
    # up serialised as int (79...) — coerce back to +E.164 here.
    if isinstance(to, int):
        to = "+" + str(to)
    elif isinstance(to, str):
        to = to.strip()
        if to and not to.startswith("+") and to.lstrip("+").isdigit():
            to = "+" + to.lstrip("+")
    if not to or not text:
        log(f"send command: missing to/text: {data!r}")
        client.publish(T_SENT, json.dumps({
            "req_id": req, "ok": False, "detail": "missing to/text",
        }), qos=1)
        return
    # Optional: HA (or any client) can set "translit":true to have Cyrillic
    # transliterated to Latin here — LLMs don't reliably follow prompt
    # instructions to reply in a specific script, so we do it downstream.
    # Also truncate to 65 chars so the reply comfortably fits in one SMS
    # (kept short-of-70 to leave room for prefix like "OK: ").
    if data.get("translit"):
        pre = text
        text = transliterate(text)
        if pre != text:
            log(f"translit applied: {len(pre)}ch cyr → {len(text)}ch latin")
    # Truncation is opt-in. Callers that genuinely need a one-segment reply
    # (the LLM SMS router) pass "max_chars"; everyone else gets the full text,
    # split across as many segments as it needs. The old fixed 65-char cut
    # silently ate the tail of every alert - exactly the part that matters.
    limit = data.get("max_chars")
    try:
        limit = int(limit) if limit is not None else 0
    except (TypeError, ValueError):
        limit = 0
    if limit > 0 and len(text) > limit:
        text = text[: max(1, limit - 3)].rstrip() + "..."
        log(f"truncated to max_chars={limit}: {len(text)}ch")
    for reason in (_expired(data), _rate_limited()):
        if reason:
            log(f"send req={req} to={to} DROPPED: {reason}")
            client.publish(T_SENT, json.dumps({
                "req_id": req, "to": to, "ok": False, "detail": reason,
            }, ensure_ascii=False), qos=1)
            return
    log(f"send req={req} to={to} chars={len(text)}")
    result = _run_send(to, text, req)
    log(f"send req={req} ok={result['ok']} segments={result.get('segments')}")
    client.publish(T_SENT, json.dumps(result, ensure_ascii=False), qos=1)


def main():
    log(f"device_id={DEVICE_ID} broker={BROKER_HOST}:{BROKER_PORT}")
    log(f"topics: status={T_STATUS} inbound={T_INBOUND} trusted={T_TRUSTED} send={T_SEND} sent={T_SENT}")
    log(f"whitelist: {sorted(WHITELIST) if WHITELIST else '(none — inbound_trusted will stay empty)'}")

    client = mqtt.Client(client_id=f"{DEVICE_ID}-bridge", clean_session=False)
    if BROKER_USER:
        client.username_pw_set(BROKER_USER, BROKER_PASS)
    client.will_set(T_STATUS, "offline", qos=1, retain=True)
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    pub_queue: queue.Queue = queue.Queue()
    threading.Thread(target=_iter_new_files, args=(pub_queue,), daemon=True).start()
    threading.Thread(target=_sender_thread, args=(client, pub_queue), daemon=True).start()

    stop_evt = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_evt.set())
    stop_evt.wait()

    log("shutting down")
    try:
        client.publish(T_STATUS, "offline", qos=1, retain=True).wait_for_publish(timeout=3)
    except Exception:
        pass
    client.disconnect()
    client.loop_stop()


if __name__ == "__main__":
    main()
