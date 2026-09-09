#!/usr/bin/env python3
"""Узкий контракт между агентом и демоном рейсов.

Агент не имеет доступа к каталогу flightwatch: границу ставит ядро
(InaccessiblePaths в юните openclaw). Всё общение идёт через эту службу,
и набор операций тут фиксированный - агент не может попросить больше,
чем здесь перечислено.

Служба НЕ трогает сам демон. Изменения она вносит в config.json, а демон
подхватывает их сам по mtime - так у правки есть ровно один путь внутрь,
и он тот же, каким пользуется человек.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE = Path("/opt/flightwatch")
# config.json в каталоге кода - симлинк на рантайм-копию: каталог кода
# намеренно закрыт на запись, а атомарная подмена требует права
# на КАТАЛОГ, а не на файл.
CONFIG = (BASE / "config.json").resolve()
LOG = BASE / "run" / "flightwatch.log"
TOKEN = Path("/etc/fwapi.token").read_text(encoding="utf-8").strip()
HOST, PORT = "127.0.0.1", 8787

SECRET_KEYS = ("pass", "token", "secret", "key")


def redact(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if any(s in k.lower() for s in SECRET_KEYS) and isinstance(v, str):
                out[k] = "<скрыто>"
            else:
                out[k] = redact(v)
        return out
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    return obj


def cfg() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def save_cfg(data: dict) -> None:
    # Атомарно: демон читает файл по mtime и не должен поймать половину.
    tmp = CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(tmp, 0o640)
    tmp.replace(CONFIG)


def db(sql: str, args=()):
    sys.path.insert(0, str(BASE))
    import pymysql
    conn = pymysql.connect(
        host="127.0.0.1", user="fwread",
        password=Path("/etc/fwapi.dbpass").read_text(encoding="utf-8").strip(),
        database="flightwatch", charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            # Пустой кортеж аргументов - это НЕ "без аргументов": pymysql всё
            # равно прогоняет строку через %-форматирование, и любой запрос с
            # процентом (DATE_FORMAT, LIKE) падает на "not enough arguments".
            cur.execute(sql, args) if args else cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()
    # datetime и Decimal в JSON не сериализуются
    return json.loads(json.dumps(rows, ensure_ascii=False, default=str))


# --- операции чтения --------------------------------------------------------

def op_status(_):
    c = cfg()
    state = {}
    p = BASE / "run" / "state.json"
    if p.exists():
        try:
            state = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    # Ключ состояния считает сам демон. Своя копия этой формулы разошлась
    # бы с ним при первой же правке, и статус молча стал бы всегда пустым.
    sys.path.insert(0, str(BASE))
    import importlib
    fw = importlib.import_module("flightwatch")
    out = []
    for f in c.get("flights", []):
        st = state.get(fw.slug_of(f), {})
        out.append({
            "flight": f.get("flight"), "airport": f.get("airport"),
            "date": f.get("date"), "direction": f.get("direction"),
            "title": f.get("title") or fw.title_of(f),
            "notify": f.get("notify", True),
            "status": st.get("status"), "delay_min": st.get("delay_min"),
            "done": bool(st.get("done_at")),
            "tracked": bool(st),
        })
    return {"notify_global": c.get("notify", True), "flights": out}


def op_config(_):
    # Файл конфига хранит только то, что отличается от умолчаний, поэтому
    # сырой файл вводит в заблуждение: спросив порог, агент получал null и
    # честно отвечал "не задан", хотя демон работает со значением 5.
    # Отдаём то, с чем демон РЕАЛЬНО работает, и помечаем, откуда значение.
    sys.path.insert(0, str(BASE))
    import importlib
    fw = importlib.import_module("flightwatch")
    file_cfg = cfg()
    merged = dict(fw.DEFAULTS)
    merged.update(file_cfg)
    return {"effective": redact(merged),
            "overridden_in_file": sorted(k for k in file_cfg if k != "flights"),
            "note": "effective - то, с чем демон работает сейчас; "
                    "ключи вне overridden_in_file взяты из умолчаний кода"}


def op_log(params):
    n = min(int(params.get("lines", 60)), 500)
    if not LOG.exists():
        return {"lines": [], "note": "лог-файла нет"}
    tail = subprocess.run(["tail", "-n", str(n), str(LOG)],
                          capture_output=True, text=True, timeout=15)
    return {"lines": tail.stdout.splitlines()}


def op_sources(params):
    """Здоровье источников - вместе с ответом на вопрос «а было что опрашивать».

    Пустой список опросов сам по себе НЕ значит поломку: когда все рейсы
    отлетали, демон нечего опрашивать и он молчит по делу. Агент, увидев
    голые нули, доложил «источник завис больше суток», хотя всё работало.
    Поэтому контекст отдаём здесь, а не надеемся, что модель догадается.
    """
    hours = min(int(params.get("hours", 24)), 24 * 30)
    rows = db("SELECT source, COUNT(*) AS polls, SUM(ok) AS ok, "
              "       MAX(polled_at) AS last "
              "FROM source_polls WHERE polled_at > NOW() - INTERVAL %s HOUR "
              "GROUP BY source ORDER BY polls DESC", (hours,))
    active = [f for f in op_status(None)["flights"] if f["tracked"] and not f["done"]]
    out = {"since_hours": hours, "rows": rows,
           "active_flights": len(active),
           "active_titles": [f["title"] for f in active]}
    if not active:
        out["note"] = ("Активных рейсов нет - все отлетели. Демон в этом "
                       "состоянии НИЧЕГО не опрашивает, поэтому отсутствие "
                       "свежих опросов здесь ожидаемо и поломкой не является. "
                       "Не докладывай это как отказ источника.")
    elif not rows:
        out["note"] = ("Рейсы в слежении есть, а опросов за окно нет - вот это "
                       "уже похоже на отказ: смотри fwctl log.")
    return out


def op_history(params):
    """Пунктуальность рейса, посчитанная ЗДЕСЬ, а не моделью.

    Раньше агент тянул строки через sql и считал сам - и ошибался
    предсказуемо: усреднял вместе утренний и дневной рейсы под одним
    номером, ставил среднее за месяц рядом со списком за неделю и молча
    пересказывал строки, где разница времён не сходится с задержкой.
    Арифметику надо делать кодом, а не напоминанием в промпте.
    """
    flight = (params.get("flight") or "").upper().replace(" ", "")
    if not flight:
        raise ValueError("нужен --flight")
    rows = db("SELECT flight_date, scheduled_local, actual_local, delay_min, "
              "       status_class, aircraft_reg "
              "FROM punctuality WHERE flight_no=%s ORDER BY flight_date DESC",
              (flight,))
    if not rows:
        return {"flight": flight, "n_rows": 0,
                "note": "в базе нет ни одной строки по этому номеру"}

    done = [r for r in rows if r["actual_local"] and r["delay_min"] is not None]

    def stats(items):
        d = sorted(x["delay_min"] for x in items)
        n = len(d)
        if not n:
            return None
        mid = n // 2
        median = d[mid] if n % 2 else round((d[mid - 1] + d[mid]) / 2, 1)
        return {"n": n,
                "avg": round(sum(d) / n, 1),
                "median": median,
                "best": d[0], "worst": d[-1],
                "ge30": sum(1 for x in d if x >= 30),
                "ge60": sum(1 for x in d if x >= 60)}

    # Один номер может летать по разному расписанию в разные дни. Усреднять
    # их вместе - значит отвечать про другой рейс.
    groups = {}
    for r in done:
        h = int(str(r["scheduled_local"])[11:13])
        groups.setdefault(h, []).append(r)

    # какой рейс оцениваем: заданная дата, иначе ближайший будущий
    target_date = params.get("date")
    target = None
    if target_date:
        target = next((r for r in rows if str(r["flight_date"]) == target_date), None)
    else:
        pend = [r for r in rows if not r["actual_local"]]
        target = pend[-1] if pend else None
    target_hour = int(str(target["scheduled_local"])[11:13]) if target else None

    group_out = []
    for h in sorted(groups):
        st = stats(groups[h])
        st["hour"] = h
        st["schedule"] = sorted({str(x["scheduled_local"])[11:16] for x in groups[h]})
        st["is_target_group"] = (h == target_hour)
        group_out.append(st)

    recent = []
    for r in done[:7]:
        sched, act = str(r["scheduled_local"]), str(r["actual_local"])
        calc = None
        try:
            from datetime import datetime as _dt
            f = "%Y-%m-%d %H:%M:%S"
            calc = round((_dt.strptime(act[:19], f) - _dt.strptime(sched[:19], f)).total_seconds() / 60)
        except Exception:
            pass
        recent.append({"date": str(r["flight_date"]), "scheduled": sched[11:16],
                       "actual": act[11:16], "delay_min": r["delay_min"],
                       "consistent": calc is None or abs(calc - r["delay_min"]) <= 1,
                       "reg": r["aircraft_reg"]})

    overall = stats(done)
    recent_st = stats(done[:7])
    tgt = next((g for g in group_out if g["is_target_group"]), None)
    bad = [r for r in recent if not r["consistent"]]

    note = ["Цифру для ответа бери из target_group, если он есть: это те же "
            "дни, что и оцениваемый рейс, а не все подряд."]
    if len(group_out) > 1:
        note.append(f"Номер {flight} летает по {len(group_out)} разным расписаниям - "
                    "общее среднее по ним смешано и для прогноза не годится.")
    if overall and recent_st and abs(overall["avg"] - recent_st["avg"]) >= 10:
        note.append(f"Среднее за всё окно ({overall['avg']}) и за последние 7 дней "
                    f"({recent_st['avg']}) сильно расходятся - называй оба и скажи, "
                    "что тянет вверх.")
    if overall and overall["avg"] - overall["median"] >= 10:
        note.append(f"Среднее ({overall['avg']}) заметно выше медианы "
                    f"({overall['median']}): его тянут выбросы. Полезнее счёт: "
                    f"{overall['ge30']} из {overall['n']} вылетов с задержкой 30+ мин.")
    if bad:
        note.append("В recent есть строки, где разница времён не сходится с "
                    "задержкой (consistent=false) - скажи об этом, не пересказывай молча.")

    return {"flight": flight,
            "target": ({"date": str(target["flight_date"]),
                        "scheduled": str(target["scheduled_local"])[11:16],
                        "status": target["status_class"]} if target else None),
            "n_rows": len(rows), "n_with_actual": len(done),
            "overall": overall, "last7": recent_st,
            "schedule_groups": group_out, "target_group": tgt,
            "recent": recent, "note": note}


SELECT_ONLY = re.compile(r"^\s*(select|with)\b", re.I)


def op_sql(params):
    q = (params.get("query") or "").strip().rstrip(";")
    if not SELECT_ONLY.match(q):
        raise ValueError("разрешены только SELECT и WITH")
    if ";" in q:
        raise ValueError("несколько запросов за раз нельзя")
    if " limit " not in q.lower():
        q += " LIMIT 200"
    return {"query": q, "rows": db(q)}


def op_board(params):
    """Живой опрос табло - тем же кодом, каким это делает демон."""
    sys.path.insert(0, str(BASE))
    import importlib
    fw = importlib.import_module("flightwatch")
    c = fw.load_config()
    ap = (params.get("airport") or "").upper()
    leg = (params.get("leg") or "DEP").upper()
    date = params.get("date") or time.strftime("%Y-%m-%d")
    src = (params.get("source") or "").lower()
    fns = {
        "montenegro": lambda: fw.fetch_montenegro(ap, {date}, {leg}, c),
        "beg":        lambda: fw.fetch_belgrade(ap, {date}, {leg}, c),
        "tav":        lambda: fw.fetch_tav(ap, {date}, {leg}, c),
        "zvartnots":  lambda: fw.fetch_zvartnots(ap, {leg}, c),
    }
    if src not in fns:
        raise ValueError(f"источник должен быть одним из {sorted(fns)}")
    res = fns[src]()
    return {"airport": ap, "leg": leg, "date": date, "source": src,
            "result": json.loads(json.dumps(res, ensure_ascii=False, default=str))}


# --- операции изменения -----------------------------------------------------

REQUIRED = ("flight", "airport", "date", "direction")


def op_add_flight(params):
    spec = params.get("flight")
    if not isinstance(spec, dict):
        raise ValueError("нужен объект flight")
    missing = [k for k in REQUIRED if not spec.get(k)]
    if missing:
        raise ValueError(f"не хватает полей: {', '.join(missing)}")
    if spec["direction"] not in ("departure", "arrival"):
        raise ValueError("direction должен быть departure или arrival")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(spec["date"])):
        raise ValueError("date в формате ГГГГ-ММ-ДД")
    c = cfg()
    for f in c.get("flights", []):
        if (f.get("flight"), f.get("airport"), f.get("date"), f.get("direction")) == \
           (spec["flight"], spec["airport"], spec["date"], spec["direction"]):
            raise ValueError("такой рейс уже есть")
    c.setdefault("flights", []).append(spec)
    save_cfg(c)
    return {"added": spec, "flights_total": len(c["flights"]),
            "note": "демон перечитает конфиг сам, цикл 90 секунд"}


def op_remove_flight(params):
    key = params.get("flight")
    date = params.get("date")
    if not key:
        raise ValueError("нужен номер рейса")
    c = cfg()
    before = len(c.get("flights", []))
    c["flights"] = [f for f in c.get("flights", [])
                    if not (f.get("flight") == key and (not date or f.get("date") == date))]
    if len(c["flights"]) == before:
        raise ValueError("подходящий рейс не найден")
    save_cfg(c)
    return {"removed": before - len(c["flights"]), "flights_total": len(c["flights"])}


# Менять можно только то, что перечислено. Остальное - через человека.
SETTABLE = {
    "notify": bool,
    "tg_shift_min": int,
    "sms_shift_min": int,
    "briefing_within_h": int,
    "airport_alert_within_h": int,
    "sms_ttl_s": int,
}


def op_set(params):
    key, value = params.get("key"), params.get("value")
    if key not in SETTABLE:
        raise ValueError(f"менять можно только: {', '.join(sorted(SETTABLE))}")
    caster = SETTABLE[key]
    value = (str(value).lower() in ("1", "true", "да", "yes")) if caster is bool else caster(value)
    c = cfg()
    old = c.get(key)
    c[key] = value
    save_cfg(c)
    return {"key": key, "was": old, "now": value}


READ_OPS = {"status": op_status, "config": op_config, "log": op_log,
            "sources": op_sources, "sql": op_sql, "board": op_board,
            "history": op_history}
WRITE_OPS = {"add-flight": op_add_flight, "remove-flight": op_remove_flight,
             "set": op_set}
OPS = {**READ_OPS, **WRITE_OPS}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.address_string(), fmt % args))

    def _reply(self, code, body: dict):
        raw = json.dumps(body, ensure_ascii=False, indent=1).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._reply(401, {"error": "нет или неверный токен"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            req = json.loads(self.rfile.read(n) or b"{}")
        except Exception as exc:
            return self._reply(400, {"error": f"тело не разобрать: {exc}"})
        op = req.get("op")
        if op not in OPS:
            return self._reply(400, {"error": f"неизвестная операция {op!r}",
                                     "known": sorted(OPS)})
        kind = "чтение" if op in READ_OPS else "ИЗМЕНЕНИЕ"
        sys.stderr.write(f"[{kind}] {op} {json.dumps(req.get('params', {}), ensure_ascii=False)[:300]}\n")
        try:
            return self._reply(200, {"ok": True, "op": op,
                                     "result": OPS[op](req.get("params") or {})})
        except Exception as exc:
            sys.stderr.write(f"[отказ] {op}: {exc}\n")
            return self._reply(400, {"ok": False, "op": op, "error": str(exc)})


if __name__ == "__main__":
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    sys.stderr.write(f"fwapi слушает {HOST}:{PORT}, операций: "
                     f"{len(READ_OPS)} на чтение, {len(WRITE_OPS)} на изменение\n")
    srv.serve_forever()
