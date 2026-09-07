#!/usr/bin/env python3
"""
flightwatch - слежение за произвольным списком рейсов через Flightradar24
с оповещением по СМС и автоматическими сенсорами в Home Assistant.

Зачем отдельный демон, а не интеграция flightradar24 в HA:
FR24 отдаёт табло аэропорта постранично. Интеграция всегда запрашивает page=1
(текущие и будущие рейсы) и режет выдачу до 50 записей. Рейс, задержанный
с утра, остаётся в своём исходном утреннем слоте и уезжает на page=-1,
которую интеграция не запрашивает никогда. Здесь обходятся обе страницы.

СМС уходит публикацией в MQTT-топик openstick-моста на том же брокере, что
использует HA. Сам Home Assistant в цепочке оповещения не участвует.

Управление списком рейсов:
    flightwatch.py --list
    flightwatch.py --add XX1234 2026-01-01 AAA
    flightwatch.py --add SU1234 2026-08-25 SVO --arrival
    flightwatch.py --remove XX1234 2026-01-01 AAA
Демон перечитывает config.json на лету, перезапуск службы не нужен.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import logging.handlers
import os
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import signal
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

warnings.filterwarnings("ignore")

import paho.mqtt.client as mqtt
from FlightRadarAPI import FlightRadar24API
from FlightRadarAPI.entities.flight import Flight

BASE = Path(__file__).resolve().parent
CONFIG_PATH = BASE / "config.json"
# Рантайм живёт отдельно от кода: каталог с кодом демону закрыт на запись,
# чтобы он не мог переписать сам себя, а писать состояние ему всё равно надо.
# Плюс атомарная подмена файла (os.replace) требует права на КАТАЛОГ, а не
# на файл, поэтому положить state.json рядом с кодом нельзя в принципе.
STATE_PATH = Path(os.environ.get("FLIGHTWATCH_STATE") or (BASE / "run" / "state.json"))

TOPIC_PREFIX = "flightwatch"
AVAILABILITY_TOPIC = "flightwatch/status"


def set_topic_prefix(cfg: dict) -> None:
    """Теневой экземпляр обязан жить на своих топиках и со своим client_id.
    Два демона с одинаковым client_id на одном брокере вышибают друг друга
    по кругу: брокер рвёт старую сессию при подключении новой с тем же id.
    Меняется только на старте, на лету перечитывать нечего - сенсоры уже
    объявлены на прежних топиках."""
    global TOPIC_PREFIX, AVAILABILITY_TOPIC
    TOPIC_PREFIX = cfg.get("mqtt_prefix", "flightwatch")
    AVAILABILITY_TOPIC = f"{TOPIC_PREFIX}/status"
    if TOPIC_PREFIX != "flightwatch":
        log.warning("теневой режим: топики %s/#, client_id %s, discovery %s",
                    TOPIC_PREFIX, cfg.get("mqtt_client_id", "flightwatch"),
                    cfg.get("discovery_prefix"))

log = logging.getLogger("flightwatch")


def setup_logging(cfg: dict) -> None:
    """stdout построчно + свой файл с ротацией.

    По умолчанию Python буферизует stdout блоками по 8 КБ, когда это не
    терминал, а journald - именно поэтому часы работы демона не попадали
    в журнал вовремя. Плюс journald на этой машине вычищает старое,
    поэтому держим собственный файл.
    """
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    sys.stdout.reconfigure(line_buffering=True)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    handlers = [stream]
    try:
        rotating = logging.handlers.RotatingFileHandler(
            cfg["log_file"], maxBytes=cfg["log_max_bytes"],
            backupCount=cfg["log_backups"], encoding="utf-8")
        rotating.setFormatter(fmt)
        handlers.append(rotating)
    except OSError as exc:
        print(f"не смог открыть файл лога: {exc}", file=sys.stderr)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = handlers
    # библиотека FR24 сыплет безобидными предупреждениями о Content-Encoding
    # тысячами и прячет в них настоящие сигналы
    for noisy in ("FlightRadar24", "FlightRadarAPI"):
        logging.getLogger(noisy).setLevel(logging.ERROR)

DEFAULTS = {
    "pages": [-1, 1],              # -1 = задержанные с утра, 1 = текущие и будущие
    # У крупных аэропортов 100 записей на страницу покрывают considerably
    # меньше суток: Шереметьево на двух страницах даёт окно всего ~10 часов.
    # Для них берём больше страниц, для мелких это лишний трафик.
    "pages_by_airport": {"SVO": [-2, -1, 1, 2, 3], "DME": [-2, -1, 1, 2, 3],
                         "VKO": [-1, 1, 2], "IST": [-2, -1, 1, 2, 3]},
    # Публичный шлюз TAV: где он знает аэропорт, данные заметно лучше FR24 -
    # готовый статус по-русски (включая посадку и закрытие гейта), гейт,
    # стойка регистрации, багажная лента и явное окно по датам вместо
    # страничной возни. Эндпоинт недокументированный, поэтому при любой
    # осечке молча откатываемся на FR24.
    "tav_url": "https://publicgateway.trevaworld.com/api/flightmanager/flights",
    "tav_airports": ["ALA", "TBS", "ESB", "ADB", "BUS", "SKP", "BJV",
                     "GZP", "NBE", "MIR"],
    "tav_timeout": 25,
    # Звартноц (и Ширак) держат собственный открытый JSON без ключа.
    # Для EVN это единственный источник со статусом и гейтом: шлюз TAV
    # армянские аэропорты не отдаёт, а FR24 не знает ни гейта, ни посадки.
    "zv_url": "https://zvartnots.aero/api/{controller}/GetGridData/RU",
    "zv_airports": {"EVN": "Flights", "LWN": "ShirakFlights"},
    "zv_timeout": 25,
    # Черногория (Тиват/Подгорица): плоский JSON, но ТОЛЬКО текущие сутки.
    "mne_url": "https://montenegroairports.com/aerodromixs/cache-flights.php?airport={code}",
    "mne_airports": {"TIV": "tv", "TGD": "pg"},
    # Белград: статический XML на 1.1 МБ, зато горизонт -2…+2 суток и
    # честный conditional GET, так что почти всегда прилетает 304.
    "beg_url": "https://beg.aero/sites/default/files/data/redLetenja.xml",
    "beg_airports": ["BEG"],
    "http_timeout": 60,
    "provider_cooldown": 1800,      # потолок отстоя
    "provider_backoff_start": 120,  # первый отстой короткий: один таймаут - не приговор
    "missing_confirm_cycles": 3,
    # Про исчезновение с табло сообщаем ТОЛЬКО когда вылет уже близко.
    # Рейс за сутки до вылета спокойно уходит за край суточного окна табло -
    # это нормальная работа источника, а не событие.
    "vanish_window_hours": 6,
    # Защита от повторов: одинаковый текст не уходит дважды за это время.
    "resend_block_min": 360,

    # --- детектор сбоя аэропорта -------------------------------------------
    # Считаем не по нашим рейсам (их пять, выборка мизерная), а по ВСЕЙ доске
    # в операционном окне. Пороги входа и выхода РАЗНЫЕ - иначе на границе
    # начнётся дребезг и то самое тявканье каждые полчаса.
    "stress_window_back_h": 2,      # смотрим рейсы от -2 ч
    "stress_window_fwd_h": 4,       # ... до +4 ч от текущего момента
    "stress_min_flights": 8,        # меньше - выборка не показательна
    # Порог верен для среднего и крупного аэропорта. В Ставрополе 15 вылетов
    # за трое суток и максимум 4 в окне - там детектор не сработает НИКОГДА.
    # Здесь можно задать свой порог по аэропорту, осознавая, что на выборке
    # в три рейса один борт задаёт всю среднюю.
    "stress_min_flights_by_airport": {},
    "stress_enter_avg": 45,         # вход в "напряжённо"
    "outage_enter_avg": 90,         # вход в "сбой"
    "outage_enter_cancelled": 4,    # ... либо столько отмен в окне
    "stress_exit_avg": 30,          # выход в "норму" - порог НИЖЕ входа
    "stress_confirm": 2,            # подтверждений подряд на вход
    "stress_clear": 3,              # ... и на выход, выход строже
    "stress_update_min": 90,        # повторное сообщение при ухудшении не чаще
    "stress_update_step": 45,       # ... и только если индекс вырос настолько
    "stress_watch_within_h": 48,    # следим за аэропортом, если рейс в этом окне

    # --- плановые сводки ---------------------------------------------------
    # Молчание недоказательно: если ничего не менялось, человек не знает,
    # работает ли система вообще. Поэтому в заданное местное время приходит
    # сводка по всем рейсам ближайших суток - даже когда всё спокойно.
    "briefings": [],
    "briefing_within_h": 24,        # какие рейсы включать в сводку    # сколько подряд не увидеть, прежде чем кричать "пропал"
    # Каналы разной цены: в Telegram можно и про 5 минут, СМС - только крупное.
    # Но у аэропорта любая мелочь важна, поэтому близко к вылету планка падает.
    "tg_shift_min": 5,              # порог для Telegram
    "sms_shift_min": 30,            # порог для СМС
    "urgent_window_min": 120,       # ближе этого к вылету СМС уходит и на мелкий сдвиг
    # Эти классы событий уходят по СМС ВСЕГДА: и по чужим рейсам тоже.
    "sms_always_classes": ["cancelled", "connect_risk", "watchdog"],
    "delay_notice_gap_min": 20,     # окно тишины - только для СМС
    # ЗАПАСНАЯ таблица смещений: рабочее значение считает ap_offset/local_dt по
    # базе часовых поясов на момент события. Здесь остались летние числа, они
    # верны только до 25.10.2026 - при старте демон сверяет их с базой и
    # ругается в лог. Держим на случай аэропорта, которого нет в AIRPORT_TZ.
    # Ротация борта: самый ранний сигнал из всех. Табло признаёт задержку
    # за час, а борт, ещё не вылетевший в нашу сторону, делает вылет по
    # расписанию невозможным за много часов до этого.
    "rotation_window_hours": 30,
    "rotation_check_every": 300,
    "rotation_min_risk": 20,
    # Насколько близко к нашему вылету должен прилететь борт, чтобы его
    # положение вообще что-то значило. Если он прилетает за сутки, между
    # ним и нами ещё целый круг рейсов, и граница "не раньше чем" бессмысленна.
    "rotation_relevant_hours": 12,
    # Стыковка: считаем не "рейс опоздал на N", а "до закрытия выхода
    # на следующий рейс осталось M минут" - решение принимается по этому.
    "connect_tight_min": 60,        # ниже этого запас считается впритык
    "connect_window_hours": 36,
    # Прогноз опозданий. Веса подобраны грубо и БУДУТ калиброваться по факту -
    # именно поэтому каждый прогноз пишется в базу вместе с горизонтом.
    "predict_window_hours": 36,
    "predict_every": 1800,          # прогноз раз в полчаса, чтобы вышла кривая
    "predict_carryover_decay": 0.5, # опоздание затухает примерно вдвое за сутки
    "predict_w_history": 0.5,
    "predict_w_carryover": 0.3,
    "predict_w_bank": 0.2,
    # Признаки из опубликованных моделей ротационной реактивной задержки
    # (FlightSense/BTS: топ-4 признака по важности - все про цепочку ротации,
    # они дают 97% прироста точности; погода почти ничего не добавляет).
    "predict_w_tail": 0.4,          # вес накопленной за сутки задержки борта
    "tight_turnaround_min": 45,     # порог "стоянка слишком короткая"
    # Теневое наблюдение: номера, по которым копим пунктуальность БЕЗ оповещений.
    # Это вся цепочка борта - без неё не видно, откуда приходит опоздание.
    "watch_numbers": [],
    "airport_utc_offset_min": {
        "TIV": 120, "TGD": 120, "BEG": 120, "LCA": 180,
        "EVN": 240, "LWN": 240, "TBS": 240, "ALA": 300,
        # российские: без них to_utc возвращал None и детектор молча пустовал
        "SVO": 180, "DME": 180, "VKO": 180, "STW": 180, "KRR": 180,
        "MRV": 180, "AER": 180, "LED": 180, "SVX": 300, "KZN": 180,
        "IST": 180, "SAW": 180, "RMO": 180, "DXB": 240, "TLV": 180},
    "db_host": "127.0.0.1",
    "db_user": "flightwatch",
    "db_name": "flightwatch",
    "db_pass_file": str(BASE / ".mysql_pass"),
    "db_enabled": True,
    "poll_seconds": 90,
    "delay_threshold_min": 5,      # изменения оценки мельче порога игнорируем
    "retire_after_hours": 6,       # через сколько после факта вылета бросить рейс
    "stale_after_days": 3,         # рейсы старше этого не опрашиваем вовсе
    "lookahead_days": 1,           # ближе этого - всегда быстрый темп
    "slow_poll_seconds": 1800,     # дальше - редкая разведка, пока рейс не появится
    "sms_fail_notice_h": 6,        # как часто напоминать, что канал СМС лежит
    # Мост подключается к брокеру с clean_session=True: пока он перезагружается,
    # команда на отправку не копится, а пропадает. Отсутствие подтверждения -
    # такой же отказ, как явная ошибка, и молчать о нём нельзя.
    "sms_ack_timeout_s": 240,
    "landing_assume_min": 30,      # столько ждём факта после расчётного прилёта
    "landing_stale_h": 6,          # посадку старше этого не объявляем вовсе
    "landing_wait_hours": 14,      # сколько держим вылетевший рейс ради факта посадки
    "airport_alert_within_h": 18,  # ближе этого к рейсу загрузка аэропорта важна
    "sms_to": "",                    # +7XXXXXXXXXX, куда слать СМС
    # Топики SMS-шлюза. По умолчанию - заглушка из tools/mock_sms_gw.py:
    # она реализует тот же контракт и позволяет проверить весь канал СМС,
    # не имея ни модема, ни симки. Настоящий шлюз просто меняет id.
    "sms_topic": "smsgw/mock/sms/send",
    "mqtt_host": "127.0.0.1",
    "mqtt_port": 1883,
    "mqtt_user": "flightwatch",
    "mqtt_pass_file": str(BASE / ".mqtt_pass"),
    "discovery_prefix": "homeassistant",
    "mqtt_prefix": "flightwatch",      # теневому экземпляру дать свой
    "mqtt_client_id": "flightwatch",   # и свой client_id, иначе дуэль сессий
    "notify": True,
    # СМС до телефона в роуминге может идти часами: SMSC копит и досылает
    # по своему графику. Telegram уходит по данным и приходит сразу, поэтому
    # он первый в списке, а СМС остаётся запасным каналом.
    "channels": ["telegram", "sms"],
    "telegram_chat_id": 0,           # id вашего чата с ботом
    "telegram_token_file": str(BASE / ".tg_token"),
    "ack_topic": "smsgw/+/sms/sent",
    "modem_hint": "шлюз СМС",        # как назвать шлюз в тексте тревоги
    "sms_ttl_s": 1800,   # позже этого СМС уже не новость, мост её выбросит
    "log_file": str(BASE / "flightwatch.log"),
    "log_max_bytes": 2000000,
    "log_backups": 3,
    "flights": [],
}


# ---------------------------------------------------------------------------
# конфигурация и состояние
# ---------------------------------------------------------------------------
def load_config() -> dict:
    cfg = dict(DEFAULTS)
    if CONFIG_PATH.exists():
        cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))

    # совместимость со старым однорейсовым форматом
    if not cfg["flights"] and cfg.get("flight"):
        cfg["flights"] = [{
            "flight": cfg["flight"],
            "date": cfg.get("date"),
            "airport": cfg.get("airport"),
            "direction": cfg.get("direction", "departure"),
        }]

    normalised = []
    for item in cfg["flights"]:
        if not item.get("flight") or not item.get("date") or not item.get("airport"):
            log.warning("пропускаю неполную запись рейса: %s", item)
            continue
        normalised.append({
            "flight": item["flight"].upper().replace(" ", ""),
            "date": item["date"],
            "airport": item["airport"].upper(),
            "direction": item.get("direction", "departure"),
            "to": (item.get("to") or "").upper().strip() or None,
            # Основной номер получает всё и всегда - подменить его нельзя.
            # Рейсовые контакты только ДОБАВЛЯЮТСЯ к нему.
            "extra_sms_to": [str(x).strip() for x in
                             (item.get("extra_sms_to") or
                              ([item["sms_to"]] if item.get("sms_to") else []))
                             if str(x).strip()],
            "aircraft_reg": (item.get("aircraft_reg") or "").upper().strip() or None,
            "turnaround_min": int(item.get("turnaround_min", 60)),
            "rotation_inbound_no": (item.get("rotation_inbound_no") or "").upper() or None,
            "rotation_chain": [x.upper() for x in (item.get("rotation_chain") or [])],
            # Чей это рейс: свой или чужой, за которым следим ради встречи.
            # Влияет только на формулировки - решения по чужому рейсу
            # принимает не пользователь, будить его "пора выезжать" нельзя.
            "owner": (item.get("owner") or "self"),
            "label": (item.get("label") or "").strip() or None,
            # Человекочитаемое имя рейса. Номера рейсов помнить никто не обязан,
            # поэтому в тексте оповещения на первом месте стоит оно.
            "title": (item.get("title") or "").strip() or None,
            "inbound_slug": item.get("inbound_slug") or None,
            "mct_min": int(item.get("mct_min", 60)),
            "gate_close_min": int(item.get("gate_close_min", 20)),
            "notify": item.get("notify", True),
        })
    cfg["flights"] = normalised
    return cfg


def save_config(cfg: dict) -> None:
    keep = {k: v for k, v in cfg.items() if k in DEFAULTS and DEFAULTS[k] != v or k == "flights"}
    CONFIG_PATH.write_text(json.dumps(keep, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")


def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            log.warning("state.json повреждён, начинаю с чистого")
    return {}


def save_state(state: dict) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def slug_of(spec: dict) -> str:
    raw = f"{spec['flight']}_{spec['airport']}_{spec['date']}_{spec['direction'][:3]}"
    return re.sub(r"[^a-z0-9_]+", "_", raw.lower())


def title_of(spec: dict) -> str:
    arrow = "вылет" if spec["direction"] == "departure" else "прилёт"
    day = datetime.strptime(spec["date"], "%Y-%m-%d").strftime("%d.%m")
    return f"{spec['flight']} {spec['airport']} {arrow} {day}"


# ---------------------------------------------------------------------------
# разбор табло FR24
# ---------------------------------------------------------------------------
def _get(d, path, default=None):
    for key in path:
        if not isinstance(d, dict):
            return default
        d = d.get(key)
        if d is None:
            return default
    return d


# ---------------------------------------------------------------------------
# Нормализация статуса: источники говорят на своих языках, а решения нужно
# принимать по классу, а не по строке. Всё, что не опознано, - UNPARSEABLE,
# и в сравнениях не участвует (Звартноц умеет подсунуть дату вместо статуса).
# ---------------------------------------------------------------------------
# Коды табло совпадают ЦЕЛИКОМ со строкой статуса. Держим их отдельно от
# подстрок: needle "dep" внутри "Estimated dep 10:30" уже приводил к ложному
# выводу "рейс вылетел" - проверено на живых данных 25.08.
STATUS_CODES = {
    "": None, "-": None, "--": None,
    "ONT": "SCHEDULED", "EXP": "SCHEDULED", "NGT": "SCHEDULED",
    "DLY": "DELAYED", "CAN": "CANCELLED",
    "NBD": "BOARDING",          # now boarding
    "GTG": "GATE_CLOSED", "GTC": "GATE_CLOSED",
    "TAX": "GATE_CLOSED",       # руление: от гейта отъехал, но ещё не взлетел
    "DEP": "DEPARTED", "LAN": "LANDED",
}

STATUS_SUBSTR = (
    ("CANCELLED",   ("отмен", "cancel", "otkaz")),
    ("LANDED",      ("приземл", "прилетел", "прибыл", "landed", "arrived", "sletio")),
    ("DEPARTED",    ("вылетел", "отправляется", "departed", "poletio", "мекнел")),
    ("GATE_CLOSED", ("гейт закрыт", "gate closed", "go to gate",
                     "пройдите к посадочному", "пройдите к гейту",
                     "заканчивается посадка", "final call")),
    ("BOARDING",    ("посадка", "boarding")),
    ("CHECKIN",     ("регистрац", "check-in", "checkin")),
    ("DELAYED",     ("задерж", "опоздан", "delay", "delayed")),
    ("SCHEDULED",   ("вовремя", "по расписанию", "on time", "scheduled",
                     "estimated", "ожидается", "expected", "раньше", "earlier")),
)

CLASS_LABEL = {
    "SCHEDULED": "по расписанию", "DELAYED": "задержан", "CHECKIN": "регистрация",
    "BOARDING": "идёт посадка", "GATE_CLOSED": "гейт закрыт",
    "DEPARTED": "вылетел", "LANDED": "приземлился", "CANCELLED": "ОТМЕНЁН",
}


def classify_status(raw: str | None, generic: str | None = None) -> str | None:
    for value in (raw, generic):
        if value is None:
            continue
        low = str(value).strip().lower()
        upper = str(value).strip().upper()
        if upper in STATUS_CODES:
            return STATUS_CODES[upper]
        if not low:
            return None
        if re.match(r"^\s*\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\s*$", low):
            return "UNPARSEABLE"
        for cls, needles in STATUS_SUBSTR:
            if any(n in low for n in needles):
                return cls
    return None if not (raw or generic) else "UNPARSEABLE"


def status_phrase(raw: str | None, cls: str | None) -> str | None:
    """Что писать человеку. Сырые коды табло (NBD, GTC, TAX) в СМС не уходят."""
    if not cls or cls == "UNPARSEABLE":
        return None
    text = (raw or "").strip()
    if not text or (len(text) <= 4 and text.upper() == text):
        return CLASS_LABEL.get(cls, cls)
    return text


# ---------------------------------------------------------------------------
# Хранилище. Любая ошибка БД гасится: мониторинг важнее журнала, падать
# из-за недоступной базы недопустимо.
# ---------------------------------------------------------------------------
class Store:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._conn_rc = None          # ответ брокера, ставит колбэк
        self.conn = None
        self.flight_ids: dict[str, int] = {}
        self.enabled = cfg.get("db_enabled", True) and Path(cfg["db_pass_file"]).exists()
        if not self.enabled:
            log.warning("БД выключена: нет файла пароля или db_enabled=false")

    def _cursor(self):
        import pymysql
        if self.conn is None:
            self.conn = pymysql.connect(
                host=self.cfg["db_host"], user=self.cfg["db_user"],
                password=Path(self.cfg["db_pass_file"]).read_text(encoding="utf-8").strip(),
                database=self.cfg["db_name"], charset="utf8mb4", autocommit=True,
                connect_timeout=10, read_timeout=20, write_timeout=20)
        else:
            self.conn.ping(reconnect=True)
        return self.conn.cursor()

    def run(self, sql: str, args=(), fetch: bool = False):
        if not self.enabled:
            return None
        try:
            with self._cursor() as cur:
                cur.execute(sql, args)
                return cur.fetchall() if fetch else cur.lastrowid
        except Exception as exc:
            log.warning("БД недоступна (%s), продолжаю без неё", exc)
            self.conn = None
            return None

    # ---- рейсы ----
    def sync_flights(self, cfg: dict) -> None:
        for spec in cfg["flights"]:
            slug = slug_of(spec)
            self.run(
                """INSERT INTO flights (slug, flight_no, flight_date, airport,
                       peer_airport, direction, sms_to, notify)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE peer_airport=COALESCE(VALUES(peer_airport), peer_airport),
                       sms_to=VALUES(sms_to), notify=VALUES(notify)""",
                (slug, spec["flight"], spec["date"], spec["airport"],
                 spec.get("to"), spec["direction"], spec.get("sms_to"),
                 1 if spec.get("notify", True) else 0))
        rows = self.run("SELECT slug, id FROM flights", fetch=True) or ()
        self.flight_ids = {r[0]: r[1] for r in rows}

    def fid(self, spec) -> int | None:
        return self.flight_ids.get(slug_of(spec))

    # ---- опросы источников ----
    def poll(self, source: str, airport: str, ok: bool, rows=None, ms=None,
             error=None, not_modified=False) -> None:
        self.run(
            """INSERT INTO source_polls (source, airport, ok, not_modified,
                   rows_returned, duration_ms, error, polled_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(3))""",
            (source[:20], airport[:3], 1 if ok else 0, 1 if not_modified else 0,
             rows, ms, (str(error)[:255] if error else None)))

    # ---- наблюдения: новая строка ТОЛЬКО при изменении ----
    def observe(self, flight_id: int, row: dict, tz: int, role: str, leg: str) -> None:
        if not flight_id:
            return
        def local(ts):
            if not ts:
                return None
            return datetime.fromtimestamp(int(ts) + tz, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        source = (row.get("source") or "")[:20]
        jurisdiction = "aggregator" if source.startswith("fr24") else role
        fields = (row.get("status"), classify_status(row.get("status"), row.get("generic")),
                  local(row.get("scheduled")), local(row.get("estimated")),
                  local(row.get("real")), row.get("gate"), row.get("checkin"),
                  row.get("carousel"), row.get("other_iata"))
        digest = hashlib.md5("|".join(str(f) for f in fields).encode("utf-8")).hexdigest()

        prev = self.run(
            """SELECT id, payload_hash FROM observations
               WHERE flight_id=%s AND source=%s AND leg=%s
               ORDER BY first_seen_at DESC LIMIT 1""",
            (flight_id, source, leg), fetch=True)
        if prev and prev[0][1] == digest:
            self.run("""UPDATE observations SET last_seen_at=NOW(3), seen_count=seen_count+1
                        WHERE id=%s""", (prev[0][0],))
            return
        self.run(
            """INSERT INTO observations (flight_id, source, jurisdiction, leg,
                   status_raw, status_class, scheduled_local, estimated_local,
                   actual_local, tz_offset_min, gate, checkin_desks, carousel,
                   peer_iata, payload_hash, first_seen_at, last_seen_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(3),NOW(3))""",
            (flight_id, source, jurisdiction, leg, (row.get("status") or None),
             fields[1], fields[2], fields[3], fields[4], tz // 60,
             (row.get("gate") or None), (row.get("checkin") or None),
             (row.get("carousel") or None), (row.get("other_iata") or None), digest))

    # ---- сводное состояние ----
    def state(self, flight_id: int, data: dict) -> None:
        if not flight_id:
            return
        cols = ["status_class", "scheduled_local", "estimated_local", "actual_local",
                "tz_offset_min", "delay_min", "gate", "checkin_desks", "conflict",
                "degraded_sources"]
        vals = [data.get(c) for c in cols]
        set_part = ", ".join(f"{c}=VALUES({c})" for c in cols)
        self.run(
            f"""INSERT INTO flight_state (flight_id, {", ".join(cols)}, updated_at)
                VALUES ({", ".join(["%s"] * (len(cols) + 1))}, NOW(3))
                ON DUPLICATE KEY UPDATE {set_part}, updated_at=NOW(3)""",
            (flight_id, *vals))

    # ---- уведомления ----
    def note(self, flight_id, event_class: str, severity: str, body: str,
             req_id: str | None) -> None:
        day = datetime.now().strftime("%Y%m%d")
        key = f"{day}|{flight_id}|{event_class}|" + \
              hashlib.md5(body.encode("utf-8")).hexdigest()
        self.run(
            """INSERT INTO notifications (flight_id, event_class, severity, dedup_key,
                   body, sms_req_id, sms_sent_at, tg_sent_at, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,NOW(3),NOW(3),NOW(3))
               ON DUPLICATE KEY UPDATE tg_repeats=tg_repeats+1""",
            (flight_id, event_class[:32], severity, key[:140], body, req_id))

    def punctuality(self, flight_no: str, airport: str, row: dict, tz: int) -> None:
        """Ежедневная пунктуальность рейса.

        Историю рейсов FR24 закрыл (flight/list.json отдаёт 404), поэтому
        копим свою: каждый день записываем экземпляр рейса, попавший на табло.
        Через неделю-другую видно, регулярно ли рейс опаздывает.
        """
        sched, actual = row.get("scheduled"), row.get("real")
        if not sched:
            return
        def local(ts):
            return None if not ts else datetime.fromtimestamp(
                int(ts) + tz, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        day = datetime.fromtimestamp(int(sched) + tz, tz=timezone.utc).strftime("%Y-%m-%d")
        delay = round((int(actual) - int(sched)) / 60) if actual else None
        self.run(
            """INSERT INTO punctuality (flight_no, airport, flight_date,
                   scheduled_local, actual_local, delay_min, status_class,
                   aircraft_reg, source, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(3))
               ON DUPLICATE KEY UPDATE
                   actual_local=COALESCE(VALUES(actual_local), actual_local),
                   delay_min=COALESCE(VALUES(delay_min), delay_min),
                   status_class=VALUES(status_class),
                   aircraft_reg=COALESCE(VALUES(aircraft_reg), aircraft_reg),
                   updated_at=NOW(3)""",
            (flight_no[:10], airport[:3], day, local(sched), local(actual), delay,
             classify_status(row.get("status"), row.get("generic")),
             (row.get("reg") or None), (row.get("source") or "")[:20]))
        if delay is not None:
            self.settle(flight_no, day, delay)

    # ---- волны: агрегат по часу, опережающий индикатор ----
    def waves(self, airport: str, board: dict, legs) -> None:
        from collections import defaultdict
        tz = board.get("tz", 0)
        buckets = defaultdict(list)
        for leg in legs:
            for r in board.get(leg) or []:
                if not r.get("scheduled"):
                    continue
                dt = datetime.fromtimestamp(int(r["scheduled"]) + tz, tz=timezone.utc)
                delay = round(((r.get("real") or r.get("estimated") or r["scheduled"])
                               - r["scheduled"]) / 60)
                carrier = (r.get("number") or "")[:2].upper()
                for who in ("", carrier):
                    buckets[(dt.strftime("%Y-%m-%d"), dt.hour, leg, who)].append(delay)
        for (day, hour, leg, who), vals in buckets.items():
            self.run(
                """INSERT INTO wave_stats (airport, stat_date, hour, leg, airline,
                       flights, avg_delay, max_delay, on_time, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(3))
                   ON DUPLICATE KEY UPDATE flights=VALUES(flights),
                       avg_delay=VALUES(avg_delay), max_delay=VALUES(max_delay),
                       on_time=VALUES(on_time), updated_at=NOW(3)""",
                (airport[:3], day, hour, leg, who[:3], len(vals),
                 round(sum(vals) / len(vals)), max(vals),
                 sum(1 for v in vals if v <= 15)))

    def rotation_log(self, reg: str, pos: dict) -> None:
        iso = lambda u: None if not u else datetime.fromtimestamp(
            int(u), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.run("""INSERT INTO rotation_log (aircraft_reg, seen_at, flight_no, origin,
                        destination, eta_utc, landed_utc, status)
                    VALUES (%s,NOW(3),%s,%s,%s,%s,%s,%s)""",
                 (reg[:12], (pos.get("flight_no") or "")[:10], pos.get("origin"),
                  pos.get("destination"), iso(pos.get("eta_utc")),
                  iso(pos.get("landed_utc")), (pos.get("status") or "")[:40]))

    # ---- прогноз ----
    def history_delays(self, flight_no: str, airport: str, days: int = 7):
        rows = self.run(
            """SELECT flight_date, delay_min FROM punctuality
               WHERE flight_no=%s AND airport=%s AND delay_min IS NOT NULL
               ORDER BY flight_date DESC LIMIT %s""",
            (flight_no, airport, days), fetch=True) or ()
        return [(r[0], r[1]) for r in rows]

    def leading_signals(self, airport: str, day: str, hour: int):
        """Опережающие индикаторы. Волна ТОГО ЖЕ часа будущего дня бесполезна:
        табло по умолчанию ставит всем нули. Работают два других сигнала -
        вечерняя волна накануне и ночной банк прилётов."""
        ev = self.run(
            """SELECT ROUND(SUM(avg_delay*CAST(flights AS SIGNED))/SUM(flights)) FROM wave_stats
               WHERE airport=%s AND stat_date=DATE_SUB(%s, INTERVAL 1 DAY)
                 AND leg='DEP' AND airline='' AND hour BETWEEN 16 AND 22
                 AND avg_delay IS NOT NULL""", (airport, day), fetch=True)
        night = self.run(
            """SELECT ROUND(SUM(avg_delay*CAST(flights AS SIGNED))/SUM(flights)) FROM wave_stats
               WHERE airport=%s AND stat_date=%s AND leg='ARR' AND airline=''
                 AND hour < 8 AND avg_delay IS NOT NULL""", (airport, day), fetch=True)
        same = self.run(
            """SELECT ROUND(SUM(avg_delay*CAST(flights AS SIGNED))/SUM(flights)) FROM wave_stats
               WHERE airport=%s AND stat_date=%s AND leg='DEP' AND airline=''
                 AND hour < %s AND hour >= %s AND avg_delay IS NOT NULL""",
            (airport, day, hour, max(hour - 4, 0)), fetch=True)
        # MySQL отдаёт Decimal, а он не умножается на float в весах
        pick = lambda r: (int(r[0][0]) if r and r[0][0] is not None else None)
        return pick(ev), pick(night), pick(same)

    def prediction(self, flight_id, flight_no, flight_date, horizon_min, sched_local,
                   predicted, dep_local, method, components, board_delay) -> None:
        self.run(
            """INSERT INTO predictions (flight_id, flight_no, flight_date, made_at,
                   horizon_min, scheduled_local, predicted_delay_min,
                   predicted_dep_local, method, components, board_delay_min)
               VALUES (%s,%s,%s,NOW(3),%s,%s,%s,%s,%s,%s,%s)""",
            (flight_id, flight_no[:10], flight_date, horizon_min, sched_local,
             predicted, dep_local, method[:24], json.dumps(components, ensure_ascii=False),
             board_delay))

    def tail_daily_delay(self, reg: str, day: str, before_hhmm: str | None = None):
        """Накопленная за сутки задержка ЭТОГО борта.

        В опубликованных моделях (FlightSense на 7 млн рейсов BTS) это один из
        сильнейших признаков: важен не средний хаос в аэропорту, а сколько
        конкретно этот самолёт уже наопаздывал сегодня.
        """
        if not reg:
            return None, 0
        sql = """SELECT SUM(delay_min), COUNT(*) FROM punctuality
                 WHERE aircraft_reg=%s AND flight_date=%s AND delay_min IS NOT NULL"""
        args = [reg, day]
        if before_hhmm:
            sql += " AND TIME(scheduled_local) < %s"
            args.append(before_hhmm)
        row = self.run(sql, tuple(args), fetch=True)
        if not row or row[0][0] is None:
            return None, 0
        return int(row[0][0]), int(row[0][1])

    def resolve_tail(self, flight_no: str, day: str):
        """Какой борт выполнял этот рейс в эту дату.

        Жёсткая привязка рейса к регистрации быстро протухает: Flyone тасует
        флот (31.08 ER-00007, 01.09 ER-00014 вместо привычного ER-00017).
        Поэтому борт берём из наблюдения входящего плеча за тот же день.
        """
        for table, col in (("punctuality", "aircraft_reg"), ("flight_history", "reg")):
            date_col = "flight_date" if table == "punctuality" else \
                       "DATE(COALESCE(takeoff_utc, first_seen))"
            row = self.run(f"""SELECT {col} FROM {table}
                               WHERE flight_no=%s AND {date_col}=%s AND {col} IS NOT NULL
                               ORDER BY 1 DESC LIMIT 1""", (flight_no, day), fetch=True)
            if row and row[0][0]:
                return row[0][0]
        return None

    def rotation_daily_delay(self, chain: list, day: str):
        """То же, что tail_daily_delay, но по НОМЕРАМ рейсов цепочки.

        Бортовой номер отдают только FR24 и платный API; родные табло аэропортов
        (Звартноц, TAV, Черногория) его не дают вовсе. Зато состав суточной
        ротации известен и стабилен, поэтому накопленную задержку считаем
        по предшествующим плечам той же цепочки.
        """
        if not chain:
            return None, 0
        marks = ",".join(["%s"] * len(chain))
        row = self.run(f"""SELECT SUM(delay_min), COUNT(*) FROM punctuality
                           WHERE flight_no IN ({marks}) AND flight_date=%s
                             AND delay_min IS NOT NULL""",
                       (*chain, day), fetch=True)
        if not row or row[0][0] is None:
            return None, 0
        return int(row[0][0]), int(row[0][1])

    def is_first_flight(self, reg: str, day: str, sched_local: str) -> bool | None:
        """Первый рейс борта за сутки: ему нечего наследовать от предыдущего плеча."""
        if not reg:
            return None
        # ВАЖНО: отличать "более ранних рейсов не было" от "данных за этот день нет".
        # На будущую дату записей нет вообще, и наивный COUNT=0 объявил бы
        # первым рейсом борта любой рейс - тот же класс ошибки, что и с
        # пропущенными посадками.
        row = self.run("""SELECT COUNT(*), SUM(scheduled_local < %s) FROM punctuality
                          WHERE aircraft_reg=%s AND flight_date=%s""",
                       (sched_local, reg, day), fetch=True)
        if not row or not row[0][0]:
            return None                      # про этот день мы ничего не знаем
        return int(row[0][1] or 0) == 0

    def late_rate(self, flight_no: str, airport: str, threshold: int = 15):
        """Эмпирическая доля дней с опозданием >= порога.

        Наш аналог классификатора из литературы: там метрика ROC AUC на бинарной
        задаче "опоздает ли на 15+ минут". Это даёт точку сравнения с бенчмарком.
        """
        row = self.run("""SELECT SUM(delay_min >= %s), COUNT(*) FROM punctuality
                          WHERE flight_no=%s AND airport=%s AND delay_min IS NOT NULL""",
                       (threshold, flight_no, airport), fetch=True)
        if not row or not row[0][1]:
            return None, 0
        return round(100 * int(row[0][0]) / int(row[0][1])), int(row[0][1])

    def latest_prediction(self, flight_no: str, flight_date: str):
        row = self.run("""SELECT predicted_delay_min FROM predictions
                          WHERE flight_no=%s AND flight_date=%s
                          ORDER BY made_at DESC LIMIT 1""",
                       (flight_no[:10], flight_date), fetch=True)
        return int(row[0][0]) if row else None

    def settle(self, flight_no: str, flight_date: str, actual_delay: int) -> None:
        """Закрываем прогнозы фактом. Ключ - номер рейса и дата: у теневых
        рейсов своего flight_id нет, а сверять их тоже нужно."""
        self.run("""UPDATE predictions
                    SET actual_delay_min=%s, error_min=predicted_delay_min-%s,
                        settled_at=NOW()
                    WHERE flight_no=%s AND flight_date=%s AND settled_at IS NULL""",
                 (actual_delay, actual_delay, flight_no[:10], flight_date))

    def alive(self) -> bool:
        """Живая ли база НА САМОМ ДЕЛЕ.

        06.09 демон бодро писал "БД: подключена", когда pymysql не
        импортировался вовсе и каждый запрос молча падал в except. Слово
        "подключена" бралось из настройки, а не из выполненного запроса -
        проверка, которая ничего не проверяет, хуже её отсутствия.
        """
        if not self.enabled:
            return False
        return bool(self.run("SELECT 1", fetch=True))

    def ack_sms(self, req_id: str, ok: bool) -> None:
        self.run("UPDATE notifications SET sms_bridge_ok=%s WHERE sms_req_id=%s",
                 (1 if ok else 0, req_id))


STORE: Store | None = None


ROW_KEYS = ("number", "callsign", "status", "generic", "scheduled", "estimated",
            "real", "other", "other_iata", "aircraft", "reg", "gate", "checkin",
            "carousel", "source", "fr24_id")


def _blank_row(**kw) -> dict:
    row = {k: None for k in ROW_KEYS}
    row.update(kw)
    return row


# ---------------------------------------------------------------------------
# провайдер 1: Flightradar24 (работает везде, но данные беднее)
# ---------------------------------------------------------------------------
def _fr24_row(fl: dict, leg: str, page: int) -> dict:
    other = "origin" if leg == "ARR" else "destination"
    key = "arrival" if leg == "ARR" else "departure"
    return _blank_row(
        number=_get(fl, ["identification", "number", "default"]) or "",
        callsign=_get(fl, ["identification", "callsign"]) or "",
        status=_get(fl, ["status", "text"]),
        generic=_get(fl, ["status", "generic", "status", "text"]),
        scheduled=_get(fl, ["time", "scheduled", key]),
        estimated=_get(fl, ["time", "estimated", key]),
        real=_get(fl, ["time", "real", key]),
        other=_get(fl, ["airport", other, "position", "region", "city"])
              or _get(fl, ["airport", other, "name"]),
        other_iata=_get(fl, ["airport", other, "code", "iata"]),
        aircraft=_get(fl, ["aircraft", "model", "text"]),
        reg=_get(fl, ["aircraft", "registration"]),
        # карточка рейса - последний источник факта посадки, когда доска молчит
        fr24_id=_get(fl, ["identification", "id"]),
        source=f"fr24 p{page}",
    )


def fetch_fr24(api: FlightRadar24API, airport: str, pages: list[int]) -> dict:
    """Один запрос на страницу отдаёт СРАЗУ и вылеты, и прилёты."""
    board = {"DEP": [], "ARR": [], "tz": 0}
    for page in pages:
        try:
            data = api.get_airport_details(airport, flight_limit=100, page=page)
        except Exception as exc:
            log.warning("FR24 %s страница %s недоступна: %s", airport, page, exc)
            continue
        plugin = _get(data, ["airport", "pluginData"], {}) or {}
        board["tz"] = _get(plugin, ["details", "timezone", "offset"], board["tz"]) or board["tz"]
        for kind, leg in (("departures", "DEP"), ("arrivals", "ARR")):
            for row in _get(plugin, ["schedule", kind, "data"], []) or []:
                board[leg].append(_fr24_row(row.get("flight") or {}, leg, page))
    return board


# ---------------------------------------------------------------------------
# провайдер 2: публичный шлюз TAV
# ---------------------------------------------------------------------------
TAV_FMT = "%d.%m.%Y %H:%M"


def _tav_ts(value: str | None):
    """Время местное, отдаётся строкой без зоны.

    Кладём его в epoch как если бы это был UTC, а смещение аэропорта держим
    нулевым: и отображение, и разница план/оценка, и определение даты
    считаются в одной системе координат, так что подмена безвредна.
    """
    if not value:
        return None
    try:
        return int(datetime.strptime(value.strip(), TAV_FMT)
                   .replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None


def _tav_row(f: dict, leg: str) -> dict:
    remark = f.get("remark") or {}
    path = f.get("path") or {}
    other = path.get("origin" if leg == "ARR" else "destination") or {}
    pref = "origin" if leg == "ARR" else "destination"
    number = f"{(f.get('airlineIata') or '').strip()}{(f.get('flightNumber') or '').strip()}"
    return _blank_row(
        number=number.upper(),
        callsign="",
        status=(remark.get("remarkRu") or "").strip() or None,
        generic=(remark.get("remarkEn") or "").strip() or None,
        scheduled=_tav_ts(f.get("stad")),
        estimated=_tav_ts(f.get("etad")),
        real=_tav_ts(f.get("atad")),
        other=(other.get(f"{pref}En") or "").strip(),
        other_iata=(other.get(f"{pref}Iata") or "").strip(),
        aircraft="", reg="",
        gate=(f.get("gate") or "").strip(),
        checkin=(f.get("checkin") or "").strip(),
        carousel=(f.get("carousel") or "").strip(),
        source="tav",
    )


def fetch_tav(airport: str, dates: set[str], legs: set[str], cfg: dict) -> dict | None:
    """Окно запрашивается явно, поэтому задержанный с утра рейс никуда не девается.

    Тянем только те направления, по которым реально есть отслеживаемые рейсы:
    эндпоинт недокументированный, лишний трафик по нему ни к чему.
    """
    days = sorted(datetime.strptime(d, "%Y-%m-%d") for d in dates)
    lo = days[0].strftime("%d.%m.%Y 00:00")
    hi = (days[-1] + timedelta(days=1)).strftime("%d.%m.%Y 06:00")
    board = {"DEP": [], "ARR": [], "tz": 0}
    for leg in sorted(legs):
        body = json.dumps({"airportCode": airport, "minStad": lo,
                           "maxStad": hi, "flightLeg": leg}).encode("utf-8")
        req = urllib.request.Request(cfg["tav_url"], data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=cfg["tav_timeout"]) as resp:
            data = json.load(resp)
        if not data.get("result"):
            log.warning("TAV %s %s: result=false", airport, leg)
            return None
        for f in (data.get("data") or {}).get("flights") or []:
            board[leg].append(_tav_row(f, leg))
    return board


# ---------------------------------------------------------------------------
# провайдер 3: Звартноц / Ширак (собственный API аэропорта)
# ---------------------------------------------------------------------------
# Статус приходит уже локализованным, но иногда вместо него подставлена дата
# (когда расчётное время уползает на следующие сутки) - такую строку нельзя
# ни показывать, ни сравнивать, поэтому распознаём и подменяем.
ZV_DATE_RE = re.compile(r"^\s*\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\s*$")
ZV_DEPARTED = {"вылетел", "departed", "прибыл", "landed", "մեկնել է", "ժամանել է"}


def _zv_prog(value: str | None):
    if not value:
        return None
    try:
        return int(datetime.strptime(value.strip(), "%Y/%m/%d %H:%M")
                   .replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None


def _zv_est(value: str | None, base: int | None):
    """EstimatedTimeText приходит без года ("23/08 02:40") - берём год из
    расписания и правим перескок через Новый год."""
    if not value or not base:
        return None
    base_dt = datetime.fromtimestamp(base, tz=timezone.utc)
    try:
        day, month, hh, mm = re.match(
            r"^\s*(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})\s*$", value).groups()
    except AttributeError:
        return None
    try:
        est = datetime(base_dt.year, int(month), int(day), int(hh), int(mm),
                       tzinfo=timezone.utc)
    except ValueError:
        return None
    if (est - base_dt).days < -180:
        est = est.replace(year=base_dt.year + 1)
    elif (est - base_dt).days > 180:
        est = est.replace(year=base_dt.year - 1)
    return int(est.timestamp())


def _zv_row(f: dict, leg: str) -> dict:
    sched = _zv_prog(f.get("ProgrammedTimeData"))
    est = _zv_est(f.get("EstimatedTimeText"), sched) or sched

    status = (f.get("FlightStatusText") or "").strip()
    if not status or ZV_DATE_RE.match(status):
        # вместо статуса пришла дата - синтезируем из флага задержки
        status = "Опоздание" if str(f.get("ProgrammedTimeDelayData")) == "1" else None

    # фактического времени вылета этот API не отдаёт вовсе, поэтому факт
    # выводим из статуса: иначе рейс никогда не считался бы завершённым
    real = est if status and status.strip().lower() in ZV_DEPARTED else None

    return _blank_row(
        number=(f.get("FlightNumber") or "").replace(" ", "").upper(),
        callsign="",
        status=status,
        generic=("delayed" if str(f.get("ProgrammedTimeDelayData")) == "1" else None),
        scheduled=sched, estimated=est, real=real,
        other=(f.get("FromTo") or "").strip(),
        other_iata=(f.get("FromToIATA") or "").strip(),
        aircraft="", reg="",
        gate=(f.get("Gate") or "").strip(),
        checkin=(f.get("CheckIn") or "").strip(),
        carousel=(f.get("Belt") or "").strip() if f.get("Belt") else "",
        source="zvartnots",
    )


def fetch_zvartnots(airport: str, legs: set[str], cfg: dict) -> dict | None:
    controller = cfg["zv_airports"][airport]
    url = cfg["zv_url"].format(controller=controller)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=cfg["zv_timeout"]) as resp:
        data = json.load(resp)

    board = {"DEP": [], "ARR": [], "tz": 0}
    sections = {"DEP": "DepartureDaysData", "ARR": "ArrivalDaysData"}
    for leg in legs:
        for day in data.get(sections[leg]) or []:
            for f in day.get("DayFlights") or []:
                board[leg].append(_zv_row(f, leg))
    return board


# ---------------------------------------------------------------------------
# провайдер 4: Черногория - Тиват и Подгорица
# ---------------------------------------------------------------------------
MNE_STATUS = {
    "arrived": "Прилетел", "departed": "Вылетел", "delayed": "Задержан",
    "boarding": "Посадка", "cancelled": "Отменён", "canceled": "Отменён",
    "check-in": "Регистрация", "checkin": "Регистрация", "on time": "Вовремя",
    "expected": "Ожидается", "gate closed": "Гейт закрыт", "landed": "Прилетел",
    "scheduled": "По расписанию", "final call": "Заканчивается посадка",
}


def _hhmm_on(date_str: str, hhmm: str, fmt: str, anchor: int | None = None):
    """Склеивает дату и время вида HHMM/HH:MM в псевдо-epoch (местное как UTC)."""
    hhmm = (hhmm or "").strip().replace(":", "")
    if not hhmm or len(hhmm) != 4 or not hhmm.isdigit():
        return None
    try:
        day = datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    ts = int(day.replace(hour=int(hhmm[:2]), minute=int(hhmm[2:])).timestamp())
    # время после полуночи относится к следующим суткам
    if anchor and ts - anchor < -12 * 3600:
        ts += 86400
    return ts


def fetch_montenegro(airport: str, dates: set[str], legs: set[str], cfg: dict) -> dict | None:
    code = cfg["mne_airports"][airport]
    req = urllib.request.Request(cfg["mne_url"].format(code=code),
                                 headers={"User-Agent": "Mozilla/5.0",
                                          "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=cfg["http_timeout"]) as resp:
        data = json.load(resp)

    covered = {datetime.strptime(f["Datum"].strip(), "%d-%m-%Y").strftime("%Y-%m-%d")
               for f in data if f.get("Datum")}
    if not covered & dates:
        return None   # у этого источника только текущий день - пусть решает FR24

    board = {"DEP": [], "ARR": [], "tz": 0}
    for f in data:
        leg = "ARR" if (f.get("TipLeta") or "").upper() == "I" else "DEP"
        if leg not in legs:
            continue
        d = f.get("Datum") or ""
        sched = _hhmm_on(d, f.get("Planirano"), "%d-%m-%Y")
        est = _hhmm_on(d, f.get("Predvidjeno"), "%d-%m-%Y", sched)
        real = _hhmm_on(d, f.get("Aktuelno"), "%d-%m-%Y", sched)
        en = (f.get("StatusEN") or "").strip()
        if en in ("-", "--"):
            en = ""
        board[leg].append(_blank_row(
            number=f"{(f.get('Kompanija') or '').strip()}"
                   f"{(f.get('BrojLeta') or '').strip()}".replace(" ", "").upper(),
            callsign="",
            status=MNE_STATUS.get(en.lower(), en) or None,
            generic=en.lower() or None,
            scheduled=sched, estimated=est or sched, real=real,
            other=(f.get("Grad") or "").strip(),
            other_iata=(f.get("IATA") or "").strip(),
            aircraft="", reg="",
            gate=(f.get("Gate") or "").strip(),
            checkin=(f.get("CheckIn") or "").strip(),
            carousel=(f.get("Karusel") or "").strip(),
            source="montenegro"))
    return board


# ---------------------------------------------------------------------------
# провайдер 5: Белград (статический XML, горизонт -2…+2 суток)
# ---------------------------------------------------------------------------
BEG_STATUS = {
    "ONT": "Вовремя", "DEP": "Вылетел", "LAN": "Приземлился", "EXP": "Ожидается",
    "DLY": "Задержан", "CAN": "Отменён", "NGT": "Ночной", "GTG - GO TO GATE": "Пройдите к гейту",
}
BEG_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
_beg_cache: dict = {}


def _beg_date(value: str) -> str | None:
    """'24-Aug-26' -> '2026-08-24'. Разбираем вручную: strptime с %b зависит от локали."""
    try:
        d, mon, y = value.strip().split("-")
        return "%04d-%02d-%02d" % (2000 + int(y), BEG_MONTHS[mon[:3].title()], int(d))
    except (ValueError, KeyError):
        return None


def fetch_belgrade(airport: str, dates: set[str], legs: set[str], cfg: dict) -> dict | None:
    headers = {"User-Agent": "Mozilla/5.0"}
    cached = _beg_cache.get("v")
    if cached:
        if cached.get("last_modified"):
            headers["If-Modified-Since"] = cached["last_modified"]
        if cached.get("etag"):
            headers["If-None-Match"] = cached["etag"]
    raw = None
    try:
        with urllib.request.urlopen(
                urllib.request.Request(cfg["beg_url"], headers=headers),
                timeout=cfg["http_timeout"]) as resp:
            raw = resp.read()
            meta = {"last_modified": resp.headers.get("last-modified"),
                    "etag": resp.headers.get("etag")}
    except urllib.error.HTTPError as exc:
        if exc.code != 304 or not cached:
            raise
        log.debug("Белград: 304, файл не менялся")
    if raw is not None:
        rows = _beg_parse(raw)
        _beg_cache["v"] = dict(meta, rows=rows)
    rows = _beg_cache["v"]["rows"]

    if not {r["_date"] for r in rows} & dates:
        return None

    board = {"DEP": [], "ARR": [], "tz": 0}
    for r in rows:
        if r["_leg"] in legs:
            board[r["_leg"]].append(r)
    return board


def _beg_parse(raw: bytes) -> list:
    root = ET.fromstring(raw)
    g = lambda e, t: (e.findtext(t) or "").strip()
    out = []
    for e in root.findall(".//LET"):
        # кодшеры лежат отдельными записями-дублями и указывают на основной рейс
        if g(e, "TIP_VEZE") not in ("0", "1"):
            continue
        date = _beg_date(g(e, "DATUM"))
        if not date:
            continue
        tip = g(e, "TIP").upper()
        leg = "ARR" if tip.endswith("A") else "DEP"
        sched = _hhmm_on(date, g(e, "ST"), "%Y-%m-%d")
        # у DLY и CAN поле ET обычно пустое: новое время Белград не публикует,
        # только флаг, поэтому длительность задержки отсюда не вытащить
        est = _hhmm_on(date, g(e, "ET"), "%Y-%m-%d", sched)
        real = _hhmm_on(date, g(e, "TIME"), "%Y-%m-%d", sched)
        remark = g(e, "REMARK")
        row = _blank_row(
            number=g(e, "BROJ_LETA").replace(" ", "").upper(),
            callsign="",
            status=BEG_STATUS.get(remark, remark) or None,
            generic=remark.lower() or None,
            scheduled=sched, estimated=est or sched, real=real,
            other=g(e, "DESTINACIJA"), other_iata="",
            aircraft=g(e, "TIP_AVIONA"), reg="",
            gate=g(e, "GATE_BAY"), checkin="", carousel="",
            source="beg")
        row["_date"], row["_leg"] = date, leg
        out.append(row)
    return out


def get_board(api, airport: str, dates: set[str], legs: set[str],
              cfg: dict, cooldown: dict) -> dict:
    """Пробуем родные источники аэропорта, иначе Flightradar24.

    Провайдер возвращает None, если просто не покрывает нужные даты - это не
    поломка, блокировать его не за что. А вот сетевая ошибка отправляет его
    в отстой на provider_cooldown секунд, чтобы не долбить впустую каждый цикл.
    """
    chain = []
    if airport in cfg["zv_airports"]:
        chain.append(("zvartnots", lambda: fetch_zvartnots(airport, legs, cfg)))
    if airport in cfg["mne_airports"]:
        chain.append(("montenegro", lambda: fetch_montenegro(airport, dates, legs, cfg)))
    if airport in cfg["beg_airports"]:
        chain.append(("beg", lambda: fetch_belgrade(airport, dates, legs, cfg)))
    if airport in cfg["tav_airports"]:
        chain.append(("tav", lambda: fetch_tav(airport, dates, legs, cfg)))

    now = time.time()
    degraded = False
    for name, fn in chain:
        until, fails = cooldown.get((airport, name), (0, 0))
        if now < until:
            degraded = True      # предпочтительный источник сейчас недоступен
            continue
        t0 = time.time()
        try:
            board = fn()
        except Exception as exc:
            if STORE:
                STORE.poll(name, airport, ok=False, ms=int((time.time()-t0)*1000), error=exc)
            fails += 1
            # Один таймаут - не приговор: отстой растёт постепенно, а не сразу
            # на полчаса, иначе лучший источник по аэропорту выключается надолго
            # и его подменяет тот, что физически не видит нужную дату.
            pause = min(cfg["provider_backoff_start"] * (2 ** (fails - 1)),
                        cfg["provider_cooldown"])
            log.warning("%s/%s: %s — попытка %d, отстой %d мин",
                        airport, name, exc, fails, pause // 60)
            cooldown[(airport, name)] = (now + pause, fails)
            degraded = True
            continue
        cooldown.pop((airport, name), None)          # успех сбрасывает счётчик
        if STORE:
            STORE.poll(name, airport, ok=True, ms=int((time.time()-t0)*1000),
                       rows=sum(len(board.get(l) or []) for l in ("DEP", "ARR")) if board else 0)
        if board and any(board.get(l) for l in legs):
            board["provider"] = name
            board["degraded"] = False
            return board
        log.debug("%s/%s: нужные даты не покрыты", airport, name)

    t0 = time.time()
    board = fetch_fr24(api, airport,
                       cfg["pages_by_airport"].get(airport, cfg["pages"]))
    if STORE:
        STORE.poll("fr24", airport, ok=True, ms=int((time.time()-t0)*1000),
                   rows=sum(len(board.get(l) or []) for l in ("DEP", "ARR")))
    board["provider"] = "fr24"
    # Отказ родного источника означает, что мы смотрим запасным глазом,
    # у которого горизонт уже. Отсутствие рейса в нём ничего не доказывает.
    board["degraded"] = degraded
    return board


def match_any(number: str, rows: list, tz_offset: int, dates: set[str]) -> dict | None:
    """То же сопоставление, но допускает несколько дат.

    Нужно для табло прилёта: ночной рейс приземляется уже следующими сутками,
    поэтому дата прибытия не обязана совпадать с датой вылета.
    """
    for row in rows:
        if number not in ((row["number"] or "").upper(), (row["callsign"] or "").upper()):
            continue
        sched = row["scheduled"]
        if not sched:
            continue
        local = datetime.fromtimestamp(int(sched) + tz_offset,
                                       tz=timezone.utc).strftime("%Y-%m-%d")
        if local in dates:
            return row
    return None


def next_day(date: str) -> str:
    return (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def match_flight(spec: dict, rows: list, tz_offset: int) -> dict | None:
    """Ищет борт по номеру И дате в местном времени аэропорта.

    Дата обязательна: у одного номера рейса на табло легко оказываются
    сегодняшний и завтрашний борт одновременно.
    """
    target = spec["flight"]
    for row in rows:
        if target not in ((row["number"] or "").upper(), (row["callsign"] or "").upper()):
            continue
        sched = row["scheduled"]
        if not sched:
            continue
        local = datetime.fromtimestamp(int(sched) + tz_offset,
                                       tz=timezone.utc).strftime("%Y-%m-%d")
        if local == spec["date"]:
            return row
    return None


# ---------------------------------------------------------------------------
# форматирование
# ---------------------------------------------------------------------------
def hhmm_full(ts, tz_offset: int):
    """Полная местная отметка для БД: время храним местное, смещение отдельно."""
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts) + tz_offset,
                                  tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def hhmm(ts, tz_offset: int) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(int(ts) + tz_offset, tz=timezone.utc).strftime("%H:%M")


def delay_minutes(fl: dict):
    sched, actual = fl.get("scheduled"), fl.get("real") or fl.get("estimated")
    if not sched or not actual:
        return None
    return round((int(actual) - int(sched)) / 60)


def minutes_until(ts, tz_offset: int, airport: str, cfg: dict):
    """Сколько минут осталось до события в реальном времени.

    Родные табло кладут местное время при tz=0, FR24 - настоящий epoch со
    смещением аэропорта. Сводим обе шкалы к UTC через таблицу смещений.
    """
    if not ts:
        return None
    if tz_offset:                      # шкала FR24: ts уже настоящий UTC
        real_utc = int(ts)
    else:                              # шкала родного табло: местное как UTC
        real_utc = local_to_utc(ts, airport, cfg)
    if real_utc is None:
        return None
    return round((real_utc - time.time()) / 60)


def human_delay(minutes: int) -> str:
    minutes = abs(minutes)
    return f"{minutes}м" if minutes < 60 else f"{minutes // 60}ч{minutes % 60:02d}м"


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------
class Bus:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        password = Path(cfg["mqtt_pass_file"]).read_text(encoding="utf-8").strip()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=cfg.get("mqtt_client_id", "flightwatch"))
        self.client.username_pw_set(cfg["mqtt_user"], password)
        self.client.will_set(AVAILABILITY_TOPIC, "offline", qos=1, retain=True)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.connect(cfg["mqtt_host"], cfg["mqtt_port"], keepalive=60)
        self.client.loop_start()
        # Отказ авторизации paho отдаёт асинхронно: connect() отработает без
        # исключения, юнит покажет active, а сенсоров и СМС не будет вовсе.
        # Ровно так демон в облаке полдня жил с чужим mqtt_user и молчал.
        for _ in range(50):
            if self.client.is_connected():
                break
            time.sleep(0.1)
        if self.client.is_connected():
            log.info("MQTT: подключён к %s:%s как %s", cfg["mqtt_host"],
                     cfg["mqtt_port"], cfg["mqtt_user"])
        else:
            log.error("MQTT НЕ ПОДКЛЮЧЁН к %s:%s как %s (код %s): не будет ни "
                      "сенсоров, ни СМС - проверь учётку и брокер",
                      cfg["mqtt_host"], cfg["mqtt_port"], cfg["mqtt_user"],
                      getattr(self, "_conn_rc", "нет ответа"))
        self.client.publish(AVAILABILITY_TOPIC, "online", qos=1, retain=True)
        self._sms_fail = 0            # с какого момента мост отвечает отказом
        self._sms_fail_told = 0       # когда об этом сказали в последний раз
        self._rate_told = 0           # лимит частоты - отдельный симптом
        self._pending: dict = {}      # отправлено, но ещё не подтверждено
        self._seq = 0                 # чтобы два адресата не делили один req_id
        self._token = None
        path = Path(cfg["telegram_token_file"])
        if path.exists():
            self._token = path.read_text(encoding="utf-8").strip()
        else:
            log.warning("нет файла токена Telegram, канал недоступен")

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        # слушаем подтверждения моста, чтобы отличать "не отправлено"
        # от "отправлено, но не доставлено абоненту"
        self._conn_rc = rc
        client.subscribe(self.cfg["ack_topic"], qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        self._pending.pop(str(data.get("req_id", "")), None)
        if str(data.get("req_id", "")).startswith("fw-"):
            log.info("мост подтвердил отправку: req_id=%s ok=%s сегментов=%s/%s",
                     data.get("req_id"), data.get("ok"),
                     data.get("segments_ok"), data.get("segments"))
            if STORE:
                STORE.ack_sms(str(data["req_id"]), bool(data.get("ok")))
            self._watch_sms(bool(data.get("ok")), data)

    def _watch_sms(self, ok: bool, data: dict, silent: bool = False) -> None:
        """Канал СМС может умереть молча - и это худший из отказов.

        04.09 модем ушёл в operating mode "shutting-down", сеть отвалилась, и
        одиннадцать сообщений подряд получили от моста отказ
        WmsMessageDeliveryFailure. В базе это записалось нулём в sms_bridge_ok
        и на этом всё: человек весь день считал, что подстраховка работает.
        Само сообщение об отказе, разумеется, идёт в Telegram - слать его по
        сломанному каналу было бы смешно.
        """
        now = time.time()
        if ok:
            if self._sms_fail:
                lay = human_delay(int((now - self._sms_fail) / 60))
                self._sms_fail = self._sms_fail_told = 0
                try:
                    self.telegram(f"Канал СМС снова работает (лежал {lay})")
                except Exception as exc:
                    log.error("не смог сказать о восстановлении СМС: %s", exc)
            return
        # Мост может отказать осознанно: команда протухла в очереди или
        # упёрлась в лимит частоты. Это не поломка канала, и считать её
        # поломкой - значит поднять ложную тревогу ровно тогда, когда связь
        # только что восстановилась и очередь разгружается.
        reason = str(data.get("detail") or "")
        if reason.startswith("expired"):
            # Содержание при этом не потеряно: Telegram идёт первым каналом
            # и доставляется сразу, СМС тут была дублем.
            log.warning("мост отбросил протухшую команду: %s", reason)
            return
        if reason.startswith("rate limit"):
            log.error("модем упёрся в лимит частоты СМС: %s", reason)
            if now - self._rate_told > self.cfg["sms_fail_notice_h"] * 3600:
                self._rate_told = now
                try:
                    self.telegram(f"Модем режет СМС: {reason}. Сообщения "
                                  f"сверх лимита не уходят, Telegram работает.")
                except Exception as exc:
                    log.error("не смог сказать о лимите: %s", exc)
            return
        if not self._sms_fail:
            self._sms_fail = now
        if now - self._sms_fail_told < self.cfg["sms_fail_notice_h"] * 3600:
            return
        self._sms_fail_told = now
        detail = (str(data.get("detail") or "").strip().splitlines() or [""])[-1][:160]
        since = datetime.fromtimestamp(self._sms_fail).strftime("%d.%m %H:%M")
        log.error("КАНАЛ СМС НЕ РАБОТАЕТ с %s: %s", since, detail)
        # Отказ и молчание - разные симптомы, и чинятся по-разному:
        # первый значит "модем не смог", второй - "моста нет на месте".
        head = (f"СМС НЕ ПОДТВЕРЖДАЮТСЯ с {since}: {detail}."
                if silent else
                f"СМС НЕ УХОДЯТ с {since}. Мост отвечает отказом: {detail}.")
        try:
            self.telegram(f"{head} Остаётся только Telegram — проверь модем "
                          f"{self.cfg['modem_hint']} (регистрация в сети, режим радио).")
        except Exception as exc:
            log.error("оба канала недоступны: %s", exc)

    def notify(self, spec: dict, text: str, event_class: str = "change",
               severity: str = "wake") -> None:
        lbl = spec.get("label")
        if lbl and not spec.get("title"):
            text = f"[{lbl}] {text}"
        """Шлёт во все включённые каналы; отказ одного не мешает остальным."""
        if not self.cfg.get("notify", True) or not spec.get("notify", True):
            log.info("[оповещения выключены] %s", text)
            return
        # СМС дорогая и медленная (в роуминге до часов), Telegram мгновенный.
        # Всё, что не "будить", уходит только в Telegram.
        # Все отслеживаемые рейсы получают одинаковый режим оповещений:
        # метка owner/label нужна, чтобы человек понимал, о чьём рейсе речь,
        # а не чтобы резать доставку. Критичные классы дополнительно
        # игнорируют пороги и окно тишины.
        critical = event_class in self.cfg["sms_always_classes"]
        wake = critical or severity == "wake"
        channels = self.cfg["channels"] if wake else \
            [c for c in self.cfg["channels"] if c != "sms"]

        # основной номер всегда первый и неотключаем, рейсовые - следом
        recipients = [self.cfg["sms_to"]]
        for extra in spec.get("extra_sms_to") or []:
            if extra not in recipients:
                recipients.append(extra)

        # Страховка от повторов: дедупликация в БД срабатывала УЖЕ ПОСЛЕ
        # отправки, поэтому одинаковые сообщения всё равно уходили.
        key = f"{spec.get('flight')}|{event_class}|{text}"
        now_ts = time.time()
        self._recent = {k: v for k, v in getattr(self, "_recent", {}).items()
                        if now_ts - v < self.cfg["resend_block_min"] * 60}
        if key in self._recent:
            log.info("повтор подавлен (%s): %s", event_class, text[:60])
            return
        self._recent[key] = now_ts

        req_id = None
        for channel in channels:
            try:
                if channel == "telegram":
                    self.telegram(text)
                elif channel == "sms":
                    for to in recipients:
                        rid = self.sms(to, text)
                        req_id = req_id or rid
            except Exception as exc:
                log.error("канал %s не сработал: %s", channel, exc)
        if STORE:
            STORE.note(STORE.fid(spec), event_class, severity, text, req_id)

    def telegram(self, text: str) -> None:
        if not self._token:
            raise RuntimeError("токен Telegram не задан")
        body = json.dumps({"chat_id": self.cfg["telegram_chat_id"],
                           "text": f"✈️ {text}"}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{self._token}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            ok = json.load(resp).get("ok")
        log.info("Telegram -> чат %s: %s (ok=%s)", self.cfg["telegram_chat_id"], text, ok)

    def sms(self, to: str, text: str) -> str | None:
        if not self.cfg.get("notify", True):
            log.info("[СМС подавлена notify=false] %s -> %s", to, text)
            return None
        # Раньше id брался из миллисекунд, и два адресата одного сообщения
        # получали ОДИН id: подтверждения затирали друг друга, а в базе
        # оставался результат только по одному номеру.
        self._seq += 1
        req_id = f"fw-{int(time.time() * 1000)}-{self._seq}"
        # Демон живёт в облаке, а модем дома: между ними очередь, которая
        # переживает обрыв туннеля. Без срока годности накопленное за
        # долгий обрыв уедет абоненту с опозданием на часы - ровно то,
        # на что уже жаловались ("прислала, что рейс вылетел, когда я
        # часов 5 как прилетел").
        payload = {"to": to, "text": text, "translit": True, "req_id": req_id,
                   "expires_at": int(time.time() + self.cfg["sms_ttl_s"])}
        self.client.publish(self.cfg["sms_topic"], json.dumps(payload), qos=1)
        self._pending[req_id] = time.time()
        log.info("СМС -> %s: %s", to, text)
        return req_id

    def sweep_sms(self) -> None:
        """Отправили и не получили ответа - значит моста нет на месте."""
        now = time.time()
        late = [r for r, t in self._pending.items()
                if now - t > self.cfg["sms_ack_timeout_s"]]
        for r in late:
            self._pending.pop(r, None)
        if late:
            self._watch_sms(False, {"detail": f"мост не подтвердил {len(late)} "
                                    f"сообщений за {self.cfg['sms_ack_timeout_s']} с "
                                    f"(мост offline либо модем перезагружается)"},
                            silent=True)

    def discovery(self, spec: dict) -> None:
        slug = slug_of(spec)
        device = {
            "identifiers": [f"flightwatch_{slug}"],
            "name": f"Рейс {title_of(spec)}",
            "manufacturer": "flightwatch",
            "model": "FR24 poller",
            "via_device": "flightwatch",
        }
        common = {
            "state_topic": f"{TOPIC_PREFIX}/{slug}/state",
            "availability_topic": AVAILABILITY_TOPIC,
            "device": device,
        }
        sensors = {
            "status": {"name": "Статус", "icon": "mdi:airplane-takeoff",
                       "value_template": "{{ value_json.status }}"},
            "delay": {"name": "Задержка", "icon": "mdi:clock-alert-outline",
                      "unit_of_measurement": "мин", "state_class": "measurement",
                      "value_template": "{{ value_json.delay_min }}"},
            "time": {"name": "Время по оценке", "icon": "mdi:clock-outline",
                     "value_template": "{{ value_json.estimated_local }}"},
        }
        for key, extra in sensors.items():
            body = dict(common, unique_id=f"flightwatch_{slug}_{key}",
                        object_id=f"flightwatch_{slug}_{key}", **extra)
            self.client.publish(
                f"{self.cfg['discovery_prefix']}/sensor/flightwatch_{slug}_{key}/config",
                json.dumps(body, ensure_ascii=False), qos=1, retain=True)

    def drop_discovery(self, slug: str) -> None:
        for key in ("status", "delay", "time"):
            self.client.publish(
                f"{self.cfg['discovery_prefix']}/sensor/flightwatch_{slug}_{key}/config",
                "", qos=1, retain=True)
        self.client.publish(f"{TOPIC_PREFIX}/{slug}/state", "", qos=1, retain=True)
        log.info("снял сенсоры для %s", slug)

    def state(self, slug: str, payload: dict) -> None:
        self.client.publish(f"{TOPIC_PREFIX}/{slug}/state",
                            json.dumps(payload, ensure_ascii=False), qos=1, retain=True)


# ---------------------------------------------------------------------------
# обработка одного рейса
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Ротация борта: где сейчас самолёт, которым полетит наш рейс
# ---------------------------------------------------------------------------
# Смещение аэропорта - НЕ константа. Словарь фиксированных смещений верен
# ровно до ближайшего перевода часов: 25.10.2026 Тиват, Подгорица и Белград
# уезжают со 120 на 60, Ларнака, Кишинёв и Тель-Авив со 180 на 120, а Ереван,
# Тбилиси, Москва, Стамбул и Дубай не двигаются вовсе. Час молчаливой ошибки
# в системе, смысл которой - не опоздать на рейс. Поэтому смещение считаем
# по базе часовых поясов НА МОМЕНТ события, а словарь оставляем запасным.
AIRPORT_TZ = {
    "TIV": "Europe/Podgorica", "TGD": "Europe/Podgorica", "BEG": "Europe/Belgrade",
    "LCA": "Asia/Nicosia", "EVN": "Asia/Yerevan", "LWN": "Asia/Yerevan",
    "TBS": "Asia/Tbilisi", "ALA": "Asia/Almaty", "SVO": "Europe/Moscow",
    "DME": "Europe/Moscow", "VKO": "Europe/Moscow", "STW": "Europe/Moscow",
    "KRR": "Europe/Moscow", "MRV": "Europe/Moscow", "AER": "Europe/Moscow",
    "LED": "Europe/Moscow", "SVX": "Asia/Yekaterinburg", "KZN": "Europe/Moscow",
    "IST": "Europe/Istanbul", "SAW": "Europe/Istanbul", "RMO": "Europe/Chisinau",
    "DXB": "Asia/Dubai", "TLV": "Asia/Jerusalem", "BJV": "Europe/Istanbul",
    "NCE": "Europe/Paris", "ALC": "Europe/Madrid",
}
_ZONES: dict = {}


def ap_zone(airport: str, cfg: dict):
    """Часовая зона аэропорта; None - если не знаем и придётся жить словарём."""
    name = (cfg.get("airport_tz") or {}).get(airport) or AIRPORT_TZ.get(airport)
    if not name:
        return None
    if name not in _ZONES:
        try:
            _ZONES[name] = ZoneInfo(name)
        except Exception as exc:
            log.warning("часовая зона %s недоступна: %s", name, exc)
            _ZONES[name] = None
    return _ZONES[name]


def local_dt(ts_utc, airport: str, cfg: dict):
    """Настоящий UTC -> местное время аэропорта (aware datetime)."""
    if not ts_utc:
        return None
    z = ap_zone(airport, cfg)
    if z is not None:
        return datetime.fromtimestamp(int(ts_utc), tz=timezone.utc).astimezone(z)
    off = cfg["airport_utc_offset_min"].get(airport, 0)
    return datetime.fromtimestamp(int(ts_utc) + off * 60, tz=timezone.utc)


def local_to_utc(ts_local, airport: str, cfg: dict):
    """Местное время, записанное как UTC (шкала родных табло) -> настоящий UTC."""
    if not ts_local:
        return None
    z = ap_zone(airport, cfg)
    if z is not None:
        wall = datetime.fromtimestamp(int(ts_local), tz=timezone.utc).replace(tzinfo=z)
        return int(wall.timestamp())
    off = cfg["airport_utc_offset_min"].get(airport)
    return None if off is None else int(ts_local) - off * 60


def check_offset_table(cfg: dict) -> None:
    """Словарь смещений протухает молча - пусть хотя бы жалуется в лог."""
    now = time.time()
    for iata, off in (cfg.get("airport_utc_offset_min") or {}).items():
        z = ap_zone(iata, cfg)
        if z is None:
            log.warning("аэропорт %s: часовая зона неизвестна, живу словарём (%+d)",
                        iata, off)
            continue
        live = int(datetime.fromtimestamp(now, tz=timezone.utc)
                   .astimezone(z).utcoffset().total_seconds() // 60)
        if live != off:
            log.warning("аэропорт %s: в конфиге %+d, по базе поясов %+d - "
                        "словарь устарел, но считаю по базе", iata, off, live)


def check_watch_numbers(cfg: dict) -> None:
    """Теневой список обещает статистику, которую иногда негде взять.

    Пунктуальность пишется только по ВЫЛЕТАМ с опрашиваемых аэропортов.
    3F534 вылетает из Кишинёва, а Кишинёв мы не опрашиваем - и номер молча
    отсутствовал в базе, хотя стоит первым звеном в цепочке ротации нашего
    3F152. Список, который тихо не работает, хуже пустого списка.
    """
    if STORE is None or not STORE.enabled:
        return
    rows = STORE.run("SELECT flight_no, MAX(flight_date) FROM punctuality "
                     "GROUP BY flight_no", fetch=True) or []
    seen = {str(r[0]): r[1] for r in rows}
    silent = [n for n in cfg.get("watch_numbers") or [] if n not in seen]
    if silent:
        log.warning("теневые номера без единой записи: %s - их аэропорт вылета "
                    "не опрашивается, статистики по ним не будет",
                    ", ".join(silent))
    today = datetime.now(timezone.utc).date()
    stale = [f"{n} ({(today - d).days} дн.)" for n, d in seen.items()
             if n in (cfg.get("watch_numbers") or []) and d
             and (today - d).days > 14]
    if stale:
        log.warning("теневые номера без свежих данных: %s - похоже, рейс снят "
                    "или сменил расписание", ", ".join(stale))


def to_utc(ts, tz_offset: int, airport: str, cfg: dict):
    """Приводит метку любой шкалы к настоящему UTC."""
    if not ts:
        return None
    if tz_offset:
        return int(ts)
    return local_to_utc(ts, airport, cfg)


def aircraft_position(api, reg: str) -> dict | None:
    """Текущий рейс борта и когда он окажется в точке назначения."""
    found = api.search(reg)
    live = found.get("live") or []
    if not live:
        return None
    item = live[0]
    d = item.get("detail") or {}
    data = [None] * 20
    data[1], data[2], data[13] = d.get("lat"), d.get("lon"), []
    fl = Flight(item["id"], data)
    fl.registration = reg
    fl.callsign = d.get("callsign")
    det = api.get_flight_details(fl)
    t = det.get("time") or {}
    ap = det.get("airport") or {}
    code = lambda side: ((ap.get(side) or {}).get("code") or {}).get("iata")
    return {
        "flight_no": ((det.get("identification") or {}).get("number") or {}).get("default"),
        "origin": code("origin"), "destination": code("destination"),
        "eta_utc": (t.get("estimated") or {}).get("arrival")
                   or (t.get("scheduled") or {}).get("arrival"),
        "landed_utc": (t.get("real") or {}).get("arrival"),
        "status": (det.get("status") or {}).get("text"),
    }


def resolve_today_tail(spec: dict, st: dict) -> str | None:
    """Борт на КОНКРЕТНЫЙ день, а не зашитый в конфиг навсегда.

    Привязка рейса к регистрации протухает: Flyone тасует флот -
    31.08 рейс ушёл на ER-00007, 01.09 на ER-00014 вместо привычного ER-00017.
    Порядок: увиденный сегодня на доске -> из базы по входящему плечу ->
    и лишь затем запасной из конфига.
    """
    if st.get("tail_today"):
        return st["tail_today"]
    inbound = spec.get("rotation_inbound_no")
    if STORE and inbound:
        found = STORE.resolve_tail(inbound, spec["date"])
        if found:
            st["tail_today"] = found
            return found
    return spec.get("aircraft_reg")


def check_rotation(api, spec, lead, lead_tz, bus, cfg, st) -> None:
    """Прогноз "не раньше чем" по физике, а не по табло.

    Пока самолёт не прилетел в аэропорт вылета и не отстоял минимальную
    стоянку, вылет невозможен - независимо от того, что показывает доска.
    """
    reg = resolve_today_tail(spec, st)
    if not reg or not lead or lead.get("real"):
        return
    sched_utc = to_utc(lead["scheduled"], lead_tz, spec["airport"], cfg)
    if sched_utc is None:
        return
    hours_left = (sched_utc - time.time()) / 3600
    if not 0 < hours_left <= cfg["rotation_window_hours"]:
        return
    if time.time() - st.get("rotation_at", 0) < cfg["rotation_check_every"]:
        return
    st["rotation_at"] = int(time.time())

    try:
        pos = aircraft_position(api, reg)
    except Exception as exc:
        log.warning("ротация %s: %s", reg, exc)
        return
    if not pos:
        log.info("%s: борт %s сейчас не в воздухе", spec["flight"], reg)
        return

    if STORE:
        STORE.rotation_log(reg, pos)
    st["rotation_flight"] = pos["flight_no"]
    if pos.get("destination") == spec["airport"]:
        eta_s = pos.get("landed_utc") or pos.get("eta_utc")
        when = (local_dt(eta_s, spec["airport"], cfg).strftime("%H:%M")
                if eta_s else "?")
        st["inbound"] = {"no": pos.get("flight_no"), "status": pos.get("status"),
                         "phase": (f"сел в {when}" if pos.get("landed_utc")
                                   else f"в воздухе, посадка {when}")}
    if pos["destination"] != spec["airport"]:
        # Между этим рейсом и нашим ещё одно плечо - прогноз по одному
        # прыжку строить нельзя, только фиксируем, где борт.
        log.info("%s: борт %s занят рейсом %s %s->%s", spec["flight"], reg,
                 pos["flight_no"], pos["origin"], pos["destination"])
        st.pop("rotation_risk", None)      # прежняя граница устарела
        return
    eta = pos.get("landed_utc") or pos.get("eta_utc")
    if not eta:
        return

    # Борт прилетает слишком задолго до нашего вылета - это другой оборот,
    # а не наше плечо. Без этой проверки получалась граница вида -1399 минут
    # (ровно сутки): прилёт СЕГОДНЯ сравнивался с вылетом ЗАВТРА.
    lead_hours = (sched_utc - int(eta)) / 3600
    if not -1 <= lead_hours <= cfg["rotation_relevant_hours"]:
        log.info("%s: борт %s прилетает за %.1f ч до вылета - это другой оборот, "
                 "прогноз по ротации не строю", spec["flight"], reg, lead_hours)
        st.pop("rotation_risk", None)
        st.pop("rotation_told", None)
        return

    earliest = int(eta) + spec["turnaround_min"] * 60
    risk = round((earliest - sched_utc) / 60)
    est_utc = to_utc(lead["estimated"], lead_tz, spec["airport"], cfg)
    board_delay = round((est_utc - sched_utc) / 60) if est_utc else 0
    st["rotation_risk"] = risk
    if risk < cfg["rotation_min_risk"] or risk <= board_delay + cfg["tg_shift_min"]:
        return                      # табло уже знает не меньше нашего
    told = st.get("rotation_told")
    if told is not None and abs(risk - told) < cfg["tg_shift_min"]:
        return

    st["rotation_told"] = risk
    when = local_dt(earliest, spec["airport"], cfg).strftime("%H:%M")
    severity = "wake" if (risk >= cfg["sms_shift_min"]
                          or hours_left * 60 <= cfg["urgent_window_min"]) else "info"
    bus.notify(spec,
               f"Рейс {spec['flight']}: борт {reg} ещё в рейсе {pos['flight_no']} "
               f"{pos['origin']}-{pos['destination']}, раньше {when} вылет невозможен "
               f"(+{human_delay(risk)} к плану). Табло пока молчит",
               event_class="rotation_risk", severity=severity)


# ---------------------------------------------------------------------------
# Модель прогноза опоздания
# ---------------------------------------------------------------------------
# Четыре сигнала, от самого твёрдого к самому мягкому:
#   1. ротация борта - физическая нижняя граница, спорить с ней нельзя;
#   2. табло - что признаёт аэропорт прямо сейчас, тоже нижняя граница;
#   3. перенос вчерашнего опоздания с затуханием - по наблюдениям оно
#      гаснет примерно вдвое за сутки (22.08 +563 -> 23.08 +258 -> 24.08 +184);
#   4. медиана последних дней и здоровье волны того же часа.
# Итог - максимум из твёрдых границ и взвешенной статистики.
# Веса заведомо грубые: они будут калиброваться по таблице predictions,
# где каждый прогноз лежит вместе с горизонтом и фактом.
# ---------------------------------------------------------------------------
def median(vals):
    if not vals:
        return None
    v = sorted(vals)
    n = len(v)
    return v[n // 2] if n % 2 else round((v[n // 2 - 1] + v[n // 2]) / 2)


def chain_estimate(spec, board, tz, cfg, sched_dep_utc):
    """Прогноз по ЦЕПОЧКЕ БОРТА, а не по независимой статистике плеча.

    Плечи одного борта физически связаны: наш рейс не может уйти раньше, чем
    прилетит самолёт и отстоит разворот. Считать каждое плечо отдельно по его
    истории - двойной счёт: эта история набрана в дни, когда предыдущее плечо
    тоже опаздывало.

    Проверено на 31.08: 3F151 ушёл с +88, стоянка в Тивате 120 мин ->
    формула даёт 3F152 +158 при факте +155. Независимая статистика давала +110.
    """
    inbound_no = spec.get("rotation_inbound_no")
    if not inbound_no:
        return None, {}
    rows = board.get("ARR") or []
    inb = None
    for r in rows:
        if (r["number"] or "").upper() == inbound_no and r["scheduled"]:
            local = datetime.fromtimestamp(int(r["scheduled"]) + tz,
                                           tz=timezone.utc).strftime("%Y-%m-%d")
            if local == spec["date"]:
                inb = r
                break
    if not inb:
        return None, {}

    sched_arr_utc = to_utc(inb["scheduled"], tz, spec["airport"], cfg)
    if sched_arr_utc is None:
        return None, {}

    # задержка входящего: сначала факт/оценка с доски, иначе наш же прогноз
    known = to_utc(inb["real"] or inb["estimated"], tz, spec["airport"], cfg)
    if known:
        inb_delay = round((known - sched_arr_utc) / 60)
        origin = "табло прилёта"
    else:
        inb_delay = (STORE.latest_prediction(inbound_no, spec["date"])
                     if STORE else None)
        origin = "прогноз по входящему"
        if inb_delay is None:
            return None, {}

    earliest = sched_arr_utc + inb_delay * 60 + spec["turnaround_min"] * 60
    delay = max(0, round((earliest - sched_dep_utc) / 60))
    # Состояние борта, который летит СЮДА - это и есть ответ на вопрос
    # "всё ок или нет". Табло аэропорта об этом ещё молчит.
    if inb.get("real"):
        phase = f"вылетел, садится {hhmm(inb['real'] or inb['estimated'], tz)}"
    elif inb_delay and inb_delay >= 15:
        phase = f"задержан на {human_delay(inb_delay)}, прилёт {hhmm(known or sched_arr_utc, tz)}"
    else:
        phase = f"по расписанию, прилёт {hhmm(inb['estimated'] or inb['scheduled'], tz)}"
    return delay, {"входящий": inbound_no, "задержка_входящего": inb_delay,
                   "источник_входящего": origin, "борт_входящего": inb.get("reg") or None,
                   "стоянка": spec["turnaround_min"], "цепочка": delay,
                   "_inbound_brief": {"no": inbound_no, "phase": phase,
                                      "status": inb.get("status")}}

def predict_delay(spec, lead, lead_tz, cfg, st, rotation_bound_min=None, board=None):
    """Возвращает (прогноз в минутах, метод, разложение по вкладам)."""
    parts = {}
    sched_utc = to_utc(lead["scheduled"], lead_tz, spec["airport"], cfg)
    if sched_utc is None:
        return None, None, parts

    board_delay = 0
    est_utc = to_utc(lead["real"] or lead["estimated"], lead_tz, spec["airport"], cfg)
    if est_utc:
        board_delay = round((est_utc - sched_utc) / 60)
    parts["табло"] = board_delay

    hist = STORE.history_delays(spec["flight"], spec["airport"]) if STORE else []
    hist_vals = [d for _, d in hist]
    med = median(hist_vals)
    parts["медиана_истории"] = med

    # перенос вчерашнего: опоздание не рассасывается за ночь, а затухает
    carry = None
    if hist:
        try:
            sched_day = datetime.fromtimestamp(
                int(lead["scheduled"]) + lead_tz, tz=timezone.utc).date()
            for day, d in hist:
                gap = (sched_day - day).days
                if 1 <= gap <= 3:
                    carry = round(d * (cfg["predict_carryover_decay"] ** gap))
                    break
        except Exception:
            carry = None
    parts["перенос_вчерашнего"] = carry

    bank = None
    if STORE:
        try:
            dt = datetime.fromtimestamp(int(lead["scheduled"]) + lead_tz, tz=timezone.utc)
            ev, night, same = STORE.leading_signals(spec["airport"],
                                                    dt.strftime("%Y-%m-%d"), dt.hour)
            parts["вечер_накануне"], parts["ночной_банк"] = ev, night
            parts["волна_ранее_сегодня"] = same
            if dt.hour < 12:
                # утро определяется ночью и вечером накануне
                cands = [v for v in (ev, night) if v is not None]
            else:
                # днём главное - что уже накопилось за сегодня
                cands = [v for v in (same, ev) if v is not None]
            bank = max(cands) if cands else None
        except Exception:
            bank = None
    parts["сигнал_волны"] = bank

    # накопленная за сутки задержка борта - сильнейший признак по литературе
    tail = tail_n = None
    first = tight = None
    if STORE and (spec.get("aircraft_reg") or spec.get("rotation_chain")):
        try:
            dt = datetime.fromtimestamp(int(lead["scheduled"]) + lead_tz, tz=timezone.utc)
            day = dt.strftime("%Y-%m-%d")
            tail, tail_n = STORE.tail_daily_delay(resolve_today_tail(spec, st), day,
                                                  dt.strftime("%H:%M:%S"))
            if tail is None:      # по регистрации данных нет - считаем по цепочке
                tail, tail_n = STORE.rotation_daily_delay(spec.get("rotation_chain"), day)
            first = STORE.is_first_flight(spec["aircraft_reg"], day,
                                          dt.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            pass
    tight = (spec.get("turnaround_min") or 999) < cfg["tight_turnaround_min"]
    parts["борт_накопил_за_сутки"] = tail
    parts["плечей_борта_до_этого"] = tail_n or None
    parts["первый_рейс_борта"] = first
    parts["короткая_стоянка"] = tight

    w = [(cfg["predict_w_history"], med), (cfg["predict_w_carryover"], carry),
         (cfg["predict_w_bank"], bank),
         (cfg["predict_w_tail"], (round(tail / tail_n) if tail_n else None))]
    known = [(wt, int(v)) for wt, v in w if v is not None]
    stat = round(sum(wt * v for wt, v in known) / sum(wt for wt, _ in known)) if known else 0
    parts["статистика"] = stat
    parts["ротация"] = rotation_bound_min

    chain = None
    if board is not None:
        try:
            chain, chain_parts = chain_estimate(spec, board, lead_tz, cfg, sched_utc)
            parts.update(chain_parts)
            if chain_parts.get("борт_входящего"):
                st["tail_today"] = chain_parts["борт_входящего"]
            if chain_parts.get("_inbound_brief"):
                st["inbound"] = chain_parts.pop("_inbound_brief")
        except Exception:
            chain = None

    hard = [board_delay]
    # Граница ротации имеет смысл, только если она ПОЛОЖИТЕЛЬНА: "борт может
    # улететь на 23 часа раньше расписания" - это не ограничение, а мусор.
    if rotation_bound_min is not None and rotation_bound_min > 0:
        hard.append(rotation_bound_min)
    elif rotation_bound_min is not None:
        parts["ротация"] = None
    # Цепочка ЗАМЕЩАЕТ независимую статистику, а не складывается с ней:
    # медиана истории плеча уже включает в себя эффект цепочки.
    basis = chain if chain is not None else stat
    predicted = max(max(hard), basis)
    if chain is not None and chain >= max(hard):
        method = "chain"
    elif rotation_bound_min is not None and rotation_bound_min >= max(basis, board_delay):
        method = "rotation"
    elif board_delay >= basis:
        method = "board"
    else:
        method = "stat"
    if STORE:
        try:
            rate, n = STORE.late_rate(spec["flight"], spec["airport"])
            if rate is not None:
                parts["доля_дней_с_опозданием_15+"] = f"{rate}% (n={n})"
        except Exception:
            pass
    return predicted, method, parts


def record_prediction(spec, lead, lead_tz, cfg, st, board=None) -> None:
    if not STORE or not lead or lead.get("real"):
        return
    sched_utc = to_utc(lead["scheduled"], lead_tz, spec["airport"], cfg)
    if sched_utc is None:
        return
    horizon = round((sched_utc - time.time()) / 60)
    if not 0 < horizon <= cfg["predict_window_hours"] * 60:
        return
    if time.time() - st.get("predict_at", 0) < cfg["predict_every"]:
        return
    st["predict_at"] = int(time.time())

    predicted, method, parts = predict_delay(spec, lead, lead_tz, cfg, st,
                                             st.get("rotation_risk"), board=board)
    if predicted is None:
        return
    fmt = lambda u: local_dt(u, spec["airport"], cfg).strftime("%Y-%m-%d %H:%M:%S")
    STORE.prediction(STORE.fid(spec), spec["flight"],
                     fmt(sched_utc)[:10], horizon, fmt(sched_utc), predicted,
                     fmt(sched_utc + predicted * 60), method, parts,
                     parts.get("табло"))
    st["last_predicted"] = predicted
    log.info("%s: прогноз +%d мин (%s), горизонт %.1f ч, вклады %s",
             spec["flight"], predicted, method, horizon / 60, parts)

# ---------------------------------------------------------------------------
# Детектор сбоя аэропорта
# ---------------------------------------------------------------------------
# Массовое закрытие (дроны, погода, ATC) выглядит иначе, чем обычная задержка:
# проседает ВСЯ доска разом, а не отдельный рейс. Поэтому индекс считается по
# всем рейсам аэропорта в операционном окне, а не по нашим пяти.
#
# Сообщения идут ТОЛЬКО на смене состояния: "начался сбой" -> "восстановилось".
# Между ними молчим, кроме заметного ухудшения не чаще раза в stress_update_min.
# ---------------------------------------------------------------------------
_STRESS_QUIET: dict = {}


def airport_stress(airport: str, board: dict, cfg: dict) -> dict | None:
    tz = board.get("tz", 0)
    now = time.time()
    lo = now - cfg["stress_window_back_h"] * 3600
    hi = now + cfg["stress_window_fwd_h"] * 3600
    delays, cancelled = [], 0
    for r in board.get("DEP") or []:
        if not r.get("scheduled"):
            continue
        real_utc = to_utc(r["scheduled"], tz, airport, cfg)
        if real_utc is None or not lo <= real_utc <= hi:
            continue
        if classify_status(r.get("status"), r.get("generic")) == "CANCELLED":
            cancelled += 1
            continue
        est = r.get("real") or r.get("estimated") or r["scheduled"]
        delays.append(round((int(est) - int(r["scheduled"])) / 60))
    need = (cfg.get("stress_min_flights_by_airport") or {}).get(
        airport, cfg["stress_min_flights"])
    if len(delays) < need:
        # Раньше здесь был молчаливый return None, и для маленького аэропорта
        # это означало ПОСТОЯННУЮ слепоту, неотличимую от "всё спокойно".
        # Молчание детектора должно быть слышно хотя бы в логе.
        last = _STRESS_QUIET.get(airport, 0)
        if now - last > 12 * 3600:
            _STRESS_QUIET[airport] = now
            log.warning("аэропорт %s: детектор неактивен - в окне %d рейсов "
                        "при пороге %d; сбои этого аэропорта я не увижу",
                        airport, len(delays), need)
        return None
    avg = round(sum(delays) / len(delays))
    bad = sum(1 for d in delays if d >= 60)
    return {"flights": len(delays), "avg": avg, "max": max(delays),
            "cancelled": cancelled, "share_bad": round(100 * bad / len(delays), 1)}


def stress_level(m: dict, cfg: dict) -> str:
    if m["avg"] >= cfg["outage_enter_avg"] or m["cancelled"] >= cfg["outage_enter_cancelled"]:
        return "outage"
    if m["avg"] >= cfg["stress_enter_avg"]:
        return "stress"
    return "norm"


RU_STATE = {"norm": "норма", "stress": "напряжённо", "outage": "СБОЙ"}
AP_NAME = {"SVO": "Шереметьево", "DME": "Домодедово", "VKO": "Внуково",
           "EVN": "Ереван", "TIV": "Тиват", "STW": "Ставрополь",
           "KRR": "Краснодар", "BEG": "Белград", "LCA": "Ларнака"}


def airport_relevant(airport: str, cfg: dict, state: dict) -> bool:
    """Есть ли рейс, которому этот аэропорт важен прямо сейчас.

    Аэропорт попадает в опрос за сутки-двое до рейса и остаётся там, пока
    рейс не выбыл, поэтому сам факт опроса ничего не значит. Значение имеет
    близость: вылет впереди в пределах окна либо только что состоявшийся.
    """
    now = time.time()
    window = cfg["airport_alert_within_h"] * 3600
    for spec in cfg["flights"]:
        st = state.get(slug_of(spec)) or {}
        if airport not in (spec.get("airport"), spec.get("to"), st.get("learned_to")):
            continue
        if retired_reason(spec, st, cfg):
            continue
        sched = st.get("sched_utc")
        if sched:
            if -4 * 3600 <= sched - now <= window:
                return True
        elif days_ahead(spec, cfg) == 0:
            # рейса ещё нет на табло: тогда ориентир один - дата в конфиге
            return True
    return False


def check_airport(airport: str, board: dict, bus, cfg: dict, state: dict) -> None:
    m = airport_stress(airport, board, cfg)
    if not m:
        return
    key = f"_ap_{airport}"
    st = state.setdefault(key, {})
    now = time.time()
    level = stress_level(m, cfg)
    current = st.get("state", "norm")

    # Гистерезис: вход по одному порогу, выход по ДРУГОМУ, более низкому,
    # и с большим числом подтверждений. Без этого на границе будет дребезг.
    if level != "norm" and current == "norm":
        st["up"] = st.get("up", 0) + 1; st["down"] = 0
        confirmed = st["up"] >= cfg["stress_confirm"]
    elif level == "norm" and current != "norm":
        st["down"] = st.get("down", 0) + 1 if m["avg"] < cfg["stress_exit_avg"] else 0
        st["up"] = 0
        confirmed = st["down"] >= cfg["stress_clear"]
    else:
        st["up"] = st["down"] = 0
        confirmed = level != current      # переход между stress и outage

    changed = False
    name = AP_NAME.get(airport, airport)
    if confirmed and level != current:
        changed = True
        st["state"] = level
        st["up"] = st["down"] = 0

    # Состояние считаем всегда: гистерезису нужна непрерывность, базе - история.
    # А вот РАССКАЗЫВАЕМ только когда через этот аэропорт кто-то вот-вот летит.
    # Человек летает нечасто, и в остальные дни "Шереметьево: напряжённо" -
    # чистый шум. Отметку told при молчании не двигаем: иначе к дню вылета
    # аэропорт мог бы уже лежать, а перехода - не случиться, и мы бы промолчали.
    now_state = st.get("state", "norm")
    told = st.get("told", "norm")
    who = {"flight": airport, "notify": True, "owner": "self",
           "title": f"Аэропорт {name}", "extra_sms_to": []}

    if not airport_relevant(airport, cfg, state):
        if now_state != told:
            log.info("аэропорт %s: сейчас %s (сообщали %s), но рейсов рядом нет - молчу",
                     airport, now_state, told)
    elif now_state != told and level == now_state:
        # объявляем, только когда гистерезис и свежий замер сошлись: иначе в
        # первый же "важный" цикл можно выпалить вчерашний СБОЙ и через пять
        # минут отбить его отбоем
        st["told"] = now_state
        st["announced_avg"] = m["avg"]
        st["announced_at"] = now
        if now_state == "norm":
            bus.notify(who, f"работа восстановилась, средняя задержка {m['avg']} мин",
                       event_class="airport_recovered", severity="digest")
        else:
            sev = "wake" if now_state == "outage" else "digest"
            extra = f", отмен {m['cancelled']}" if m["cancelled"] else ""
            bus.notify(who,
                       f"{RU_STATE[now_state]}: средняя задержка {m['avg']} мин по "
                       f"{m['flights']} рейсам, тяжёлых {m['share_bad']}%{extra}",
                       event_class="airport_stress", severity=sev)
    elif now_state == told and now_state != "norm":
        # уже сообщили - молчим, пока не станет ЗАМЕТНО хуже, и не чаще раза в час-полтора
        grew = m["avg"] - st.get("announced_avg", 0)
        if grew >= cfg["stress_update_step"] and \
                now - st.get("announced_at", 0) >= cfg["stress_update_min"] * 60:
            st["announced_avg"] = m["avg"]; st["announced_at"] = now
            bus.notify(who, f"хуже: средняя задержка выросла до {m['avg']} мин",
                       event_class="airport_stress", severity="digest")

    if STORE:
        STORE.run("""INSERT INTO airport_status (airport, checked_at, flights, avg_delay,
                        max_delay, cancelled, share_bad, state, changed)
                     VALUES (%s,NOW(3),%s,%s,%s,%s,%s,%s,%s)""",
                  (airport[:3], m["flights"], m["avg"], m["max"], m["cancelled"],
                   m["share_bad"], st.get("state", "norm"), 1 if changed else 0))

# ---------------------------------------------------------------------------
# Плановые сводки: сообщение приходит по расписанию, а не только по событию
# ---------------------------------------------------------------------------
def check_briefings(cfg, state, bus) -> None:
    """Отправляет сводку в заданное местное время аэропорта, один раз в сутки.

    Смысл не в новой информации, а в доказательстве, что система жива:
    без этого тишина неотличима от отказа.
    """
    now = time.time()
    for b in cfg.get("briefings") or []:
        local = local_dt(now, b["airport"], cfg)
        if local is None:
            continue
        hh, mm = (int(x) for x in b["at"].split(":"))
        due = local.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if local < due:
            continue                                  # ещё рано
        key = f"_brief_{b['name']}"
        if state.get(key, {}).get("sent") == local.strftime("%Y-%m-%d"):
            continue                                  # сегодня уже отправляли
        # если проспали больше двух часов (демон лежал) - молча помечаем
        stale = (local - due).total_seconds() > 2 * 3600

        lines = []
        for spec in cfg["flights"]:
            st = state.get(slug_of(spec)) or {}
            br = st.get("brief")
            if not br:
                continue
            left = (br.get("sched_utc", 0) - now) / 3600
            if not -2 <= left <= cfg["briefing_within_h"]:
                continue
            row = f"{br['title']} ({spec['flight']}): план {br['sched']}"
            if br.get("est") and br["est"] != br["sched"]:
                row += f", ожидается {br['est']}"
            if br.get("status"):
                row += f", {br['status']}"
            if br.get("gate"):
                row += f", гейт {br['gate']}"
            if br.get("predicted") is not None:
                row += f". Прогноз +{br['predicted']} мин"
            inb = br.get("inbound")
            if inb:
                row += f". Борт сюда ({inb['no']}): {inb['phase']}"
            lines.append(row)

        state.setdefault(key, {})["sent"] = local.strftime("%Y-%m-%d")
        if stale:
            log.warning("сводка %s пропущена: время вышло %.1f ч назад",
                        b["name"], (local - due).total_seconds() / 3600)
            continue
        if not lines:
            # летает человек нечасто - ежедневное "рейсов нет" было бы шумом
            log.info("сводка %s: рейсов в окне нет, не отправляю", b["name"])
            continue
        text = f"Сводка {b['at']}. " + " | ".join(lines)
        bus.notify({"flight": "brief", "notify": True, "owner": "self",
                    "title": None, "label": None, "extra_sms_to": []},
                   text, event_class="briefing", severity="wake")

class _FlightRef:
    """FlightRadarAPI.get_flight_details берёт от объекта только .id."""

    def __init__(self, fid: str):
        self.id = fid


def utc_hhmm(ts_utc, airport: str, cfg: dict) -> str:
    """Настоящий epoch -> местное время аэропорта."""
    dt = local_dt(ts_utc, airport, cfg)
    return dt.strftime("%H:%M") if dt else "?"


def check_landing(spec, sec, sec_tz, bus, cfg, st, api) -> None:
    """Факт прилёта в аэропорт назначения - отдельное событие.

    Взлёт закрывает вопрос только для того, кто летит. Встречающему нужен
    ровно момент посадки, а рейс после "ВЫЛЕТЕЛ" замолкал навсегда: сверка со
    встречным табло стоит под "not lead['real']", а слово "ПРИЛЕТЕЛ" было
    доступно только рейсам с direction=arrival.

    Юрисдикция посадки - у аэропорта прибытия, поэтому порядок такой: сначала
    его доска, затем карточка рейса в FR24 (Шереметьево по части рейсов факт
    на доске не публикует вовсе: SU1365 04.09 так и остался с "Unknown"), и
    лишь в конце расчёт по времени. Молчание после посадки хуже неточной
    минуты - но неточность мы называем вслух.
    """
    if st.get("landed_told") or not st.get("real"):
        return
    dest = spec.get("to") or st.get("learned_to")
    if not dest:
        return
    subj = st.get("subj") or spec.get("title") or f"Рейс {spec['flight']}"
    when_utc, local, exact, extra = None, None, True, ""

    if sec:
        cls = classify_status(sec.get("status"), sec.get("generic"))
        if sec.get("real") or cls == "LANDED":
            stamp = sec.get("real") or sec.get("estimated") or sec.get("scheduled")
            when_utc = to_utc(stamp, sec_tz, dest, cfg)
            local = hhmm(stamp, sec_tz)
        if sec.get("carousel"):
            extra = f", лента {sec['carousel']}"

    if when_utc is None and st.get("fr24_id") and api is not None:
        try:
            det = api.get_flight_details(_FlightRef(st["fr24_id"]))
            real = ((det.get("time") or {}).get("real") or {}).get("arrival")
            if real:
                when_utc = int(real)
                local = utc_hhmm(when_utc, dest, cfg)
        except Exception as exc:
            log.warning("%s: карточка FR24 недоступна: %s", spec["flight"], exc)

    if when_utc is None:
        est = st.get("arr_est_utc")
        if not est or time.time() - est < cfg["landing_assume_min"] * 60:
            return
        when_utc, exact = est, False
        local = utc_hhmm(est, dest, cfg)

    st["landed_utc"] = when_utc
    st["landed_told"] = int(time.time())
    st.pop("await_landing", None)

    # перезапуск демона не повод объявлять вчерашнюю посадку
    if time.time() - when_utc > cfg["landing_stale_h"] * 3600:
        log.info("%s: посадка в %s была %.1f ч назад, не объявляю",
                 spec["flight"], local, (time.time() - when_utc) / 3600)
        return

    where = AP_NAME.get(dest, dest)
    if exact:
        text = f"{subj}: ПРИЛЕТЕЛ в {where} в {local}{extra}"
    else:
        text = (f"{subj}: по расчёту сел в {where} около {local}, "
                f"факт табло не подтвердило")
    bus.notify(spec, text, event_class="landed", severity="wake")


def check_connection(spec, lead, lead_tz, bus, cfg, state, st) -> None:
    """Запас на стыковку: считаем от ФАКТИЧЕСКОГО прилёта входящего рейса.

    В режиме пересадки "время выезда" бессмысленно - выезжать неоткуда.
    Значимая величина одна: успеваем ли от прилёта предыдущего борта
    до закрытия выхода на следующий, с учётом минимального времени
    на пересадку.
    """
    inbound_slug = spec.get("inbound_slug")
    if not inbound_slug or not lead or lead.get("real"):
        return
    inb = state.get(inbound_slug) or {}
    arr_utc = inb.get("arr_real_utc") or inb.get("arr_est_utc")
    if not arr_utc:
        return
    dep_utc = to_utc(lead["real"] or lead["estimated"] or lead["scheduled"],
                     lead_tz, spec["airport"], cfg)
    if dep_utc is None:
        return
    if not 0 < (dep_utc - time.time()) / 3600 <= cfg["connect_window_hours"]:
        return

    gate_utc = dep_utc - spec["gate_close_min"] * 60
    buffer_min = round((gate_utc - (int(arr_utc) + spec["mct_min"] * 60)) / 60)
    risk = "broken" if buffer_min < 0 else (
        "tight" if buffer_min < cfg["connect_tight_min"] else "ok")
    st["connect_buffer"] = buffer_min
    st["connect_risk"] = risk

    if STORE:
        STORE.run("""UPDATE flight_state SET regime="connection",
                        connect_buffer_min=%s, connect_risk=%s,
                        inbound_actual_local=%s
                     WHERE flight_id=%s""",
                  (buffer_min, risk,
                   local_dt(arr_utc, spec["airport"], cfg).strftime("%Y-%m-%d %H:%M:%S"),
                   STORE.fid(spec)))

    if risk == "ok":
        st.pop("connect_told", None)
        return
    # не повторяем одно и то же: сообщаем при смене класса риска или при
    # заметном ухудшении запаса
    told = st.get("connect_told")
    if told and told[0] == risk and abs(told[1] - buffer_min) < cfg["tg_shift_min"]:
        return
    st["connect_told"] = (risk, buffer_min)

    fmt = lambda u: local_dt(u, spec["airport"], cfg).strftime("%H:%M")
    if risk == "broken":
        text = (f"СТЫКОВКА НЕ СХОДИТСЯ: входящий прилетает {fmt(arr_utc)}, "
                f"выход на {spec['flight']} закрывается {fmt(gate_utc)}. "
                f"Не хватает {human_delay(abs(buffer_min))}")
    else:
        text = (f"Стыковка впритык: входящий прилетает {fmt(arr_utc)}, "
                f"на пересадку останется {human_delay(buffer_min)} "
                f"при минимуме {spec['mct_min']} мин (выход {spec['flight']} "
                f"закрывается {fmt(gate_utc)})")
    bus.notify(spec, text, event_class="connect_risk", severity="wake")


def process(spec, pri, tz, sec, sec_tz, bus: Bus, cfg: dict, state: dict,
            degraded: bool = False) -> None:
    """pri - запись со своего табло, sec - она же с табло аэропорта-контрагента."""
    slug = slug_of(spec)
    st = state.setdefault(slug, {})
    st["last_probe"] = int(time.time())
    label = spec["flight"]
    depart = spec["direction"] == "departure"
    verb = "вылет" if depart else "прилёт"

    if pri is None and sec is None:
        # Источник упал - это НЕ исчезновение рейса. Держим последнее известное
        # состояние и молчим: отсутствие в запасном источнике с более узким
        # горизонтом не является доказательством отсутствия рейса.
        if degraded:
            log.info("%s: источники деградировали, пропуск цикла без выводов", label)
            return
        misses = st.get("misses", 0) + 1
        st["misses"] = misses
        need = cfg["missing_confirm_cycles"]

        # далеко до вылета - молчим: пропасть с табло там нормально
        near = False
        sched = st.get("sched_utc")
        if sched:
            near = 0 < (sched - time.time()) / 3600 <= cfg["vanish_window_hours"]

        if st.get("ever_seen") and st.get("present") and misses >= need \
                and near and not st.get("vanished_told"):
            bus.notify(spec, f"Рейс {label}: пропал с табло {spec['airport']}"
                             f" ({misses} проверки подряд)",
                       event_class="vanished", severity="wake")
            st["present"] = False
            st["vanished_told"] = int(time.time())
        else:
            if misses <= need or misses % 20 == 0:
                log.info("%s: не найден (%d проверок подряд, порог %d)",
                         label, misses, need)
        return

    st["misses"] = 0
    st["ever_seen"] = True
    st.pop("vanished_told", None)

    # ведущая запись - своя; если своё табло рейс потеряло, ведём по чужому
    lead, lead_tz, by_other = (pri, tz, False) if pri else (sec, sec_tz, True)
    if by_other:
        log.info("%s: своё табло %s рейс не показывает, веду по контрагенту",
                 label, spec["airport"])
    # Ведущая запись со встречного табло описывает ДРУГОЕ плечо: там "факт" -
    # это посадка, а не вылет. Подписывать её своим глаголом нельзя, иначе
    # приходит "ВЫЛЕТЕЛ в 12:48" про борт, который в 12:48 сел.
    lead_verb = verb if not by_other else ("прилёт" if depart else "вылет")

    # запоминаем контрагента, чтобы в следующий раз сверяться без ручной настройки
    if pri and pri["other_iata"] and not spec.get("to"):
        st["learned_to"] = pri["other_iata"]

    su = to_utc(lead["scheduled"], lead_tz, spec["airport"], cfg)
    if su:
        st["sched_utc"] = su

    delay = delay_minutes(lead)
    sched_s = hhmm(lead["scheduled"], lead_tz)
    est_s = hhmm(lead["real"] or lead["estimated"] or lead["scheduled"], lead_tz)
    # BEG отдаёт город ("Tivat"), а не IATA - иначе маршрут вырождался в "BEG-"
    other_iata = ((pri or sec)["other_iata"] or spec.get("to")
                  or st.get("learned_to") or "?")
    route = f"{spec['airport']}-{other_iata}" if depart else f"{other_iata}-{spec['airport']}"

    # Имя рейса впереди, чтобы уведомление читалось без словаря, но номер
    # остаётся рядом: по нему отслеживают вручную и различают соседние рейсы.
    # Собирается ПОСЛЕ route: раньше строка стояла выше и на рейсе без title
    # роняла весь разбор через UnboundLocalError.
    subj = f"{spec['title']} ({label})" if spec.get("title") else f"Рейс {label} {route}"
    st["subj"] = subj

    # вторая доска: расчётное время на той стороне
    sec_est = (sec["real"] or sec["estimated"]) if sec else None
    sec_delay = delay_minutes(sec) if sec else None

    bus.state(slug, {
        "status": lead["status"], "generic": lead["generic"], "delay_min": delay,
        "estimated_local": est_s, "scheduled_local": sched_s,
        "flight": lead["number"], "route": route,
        "counterpart": f"{(pri or sec)['other'] or ''} ({other_iata or ''})".strip(),
        "aircraft": f"{lead['aircraft'] or ''} {lead['reg'] or ''}".strip(),
        "gate": lead["gate"] or "", "checkin": lead["checkin"] or "",
        "carousel": (sec["carousel"] if sec else lead["carousel"]) or "",
        "done": bool(lead["real"]), "direction": spec["direction"],
        "source": lead["source"],
        # что видно на встречном табло - для сверки глазами
        "peer_source": sec["source"] if sec else "",
        "peer_status": sec["status"] if sec else "",
        "peer_time_local": hhmm(sec_est, sec_tz) if sec_est else "",
        "peer_delay_min": sec_delay,
        "updated": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    })

    # У источников РАЗНАЯ шкала времени: FR24 отдаёт настоящий epoch со
    # смещением аэропорта, родные табло - местное время при tz=0. Сравнивать
    # метки, снятые разными провайдерами, нельзя: смена источника выглядела бы
    # как скачок расписания на часы. Поэтому при смене провайдера просто
    # переснимаем базу, ничего не рассылая.
    src_kind = (lead["source"] or "").split()[0]
    switched = st.get("src_kind") not in (None, src_kind)

    if STORE:
        fid = STORE.fid(spec)
        pri_leg = "ARR" if spec["direction"] == "arrival" else "DEP"
        if pri:
            STORE.observe(fid, pri, tz,
                          "destination" if spec["direction"] == "arrival" else "origin", pri_leg)
        if sec:
            STORE.observe(fid, sec, sec_tz,
                          "origin" if spec["direction"] == "arrival" else "destination",
                          "DEP" if pri_leg == "ARR" else "ARR")
        STORE.state(fid, {
            "status_class": classify_status(lead["status"], lead["generic"]),
            "scheduled_local": hhmm_full(lead["scheduled"], lead_tz),
            "estimated_local": hhmm_full(lead["estimated"], lead_tz),
            "actual_local": hhmm_full(lead["real"], lead_tz),
            "tz_offset_min": lead_tz // 60, "delay_min": delay,
            "gate": lead["gate"] or None, "checkin_desks": lead["checkin"] or None,
            "conflict": 0, "degraded_sources": "деградация" if degraded else None,
        })

    dest_iata = spec.get("to") or st.get("learned_to")
    if sec and dest_iata:
        st["arr_est_utc"] = to_utc(sec["real"] or sec["estimated"] or sec["scheduled"],
                                   sec_tz, dest_iata, cfg)
        st["arr_real_utc"] = to_utc(sec["real"], sec_tz, dest_iata, cfg)
    for row in (sec, lead):
        if row and row.get("fr24_id"):
            st["fr24_id"] = row["fr24_id"]
            break

    st["brief"] = {"title": spec.get("title") or spec["flight"],
                   "sched": sched_s, "est": est_s, "status": lead["status"],
                   "gate": lead["gate"] or None, "sched_utc": su or 0,
                   "predicted": st.get("last_predicted"),
                   "inbound": st.get("inbound")}

    snapshot = {"present": True, "status": lead["status"], "src_kind": src_kind,
                "estimated": lead["real"] or lead["estimated"],
                "real": lead["real"], "delay_min": delay,
                "gate": lead["gate"], "checkin": lead["checkin"],
                "peer_est": sec_est, "peer_status": sec["status"] if sec else None,
                "peer_kind": (sec["source"] or "").split()[0] if sec else None}

    if switched:
        log.info("%s: источник сменился %s -> %s, переснимаю базу без оповещения",
                 label, st.get("src_kind"), src_kind)
        st.update(snapshot)
        return

    if "status" not in st:
        log.info("%s: базовое состояние без оповещения — %s, %s %s (план %s), задержка %s%s",
                 label, lead["status"], lead_verb, est_s, sched_s,
                 human_delay(delay) if delay is not None else "?",
                 f"; встречное табло: {sec['status']}" if sec else "")
        st.update(snapshot)
        return

    messages: list[tuple[str, str, str]] = []   # (класс события, важность, текст)
    tail = f"{lead_verb.capitalize()} {est_s} (план {sched_s})"
    threshold = cfg["delay_threshold_min"]
    now_ts = int(time.time())
    lead_class = classify_status(lead["status"], lead["generic"])

    if lead["real"] and not st.get("real") and STORE:
        # факт известен - закрываем все прогнозы по этому рейсу
        try:
            STORE.settle(spec["flight"], spec["date"],
                         delay if delay is not None else 0)
        except Exception:
            log.exception("не смог закрыть прогнозы по %s", label)

    if lead["real"] and not st.get("real"):
        # Факт объявляем только по СВОЕЙ доске - юрисдикция. Если ведём по
        # встречной, её факт закроет check_landing правильным словом.
        if not by_other:
            word = "ВЫЛЕТЕЛ" if depart else "ПРИЛЕТЕЛ"
            messages.append(("departed", "wake",
                             f"{subj}: {word} в {hhmm(lead['real'], lead_tz)}"))
        else:
            log.info("%s: факт %s пришёл со встречного табло - своим глаголом "
                     "не подписываю", label, hhmm(lead["real"], lead_tz))
        st["done_at"] = now_ts
        if depart and (spec.get("to") or st.get("learned_to")):
            # тому, кто встречает, нужен не взлёт, а посадка - рейс не бросаем
            st["await_landing"] = True

    # Сравниваем не с прошлым наблюдением, а с тем, о чём УЖЕ сообщали:
    # иначе ползучий сдвиг 10:05→10:30→10:40→10:45 даёт три сообщения подряд.
    new_est = snapshot["estimated"]
    if new_est and not lead["real"]:
        base_tg = st.get("notified_est") or st.get("estimated")
        base_sms = st.get("sms_est") or st.get("estimated")
        shift_tg = round((int(new_est) - int(base_tg)) / 60) if base_tg else 0
        shift_sms = round((int(new_est) - int(base_sms)) / 60) if base_sms else 0
        left = minutes_until(new_est, lead_tz, spec["airport"], cfg)
        gap_ok = (now_ts - st.get("sms_est_at", 0)) >= cfg["delay_notice_gap_min"] * 60

        # СМС: крупный сдвиг ЛИБО любой заметный, когда до вылета уже близко -
        # в аэропорту важны и пять минут
        urgent = abs(shift_sms) >= cfg["sms_shift_min"] or (
            left is not None and left <= cfg["urgent_window_min"]
            and abs(shift_sms) >= threshold)

        def phrase_for(sh):
            word = "сдвинут на" if sh > 0 else "перенесён раньше на"
            extra = f", всего {human_delay(delay)}" if delay else ""
            return f"{subj}: {word} {human_delay(sh)}. {tail}{extra}"

        if urgent and (gap_ok or abs(shift_sms) >= cfg["sms_shift_min"]):
            messages.append(("delay_shift", "wake", phrase_for(shift_sms)))
            st["notified_est"] = st["sms_est"] = new_est
            st["notified_est_at"] = st["sms_est_at"] = now_ts
        elif abs(shift_tg) >= cfg["tg_shift_min"]:
            messages.append(("delay_shift", "info", phrase_for(shift_tg)))
            st["notified_est"] = new_est
            st["notified_est_at"] = now_ts

    # Сырые коды табло (NBD, GTC, TAX) человеку не отправляем - только то,
    # что удалось опознать. Неопознанное копится в БД для разбора.
    phrase = status_phrase(lead["status"], lead_class)
    # У посадки есть собственное сообщение с аэропортом и лентой (check_landing).
    # Смена статуса на LANDED продублировала бы его вторым уведомлением подряд.
    if depart and lead_class == "LANDED":
        phrase = None
    if phrase and lead_class != st.get("status_class") and not messages:
        where = f" Гейт {lead['gate']}." if lead["gate"] else ""
        sev = "wake" if lead_class in ("CANCELLED", "GATE_CLOSED", "BOARDING") else "digest"
        # отмена - отдельный класс: пробивает и чужой рейс, и ночь
        kind = "cancelled" if lead_class == "CANCELLED" else "status_change"
        messages.append((kind, sev,
                         f"{subj}: {phrase}.{where} {tail}"))

    # гейт меняют молча и в последний момент - это стоит отдельного сообщения
    if lead["gate"] and st.get("gate") and lead["gate"] != st["gate"]:
        messages.append(("gate_change", "wake",
                         f"{subj}: гейт изменён "
                         f"{st['gate']} -> {lead['gate']}. {tail}"))

    # Встречное табло как отдельный источник: доски обновляются независимо,
    # и перенос там иногда виден раньше. Сообщаем только если своя доска
    # промолчала - иначе на одно событие пришло бы два сообщения.
    if sec and not messages and not lead["real"]:
        peer_word = "прибытие" if depart else "отправление"
        old_peer = st.get("peer_est")
        peer_same = st.get("peer_kind") == snapshot["peer_kind"]
        if old_peer and sec_est and peer_same:
            shift = round((int(sec_est) - int(old_peer)) / 60)
            if abs(shift) >= threshold:
                messages.append(("peer_shift", "wake",
                    f"{subj}: по табло {sec['source']} "
                    f"{peer_word} сдвинуто на {human_delay(shift)} -> "
                    f"{hhmm(sec_est, sec_tz)}"))
        # первое появление встречной доски - это не событие, а точка отсчёта:
        # иначе на каждый рейс приходило бы "встречное табло — Вовремя"
        first_peer = st.get("peer_status") is None or not peer_same
        peer_phrase = status_phrase(sec["status"], classify_status(sec["status"], sec["generic"]))
        if not messages and peer_phrase and not first_peer \
                and sec["status"] != st.get("peer_status"):
            messages.append(("peer_status", "digest",
                             f"{subj}: встречное табло — {peer_phrase}. {tail}"))

    for event_class, severity, text in messages:
        bus.notify(spec, text, event_class=event_class, severity=severity)

    st.update(snapshot)
    st["status_class"] = lead_class


def days_ahead(spec: dict, cfg: dict | None = None) -> int:
    """Календарные дни до вылета.

    Именно календарные: timedelta.days округляет вниз, и рейс послезавтра
    утром выглядел бы как "через 1 день".

    "Сегодня" считаем по местной дате АЭРОПОРТА, а не сервера: в Алматы уже
    завтра, когда в Москве ещё сегодня, и один и тот же рейс на протяжении
    пары часов около полуночи оказывался то ближним, то дальним.
    """
    try:
        day = datetime.strptime(spec["date"], "%Y-%m-%d").date()
    except ValueError:
        return 0
    here = None
    if cfg is not None:
        here = local_dt(time.time(), spec.get("airport"), cfg)
    return (day - (here.date() if here else datetime.now().date())).days


def retired_reason(spec: dict, st: dict, cfg: dict) -> str | None:
    """Насовсем ли рейс выбыл из опроса."""
    done_at = st.get("done_at")
    if done_at and time.time() - done_at > cfg["retire_after_hours"] * 3600:
        # вылетел, но факта посадки ещё нет: держим - но не бесконечно
        if st.get("await_landing") and not st.get("landed_told") \
                and time.time() - done_at < cfg["landing_wait_hours"] * 3600:
            return None
        return "завершён"
    if -days_ahead(spec, cfg) > cfg["stale_after_days"]:
        return "устарел"
    return None


def poll_plan(cfg: dict, state: dict) -> tuple[list, list]:
    """Кого опрашиваем в этом цикле, кого пропускаем и почему.

    Горизонт табло у каждого аэропорта свой и зависит от загруженности:
    в тихом Тивате 118 рейсов растянуты на четверо суток, в Ереване 150 -
    на двое. Поэтому горизонт не угадываем, а раз в полчаса заглядываем:
    как только рейс появился на табло, переходим на быстрый темп.
    """
    now = time.time()
    due, skipped = [], []
    for spec in cfg["flights"]:
        st = state.get(slug_of(spec), {})
        reason = retired_reason(spec, st, cfg)
        if reason:
            skipped.append((spec, reason))
            continue
        if days_ahead(spec, cfg) <= cfg["lookahead_days"] or st.get("ever_seen"):
            due.append(spec)
            continue
        waited = now - st.get("last_probe", 0)
        if waited >= cfg["slow_poll_seconds"]:
            due.append(spec)
        else:
            left = int((cfg["slow_poll_seconds"] - waited) / 60)
            skipped.append((spec, f"нет на табло, до вылета {days_ahead(spec, cfg)} дн., "
                                  f"проверю через {left} мин"))
    return due, skipped


# ---------------------------------------------------------------------------
# цикл
# ---------------------------------------------------------------------------
running = True
PROVIDER_COOLDOWN: dict = {}


def stop(signum, frame):
    global running
    running = False
    log.info("сигнал %s, останавливаюсь", signum)


def cycle(api, bus: Bus, cfg: dict, state: dict) -> None:
    active, dormant = poll_plan(cfg, state)

    if dormant:
        log.debug("вне опроса: %s",
                  "; ".join(f"{title_of(s)} — {r}" for s, r in dormant))
    if not active:
        log.info("активных рейсов нет (в ожидании: %s)",
                 ", ".join(f"{title_of(s)} — {r}" for s, r in dormant) or "-")
        return

    # Один и тот же рейс стоит на ДВУХ табло: вылета в аэропорту отправления
    # и прилёта в аэропорту назначения. Сверяемся по обоим - это и запасной
    # источник, если своё табло рейс потеряло, и иногда более раннее
    # обновление, потому что доски обновляются независимо друг от друга.
    needs: dict[str, dict] = {}

    def want(airport, date_set, leg):
        if not airport:
            return
        slot = needs.setdefault(airport, {"dates": set(), "legs": set()})
        slot["dates"] |= date_set
        slot["legs"].add(leg)

    plan = []
    for spec in active:
        pri_leg = "ARR" if spec["direction"] == "arrival" else "DEP"
        sec_leg = "DEP" if pri_leg == "ARR" else "ARR"
        other = spec.get("to") or state.get(slug_of(spec), {}).get("learned_to")
        want(spec["airport"], {spec["date"]}, pri_leg)
        # прилёт может прийтись уже на следующие сутки
        sec_dates = {spec["date"], next_day(spec["date"])}
        want(other, sec_dates, sec_leg)
        plan.append((spec, pri_leg, sec_leg, other, sec_dates))

    boards = {}
    for airport, slot in needs.items():
        boards[airport] = get_board(api, airport, slot["dates"], slot["legs"],
                                    cfg, PROVIDER_COOLDOWN)
        try:
            check_airport(airport, boards[airport], bus, cfg, state)
        except Exception:
            log.exception("сбой детектора по аэропорту %s", airport)

    for spec, pri_leg, sec_leg, other, sec_dates in plan:
        pb = boards[spec["airport"]]
        pri = match_flight(spec, pb[pri_leg], pb["tz"])
        sec, sec_tz, sb_degraded = None, 0, False
        if other and other in boards:
            sb = boards[other]
            sec_tz = sb["tz"]
            sb_degraded = bool(sb.get("degraded"))
            sec = match_any(spec["flight"], sb[sec_leg], sec_tz, sec_dates)
        degraded = bool(pb.get("degraded")) or (sb_degraded if other else False)
        process(spec, pri, pb["tz"], sec, sec_tz, bus, cfg, state, degraded)
        if STORE:
            # тот же номер рейса за другие дни - бесплатная статистика
            # пунктуальности: доски всё равно уже скачаны
            watch = set(cfg["watch_numbers"]) | {spec["flight"]}
            for leg in ("DEP", "ARR"):
                for row in pb.get(leg) or []:
                    num = (row["number"] or "").upper()
                    if num in watch and leg == "DEP":
                        STORE.punctuality(num, spec["airport"], row, pb["tz"])
            STORE.waves(spec["airport"], pb, [pri_leg])
            if pri:
                record_prediction(spec, pri, pb["tz"], cfg,
                                  state.setdefault(slug_of(spec), {}), board=pb)
        if spec["direction"] == "departure":
            try:
                check_landing(spec, sec, sec_tz, bus, cfg,
                              state.setdefault(slug_of(spec), {}), api)
            except Exception:
                log.exception("сбой проверки посадки по %s", spec["flight"])
        if pri and spec.get("inbound_slug"):
            try:
                check_connection(spec, pri, pb["tz"], bus, cfg, state,
                                 state.setdefault(slug_of(spec), {}))
            except Exception:
                log.exception("сбой расчёта стыковки по %s", spec["flight"])
        if pri and spec.get("aircraft_reg"):
            try:
                check_rotation(api, spec, pri, pb["tz"], bus, cfg,
                               state.setdefault(slug_of(spec), {}))
            except Exception:
                log.exception("сбой проверки ротации по %s", spec["flight"])

    try:
        bus.sweep_sms()
    except Exception:
        log.exception("сбой проверки подтверждений СМС")

    try:
        check_briefings(cfg, state, bus)
    except Exception:
        log.exception("сбой плановой сводки")

    save_state(state)


def sync_discovery(bus: Bus, cfg: dict, state: dict) -> None:
    """Заводит сенсоры для новых рейсов и снимает для выбывших."""
    want = {slug_of(s): s for s in cfg["flights"]}
    known = set(state.get("_discovered", []))
    for slug, spec in want.items():
        if slug not in known:
            bus.discovery(spec)
    for slug in known - set(want):
        bus.drop_discovery(slug)
        state.pop(slug, None)
    state["_discovered"] = sorted(want)


def run_daemon() -> int:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    global STORE
    cfg = load_config()
    setup_logging(cfg)
    set_topic_prefix(cfg)
    state = load_state()
    check_offset_table(cfg)
    STORE = Store(cfg)
    STORE.sync_flights(cfg)
    check_watch_numbers(cfg)
    if STORE.alive():
        log.info("БД: подключена, рейсов в реестре %d", len(STORE.flight_ids))
    elif STORE.enabled:
        log.error("БД ВКЛЮЧЕНА В КОНФИГЕ, НО НЕ ОТВЕЧАЕТ: истории, прогнозов и "
                  "статистики не будет, уведомления продолжат работать")
    else:
        log.info("БД: выключена настройкой")
    api = FlightRadar24API()
    bus = Bus(cfg)
    sync_discovery(bus, cfg, state)
    save_state(state)

    stamp = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0
    log.info("слежу за %s рейсами: %s", len(cfg["flights"]),
             ", ".join(title_of(s) for s in cfg["flights"]) or "-")

    once = "--once" in sys.argv
    while running:
        try:
            new_stamp = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0
            if new_stamp != stamp:
                stamp = new_stamp
                was = {slug_of(f): title_of(f) for f in cfg["flights"]}
                cfg = load_config()
                bus.cfg = cfg
                # Список рейсов правит не только человек: агент делает это
                # без подтверждения. Снятое накануне вылета иначе исчезло бы
                # молча, и человек узнал бы об этом по отсутствию сообщений.
                now_ = {slug_of(f): title_of(f) for f in cfg["flights"]}
                added = [now_[k] for k in now_.keys() - was.keys()]
                gone = [was[k] for k in was.keys() - now_.keys()]
                # Telegram может быть намеренно не настроен - тогда это не
                # отказ канала, а выбранная конфигурация, и ошибкой в журнале
                # она быть не должна: на каждой правке списка получался ERROR.
                # Проверять надо именно токен, а не channels: в DEFAULTS
                # telegram в списке каналов есть всегда.
                if (added or gone) and cfg.get("notify", True) and bus._token:
                    parts = []
                    if added:
                        parts.append("добавлено: " + ", ".join(sorted(added)))
                    if gone:
                        parts.append("снято: " + ", ".join(sorted(gone)))
                    try:
                        bus.telegram("Список рейсов изменён — " + "; ".join(parts))
                    except Exception as exc:
                        log.error("не смог сообщить об изменении списка: %s", exc)
                sync_discovery(bus, cfg, state)
                if STORE:
                    STORE.sync_flights(cfg)
                log.info("конфиг перечитан, рейсов: %s — %s", len(cfg["flights"]),
                         ", ".join(title_of(s) for s in cfg["flights"]) or "-")
            cycle(api, bus, cfg, state)
        except Exception:
            log.exception("сбой цикла, продолжаю")
        if once:
            break
        for _ in range(cfg["poll_seconds"]):
            if not running:
                break
            time.sleep(1)

    bus.client.publish(AVAILABILITY_TOPIC, "offline", qos=1, retain=True)
    time.sleep(0.3)
    bus.client.loop_stop()
    return 0


# ---------------------------------------------------------------------------
# CLI управления списком
# ---------------------------------------------------------------------------
def cli_list(cfg: dict) -> int:
    if not cfg["flights"]:
        print("список пуст")
        return 0
    state = load_state()
    for spec in cfg["flights"]:
        st = state.get(slug_of(spec), {})
        reason = retired_reason(spec, st, cfg)
        if not reason and not st.get("ever_seen") and days_ahead(spec, cfg) > cfg["lookahead_days"]:
            reason = f"ещё нет на табло, до вылета {days_ahead(spec, cfg)} дн."
        mark = reason or ("завершён" if st.get("real")
                          else (st.get("status") or "нет данных"))
        peer = st.get("peer_status")
        if peer and not reason:
            mark += f"  | встречное: {peer}"
        d = st.get("delay_min")
        extra = f", задержка {human_delay(d)}" if d else ""
        print(f"  {title_of(spec):<34} {mark}{extra}")
    return 0


def cli_add(cfg: dict, args) -> int:
    spec = {"flight": args.add[0].upper().replace(" ", ""), "date": args.add[1],
            "airport": args.add[2].upper(),
            "direction": "arrival" if args.arrival else "departure"}
    datetime.strptime(spec["date"], "%Y-%m-%d")   # валидация даты
    if any(slug_of(s) == slug_of(spec) for s in cfg["flights"]):
        print("такой рейс уже в списке")
        return 1
    if args.sms_to:
        spec["sms_to"] = args.sms_to
    if args.to:
        spec["to"] = args.to.upper().strip()
    cfg["flights"].append(spec)
    save_config(cfg)
    print(f"добавлен: {title_of(spec)} — демон подхватит в течение {cfg['poll_seconds']} с")
    return 0


def cli_remove(cfg: dict, args) -> int:
    target = {"flight": args.remove[0].upper().replace(" ", ""), "date": args.remove[1],
              "airport": args.remove[2].upper(),
              "direction": "arrival" if args.arrival else "departure"}
    before = len(cfg["flights"])
    cfg["flights"] = [s for s in cfg["flights"] if slug_of(s) != slug_of(target)]
    if len(cfg["flights"]) == before:
        print("такого рейса в списке нет")
        return 1
    save_config(cfg)
    print(f"удалён: {title_of(target)}")
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    sys.stdout.reconfigure(line_buffering=True)
    p = argparse.ArgumentParser(description="слежение за рейсами через FR24")
    p.add_argument("--list", action="store_true", help="показать отслеживаемые рейсы")
    p.add_argument("--add", nargs=3, metavar=("РЕЙС", "ДАТА", "IATA"),
                   help="добавить рейс, дата в формате ГГГГ-ММ-ДД")
    p.add_argument("--remove", nargs=3, metavar=("РЕЙС", "ДАТА", "IATA"),
                   help="убрать рейс")
    p.add_argument("--arrival", action="store_true",
                   help="следить за прилётом, а не вылетом")
    p.add_argument("--sms-to", help="отдельный номер для этого рейса")
    p.add_argument("--to", metavar="IATA",
                   help="аэропорт-контрагент для сверки по встречному табло "
                        "(определяется сам, если не задать)")
    p.add_argument("--once", action="store_true", help="один проход и выход")
    args = p.parse_args()

    if args.list or args.add or args.remove:
        cfg = load_config()
        if args.list:
            return cli_list(cfg)
        return cli_add(cfg, args) if args.add else cli_remove(cfg, args)

    return run_daemon()


if __name__ == "__main__":
    sys.exit(main())
