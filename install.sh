#!/usr/bin/env bash
# Установка flightwatch на чистый сервер Debian 12 / Ubuntu 22.04+.
#
# Скрипт идемпотентный: повторный запуск ничего не ломает и не перетирает
# уже заданные секреты. Каждый шаг заканчивается проверкой; при неудаче
# скрипт останавливается и печатает, что именно не сошлось.
#
#   sudo ./install.sh                       # спросит секреты
#   FW_TG_TOKEN=... FW_TG_CHAT=... \
#   FW_SMS_TO=+7... sudo -E ./install.sh --yes    # без вопросов
#
# Флаги:
#   --yes            не задавать вопросов (значения берутся из переменных)
#   --with-fwapi     поставить ещё и контракт для агента (fwapi + fwctl)
#   --skip-db        база уже готова, не трогать
#   --skip-broker    mosquitto уже настроен, не трогать
#   --skip-packages  ничего не ставить через apt
set -euo pipefail

BASE=/opt/flightwatch
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSUME_YES=0; WITH_FWAPI=0; SKIP_DB=0; SKIP_BROKER=0; SKIP_PKGS=0
for a in "$@"; do case "$a" in
  --yes|-y) ASSUME_YES=1 ;;
  --with-fwapi) WITH_FWAPI=1 ;;
  --skip-db) SKIP_DB=1 ;;
  --skip-broker) SKIP_BROKER=1 ;;
  --skip-packages) SKIP_PKGS=1 ;;
  -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
  *) echo "неизвестный флаг: $a" >&2; exit 2 ;;
esac; done

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '   ok: %s\n' "$*"; }
die()  { printf '\n\033[31mОСТАНОВ: %s\033[0m\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "запускать под root (sudo ./install.sh)"
command -v apt-get >/dev/null || die "скрипт рассчитан на Debian/Ubuntu (нет apt-get)"

# systemd есть не везде (контейнер, WSL). Без него ставим всё, кроме служб.
if [ -d /run/systemd/system ]; then HAVE_SYSTEMD=1; else HAVE_SYSTEMD=0
  echo "ВНИМАНИЕ: systemd не обнаружен — юниты будут скопированы, но не запущены."
fi

ask() { # ask ПЕРЕМЕННАЯ "вопрос" "значение-по-умолчанию"
  local var="$1" prompt="$2" def="${3:-}" cur="${!1:-}"
  if [ -n "$cur" ]; then printf -v "$var" '%s' "$cur"; return; fi
  if [ "$ASSUME_YES" = 1 ]; then printf -v "$var" '%s' "$def"; return; fi
  read -r -p "$prompt${def:+ [$def]}: " val </dev/tty || true
  printf -v "$var" '%s' "${val:-$def}"
}
rndpass() { head -c 18 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

# --- 1. пакеты -------------------------------------------------------------
say "1/9 пакеты"
if [ "$SKIP_PKGS" = 0 ]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq python3 python3-venv python3-pip tzdata ca-certificates \
      mariadb-server mariadb-client mosquitto mosquitto-clients >/dev/null
fi
for b in python3 mysql mosquitto_passwd; do command -v "$b" >/dev/null || die "нет $b"; done
ok "python $(python3 -V 2>&1 | cut -d' ' -f2), $(mysql --version | cut -d, -f1)"

# --- 2. пользователь и каталоги -------------------------------------------
say "2/9 пользователь и каталоги"
id -u flightwatch >/dev/null 2>&1 || useradd -r -M -s /usr/sbin/nologin flightwatch
install -d -o root -g root -m 755 "$BASE"
install -d -o flightwatch -g flightwatch -m 750 "$BASE/run"
install -o root -g root -m 755 "$SRC/src/flightwatch.py" "$BASE/flightwatch.py"
ok "$BASE (код root:root, запись только в $BASE/run)"

# --- 3. окружение python ---------------------------------------------------
say "3/9 виртуальное окружение"
[ -x "$BASE/venv/bin/python" ] || python3 -m venv "$BASE/venv"
"$BASE/venv/bin/pip" install -q --upgrade pip >/dev/null
"$BASE/venv/bin/pip" install -q -r "$SRC/requirements.txt"
"$BASE/venv/bin/python" -c "import paho.mqtt.client, pymysql, FlightRadarAPI" \
  || die "зависимости не импортируются"
ok "$($BASE/venv/bin/pip list --format=freeze | grep -ciE 'paho|pymysql|flightradar') из 3 пакетов на месте"

# --- 4. база ---------------------------------------------------------------
say "4/9 база данных"
DB_PASS_FILE="$BASE/.mysql_pass"
mysqladmin ping >/dev/null 2>&1 || die "MariaDB не отвечает. Запустите её: systemctl start mariadb (в контейнере без systemd: mariadbd-safe --user=mysql &)"
if [ "$SKIP_DB" = 0 ]; then
  [ -s "$DB_PASS_FILE" ] || { rndpass > "$DB_PASS_FILE"; }
  DB_PASS="$(cat "$DB_PASS_FILE")"
  mysql -e "CREATE DATABASE IF NOT EXISTS flightwatch CHARACTER SET utf8mb4;"
  mysql -e "CREATE USER IF NOT EXISTS 'flightwatch'@'127.0.0.1' IDENTIFIED BY '$DB_PASS';"
  mysql -e "ALTER USER 'flightwatch'@'127.0.0.1' IDENTIFIED BY '$DB_PASS';"
  mysql -e "GRANT ALL ON flightwatch.* TO 'flightwatch'@'127.0.0.1'; FLUSH PRIVILEGES;"
  # схема заливается один раз: наличие flights считаем признаком, что уже залита
  if ! mysql -N -B -e "SHOW TABLES FROM flightwatch LIKE 'flights';" | grep -q flights; then
      mysql flightwatch < "$SRC/docs/schema.sql"
  fi
  if [ "$WITH_FWAPI" = 1 ]; then
      # путь зашит в fwapi.py — менять его надо в обоих местах сразу
      RO_PASS_FILE=/etc/fwapi.dbpass
      [ -s "$RO_PASS_FILE" ] || rndpass > "$RO_PASS_FILE"
      RO_PASS="$(cat "$RO_PASS_FILE")"
      mysql -e "CREATE USER IF NOT EXISTS 'fwread'@'127.0.0.1' IDENTIFIED BY '$RO_PASS';"
      mysql -e "ALTER USER 'fwread'@'127.0.0.1' IDENTIFIED BY '$RO_PASS';"
      mysql -e "GRANT SELECT ON flightwatch.* TO 'fwread'@'127.0.0.1'; FLUSH PRIVILEGES;"
      chown root:flightwatch "$RO_PASS_FILE"; chmod 640 "$RO_PASS_FILE"
  fi
fi
N_TABLES=$(mysql -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='flightwatch';")
[ "$N_TABLES" -ge 15 ] || die "в базе $N_TABLES таблиц, ожидалось не меньше 15 — схема не залилась"
ok "таблиц и представлений: $N_TABLES"

# --- 5. брокер -------------------------------------------------------------
say "5/9 брокер MQTT"
MQ_PASS_FILE="$BASE/.mqtt_pass"
[ -s "$MQ_PASS_FILE" ] || rndpass > "$MQ_PASS_FILE"
MQ_PASS="$(cat "$MQ_PASS_FILE")"
if [ "$SKIP_BROKER" = 0 ]; then
  install -d -m 755 /etc/mosquitto/conf.d
  touch /etc/mosquitto/passwd
  mosquitto_passwd -b /etc/mosquitto/passwd flightwatch "$MQ_PASS"
  # пароли читает процесс mosquitto — ему нужна группа, но не всем подряд
  chown root:mosquitto /etc/mosquitto/passwd && chmod 640 /etc/mosquitto/passwd
  cat > /etc/mosquitto/conf.d/flightwatch.conf <<EOF
# слушаем только петлю: наружу брокер не торчит
listener 1883 127.0.0.1
allow_anonymous false
password_file /etc/mosquitto/passwd
# persistence здесь НЕ задаём: в стоковом /etc/mosquitto/mosquitto.conf Debian
# она уже есть, а повторное значение mosquitto считает ошибкой и не стартует
EOF
  if [ "$HAVE_SYSTEMD" = 1 ]; then systemctl restart mosquitto; sleep 1; else
    echo "   без systemd брокер не перезапущен — сделайте это сами, иначе"
    echo "   он не увидит ни новый пароль, ни conf.d/flightwatch.conf"
  fi
fi
ok "учётка flightwatch заведена, слушает 127.0.0.1:1883"

# --- 6. секреты ------------------------------------------------------------
say "6/9 секреты и конфиг"
ask FW_TG_TOKEN "Токен Telegram-бота (от @BotFather, пусто = без Telegram)" ""
ask FW_TG_CHAT  "chat_id, куда писать (число)" "0"
ask FW_SMS_TO   "Номер для СМС в формате +7... (пусто = без СМС)" ""
ask FW_SMS_TOPIC "Топик отправки СМС вашего моста" "openstick/CHANGE_ME/sms/send"

[ -n "$FW_TG_TOKEN" ] && printf '%s' "$FW_TG_TOKEN" > "$BASE/.tg_token"
touch "$BASE/.tg_token"
chown root:flightwatch "$BASE"/.mysql_pass "$BASE"/.mqtt_pass "$BASE"/.tg_token
chmod 640 "$BASE"/.mysql_pass "$BASE"/.mqtt_pass "$BASE"/.tg_token

CFG="$BASE/run/config.json"
if [ ! -s "$CFG" ]; then
  CH='["telegram","sms"]'
  [ -z "$FW_SMS_TO" ] && CH='["telegram"]'
  [ -z "$FW_TG_TOKEN" ] && CH='["sms"]'
  [ -z "$FW_TG_TOKEN$FW_SMS_TO" ] && CH='[]'
  python3 - "$SRC/config.example.json" "$CFG" <<PY
import json, sys
cfg = json.load(open(sys.argv[1], encoding="utf-8"))
cfg.update({
  "db_pass_file": "$BASE/.mysql_pass",
  "mqtt_pass_file": "$BASE/.mqtt_pass",
  "telegram_token_file": "$BASE/.tg_token",
  "telegram_chat_id": int("${FW_TG_CHAT:-0}" or 0),
  "sms_to": "$FW_SMS_TO",
  "sms_topic": "$FW_SMS_TOPIC",
  "log_file": "$BASE/run/flightwatch.log",
  "channels": json.loads('$CH'),
  "briefings": [], "flights": [],
})
json.dump(cfg, open(sys.argv[2], "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY
fi
chown flightwatch:flightwatch "$CFG"; chmod 640 "$CFG"
# в каталоге кода — симлинк: каталог закрыт на запись, а атомарная подмена
# файла требует права на КАТАЛОГ, а не на файл
[ -e "$BASE/config.json" ] || ln -s run/config.json "$BASE/config.json"
python3 -c "import json;json.load(open('$CFG'))" || die "config.json невалиден"
ok "конфиг $CFG, каналы $(python3 -c "import json;print(json.load(open('$CFG'))['channels'])")"

# --- 7. проверка связности ДО запуска службы -------------------------------
say "7/9 проверка связности"
"$BASE/venv/bin/python" - <<PY || die "демон не может подключиться к базе или брокеру — см. сообщение выше"
import json, sys, pymysql, paho.mqtt.client as mqtt
cfg = json.load(open("$CFG", encoding="utf-8"))
pw = open(cfg["db_pass_file"]).read().strip()
c = pymysql.connect(host=cfg["db_host"], user=cfg["db_user"], password=pw,
                    database=cfg["db_name"], connect_timeout=10)
c.cursor().execute("SELECT 1"); c.close()
print("   ok: база отвечает")
rc = {}
cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="fw-install-check")
cl.username_pw_set(cfg["mqtt_user"], open(cfg["mqtt_pass_file"]).read().strip())
cl.on_connect = lambda c,u,f,reason,p=None: rc.update(r=str(reason))
try:
    cl.connect(cfg["mqtt_host"], cfg["mqtt_port"], 20)
except OSError as e:
    sys.exit(f"   БРОКЕР НЕ ОТВЕЧАЕТ на {cfg['mqtt_host']}:{cfg['mqtt_port']}: {e}\n   Это не пароль, а сам mosquitto: systemctl status mosquitto")
cl.loop_start()
import time
for _ in range(50):
    if cl.is_connected(): break
    time.sleep(0.1)
if not cl.is_connected():
    sys.exit(f"   БРОКЕР НЕ ПУСТИЛ: {rc.get('r')}\n"
             "   'Not authorized' = неверный логин или пароль в /etc/mosquitto/passwd;\n"
             "   пароль лежит в " + cfg["mqtt_pass_file"])
print("   ok: брокер пустил")
cl.loop_stop(); cl.disconnect()
PY

# --- 8. службы -------------------------------------------------------------
say "8/9 службы"
install -m 644 "$SRC/systemd/flightwatch.service" /etc/systemd/system/
if [ "$WITH_FWAPI" = 1 ]; then
  install -d -m 755 /opt/fwapi
  install -m 755 "$SRC/src/fwapi.py" /opt/fwapi/fwapi.py
  install -m 755 "$SRC/cli/fwctl" "$SRC/cli/fwctl-write" "$SRC/cli/fwctl-config" /usr/local/bin/
  getent group fwapi >/dev/null || groupadd fwapi
  [ -s /etc/fwapi.token ] || rndpass > /etc/fwapi.token
  chown root:fwapi /etc/fwapi.token && chmod 640 /etc/fwapi.token
  install -m 644 "$SRC/systemd/fwapi.service" /etc/systemd/system/
fi
if [ "$HAVE_SYSTEMD" = 1 ]; then
  systemctl daemon-reload
  systemctl enable --now flightwatch
  [ "$WITH_FWAPI" = 1 ] && systemctl enable --now fwapi
  sleep 8
else
  ok "юниты скопированы; без systemd запускать вручную:"
  echo "      sudo -u flightwatch $BASE/venv/bin/python $BASE/flightwatch.py --once"
fi

# --- 9. итоговая проверка --------------------------------------------------
say "9/9 итог"
if [ "$HAVE_SYSTEMD" = 1 ]; then
  systemctl is-active --quiet flightwatch || die "служба не поднялась: journalctl -u flightwatch -n 50"
  # "active" ничего не доказывает: paho отдаёт отказ аутентификации асинхронно,
  # поэтому смотрим именно строку подключения в журнале
  if journalctl -u flightwatch --since "-2 min" --no-pager | grep -q "MQTT НЕ ПОДКЛЮЧЁН"; then
      die "служба живёт, но брокер её не пустил — см. journalctl -u flightwatch"
  fi
  journalctl -u flightwatch --since "-2 min" --no-pager | grep -q "MQTT: подключён" \
      && ok "демон подключился к брокеру" \
      || echo "   (строки про MQTT ещё нет — проверьте journalctl -u flightwatch через минуту)"
fi
cat <<EOF

Готово. Дальше:
  1) добавить рейс:
       sudo -u flightwatch $BASE/venv/bin/python $BASE/flightwatch.py \\
            --add XX1234 $(date -d '+1 day' +%F 2>/dev/null || date -v+1d +%F) AAA
  2) посмотреть список:
       sudo -u flightwatch $BASE/venv/bin/python $BASE/flightwatch.py --list
  3) журнал:
       journalctl -u flightwatch -f
EOF
