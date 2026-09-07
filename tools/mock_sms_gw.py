#!/usr/bin/env python3
"""Заглушка SMS-шлюза: принимает команды по MQTT, никуда ничего не шлёт.

Нужна затем, что настоящий шлюз - это чей-то конкретный модем, симка и
тариф, а весь остальной код от них не зависит. Заглушка реализует ТОТ ЖЕ
контракт (см. docs/sms-gateway-contract.md), поэтому с ней проверяются
все ветки канала СМС: очередь через обрыв, срок годности, лимит частоты,
подтверждение, отказ и молчание моста.

  mock_sms_gw.py --id mock                    # обычная работа
  mock_sms_gw.py --fail-rate 1.0              # шлюз всегда отвечает отказом
  mock_sms_gw.py --silent                     # не подтверждает вообще
  mock_sms_gw.py --max-per-hour 3             # упереться в лимит частоты

Отправленное пишется строками JSON в --out (по умолчанию stdout), поэтому
проверять доставку можно тем же способом, что и в бою: не глазами, а
командой.
"""
import argparse
import json
import os
import sys
import time

import paho.mqtt.client as mqtt


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--id", default=os.environ.get("SMSGW_ID", "mock"),
                   help="идентификатор шлюза в топиках (по умолчанию mock)")
    p.add_argument("--prefix", default=os.environ.get("SMSGW_PREFIX", "smsgw"))
    p.add_argument("--host", default=os.environ.get("MQTT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--user", default=os.environ.get("MQTT_USER", ""))
    p.add_argument("--pass-file", default=os.environ.get("MQTT_PASS_FILE", ""))
    p.add_argument("--out", default=os.environ.get("SMSGW_OUT", ""),
                   help="файл, куда писать JSONL с 'отправленным'")
    p.add_argument("--fail-rate", type=float, default=0.0,
                   help="доля команд, на которые отвечать отказом (0..1)")
    p.add_argument("--silent", action="store_true",
                   help="не отвечать вовсе - проверка ветки 'мост не подтвердил'")
    p.add_argument("--max-per-hour", type=int, default=0,
                   help="лимит частоты; 0 = без лимита")
    p.add_argument("--delay", type=float, default=0.0,
                   help="задержка перед подтверждением, секунды")
    return p.parse_args()


class MockGateway:
    def __init__(self, a):
        self.a = a
        self.topic_in = f"{a.prefix}/{a.id}/sms/send"
        self.topic_out = f"{a.prefix}/{a.id}/sms/sent"
        self.status = f"{a.prefix}/{a.id}/status"
        self.sent_at: list[float] = []
        self.n = 0
        self.out = open(a.out, "a", encoding="utf-8") if a.out else sys.stdout
        # clean_session=False намеренно: пока шлюз перезагружается, команды
        # копятся в брокере, а не пропадают. Это то же поведение, что у
        # настоящего моста, и без него очередь через обрыв не проверить.
        self.cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                              client_id=f"smsgw-{a.id}", clean_session=False)
        if a.user:
            pw = open(a.pass_file, encoding="utf-8").read().strip() if a.pass_file else ""
            self.cl.username_pw_set(a.user, pw)
        self.cl.will_set(self.status, "offline", qos=1, retain=True)
        self.cl.on_connect = self._on_connect
        self.cl.on_message = self._on_message

    def _on_connect(self, cl, userdata, flags, reason, properties=None):
        if getattr(reason, "value", reason) != 0:
            print(f"брокер отказал: {reason}", file=sys.stderr, flush=True)
            return
        cl.publish(self.status, "online", qos=1, retain=True)
        cl.subscribe(self.topic_in, qos=1)
        print(f"заглушка слушает {self.topic_in}, отвечает в {self.topic_out}",
              file=sys.stderr, flush=True)

    def _rate_limited(self) -> str:
        if not self.a.max_per_hour:
            return ""
        now = time.time()
        self.sent_at = [t for t in self.sent_at if now - t < 3600]
        if len(self.sent_at) >= self.a.max_per_hour:
            return f"rate limit {len(self.sent_at)}/{self.a.max_per_hour} за час"
        return ""

    def _on_message(self, cl, userdata, msg):
        try:
            cmd = json.loads(msg.payload.decode("utf-8"))
        except Exception as exc:
            print(f"нечитаемая команда: {exc}", file=sys.stderr, flush=True)
            return
        req_id = str(cmd.get("req_id") or "")
        to = str(cmd.get("to") or "")
        text = str(cmd.get("text") or cmd.get("message") or "")
        self.n += 1

        # Порядок проверок важен: протухшая команда не должна съедать лимит
        # частоты, иначе после долгого обрыва лимит выберут мертвецы из
        # очереди, а свежее сообщение получит отказ.
        exp = cmd.get("expires_at")
        if exp and time.time() > float(exp):
            late = int(time.time() - float(exp))
            return self._ack(req_id, to, False, f"expired {late} с назад", text)
        limited = self._rate_limited()
        if limited:
            return self._ack(req_id, to, False, limited, text)

        self.sent_at.append(time.time())
        rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "to": to,
               "text": text, "req_id": req_id, "n": self.n}
        self.out.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.out.flush()
        print(f"[СМС #{self.n}] {to}: {text}", file=sys.stderr, flush=True)

        # Отказы раздаём детерминированно, а не случайно: тест, который
        # иногда проходит, ничего не доказывает.
        if int(self.n * self.a.fail_rate) > int((self.n - 1) * self.a.fail_rate):
            return self._ack(req_id, to, False, "MockDeliveryFailure", text)
        self._ack(req_id, to, True, "ok", text)

    def _ack(self, req_id, to, ok, detail, text):
        if not ok:
            print(f"отказ по {req_id}: {detail}", file=sys.stderr, flush=True)
        if self.a.silent:
            return
        if self.a.delay:
            time.sleep(self.a.delay)
        # Длину считаем как GSM-7: 160 символов в одиночном сообщении,
        # 153 в каждом сегменте склейки. Демону это нужно только для лога.
        n = len(text)
        segments = 1 if n <= 160 else (n + 152) // 153
        self.cl.publish(self.topic_out, json.dumps({
            "req_id": req_id, "to": to, "ok": ok, "chars": n,
            "segments": segments, "segments_ok": segments if ok else 0,
            "detail": detail}, ensure_ascii=False), qos=1)

    def run(self):
        self.cl.connect(self.a.host, self.a.port, 30)
        self.cl.loop_forever()


if __name__ == "__main__":
    MockGateway(parse_args()).run()
