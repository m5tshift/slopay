"""Short profile against a stub of the stand API: python3 -m unittest discover testkit/locust."""
import csv
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))


class Stub(ThreadingHTTPServer):
    """token, createInvoice, getInvoice as in the contracts; createInvoice limit is 1/s per merchant."""

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.tokens = {}   # token -> merchant
        self.invoices = {}
        self.last_create = {}
        self.calls = {"token": 0, "create": 0, "status": 0, "429": 0}
        self.errors = []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def _send(self, code, body=None, headers=None):
        data = json.dumps(body or {}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _merchant(self):
        auth = self.headers.get("Authorization", "")
        return self.server.tokens.get(auth.removeprefix("Bearer "))

    def do_POST(self):
        s = self.server
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/api/v1/auth/token":
            s.calls["token"] += 1
            if not body.get("apiKey", "").startswith("key-"):
                return self._send(401)
            token = uuid.uuid4().hex
            s.tokens[token] = body["apiKey"]
            return self._send(200, {"accessToken": token, "tokenType": "Bearer", "expiresIn": 900})
        if self.path != "/api/v1/invoices":
            return self._send(404)
        merchant = self._merchant()
        if merchant is None:
            return self._send(401)
        if not self.headers.get("Idempotency-Key") or set(body) != {"amountUsdCents", "orderId"} \
                or not isinstance(body["amountUsdCents"], int) or body["amountUsdCents"] < 1:
            s.errors.append(("bad create", body))
            return self._send(400)
        s.calls["create"] += 1
        now = time.monotonic()
        if now - s.last_create.get(merchant, -10) < 1.0:
            s.calls["429"] += 1
            return self._send(429, headers={"Retry-After": "1"})
        s.last_create[merchant] = now
        invoice_id = str(uuid.uuid4())
        s.invoices[invoice_id] = merchant
        self._send(201, {"invoiceId": invoice_id, "status": "NEW"})

    def do_GET(self):
        s = self.server
        merchant = self._merchant()
        if merchant is None:
            return self._send(401)
        invoice_id = self.path.removeprefix("/api/v1/invoices/")
        if s.invoices.get(invoice_id) != merchant:
            return self._send(404)
        s.calls["status"] += 1
        self._send(200, {"invoiceId": invoice_id, "status": "NEW"})


class LocustProfile(unittest.TestCase):
    def setUp(self):
        self.stub = Stub()
        threading.Thread(target=self.stub.serve_forever, daemon=True).start()
        self.tmp = tempfile.TemporaryDirectory()
        keys = [{"name": f"m-{i:02d}", "merchantId": str(uuid.uuid4()), "apiKey": f"key-{i}"} for i in range(4)]
        keys.append({"name": "m-extra-00", "merchantId": str(uuid.uuid4()), "apiKey": "key-extra"})
        self.keys = os.path.join(self.tmp.name, "keys.json")
        with open(self.keys, "w", encoding="utf-8") as f:
            json.dump({"merchants": keys}, f)

    def tearDown(self):
        self.stub.shutdown()
        self.stub.server_close()
        self.tmp.cleanup()

    def run_locust(self, users, seconds, cls):
        prefix = os.path.join(self.tmp.name, cls)
        host = f"http://127.0.0.1:{self.stub.server_port}"
        p = subprocess.run(
            [sys.executable, "-m", "locust", "-f", os.path.join(HERE, "locustfile.py"), "--headless",
             "-u", str(users), "-r", str(users), "-t", f"{seconds}s", "--host", host, "--csv", prefix, cls],
            env={**os.environ, "WEIGATE_KEYS_FILE": self.keys}, capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        with open(prefix + "_stats.csv", encoding="utf-8") as f:
            return {r["Name"]: r for r in csv.DictReader(f)}

    def test_merchants_rates_and_no_failures(self):
        users, seconds = 4, 10
        stats = self.run_locust(users, seconds, "Merchant")
        creates = int(stats["create"]["Request Count"])
        polls = int(stats["status"]["Request Count"])
        # 0.75 and 3 requests/s per merchant (the first second is the start)
        self.assertTrue(0.5 * 0.75 * users * seconds <= creates <= 1.2 * 0.75 * users * seconds, creates)
        self.assertTrue(0.5 * 3 * users * seconds <= polls <= 1.2 * 3 * users * seconds, polls)
        self.assertEqual(self.stub.calls["token"], users)  # one token each, no renewal inside 13 minutes
        self.assertEqual(self.stub.calls["429"], 0)
        self.assertEqual(self.stub.errors, [])

    def test_violator_gets_429_with_retry_after(self):
        stats = self.run_locust(1, 5, "Violator")
        self.assertGreater(self.stub.calls["429"], 0)
        self.assertEqual(int(stats["create (violator)"]["Failure Count"]), 0)


if __name__ == "__main__":
    unittest.main()
