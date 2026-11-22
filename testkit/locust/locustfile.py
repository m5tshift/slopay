"""Load profile for SRS 8.1 and 8.3: merchants of the stand through the VIP.

API keys come from the stand-setup keys file (WEIGATE_KEYS_FILE, outside git).
Contracts: contracts/openapi/merchant-service.yaml, contracts/openapi/invoice-service.yaml.
"""
import json
import os
import random
import time
import uuid

from locust import HttpUser, constant_throughput, task

# SRS 8.1: per merchant ~0.75 invoices/s and ~3 status requests/s: one request in five creates an invoice.
CREATE_PER_S = 0.75
POLL_PER_S = 3.0
# SRS 8.3: the extra merchant creates 3 invoices/s (limit 1/s) for 1 minute.
VIOLATOR_CREATE_PER_S = 3.0
VIOLATOR_SECONDS = 60
# The JWT lives 15 minutes. It is renewed after 13 minutes plus up to 60 s of jitter; first tokens are spread by
# the ramp-up (60 merchants in 3 minutes), so renewals stay far below the limit of 30 requests/min per IP.
TOKEN_RENEW_S = 13 * 60
TOKEN_JITTER_S = 60
KNOWN_INVOICES = 50

_keys = None
_taken = {"regular": 0, "extra": 0}


def _entries(extra):
    global _keys
    if _keys is None:
        with open(os.environ["WEIGATE_KEYS_FILE"], encoding="utf-8") as f:
            _keys = json.load(f)["merchants"]
    return [e for e in _keys if ("-extra-" in e["name"]) == extra]


def _take(kind):
    entries = _entries(kind == "extra")
    i = _taken[kind]
    if i >= len(entries):
        raise RuntimeError(f"not enough {kind} merchants in the keys file")
    _taken[kind] = i + 1
    return entries[i]


class _Merchant(HttpUser):
    abstract = True
    kind = "regular"

    def on_start(self):
        self.entry = _take(self.kind)
        self.token = None
        self.renew_at = 0.0
        self.invoices = []
        self.seq = 0
        self.tick_no = 0

    def _auth(self):
        """Returns headers with a valid JWT, exchanging the API key when it is missing or about to expire."""
        while self.token is None or time.time() >= self.renew_at:
            with self.client.post("/api/v1/auth/token", json={"apiKey": self.entry["apiKey"]},
                                  name="token", catch_response=True) as r:
                if r.status_code == 200:
                    self.token = r.json()["accessToken"]
                    self.renew_at = time.time() + TOKEN_RENEW_S + random.uniform(0, TOKEN_JITTER_S)
                elif r.status_code == 429:
                    r.failure("429 on token")
                    time.sleep(float(r.headers.get("Retry-After", "5")))
                else:
                    r.failure(f"token {r.status_code}")
                    time.sleep(5)
        return {"Authorization": f"Bearer {self.token}"}

    def _create(self, name="create", ok_429=False):
        self.seq += 1
        body = {"amountUsdCents": random.randint(1000, 50000), "orderId": f"{self.entry['name']}-{self.seq}"}
        headers = {**self._auth(), "Idempotency-Key": str(uuid.uuid4())}
        with self.client.post("/api/v1/invoices", json=body, headers=headers, name=name, catch_response=True) as r:
            if r.status_code == 201:
                self.invoices.append(r.json()["invoiceId"])
                del self.invoices[:-KNOWN_INVOICES]
            elif ok_429 and r.status_code == 429:
                if r.headers.get("Retry-After"):
                    r.success()
                else:
                    r.failure("429 without Retry-After")
            else:
                r.failure(f"create {r.status_code}")

    def _poll(self):
        invoice_id = random.choice(self.invoices)
        with self.client.get(f"/api/v1/invoices/{invoice_id}", headers=self._auth(),
                             name="status", catch_response=True) as r:
            if r.status_code != 200:
                r.failure(f"status {r.status_code}")


class Merchant(_Merchant):
    """SRS 8.1 merchant: a cycle of five requests, one invoice and four status polls."""
    wait_time = constant_throughput(CREATE_PER_S + POLL_PER_S)

    @task
    def tick(self):
        self.tick_no += 1
        if self.tick_no % 5 == 1 or not self.invoices:
            self._create()
        else:
            self._poll()


class Violator(_Merchant):
    """SRS 8.3 extra merchant: 3 invoices/s for one minute, 429 with Retry-After is the expected answer."""
    kind = "extra"
    wait_time = constant_throughput(VIOLATOR_CREATE_PER_S)

    def on_start(self):
        super().on_start()
        self.stop_at = time.time() + VIOLATOR_SECONDS

    @task
    def tick(self):
        if time.time() >= self.stop_at:
            self.environment.runner.quit()
            return
        self._create(name="create (violator)", ok_429=True)
