"""python3 -m unittest discover tests/acceptance"""
import csv
import http.server
import json
import os
import tempfile
import threading
import unittest

import collect

OK81 = {"rps": 231.4, "p95_create_ms": 182, "p95_status_ms": 41, "error_rate_5xx": 0.0012, "detect_p95_s": 1.4,
        "webhook_p95_s": 2.2, "rate_age_max_s": 31, "missed_transfers": 0, "duplicates": 0,
        "reconciliation_mismatches": 0}


class Status(unittest.TestCase):
    def test_passed(self):
        self.assertEqual(collect.status("8.1", OK81), "passed")

    def test_boundaries(self):
        # (key, last passing value, first failing value)
        cases = [("rps", 200, 199.999), ("p95_create_ms", 300, 300.001), ("p95_status_ms", 100, 100.001),
                 ("error_rate_5xx", 0.01, 0.010001), ("detect_p95_s", 3, 3.001), ("webhook_p95_s", 5, 5.001),
                 ("rate_age_max_s", 60, 60.001), ("missed_transfers", 0, 1), ("duplicates", 0, 1),
                 ("reconciliation_mismatches", 0, 1)]
        for key, good, bad in cases:
            with self.subTest(key=key):
                self.assertEqual(collect.status("8.1", {**OK81, key: good}), "passed")
                self.assertEqual(collect.status("8.1", {**OK81, key: bad}), "failed")

    def test_missing_key_is_failed(self):
        for key in OK81:
            with self.subTest(key=key):
                m = dict(OK81)
                del m[key]
                self.assertEqual(collect.status("8.1", m), "failed")

    def test_other_tests(self):
        self.assertEqual(collect.status("8.4", {"vip_failover_s": 5}), "passed")
        self.assertEqual(collect.status("8.4", {"vip_failover_s": 5.1}), "failed")
        self.assertEqual(collect.status("deploy", {"minutes": 60}), "passed")
        self.assertEqual(collect.status("deploy", {"minutes": 61}), "failed")
        self.assertEqual(collect.status("8.2в", {"cb_seconds": 30}), "failed")
        self.assertEqual(collect.status("8.7", {"missed_transfers": 0, "duplicates": 0}), "passed")

    def test_keyless(self):
        r = collect.result("8.3", {}, "01.01.2027", "abc1234", "s", "passed")
        self.assertEqual((r["status"], r["metrics"]), ("passed", {}))
        self.assertEqual(collect.result("8.3", {}, "01.01.2027", "abc1234", "s")["status"], "failed")


class Number(unittest.TestCase):
    def test_not_rounded(self):
        self.assertEqual(collect.number("0.0012345678"), 0.0012345678)
        self.assertIsInstance(collect.number("182"), int)

    def test_invalid(self):
        for bad in ("nan", "inf", "x"):
            with self.assertRaises(ValueError):
                collect.number(bad)


def write_csv(path, failures="3", count="1000"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Type", "Name", "Request Count", "Failure Count", "Requests/s", "95%"])
        w.writerow(["POST", "create", "200", "0", "40.5", "182"])
        w.writerow(["GET", "status", "800", failures, "190.9", "41"])
        w.writerow(["", "Aggregated", count, failures, "231.4", "60"])


class Handler(http.server.BaseHTTPRequestHandler):
    samples = {}

    def log_message(self, *_):
        pass

    def do_GET(self):
        q = self.path
        result = []
        for key, value in self.samples.items():
            if key in q:
                result = [] if value is None else [{"value": [0, value]}]
        body = json.dumps({"data": {"result": result}}).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)


class Collect(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.csv = os.path.join(self.tmp.name, "locust_stats.csv")
        write_csv(self.csv)
        Handler.samples = {"weigate_chain_transfer_delay": "1.4", "weigate_rate_age_seconds": "31",
                           "weigate_reconciliation_mismatches": "0"}
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.addCleanup(self.srv.server_close)
        self.addCleanup(self.srv.shutdown)
        self.url = f"http://127.0.0.1:{self.srv.server_port}"

    def run_main(self, *extra, id="8.1"):
        args = [id, "--date", "28.11.2026", "--commit", "abc1234", "--summary", "ok", "--out", self.tmp.name, *extra]
        collect.main(args)
        with open(os.path.join(self.tmp.name, f"{id}.json"), encoding="utf-8") as f:
            return json.load(f)

    def test_locust_metrics(self):
        m = collect.locust_metrics(self.csv)
        self.assertEqual(m, {"rps": 231.4, "p95_create_ms": 182, "p95_status_ms": 41, "error_rate_5xx": 0.003})

    def test_locust_empty(self):
        write_csv(self.csv, failures="0", count="0")
        with self.assertRaises(ValueError):
            collect.locust_metrics(self.csv)

    def test_full_81(self):
        r = self.run_main("--locust-csv", self.csv, "--vm-url", self.url, "--window", "43m",
                          "--value", "webhook_p95_s=2.2", "--value", "missed_transfers=0",
                          "--value", "duplicates=0")
        self.assertEqual(r["status"], "passed")
        self.assertEqual(r["metrics"]["detect_p95_s"], 1.4)
        self.assertEqual(set(r), {"id", "status", "date", "commit", "summary", "metrics"})
        self.assertEqual(set(r["metrics"]), set(collect.THRESHOLDS["8.1"]))

    def test_missing_value_fails(self):
        r = self.run_main("--locust-csv", self.csv, "--vm-url", self.url, "--window", "43m")
        self.assertEqual(r["status"], "failed")

    def test_threshold_fails(self):
        Handler.samples["weigate_reconciliation_mismatches"] = "2"
        r = self.run_main("--locust-csv", self.csv, "--vm-url", self.url, "--window", "43m",
                          "--value", "webhook_p95_s=2.2", "--value", "missed_transfers=0",
                          "--value", "duplicates=0")
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["metrics"]["reconciliation_mismatches"], 2)

    def test_vm_no_sample(self):
        Handler.samples["weigate_rate_age_seconds"] = None
        with self.assertRaises(ValueError):
            collect.vm_metrics(self.url, {"rate_age_max_s": "weigate_rate_age_seconds"}, "1m")

    def test_value_only(self):
        r = self.run_main("--value", "vip_failover_s=4.2", id="8.4")
        self.assertEqual((r["status"], r["metrics"]), ("passed", {"vip_failover_s": 4.2}))

    def test_keyless_status(self):
        self.assertEqual(self.run_main("--status", "passed", id="8.6")["status"], "passed")

    def test_unknown_key(self):
        with self.assertRaises(SystemExit):
            self.run_main("--value", "rps=1", id="8.4")

    def test_vm_needs_window(self):
        with self.assertRaises(SystemExit):
            self.run_main("--vm-url", self.url)


if __name__ == "__main__":
    unittest.main()
