"""Collect measured values of an acceptance run into tests/acceptance/results/<id>.json (AGENTS.md, SRS 8).

Sources: the Locust CSV report (rps, p95 of create and status, error share), VictoriaMetrics instant queries from
queries.json, and --value key=number for values measured elsewhere. Numbers are written as measured, not rounded.
The status is computed from the thresholds of SRS 8: "passed" needs every key of the run within its threshold.

  python3 tests/acceptance/collect.py 8.1 --commit <sha> --date DD.MM.YYYY --summary "..." \
      --locust-csv <prefix>_stats.csv --vm-url http://<vm>:8428 --window 43m
"""
import argparse
import csv
import json
import os
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))

# id -> key -> (comparison, threshold). SRS 8 and NFR-01..06, 08, 09, 10, 12.
THRESHOLDS = {
    "8.1": {"rps": (">=", 200), "p95_create_ms": ("<=", 300), "p95_status_ms": ("<=", 100),
            "error_rate_5xx": ("<=", 0.01), "detect_p95_s": ("<=", 3), "webhook_p95_s": ("<=", 5),
            "rate_age_max_s": ("<=", 60), "missed_transfers": ("==", 0), "duplicates": ("==", 0),
            "reconciliation_mismatches": ("==", 0)},
    "8.2а": {"cb_seconds": ("<=", 30), "error_rate_5xx": ("<=", 0.01)},
    "8.2б": {"cb_seconds": ("<=", 30)},
    "8.2в": {"cb_seconds": ("<=", 30), "missed_transfers": ("==", 0)},
    "8.2г": {}, "8.3": {},
    "8.2д": {"cb_seconds": ("<=", 30)},
    "8.4": {"vip_failover_s": ("<=", 5)},
    "8.5": {}, "8.6": {},
    "8.7": {"missed_transfers": ("==", 0), "duplicates": ("==", 0)},
    "8.8": {}, "8.9": {},
    "deploy": {"minutes": ("<=", 60)},
}

OPS = {">=": lambda v, t: v >= t, "<=": lambda v, t: v <= t, "==": lambda v, t: v == t}

LOCUST_KEYS = ("rps", "p95_create_ms", "p95_status_ms", "error_rate_5xx")


def number(text):
    """Parse a measured number without rounding: int if written as int, else float."""
    text = str(text).strip()
    try:
        return int(text)
    except ValueError:
        value = float(text)
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError(f"not a finite number: {text}")
        return value


def status(test_id, metrics):
    """passed only if every key of the test is present and within its SRS threshold; keyless tests are decided
    by the caller (status argument)."""
    rules = THRESHOLDS[test_id]
    for key, (op, limit) in rules.items():
        if key not in metrics or not OPS[op](metrics[key], limit):
            return "failed"
    return "passed"


def locust_metrics(path):
    """rps, p95_create_ms, p95_status_ms, error_rate_5xx from <prefix>_stats.csv.
    error_rate_5xx is the Locust failure share: every answer except 201 and 200 counts as a failure."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = {r["Name"]: r for r in csv.DictReader(f)}
    agg = rows["Aggregated"]
    requests = number(agg["Request Count"])
    if requests == 0:
        raise ValueError("Locust report has no requests")
    return {
        "rps": number(agg["Requests/s"]),
        "p95_create_ms": number(rows["create"]["95%"]),
        "p95_status_ms": number(rows["status"]["95%"]),
        "error_rate_5xx": number(agg["Failure Count"]) / requests,
    }


def vm_metrics(url, queries, window):
    """One instant query per key; the query must return exactly one sample. {window} is the run length."""
    out = {}
    for key, query in queries.items():
        q = urllib.parse.urlencode({"query": query.replace("{window}", window)})
        with urllib.request.urlopen(f"{url.rstrip('/')}/api/v1/query?{q}", timeout=30) as r:
            result = json.load(r)["data"]["result"]
        if len(result) != 1:
            raise ValueError(f"{key}: expected one sample, got {len(result)}")
        out[key] = number(result[0]["value"][1])
    return out


def result(test_id, metrics, date, commit, summary, forced_status=None):
    rules = THRESHOLDS[test_id]
    metrics = {k: v for k, v in metrics.items() if k in rules}
    st = status(test_id, metrics) if rules else (forced_status or "failed")
    return {"id": test_id, "status": st, "date": date, "commit": commit, "summary": summary, "metrics": metrics}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("id", choices=sorted(THRESHOLDS))
    p.add_argument("--date", required=True, help="DD.MM.YYYY")
    p.add_argument("--commit", required=True)
    p.add_argument("--summary", required=True, help="conclusion without numbers")
    p.add_argument("--locust-csv")
    p.add_argument("--vm-url")
    p.add_argument("--window", help="run length for VictoriaMetrics queries, e.g. 43m")
    p.add_argument("--value", action="append", default=[], metavar="KEY=NUMBER")
    p.add_argument("--status", choices=("passed", "failed"), help="only for tests without keys")
    p.add_argument("--out", default=os.path.join(HERE, "results"))
    a = p.parse_args(argv)

    metrics = {}
    if a.locust_csv:
        metrics.update(locust_metrics(a.locust_csv))
    if a.vm_url:
        if not a.window:
            p.error("--vm-url needs --window")
        with open(os.path.join(HERE, "queries.json"), encoding="utf-8") as f:
            queries = {k: q for k, q in json.load(f).items() if k in THRESHOLDS[a.id]}
        metrics.update(vm_metrics(a.vm_url, queries, a.window))
    for item in a.value:
        key, _, text = item.partition("=")
        if key not in THRESHOLDS[a.id]:
            p.error(f"key {key} is not defined for {a.id}")
        metrics[key] = number(text)

    res = result(a.id, metrics, a.date, a.commit, a.summary, a.status)
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"{a.id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False)
        f.write("\n")
    print(f"{path}: {res['status']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
