import sys
from pathlib import Path
from pymongo import MongoClient

sys.path.insert(0, str(Path("queries/optimized")))
import mina_queries_optimized as opt

db = MongoClient("mongodb://localhost:27017")["rt_analytics"]

def measure(fn):
    name, pipeline, coll = fn()
    out = db.command({
        "explain": {"aggregate": coll, "pipeline": pipeline, "cursor": {}, "allowDiskUse": True},
        "verbosity": "executionStats",
    })
    times = []
    def walk(n):
        if isinstance(n, dict):
            for k in ("executionTimeMillis", "executionTimeMillisEstimate"):
                if isinstance(n.get(k), (int, float)): times.append(n[k])
            for v in n.values(): walk(v)
        elif isinstance(n, list):
            for x in n: walk(x)
    walk(out)
    print(f"{name}: {max(times) if times else 0} ms")

for fn in opt.ALL_QUERIES:
    measure(fn)