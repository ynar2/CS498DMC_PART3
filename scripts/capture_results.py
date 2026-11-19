"""
Run every query with its default parameters and save the evidence used in the slides:

    results/summary.md        one table: rows, time, indexes, keys/docs examined
    results/qN.json           parameters, pipeline, plan summary and the first 50 rows

    python scripts/capture_results.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pymongo import MongoClient  # noqa: E402

import config  # noqa: E402
import queries  # noqa: E402


def main():
    db = MongoClient(config.MONGO_URI)[config.DB_NAME]
    os.makedirs("results", exist_ok=True)
    defaults = queries.default_params(db)
    lines = ["| Query | Params | Rows | ms (best of 3) | Indexes | Keys examined | Docs examined | COLLSCAN |",
             "|---|---|---|---|---|---|---|---|"]
    for qid, meta in queries.QUERIES.items():
        p = defaults[qid]
        runs = [queries.run(db, qid, **p) for _ in range(3)]  # first run warms the cache
        res = min(runs, key=lambda r: r["ms"])
        plan = queries.explain(db, qid, **p)
        with open(f"results/{qid}.json", "w") as f:
            json.dump({"title": meta["title"], "params": p, "rows_returned": len(res["rows"]), "ms": res["ms"],
                       "plan": plan, "pipeline": res["pipeline"], "first_rows": res["rows"][:50]},
                      f, indent=2, default=str)
        lines.append(f"| {qid.upper()} | {json.dumps(p)} | {len(res['rows']):,} | {res['ms']} | "
                     f"{', '.join(plan['indexes']) or '-'} | {plan['keys_examined']:,} | "
                     f"{plan['docs_examined']:,} | {'yes' if plan['collscan'] else 'no'} |")
        print(lines[-1])
    with open("results/summary.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print("Wrote results/")


if __name__ == "__main__":
    main()
