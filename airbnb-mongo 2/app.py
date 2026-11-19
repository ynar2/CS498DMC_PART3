"""
Local web frontend: pick a query, set its parameters, run it, see the rows,
the run time, the index usage from explain(), and the exact pipeline sent.

    python app.py            ->  http://127.0.0.1:5000
"""
import json
from datetime import datetime

from flask import Flask, render_template, request
from pymongo import MongoClient

import config
import queries

app = Flask(__name__)
db = MongoClient(config.MONGO_URI)[config.DB_NAME]

# Preferred column order per query (anything else follows).
COLUMNS = {
    "q1": ["name", "neighbourhood", "room_type", "accommodates", "property_type", "amenities", "price", "rating"],
    "q2": ["city", "neighbourhood", "month", "total_listings"],
    "q3": ["name", "month", "start", "end", "nights", "min_nights_at_start", "first_valid_checkin", "bookable"],
    "q4": ["month", "year", "available_nights", "listings_with_availability"],
    "q5": ["city", "year", "december_reviews"],
    "q6": ["name", "listing_url", "description", "host_name", "reviewer_name", "times_reviewed",
           "previously_booked", "previously_booked_months", "available_again_in", "minimum_nights",
           "maximum_nights", "other_listings_same_host_city", "city"],
}


def fmt(value, key=""):
    """Render one cell as short readable text."""
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float):
        return f"${value:,.2f}" if key == "price" else f"{value:,.2f}"
    if isinstance(value, list):
        if value and isinstance(value[0], dict):  # e.g. other listings by the same host
            return "\n".join(f"{v.get('name')} (min {v.get('minimum_nights')}, max {v.get('maximum_nights')})"
                             for v in value[:5]) + (f"\n… +{len(value) - 5} more" if len(value) > 5 else "")
        return ", ".join(map(str, value[:8])) + (f" … (+{len(value) - 8})" if len(value) > 8 else "")
    text = str(value)
    if key == "description" and len(text) > 140:
        return text[:140] + "…"
    return text


@app.route("/")
def index():
    qid = request.args.get("q", "q1")
    if qid not in queries.QUERIES:
        qid = "q1"
    meta = queries.QUERIES[qid]
    defaults = queries.default_params(db)[qid]
    params = {name: request.args.get(name, defaults.get(name, "")) for name, _, _ in meta["params"]}

    result = plan = error = None
    if request.args.get("run"):
        try:
            result = queries.run(db, qid, **params)
            if request.args.get("explain"):
                plan = queries.explain(db, qid, **params)
        except Exception as e:  # show the error in the page instead of a stack trace
            error = f"{type(e).__name__}: {e}"

    cols, table = [], []
    if result and result["rows"]:
        seen = {k for r in result["rows"] for k in r}
        cols = [c for c in COLUMNS[qid] if c in seen] + sorted(seen - set(COLUMNS[qid]) - {"listing_id", "reviewer_id", "month_num"})
        table = [[(fmt(r.get(c), c), c) for c in cols] for r in result["rows"]]

    return render_template(
        "index.html", queries=queries.QUERIES, qid=qid, meta=meta, params=params,
        result=result, plan=plan, error=error, cols=cols, table=table,
        pipeline=json.dumps(result["pipeline"], indent=2, default=str) if result else "",
        counts={c: db[c].estimated_document_count() for c in ("listings", "calendar", "reviews", "neighbourhoods")},
    )


if __name__ == "__main__":
    app.run(debug=False, port=5000)
