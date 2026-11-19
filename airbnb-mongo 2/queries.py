"""
The six Stage 2 queries, implemented as MongoDB aggregation pipelines.

Each build_qN(...) returns (collection_name, pipeline). run() executes one and
returns rows plus timing; explain() returns the index/scan summary that the
frontend shows and the slides quote.

Run from the command line:
    python queries.py q1 --start 2026-03-14
    python queries.py q5
    python queries.py all            # every query with default parameters
"""
import argparse
import calendar as cal
import json
import sys
import time
from datetime import datetime, timedelta

from pymongo import MongoClient

import config

ENTIRE = "Entire home/apt"
MONTHS = list(cal.month_name)  # MONTHS[3] == "March"


def _d(s):
    """'2026-03-14' -> datetime; datetimes pass through."""
    return s if isinstance(s, datetime) else datetime.strptime(s, "%Y-%m-%d")


def _month_bounds(year, month):
    start = datetime(year, month, 1)
    end = datetime(year + (month == 12), month % 12 + 1, 1)
    return start, end


# --------------------------------------------------------------------------- Q1
def build_q1(start, city="Portland", limit=5000):
    """Listings available on both nights of a two-day period, best-rated first.

    Plan: the calendar index (city, available, date) finds the available nights
    for the two dates; group by listing and keep listings with both nights;
    join to listings by _id (primary key); sort by rating.
    """
    d1 = _d(start)
    d2 = d1 + timedelta(days=1)
    pipeline = [
        {"$match": {"city": city, "available": True, "date": {"$gte": d1, "$lte": d2}}},
        {"$group": {"_id": "$listing_id", "nights": {"$sum": 1}}},
        {"$match": {"nights": 2}},
        {"$lookup": {"from": "listings", "localField": "_id", "foreignField": "_id", "as": "l"}},
        {"$unwind": "$l"},
        {"$replaceRoot": {"newRoot": "$l"}},
        # Unrated listings (null) sort after rated ones in a descending sort.
        {"$sort": {"review_scores_rating": -1, "_id": 1}},
        {"$limit": int(limit)},
        {"$project": {"_id": 0, "listing_id": "$_id", "name": 1, "neighbourhood": 1, "room_type": 1,
                      "accommodates": 1, "property_type": 1, "amenities": 1, "price": 1,
                      "rating": "$review_scores_rating"}},
    ]
    return "calendar", pipeline


# --------------------------------------------------------------------------- Q2
def build_q2(year, month, cities=None):
    """Neighbourhoods (in any city) with zero available listings in a month.

    Plan: start from the neighbourhoods collection (the full list, including
    neighbourhoods with no listings at all), tag each "all"; union in the
    (city, neighbourhood) pairs that have at least one available night that
    month, tagged "avail"; keep pairs that never got an "avail" tag.
    """
    start, end = _month_bounds(int(year), int(month))
    cities = cities or list(config.CITIES.values())
    pipeline = [
        {"$match": {"city": {"$in": cities}}},
        {"$project": {"_id": 0, "city": 1, "neighbourhood": 1, "tag": "all"}},
        {"$unionWith": {"coll": "calendar", "pipeline": [
            {"$match": {"city": {"$in": cities}, "available": True, "date": {"$gte": start, "$lt": end}}},
            {"$group": {"_id": {"city": "$city", "neighbourhood": "$neighbourhood"}}},
            {"$project": {"_id": 0, "city": "$_id.city", "neighbourhood": "$_id.neighbourhood", "tag": "avail"}},
        ]}},
        {"$group": {"_id": {"city": "$city", "neighbourhood": "$neighbourhood"}, "tags": {"$addToSet": "$tag"}}},
        {"$match": {"tags": "all", "tags.1": {"$exists": False}}},   # only ever tagged "all"
        {"$lookup": {"from": "listings", "let": {"c": "$_id.city", "n": "$_id.neighbourhood"},
                     "pipeline": [{"$match": {"$expr": {"$and": [{"$eq": ["$city", "$$c"]},
                                                                  {"$eq": ["$neighbourhood", "$$n"]}]}}},
                                  {"$count": "n"}],
                     "as": "listing_count"}},
        {"$project": {"_id": 0, "city": "$_id.city", "neighbourhood": "$_id.neighbourhood",
                      "month": {"$literal": f"{MONTHS[int(month)]} {year}"},
                      "total_listings": {"$ifNull": [{"$first": "$listing_count.n"}, 0]}}},
        {"$sort": {"city": 1, "neighbourhood": 1}},
    ]
    return "neighbourhoods", pipeline


# --------------------------------------------------------------------------- Q3
def build_q3(year, month, city="Salem", only_bookable=True):
    """Bookable availability periods for entire homes in a city for one month.

    A period is a run of consecutive available nights ("gaps and islands").
    It is bookable if some night in it can start a stay that satisfies that
    night's minimum_nights and still fits inside the run.
    """
    start, end = _month_bounds(int(year), int(month))
    day_ms = 86_400_000
    pipeline = [
        {"$match": {"city": city, "room_type": ENTIRE}},                     # index city_roomtype
        {"$project": {"name": 1}},
        {"$lookup": {"from": "calendar", "localField": "_id", "foreignField": "listing_id",  # index listing_date
                     "pipeline": [{"$match": {"available": True, "date": {"$gte": start, "$lt": end}}},
                                  {"$project": {"_id": 0, "date": 1, "minimum_nights": 1}}],
                     "as": "night"}},
        {"$unwind": "$night"},
        # Number the available nights 1,2,3... per listing in date order.
        {"$setWindowFields": {"partitionBy": "$_id", "sortBy": {"night.date": 1},
                              "output": {"rn": {"$documentNumber": {}}}}},
        # Day number minus row number is constant inside a run of consecutive days.
        {"$addFields": {"island": {"$subtract": [{"$floor": {"$divide": [{"$toLong": "$night.date"}, day_ms]}},
                                                 "$rn"]}}},
        {"$setWindowFields": {"partitionBy": {"l": "$_id", "i": "$island"}, "sortBy": {"night.date": 1},
                              "output": {"block_end": {"$max": "$night.date",
                                                       "window": {"documents": ["unbounded", "unbounded"]}}}}},
        # Nights left in the run if a guest checks in on this night.
        {"$addFields": {"nights_left": {"$add": [{"$dateDiff": {"startDate": "$night.date", "endDate": "$block_end",
                                                               "unit": "day"}}, 1]}}},
        {"$addFields": {"can_start": {"$gte": ["$nights_left", {"$ifNull": ["$night.minimum_nights", 1]}]}}},
        {"$group": {"_id": {"listing_id": "$_id", "island": "$island"},
                    "name": {"$first": "$name"},
                    "start": {"$min": "$night.date"}, "end": {"$max": "$night.date"},
                    "nights": {"$sum": 1},
                    "min_nights_at_start": {"$top": {"sortBy": {"night.date": 1}, "output": "$night.minimum_nights"}},
                    "bookable": {"$max": "$can_start"},
                    "first_valid_checkin": {"$min": {"$cond": ["$can_start", "$night.date", None]}}}},
    ]
    if only_bookable:
        pipeline.append({"$match": {"bookable": True}})
    pipeline += [
        {"$sort": {"name": 1, "start": 1}},
        {"$project": {"_id": 0, "listing_id": "$_id.listing_id", "name": 1,
                      "month": {"$literal": f"{MONTHS[int(month)]} {year}"},
                      "start": {"$dateToString": {"date": "$start", "format": "%Y-%m-%d"}},
                      "end": {"$dateToString": {"date": "$end", "format": "%Y-%m-%d"}},
                      "nights": 1, "min_nights_at_start": 1, "bookable": 1,
                      "first_valid_checkin": {"$dateToString": {"date": "$first_valid_checkin", "format": "%Y-%m-%d"}}}},
    ]
    return "listings", pipeline


# --------------------------------------------------------------------------- Q4
def build_q4(year, city="Portland", first_month=3, last_month=8):
    """Total available nights per month (March-August) for entire homes in a city.

    city and room_type are copied into each calendar document at load time, so
    this is a single-collection pipeline on the (city, available, date) index.
    """
    start, _ = _month_bounds(int(year), int(first_month))
    _, end = _month_bounds(int(year), int(last_month))
    pipeline = [
        {"$match": {"city": city, "available": True, "date": {"$gte": start, "$lt": end}, "room_type": ENTIRE}},
        {"$group": {"_id": {"$month": "$date"}, "available_nights": {"$sum": 1},
                    "listings": {"$addToSet": "$listing_id"}}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "month_num": "$_id",
                      "month": {"$arrayElemAt": [MONTHS, "$_id"]},
                      "year": {"$literal": int(year)},
                      "available_nights": 1, "listings_with_availability": {"$size": "$listings"}}},
    ]
    return "calendar", pipeline


# --------------------------------------------------------------------------- Q5
def build_q5(first_year=2009, last_year=None):
    """Reviews received in December of each year, per city.

    One date range per December lets the planner use the (date, city) index;
    because the pipeline needs only date and city, the scan is covered (no
    documents fetched).
    """
    last_year = last_year or datetime.now().year
    decembers = [{"date": {"$gte": datetime(y, 12, 1), "$lt": datetime(y + 1, 1, 1)}}
                 for y in range(int(first_year), int(last_year) + 1)]
    pipeline = [
        {"$match": {"$or": decembers}},
        {"$project": {"_id": 0, "city": 1, "date": 1}},
        {"$group": {"_id": {"city": "$city", "year": {"$year": "$date"}}, "december_reviews": {"$sum": 1}}},
        {"$sort": {"_id.city": 1, "_id.year": 1}},
        {"$project": {"_id": 0, "city": "$_id.city", "year": "$_id.year", "december_reviews": 1}},
    ]
    return "reviews", pipeline


# --------------------------------------------------------------------------- Q6
def build_q6(limit=100):
    """Repeat reviewers whose listing is available again in a month they reviewed in,
    plus the host's other listings in the same city.

    1. repeat_reviewers (built at load time from reviews: every (reviewer, listing) pair with
       2+ reviews and the months they were written in; a review implies a past stay),
       most-repeated first;
    2. join the listing's calendar (listing_available_date index) for available nights in
       any of those months, and keep pairs that have some;
    3. join the listing itself (_id) and the host's other listings in that city (host_city).
    Because the sort comes first, $limit stops the joins early; limit=0 returns every pair.
    """
    pipeline = [
        {"$sort": {"reviews": -1, "last_review": -1}},                       # index most_repeat
        {"$lookup": {"from": "calendar", "localField": "_id.listing_id", "foreignField": "listing_id",
                     "let": {"months": "$review_months"},
                     "pipeline": [{"$match": {"available": True}},
                                  {"$match": {"$expr": {"$in": [{"$month": "$date"}, "$$months"]}}},
                                  {"$group": {"_id": {"$month": "$date"}, "nights": {"$sum": 1}}}],
                     "as": "available_in_review_months"}},
        {"$match": {"available_in_review_months.0": {"$exists": True}}},
    ]
    if int(limit):
        pipeline.append({"$limit": int(limit)})
    pipeline += [
        {"$lookup": {"from": "listings", "localField": "_id.listing_id", "foreignField": "_id", "as": "listing"}},
        {"$unwind": "$listing"},
        {"$lookup": {"from": "listings",
                     "let": {"h": "$listing.host.host_id", "c": "$listing.city", "self": "$listing._id"},
                     "pipeline": [{"$match": {"$expr": {"$and": [{"$eq": ["$host.host_id", "$$h"]},
                                                                  {"$eq": ["$city", "$$c"]},
                                                                  {"$ne": ["$_id", "$$self"]}]}}},
                                  {"$project": {"_id": 0, "listing_id": "$_id", "name": 1, "listing_url": 1,
                                                "minimum_nights": 1, "maximum_nights": 1}}],
                     "as": "other_listings_same_host_city"}},
        {"$project": {"_id": 0,
                      "reviewer_name": 1, "reviewer_id": "$_id.reviewer_id", "times_reviewed": "$reviews",
                      "listing_id": "$listing._id", "name": "$listing.name", "listing_url": "$listing.listing_url",
                      "description": "$listing.description", "host_name": "$listing.host.host_name",
                      "city": "$listing.city",
                      "previously_booked": {"$literal": True},
                      "previously_booked_months": {"$map": {"input": {"$sortArray": {"input": "$review_months",
                                                                                     "sortBy": 1}},
                                                            "in": {"$arrayElemAt": [MONTHS, "$$this"]}}},
                      "available_again_in": {"$map": {"input": {"$sortArray": {"input": "$available_in_review_months",
                                                                               "sortBy": {"_id": 1}}},
                                                      "in": {"$arrayElemAt": [MONTHS, "$$this._id"]}}},
                      "minimum_nights": "$listing.minimum_nights", "maximum_nights": "$listing.maximum_nights",
                      "other_listings_same_host_city": 1}},
    ]
    return "repeat_reviewers", pipeline


# --------------------------------------------------------------------------- registry
QUERIES = {
    "q1": dict(title="Q1. Listings available for a two-day period, by rating", build=build_q1,
               params=[("start", "date", "First night (YYYY-MM-DD)"), ("city", "text", "City")]),
    "q2": dict(title="Q2. Neighbourhoods with zero available listings in a month", build=build_q2,
               params=[("year", "int", "Year"), ("month", "int", "Month (1-12)")]),
    "q3": dict(title="Q3. Bookable availability periods for entire homes in Salem", build=build_q3,
               params=[("year", "int", "Year"), ("month", "int", "Month (1-12)"), ("city", "text", "City")]),
    "q4": dict(title="Q4. Available nights per month, March-August, entire homes in Portland", build=build_q4,
               params=[("year", "int", "Year"), ("city", "text", "City")]),
    "q5": dict(title="Q5. December reviews per city per year", build=build_q5,
               params=[("first_year", "int", "From year"), ("last_year", "int", "To year")]),
    "q6": dict(title="Q6. Repeat reviewers whose listing is available again + host's other listings",
               build=build_q6, params=[("limit", "int", "Max rows (0 = all)")]),
}


def default_params(db):
    """Sensible defaults derived from the loaded data, so every query returns rows."""
    # Uses the (city, available, date) index, so this is two index seeks, not a scan.
    first = db.calendar.find_one({"city": "Portland", "available": True}, {"date": 1}, sort=[("date", 1)])
    last = db.calendar.find_one({"city": "Portland", "available": True}, {"date": 1}, sort=[("date", -1)])
    cal_start = first["date"] if first else datetime(2026, 1, 1)
    cal_end = last["date"] if last else datetime(2026, 12, 31)
    # A whole month that the calendar covers: the month after the first scraped day.
    m = datetime(cal_start.year + (cal_start.month == 12), cal_start.month % 12 + 1, 1)
    # Q4 needs March-August inside the calendar window: pick the year whose August ends before cal_end.
    q4_year = cal_end.year if cal_end >= datetime(cal_end.year, 8, 31) else cal_end.year - 1
    rv = db.reviews.find_one({}, {"date": 1}, sort=[("date", 1)])
    return {
        "q1": {"start": (cal_start + timedelta(days=30)).strftime("%Y-%m-%d"), "city": "Portland"},
        "q2": {"year": m.year, "month": m.month},
        "q3": {"year": m.year, "month": m.month, "city": "Salem"},
        "q4": {"year": q4_year, "city": "Portland"},
        "q5": {"first_year": rv["date"].year if rv else 2009, "last_year": datetime.now().year},
        "q6": {"limit": 100},
    }


def _coerce(qid, params):
    out = {}
    for name, kind, _ in QUERIES[qid]["params"]:
        if name in params and params[name] not in (None, ""):
            out[name] = int(params[name]) if kind == "int" else params[name]
    return out


def run(db, qid, **params):
    coll, pipeline = QUERIES[qid]["build"](**_coerce(qid, params))
    t0 = time.perf_counter()
    rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    ms = (time.perf_counter() - t0) * 1000
    return {"collection": coll, "pipeline": pipeline, "rows": rows, "ms": round(ms, 1)}


def explain(db, qid, **params):
    """Summarise the execution plan: indexes used, keys/docs examined, collection scans."""
    coll, pipeline = QUERIES[qid]["build"](**_coerce(qid, params))
    plan = db.command({"explain": {"aggregate": coll, "pipeline": pipeline, "cursor": {}},
                       "verbosity": "executionStats"})
    summary = {"indexes": set(), "stages": set(), "keys_examined": 0, "docs_examined": 0, "collscan": False}

    def walk(node):
        if isinstance(node, dict):
            if "indexName" in node:
                summary["indexes"].add(node["indexName"])
            if "stage" in node and isinstance(node["stage"], str):
                summary["stages"].add(node["stage"])
                if node["stage"] == "COLLSCAN":
                    summary["collscan"] = True
            for k in ("totalKeysExamined", "totalDocsExamined"):
                if isinstance(node.get(k), int):
                    summary["keys_examined" if "Keys" in k else "docs_examined"] += node[k]
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(plan)
    summary["indexes"] = sorted(summary["indexes"])
    summary["stages"] = sorted(summary["stages"])
    return summary


# --------------------------------------------------------------------------- CLI
def main():
    ap = argparse.ArgumentParser(description="Run a project query and print JSON results.")
    ap.add_argument("query", choices=list(QUERIES) + ["all"])
    ap.add_argument("--explain", action="store_true", help="print the plan summary as well")
    ap.add_argument("--max-rows", type=int, default=20, help="rows to print (all rows are computed)")
    for p in {p for q in QUERIES.values() for p, _, _ in q["params"]}:
        ap.add_argument(f"--{p.replace('_', '-')}", dest=p)
    args = ap.parse_args()

    db = MongoClient(config.MONGO_URI)[config.DB_NAME]
    defaults = default_params(db)
    for qid in (QUERIES if args.query == "all" else [args.query]):
        params = {**defaults[qid], **{k: v for k, v in vars(args).items() if v is not None and
                                     k in [p for p, _, _ in QUERIES[qid]["params"]]}}
        res = run(db, qid, **params)
        print(f"\n### {QUERIES[qid]['title']}  params={params}")
        print(f"{len(res['rows'])} rows in {res['ms']} ms")
        if args.explain:
            print("plan:", explain(db, qid, **params))
        for r in res["rows"][:args.max_rows]:
            print(json.dumps(r, default=str))


if __name__ == "__main__":
    sys.exit(main())
