"""
ETL: clean the Inside Airbnb CSV files and bulk-load them into MongoDB.

Usage
-----
    python -m etl.load                     # load every city found in ./data
    python -m etl.load --data-dir /path    # load from another folder
    python -m etl.load --keep-comments     # also store review text (not needed by queries)

Expected layout (gzipped or plain CSV both work):

    data/
      portland/  listings.csv.gz  calendar.csv.gz  reviews.csv.gz
                 neighbourhoods.csv  neighbourhoods.geojson
      salem/ ...  los-angeles/ ...  san-diego/ ...

Use the *detailed* listings and reviews files from Inside Airbnb (listings.csv.gz,
reviews.csv.gz): the summary versions lack listing_url, description,
reviewer_id and other fields the queries need.

What the loader does
--------------------
1. Drops and recreates the four collections (the load is repeatable).
2. listings  -> one document per listing, _id = Airbnb listing id, host embedded.
3. calendar  -> one document per listing per night. city, neighbourhood and
                room_type are copied from the listing (denormalised) so the
                calendar queries never need a join just to filter.
4. reviews   -> one document per review row (review_id kept as a field), city copied in.
5. neighbourhoods -> one document per neighbourhood, with its GeoJSON boundary.
6. Builds the indexes the six queries use, and the small repeat_reviewers
   collection that Q6 reads (see build_repeat_reviewers).
7. Writes load_report.json with rows read / loaded / dropped per file. Those
   numbers go on the "data coverage" slide.
"""
import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import BulkWriteError, OperationFailure

# Allow "python etl/load.py" as well as "python -m etl.load".
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

CHUNK_ROWS = 200_000  # rows read from a CSV at a time (calendar files are tens of millions of rows)


# --------------------------------------------------------------------------- helpers
def find_file(folder, stem):
    """Return the path of <stem>.csv.gz or <stem>.csv inside folder, or None."""
    for name in (f"{stem}.csv.gz", f"{stem}.csv"):
        path = os.path.join(folder, name)
        if os.path.exists(path):
            return path
    return None


def parse_price(series):
    """'$1,234.00' -> 1234.0 ; blanks -> None."""
    cleaned = series.str.replace(r"[$,]", "", regex=True).str.strip()
    return pd.to_numeric(cleaned, errors="coerce")


def parse_int(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def parse_float(series):
    return pd.to_numeric(series, errors="coerce")


def parse_date(series):
    """'2026-03-01' -> datetime (stored as a BSON Date)."""
    return pd.to_datetime(series, errors="coerce", format="%Y-%m-%d")


def parse_amenities(text):
    """Amenities arrive as a JSON-style list in a string: '["Wifi", "Kitchen"]'."""
    if not text:
        return []
    try:
        value = json.loads(text)
        return [str(a) for a in value] if isinstance(value, list) else []
    except ValueError:
        # Very old snapshots used '{Wifi,Kitchen}'.
        return [a.strip(' "') for a in text.strip("{}[]").split(",") if a.strip(' "')]


def clean_value(v):
    """Convert pandas/numpy scalars to plain Python (None for missing)."""
    if v is None:
        return None
    if v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and v != v:  # NaN
        return None
    if hasattr(v, "item"):  # numpy scalar
        return v.item()
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def records(df):
    """DataFrame -> list of dicts with plain Python values (None for missing), ready for BSON."""
    cols = list(df.columns)
    columns = []
    for c in cols:
        s = df[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            columns.append([None if pd.isna(v) else v.to_pydatetime() for v in s])
        elif pd.api.types.is_bool_dtype(s):
            columns.append([bool(v) for v in s])
        elif pd.api.types.is_integer_dtype(s):          # nullable Int64
            columns.append([None if pd.isna(v) else int(v) for v in s])
        elif pd.api.types.is_float_dtype(s):
            columns.append([None if v != v else float(v) for v in s.tolist()])
        else:                                            # strings, lists
            columns.append([clean_value(v) for v in s.tolist()])
    return [dict(zip(cols, row)) for row in zip(*columns)]


def insert_batches(coll, docs):
    """insert_many in fixed-size, unordered batches. Returns number inserted."""
    inserted = 0
    for i in range(0, len(docs), config.BATCH_SIZE):
        batch = docs[i:i + config.BATCH_SIZE]
        try:
            inserted += len(coll.insert_many(batch, ordered=False).inserted_ids)
        except BulkWriteError as e:  # duplicate _id etc.: keep the rest of the batch
            inserted += e.details.get("nInserted", 0)
    return inserted


def count_rows(path):
    """Number of data rows in a CSV (used for the 'total records' figure)."""
    total = 0
    for chunk in pd.read_csv(path, usecols=[0], dtype=str, chunksize=CHUNK_ROWS,
                             keep_default_na=False, on_bad_lines="skip", engine="c"):
        total += len(chunk)
    return total


# --------------------------------------------------------------------------- loaders
LISTING_COLS = [
    "id", "listing_url", "name", "description", "host_id", "host_name",
    "neighbourhood_cleansed", "latitude", "longitude", "property_type", "room_type",
    "accommodates", "amenities", "price", "minimum_nights", "maximum_nights",
    "number_of_reviews", "review_scores_rating",
]


def load_listings(db, path, city, report):
    """Load one city's listings. Returns {listing_id: (neighbourhood, room_type)}."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, on_bad_lines="skip")
    rows_read = len(df)
    missing = [c for c in LISTING_COLS if c not in df.columns]
    if "id" in missing:
        raise SystemExit(f"{path}: no 'id' column - is this an Inside Airbnb listings file?")
    if missing:
        print(f"  WARNING {path}: missing columns {missing} (summary file?). They will be null.")
        for c in missing:
            df[c] = ""

    df = df[LISTING_COLS].copy()
    df["id"] = parse_int(df["id"])
    df = df.dropna(subset=["id"]).drop_duplicates(subset=["id"])

    out = pd.DataFrame({
        "_id": df["id"],
        "city": city,
        "name": df["name"],
        "listing_url": df["listing_url"],
        "description": df["description"],
        "host_id": parse_int(df["host_id"]),
        "host_name": df["host_name"],
        "neighbourhood": df["neighbourhood_cleansed"],
        "latitude": parse_float(df["latitude"]),
        "longitude": parse_float(df["longitude"]),
        "property_type": df["property_type"],
        "room_type": df["room_type"],
        "accommodates": parse_int(df["accommodates"]),
        "amenities": df["amenities"].map(parse_amenities),
        "price": parse_price(df["price"]),
        "minimum_nights": parse_int(df["minimum_nights"]),
        "maximum_nights": parse_int(df["maximum_nights"]),
        "number_of_reviews": parse_int(df["number_of_reviews"]),
        "review_scores_rating": parse_float(df["review_scores_rating"]),
    })

    docs = []
    for r in records(out):
        # Embed the host as a sub-document and the coordinates as GeoJSON.
        r["host"] = {"host_id": r.pop("host_id"), "host_name": r.pop("host_name")}
        lat, lon = r.pop("latitude"), r.pop("longitude")
        r["location"] = {"type": "Point", "coordinates": [lon, lat]} if lat is not None and lon is not None else None
        docs.append(r)

    loaded = insert_batches(db.listings, docs)
    report.append(dict(file=path, collection="listings", city=city, rows_read=rows_read,
                       loaded=loaded, dropped=rows_read - loaded))
    return {d["_id"]: (d["neighbourhood"], d["room_type"]) for d in docs}


def load_calendar(db, path, city, listing_info, report):
    rows_read = loaded = orphans = 0
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK_ROWS,
                             on_bad_lines="skip"):
        rows_read += len(chunk)
        df = pd.DataFrame({
            "listing_id": parse_int(chunk["listing_id"]),
            "date": parse_date(chunk["date"]),
            "available": chunk["available"].str.lower().eq("t"),
            "price": parse_price(chunk["price"]) if "price" in chunk else None,
            "minimum_nights": parse_int(chunk["minimum_nights"]),
            "maximum_nights": parse_int(chunk["maximum_nights"]),
        }).dropna(subset=["listing_id", "date"])

        docs = records(df)
        for d in docs:
            info = listing_info.get(d["listing_id"])
            if info is None:
                orphans += 1  # listing not in listings file; keep the row, attributes unknown
                info = (None, None)
            d["city"], d["neighbourhood"], d["room_type"] = city, info[0], info[1]
        loaded += insert_batches(db.calendar, docs)
        print(f"    calendar {city}: {loaded:,} rows loaded", end="\r")
    print()
    report.append(dict(file=path, collection="calendar", city=city, rows_read=rows_read,
                       loaded=loaded, dropped=rows_read - loaded, orphan_rows_kept=orphans))


def load_reviews(db, path, city, keep_comments, report):
    rows_read = loaded = 0
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK_ROWS,
                             on_bad_lines="skip"):
        rows_read += len(chunk)
        data = {
            # Not _id: Airbnb attaches one review to every unit of a multi-unit listing,
            # so the same review id appears under several listing ids. Each row is kept.
            "review_id": parse_int(chunk["id"]),
            "listing_id": parse_int(chunk["listing_id"]),
            "city": city,
            "date": parse_date(chunk["date"]),
            "reviewer_id": parse_int(chunk["reviewer_id"]) if "reviewer_id" in chunk else pd.NA,
            "reviewer_name": chunk["reviewer_name"] if "reviewer_name" in chunk else None,
        }
        if keep_comments and "comments" in chunk:
            data["comments"] = chunk["comments"]
        df = pd.DataFrame(data).dropna(subset=["review_id", "listing_id", "date"])
        loaded += insert_batches(db.reviews, records(df))
    report.append(dict(file=path, collection="reviews", city=city, rows_read=rows_read,
                       loaded=loaded, dropped=rows_read - loaded))


def load_neighbourhoods(db, folder, city, report):
    csv_path = os.path.join(folder, "neighbourhoods.csv")
    geo_path = os.path.join(folder, "neighbourhoods.geojson")
    docs = {}
    rows_read = 0
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        rows_read += len(df)
        for _, r in df.iterrows():
            name = r["neighbourhood"]
            docs[name] = {"_id": f"{city}|{name}", "city": city, "neighbourhood": name,
                          "neighbourhood_group": r.get("neighbourhood_group") or None, "boundary": None}
    if os.path.exists(geo_path):
        with open(geo_path) as f:
            for feat in json.load(f).get("features", []):
                name = feat["properties"].get("neighbourhood")
                if not name:
                    continue
                d = docs.setdefault(name, {"_id": f"{city}|{name}", "city": city, "neighbourhood": name,
                                           "neighbourhood_group": feat["properties"].get("neighbourhood_group")})
                d["boundary"] = feat.get("geometry")
    loaded = insert_batches(db.neighbourhoods, list(docs.values()))
    report.append(dict(file=csv_path, collection="neighbourhoods", city=city, rows_read=rows_read or loaded,
                       loaded=loaded, dropped=max(0, rows_read - loaded)))


# --------------------------------------------------------------------------- indexes
def create_indexes(db):
    """Indexes chosen for the six queries (see DESIGN.md for the reasoning)."""
    db.listings.create_index([("city", ASCENDING), ("room_type", ASCENDING)], name="city_roomtype")      # Q3
    db.listings.create_index([("host.host_id", ASCENDING), ("city", ASCENDING)], name="host_city")       # Q6
    db.listings.create_index([("review_scores_rating", DESCENDING)], name="rating_desc")                 # Q1 sort
    db.listings.create_index([("city", ASCENDING), ("neighbourhood", ASCENDING)], name="city_nbhd")       # Q2 counts
    # Equality (city, available) before range (date): the ESR rule. The trailing fields make
    # Q1, Q2 and Q4 covered: they are answered from the index without reading documents.
    db.calendar.create_index([("city", ASCENDING), ("available", ASCENDING), ("date", ASCENDING),
                              ("neighbourhood", ASCENDING), ("room_type", ASCENDING), ("listing_id", ASCENDING)],
                             name="city_available_date")
    # Per-listing joins (Q3, Q6): listing_id and available are equalities, date the range.
    db.calendar.create_index([("listing_id", ASCENDING), ("available", ASCENDING), ("date", ASCENDING)],
                             name="listing_available_date")
    db.reviews.create_index([("date", ASCENDING), ("city", ASCENDING)], name="date_city")                # Q5 (covered)
    db.reviews.create_index([("listing_id", ASCENDING)], name="listing")
    db.reviews.create_index([("reviewer_id", ASCENDING), ("listing_id", ASCENDING), ("date", ASCENDING)],
                            name="reviewer_listing_date")                                             # builds Q6 view
    db.neighbourhoods.create_index([("city", ASCENDING), ("neighbourhood", ASCENDING)], name="city_nbhd")
    db.reviews.create_index([("review_id", ASCENDING)], name="review_id")
    # Geo indexes are not used by the six queries; built for map-style extensions.
    for coll, field in ((db.listings, "location"), (db.neighbourhoods, "boundary")):
        try:
            coll.create_index([(field, "2dsphere")], name=f"{field}_geo")
        except OperationFailure as e:  # some boundary files contain self-intersecting polygons
            print(f"  WARNING: {coll.name}.{field} 2dsphere index skipped: "
                  f"{str(e.details.get('errmsg', e)).split(' :: ')[0][:120]}")


# --------------------------------------------------------------------------- Q6 view
def build_repeat_reviewers(db):
    """Precompute (reviewer, listing) pairs with 2+ reviews into `repeat_reviewers`.

    Grouping all 3.4M reviews takes ~30 s, too slow to run per request; the result is only
    ~65k documents and changes only when the data is reloaded, so it is materialised here
    (a "materialized view" via $out) and Q6 reads it instead.
    """
    t0 = time.time()
    db.reviews.aggregate([
        {"$match": {"reviewer_id": {"$gte": 0}}},
        {"$sort": {"reviewer_id": 1, "listing_id": 1}},  # walks the reviewer_listing_date index
        {"$group": {"_id": {"reviewer_id": "$reviewer_id", "listing_id": "$listing_id"},
                    "reviewer_name": {"$first": "$reviewer_name"},
                    "reviews": {"$sum": 1},
                    "review_months": {"$addToSet": {"$month": "$date"}},
                    "last_review": {"$max": "$date"}}},
        {"$match": {"reviews": {"$gt": 1}}},
        {"$out": "repeat_reviewers"},
    ], allowDiskUse=True)
    db.repeat_reviewers.create_index([("reviews", DESCENDING), ("last_review", DESCENDING)], name="most_repeat")
    n = db.repeat_reviewers.estimated_document_count()
    print(f"  repeat_reviewers: {n:,} pairs in {time.time() - t0:.0f}s")
    return n


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default=config.DATA_DIR)
    ap.add_argument("--keep-comments", action="store_true", help="store review text (adds several GB)")
    args = ap.parse_args()

    client = MongoClient(config.MONGO_URI)
    db = client[config.DB_NAME]
    for name in ("listings", "calendar", "reviews", "neighbourhoods", "repeat_reviewers"):
        db.drop_collection(name)

    report, t0 = [], time.time()
    folders = sorted(d for d in glob.glob(os.path.join(args.data_dir, "*")) if os.path.isdir(d))
    if not folders:
        raise SystemExit(f"No city folders found in {args.data_dir}. See README for the layout.")

    for folder in folders:
        key = os.path.basename(folder).lower()
        city = config.CITIES.get(key, key.replace("-", " ").title())
        print(f"== {city}  ({folder})")
        lst = find_file(folder, "listings")
        if not lst:
            print("  no listings file, skipping city")
            continue
        info = load_listings(db, lst, city, report)
        print(f"  listings: {len(info):,}")
        cal = find_file(folder, "calendar")
        if cal:
            load_calendar(db, cal, city, info, report)
        rev = find_file(folder, "reviews")
        if rev:
            load_reviews(db, rev, city, args.keep_comments, report)
            print(f"  reviews loaded")
        load_neighbourhoods(db, folder, city, report)

    print("== building indexes")
    create_indexes(db)
    print("== building Q6 view")
    repeat_pairs = build_repeat_reviewers(db)

    # ---------------- summary for the data-coverage slide
    totals = {}
    for r in report:
        t = totals.setdefault(r["collection"], {"rows_read": 0, "loaded": 0, "dropped": 0})
        for k in t:
            t[k] += r[k]
    grand_read = sum(t["rows_read"] for t in totals.values())
    grand_loaded = sum(t["loaded"] for t in totals.values())
    summary = dict(
        generated=datetime.now().isoformat(timespec="seconds"),
        seconds=round(time.time() - t0, 1),
        per_file=report,
        per_collection=totals,
        total_rows_read=grand_read,
        total_loaded=grand_loaded,
        derived_repeat_reviewer_pairs=repeat_pairs,
        proportion_loaded=round(grand_loaded / grand_read, 6) if grand_read else None,
        calendar_date_range=[str(x) for x in _date_range(db.calendar)],
        review_date_range=[str(x) for x in _date_range(db.reviews)],
    )
    with open("load_report.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'collection':<16}{'rows read':>14}{'loaded':>14}{'dropped':>10}")
    for c, t in totals.items():
        print(f"{c:<16}{t['rows_read']:>14,}{t['loaded']:>14,}{t['dropped']:>10,}")
    print(f"{'TOTAL':<16}{grand_read:>14,}{grand_loaded:>14,}   "
          f"({100 * summary['proportion_loaded']:.3f}% loaded) in {summary['seconds']}s")
    print("Wrote load_report.json")


def _date_range(coll):
    first = coll.find_one({}, {"date": 1}, sort=[("date", 1)])
    last = coll.find_one({}, {"date": 1}, sort=[("date", -1)])
    return (first or {}).get("date"), (last or {}).get("date")


if __name__ == "__main__":
    main()
