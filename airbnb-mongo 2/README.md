# Airbnb on MongoDB: CS 498 Stage 3 prototype

**Team Cloud Data Pioneers:** Yash Naroju (ynaroju2), Yogi Patel (ypate68)

This prototype loads the Inside Airbnb data for **Los Angeles, Portland, Salem and San Diego** into
MongoDB and implements **all six** Stage 2 queries as aggregation pipelines. A small Flask web page
runs each query and shows its results, run time and index usage.
See [DESIGN.md](DESIGN.md) for the data model, the indexes and the changes from Stage 2.

```
config.py                   connection string, database name, city folder names
etl/load.py                 clean + bulk-load the CSVs, build indexes, write load_report.json
queries.py                  the six aggregation pipelines (+ command-line runner, explain helper)
app.py, templates/, static/ local web frontend
scripts/capture_results.py  saves results/summary.md and results/qN.json (evidence for the slides)
scripts/make_sample_data.py synthetic files in the same format, for testing without the real data
```

## 1. Setup

Requirements: **Python 3.10+** and **MongoDB 6.0 or newer** (the pipelines use `$top`,
`$sortArray` and `$setWindowFields`). The prototype was developed on MongoDB 8.0.

```bash
# MongoDB: any one of these
brew install mongodb-community && brew services start mongodb-community      # macOS
docker run -d --name mongo -p 27017:27017 mongo:8                             # Docker
# or use an Atlas cluster and set MONGO_URI (below)

python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Settings (optional, environment variables):

| Variable | Default |
|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` |
| `AIRBNB_DB` | `airbnb` |
| `AIRBNB_DATA_DIR` | `./data` |

## 2. Get the data

Download from <https://insideairbnb.com/get-the-data/> for each city: **`listings.csv.gz`** and
**`reviews.csv.gz`** (the *detailed* versions), plus **`calendar.csv.gz`**, `neighbourhoods.csv` and
`neighbourhoods.geojson`. Keep the files compressed and put them in one folder per city:

```
data/
  los-angeles/  listings.csv.gz  calendar.csv.gz  reviews.csv.gz  neighbourhoods.csv  neighbourhoods.geojson
  portland/     ...
  salem/        ...
  san-diego/    ...
```

## 3. Clean and load

```bash
python -m etl.load
```

What the loader does (see the docstring in `etl/load.py`):

* **Cleaning:** strips `$` and `,` from prices, converts `t`/`f` to booleans, turns dates into BSON
  dates, parses the amenities string into an array, and turns numeric fields into numbers.
* **Restructuring:** embeds the host inside each listing and stores coordinates as GeoJSON.
  `city`, `neighbourhood` and `room_type` are copied into calendar rows, and `city` into reviews.
* **Records removed:** only rows with a missing or unparseable id or date (none in the September 2026
  snapshot: 25,932,223 of 25,932,223 rows load). Reviews that share an id across units of a
  multi-unit listing are kept, as are calendar rows whose listing is missing from the listings file.
* **Fields omitted:** columns no query uses (scrape metadata, host profile details, review text, etc.).
  Add `--keep-comments` to store review text as well.
* **Loading:** reads in 200k-row chunks and inserts unordered batches of 10k with PyMongo's `insert_many`.
* **Indexes:** builds the indexes listed in DESIGN.md, then the small `repeat_reviewers` collection Q6 reads.
* Prints a table of rows read, loaded and dropped, and writes **`load_report.json`**.

The load drops and recreates the collections, so it can be re-run safely. The full four-city load
(about 26 million rows, ~2 GB of CSV) took about 12 minutes on a 2-core, 8 GB machine and needs
roughly 7 GB of free disk for MongoDB's data and indexes. On a laptop with little RAM, start mongod
with `--wiredTigerCacheSizeGB 2`.

## 4. Run the queries (command line)

```bash
python queries.py all --explain              # every query, default parameters, with plan summary
python queries.py q1 --start 2026-03-14 --city Portland
python queries.py q2 --year 2026 --month 1
python queries.py q3 --year 2026 --month 1 --city Salem
python queries.py q4 --year 2026 --city Portland
python queries.py q5 --first-year 2015 --last-year 2025
python queries.py q6 --limit 50              # --limit 0 = every pair (~1 minute)
python scripts/capture_results.py            # writes results/summary.md + results/qN.json
```

If you leave out the parameters, defaults are derived from the loaded calendar's date range, so
every query returns rows.

| # | Question | Main collection: plan |
|---|---|---|
| Q1 | Listings available on both nights of a 2-day period in Portland, by rating (desc) | calendar `{city, available, date}` → group → join listings by `_id` → sort |
| Q2 | Neighbourhoods in any city with zero available listings in a month | neighbourhoods ∪ available (city, neighbourhood) pairs from calendar → keep those never available |
| Q3 | Bookable availability periods for entire homes in Salem for a month | listings `{city, room_type}` → join calendar `{listing_id, date}` → find runs of consecutive nights (`$setWindowFields`) → compare with `minimum_nights` |
| Q4 | Available nights per month, March–August, entire homes in Portland | calendar `{city, available, date}` → group by month |
| Q5 | December reviews per city per year | reviews `{date, city}`, one range per December (covered by the index) → group |
| Q6 | Repeat reviewers whose listing is available again in a month they reviewed in, plus the host's other listings in the same city | `repeat_reviewers` (precomputed at load) sorted by repeat count → join calendar → join listings `_id` and `{host.host_id, city}` |

## 5. Start the frontend

```bash
python app.py
```
Open <http://127.0.0.1:5000>, pick a query on the left, adjust the parameters and press **Run query**.
Tick **Show execution plan** to see the indexes used and the keys and documents examined.
The aggregation pipeline that was sent is shown under the results.

## Checking the answers

`checks/verify_real.py` recomputes all six queries with pandas straight from the CSVs and compares
them with MongoDB's results (all six match on the September 2026 data):

```bash
python checks/verify_real.py data
```

## Testing without the real data

```bash
python scripts/make_sample_data.py --out sample_data
AIRBNB_DATA_DIR=sample_data python -m etl.load
```
The sample files are random and only for checking that the code runs.
