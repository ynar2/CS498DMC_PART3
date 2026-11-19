# Data model and design decisions (Stage 3)

Team: Cloud Data Pioneers. Yash Naroju (ynaroju2), Yogi Patel (ypate68)
System: MongoDB 8.0 · Dataset: Inside Airbnb (Los Angeles, Portland, Salem, San Diego)

## Collections

### `listings`: one document per listing (`_id` = Airbnb listing id)
```javascript
{
  _id: 6421,
  city: "Portland",
  name: "Cozy Pearl District Loft",
  listing_url: "https://www.airbnb.com/rooms/6421",
  description: "...",
  host: { host_id: 9921, host_name: "Dana" },          // embedded: always read with the listing
  neighbourhood: "Pearl",
  location: { type: "Point", coordinates: [-122.68, 45.53] },
  property_type: "Entire loft",
  room_type: "Entire home/apt",
  accommodates: 4,
  amenities: ["Wifi", "Kitchen", "Free parking"],       // array, not a separate table
  price: 145.0,                                          // "$145.00" cleaned to a number
  minimum_nights: 2, maximum_nights: 365,
  number_of_reviews: 87,
  review_scores_rating: 4.87
}
```

### `calendar`: one document per listing per night
```javascript
{ listing_id: 6421, date: ISODate("2026-03-14"), available: true, price: null,
  minimum_nights: 2, maximum_nights: 365,
  city: "Portland", neighbourhood: "Pearl", room_type: "Entire home/apt" }  // copied from the listing
```

### `reviews`: one document per review row (automatic `_id`)
```javascript
{ review_id: 98123, listing_id: 6421, city: "Portland", date: ISODate("2024-12-03"),
  reviewer_id: 55102, reviewer_name: "Chen" }            // comments omitted: no query uses them
```

### `repeat_reviewers`: derived at load time for Q6 (65,000 documents)
```javascript
{ _id: { reviewer_id: 55102, listing_id: 6421 }, reviewer_name: "Chen", reviews: 3,
  review_months: [7, 12], last_review: ISODate("2025-07-14") }
```
Grouping all 3.4M reviews takes ~40 s, so it is done once with `$out` instead of on every Q6 run.

### `neighbourhoods`: one document per neighbourhood (`_id` = "city|name")
```javascript
{ _id: "Portland|Pearl", city: "Portland", neighbourhood: "Pearl",
  neighbourhood_group: null, boundary: { type: "MultiPolygon", coordinates: [...] } }
```

## Indexes

| Collection | Index | Used by |
|---|---|---|
| listings | `_id` (automatic) | Q1, Q6 joins by listing id |
| listings | `{city, room_type}` | Q3 |
| listings | `{host.host_id, city}` | Q6 other listings by the same host |
| calendar | `{city, available, date, neighbourhood, room_type, listing_id}` | Q1, Q2, Q4: equality fields, then the date range, then fields that make the query covered |
| calendar | `{listing_id, available, date}` | Q3, Q6 per-listing calendar joins |
| reviews | `{date, city}` | Q5 (covered: no documents fetched) |
| reviews | `{listing_id}` | per-listing review lookups |
| reviews | `{reviewer_id, listing_id, date}` | building `repeat_reviewers` at load time |
| repeat_reviewers | `{reviews: -1, last_review: -1}` | Q6 order (most-repeated guests first) |
| listings | `{city, neighbourhood}` | Q2 listing counts |
| neighbourhoods | `{city, neighbourhood}` | Q2 |

Shell equivalents:
```javascript
db.calendar.createIndex({ city: 1, available: 1, date: 1, neighbourhood: 1, room_type: 1, listing_id: 1 })
db.listings.createIndex({ "host.host_id": 1, city: 1 })
```

## Changes from the Stage 2 design

| Stage 2 | Stage 3 | Why |
|---|---|---|
| Listings schema had no `city`, rating, min/max nights, host, URL or description | Added | Q1, Q3 and Q6 need them |
| Reviews had no `reviewer_id` | Added | Q6 groups by reviewer, and names are not unique |
| Q1 filtered availability on `listings` | Q1 starts on `calendar`, then joins listings | Availability lives in the calendar |
| Secondary index on `neighbourhood` | `{city, available, date}` on calendar plus the others above | The neighbourhood index matched none of the query filters |
| Calendar linked to listings only by id | `city`, `neighbourhood` and `room_type` copied into each calendar row | Q2 and Q4 filter the calendar without a join |
| Reviews had no city | `city` copied into reviews | Q5 groups by city without a join to listings |
| Calendar kept separate "to avoid the 16 MB limit" | Still separate, with a corrected reason | A year of nights is only about 30 KB per listing. The real reasons are unbounded array growth and rewriting a large document on every availability change |
| Review comments stored | Dropped (optional with `--keep-comments`) | No query uses them, and they are the largest field |
| Review id as `_id` | Automatic `_id`, `review_id` kept as a field | 2,509 reviews share an id: Airbnb copies a review onto every unit of a multi-unit listing |
| Q6 grouped all reviews per run | `repeat_reviewers` precomputed at load | ~40 s grouping per run → top 100 in 0.13 s |
| Narrow calendar index | Covering index (see above) | On full data Q2 took 32 s → 4.2 s; Q4 3.0 s → 1.4 s with 0 documents read |

## Results on the full data (September 2026 snapshot)

25,932,223 of 25,932,223 source rows loaded (100%). See `load_report.json` and `results/summary.md`.
All six queries were cross-checked against an independent pandas computation over the raw CSVs and match exactly.

## Replication and sharding (how this would scale on a cluster)
```javascript
rs.initiate({ _id: "rs0", members: [
  { _id: 0, host: "mongo1:27017" }, { _id: 1, host: "mongo2:27017" }, { _id: 2, host: "mongo3:27017" } ] })

sh.enableSharding("airbnb")
sh.shardCollection("airbnb.calendar", { city: 1, listing_id: 1 })   // keeps a city's nights together
sh.shardCollection("airbnb.reviews",  { listing_id: "hashed" })
```
The prototype runs on a single local `mongod`: all four cities fit on one machine.
