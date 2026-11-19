"""
Generate small, synthetic Inside-Airbnb-format files for testing without the real data.

    python scripts/make_sample_data.py --out sample_data
    AIRBNB_DATA_DIR=sample_data python -m etl.load

The column names and value formats ("$1,234.00", "t"/"f", JSON amenity lists)
match the real files, so the same loader and queries run unchanged. The numbers
are random: never present results from this data as real findings.
"""
import argparse
import csv
import gzip
import json
import os
import random
from datetime import date, timedelta

CITIES = {
    "portland": (["Pearl", "Buckman", "Kerns", "Sellwood-Moreland", "Alberta Arts", "Linnton"], 120),
    "salem": (["Downtown", "Grant", "SESNA", "Morningside", "Faye Wright"], 40),
    "los-angeles": (["Venice", "Hollywood", "Silver Lake", "Echo Park", "Downtown", "Vernon"], 160),
    "san-diego": (["Pacific Beach", "North Park", "La Jolla", "Gaslamp Quarter", "Tijuana River Valley"], 100),
}
ROOM_TYPES = ["Entire home/apt"] * 6 + ["Private room"] * 3 + ["Shared room", "Hotel room"]
AMENITIES = ["Wifi", "Kitchen", "Free parking on premises", "Washer", "Dryer", "Air conditioning",
             "Heating", "Dedicated workspace", "Pool", "Hot tub", "Pets allowed"]
SCRAPE = date(2025, 9, 15)


def gz_writer(path, header):
    f = gzip.open(path, "wt", newline="")
    w = csv.writer(f)
    w.writerow(header)
    return f, w


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="sample_data")
    ap.add_argument("--seed", type=int, default=498)
    args = ap.parse_args()
    rnd = random.Random(args.seed)
    next_listing, next_review, next_host = 10_000, 1, 500

    for key, (nbhds, n_listings) in CITIES.items():
        folder = os.path.join(args.out, key)
        os.makedirs(folder, exist_ok=True)
        # The last neighbourhood in each list deliberately gets no listings (tests Q2).
        populated = nbhds[:-1]

        with open(os.path.join(folder, "neighbourhoods.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["neighbourhood_group", "neighbourhood"])
            for n in nbhds:
                w.writerow(["", n])
        feats = []
        for i, n in enumerate(nbhds):  # tiny square boundary per neighbourhood
            x, y = -122.6 + i * 0.01, 45.5
            feats.append({"type": "Feature", "properties": {"neighbourhood": n, "neighbourhood_group": None},
                          "geometry": {"type": "MultiPolygon", "coordinates": [[[[x, y], [x + .01, y], [x + .01, y + .01],
                                                                                 [x, y + .01], [x, y]]]]}})
        with open(os.path.join(folder, "neighbourhoods.geojson"), "w") as f:
            json.dump({"type": "FeatureCollection", "features": feats}, f)

        lf, lw = gz_writer(os.path.join(folder, "listings.csv.gz"),
                           ["id", "listing_url", "scrape_id", "name", "description", "host_id", "host_name",
                            "neighbourhood_cleansed", "latitude", "longitude", "property_type", "room_type",
                            "accommodates", "amenities", "price", "minimum_nights", "maximum_nights",
                            "number_of_reviews", "review_scores_rating"])
        cf, cw = gz_writer(os.path.join(folder, "calendar.csv.gz"),
                           ["listing_id", "date", "available", "price", "adjusted_price",
                            "minimum_nights", "maximum_nights"])
        rf, rw = gz_writer(os.path.join(folder, "reviews.csv.gz"),
                           ["listing_id", "id", "date", "reviewer_id", "reviewer_name", "comments"])

        hosts = [next_host + i for i in range(max(3, n_listings // 3))]
        next_host += len(hosts)
        reviewers = [(100_000 + rnd.randint(0, 4000), rnd.choice(["Ana", "Ben", "Chen", "Dee", "Eli", "Fay"]))
                     for _ in range(n_listings * 2)]
        for _ in range(n_listings):
            lid = next_listing
            next_listing += 1
            room = rnd.choice(ROOM_TYPES)
            min_n = rnd.choice([1, 1, 2, 2, 3, 5, 7, 30])
            rating = "" if rnd.random() < 0.1 else f"{rnd.uniform(3.5, 5):.2f}"
            host = rnd.choice(hosts)
            lw.writerow([lid, f"https://www.airbnb.com/rooms/{lid}", 1, f"{room} in {key} #{lid}",
                         "A synthetic listing used for testing.", host, f"Host{host}", rnd.choice(populated),
                         45.5 + rnd.random() / 10, -122.6 + rnd.random() / 10,
                         "Entire rental unit" if room.startswith("Entire") else "Private room in home", room,
                         rnd.randint(1, 8), json.dumps(rnd.sample(AMENITIES, rnd.randint(2, 7))),
                         f"${rnd.randint(45, 1200):,}.00", min_n, rnd.choice([30, 90, 365, 1125]),
                         rnd.randint(0, 80), rating])
            avail = rnd.random() < 0.8
            for d in range(365):
                if rnd.random() < 0.12:
                    avail = not avail  # produces runs of available / booked days
                day = SCRAPE + timedelta(days=d)
                cw.writerow([lid, day.isoformat(), "t" if avail else "f", "", "",
                             min_n if day.weekday() < 5 else max(1, min_n - 1), 1125])
            for _ in range(rnd.randint(0, 25)):
                rid, rname = rnd.choice(reviewers)
                rdate = date(rnd.randint(2015, 2025), rnd.randint(1, 12), rnd.randint(1, 28))
                if rdate > SCRAPE:
                    continue
                rw.writerow([lid, next_review, rdate.isoformat(), rid, rname, "Great stay!"])
                next_review += 1
            if rnd.random() < 0.15:  # a repeat guest (tests Q6)
                rid, rname = rnd.choice(reviewers)
                for y in (2022, 2024):
                    rw.writerow([lid, next_review, date(y, 7, 10).isoformat(), rid, rname, "Back again!"])
                    next_review += 1
        for f in (lf, cf, rf):
            f.close()
        print(f"{key}: {n_listings} listings")


if __name__ == "__main__":
    main()
