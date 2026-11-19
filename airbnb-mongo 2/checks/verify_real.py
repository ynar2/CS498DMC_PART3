"""Independent pandas recomputation of Q1-Q6 on the real CSVs (chunked), compared with MongoDB."""
"""Usage: python checks/verify_real.py [data-dir]   (default: ./data)"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from pymongo import MongoClient
import queries, config
D = sys.argv[1] if len(sys.argv) > 1 else config.DATA_DIR; CITIES = config.CITIES
db = MongoClient()[config.DB_NAME]; P = queries.default_params(db); ok = True
def check(name, a, b):
    global ok; good = a == b; ok &= good
    print(f"{name}: {'MATCH' if good else 'MISMATCH'} (mongo={len(a)}, pandas={len(b)})", flush=True)
    if not good: print("  mongo-only", list(a-b)[:5], "\n  pandas-only", list(b-a)[:5])
L = []
for k, c in CITIES.items():
    l = pd.read_csv(f"{D}/{k}/listings.csv", dtype=str, keep_default_na=False,
                    usecols=["id","room_type","neighbourhood_cleansed","host_id","name"]); l["city"] = c; L.append(l)
L = pd.concat(L); L["id"] = L.id.astype("int64"); info = L.set_index("id")
def cal(k, months=None):
    for ch in pd.read_csv(f"{D}/{k}/calendar.csv", dtype=str, keep_default_na=False, chunksize=2_000_000,
                          usecols=["listing_id","date","available","minimum_nights"]):
        ch = ch[ch.available == "t"].copy(); ch["date"] = pd.to_datetime(ch.date); ch["listing_id"] = ch.listing_id.astype("int64")
        yield ch
# Q1
d1 = pd.Timestamp(P["q1"]["start"]); c = pd.concat(x[x.date.between(d1, d1 + pd.Timedelta(days=1))] for x in cal("portland"))
check("Q1", {r["listing_id"] for r in queries.run(db, "q1", **P["q1"])["rows"]}, set(c.groupby("listing_id").size().loc[lambda s: s == 2].index))
# Q2 + Q6 calendar facts in one pass over every city
y, m = P["q2"]["year"], P["q2"]["month"]; have = set(); avail_months = {}
for k, city in CITIES.items():
    for x in cal(k):
        mm = x[(x.date.dt.year == y) & (x.date.dt.month == m)]
        nb = info.neighbourhood_cleansed.reindex(mm.listing_id.unique())
        have |= {(city, n) for n in nb.dropna().unique()}
        for lid, mon in set(zip(x.listing_id, x.date.dt.month)):
            avail_months.setdefault(lid, set()).add(mon)
N = set()
for k, c in CITIES.items():
    n = pd.read_csv(f"{D}/{k}/neighbourhoods.csv", dtype=str, keep_default_na=False); N |= {(c, v) for v in n.neighbourhood}
check("Q2", {(r["city"], r["neighbourhood"]) for r in queries.run(db, "q2", **P["q2"])["rows"]}, N - have)
# Q3
y, m = P["q3"]["year"], P["q3"]["month"]; sal = set(L[(L.city == "Salem") & (L.room_type == "Entire home/apt")].id)
c = pd.concat(x[x.listing_id.isin(sal) & (x.date.dt.year == y) & (x.date.dt.month == m)] for x in cal("salem")).sort_values(["listing_id", "date"])
exp = set()
for lid, g in c.groupby("listing_id"):
    days = list(g.date); mins = [int(v or 1) for v in g.minimum_nights]; i = 0
    while i < len(days):
        j = i
        while j + 1 < len(days) and days[j + 1] - days[j] == pd.Timedelta(days=1): j += 1
        if any((j - q + 1) >= mins[q] for q in range(i, j + 1)): exp.add((lid, str(days[i].date()), str(days[j].date())))
        i = j + 1
check("Q3", {(r["listing_id"], r["start"], r["end"]) for r in queries.run(db, "q3", **P["q3"])["rows"]}, exp)
# Q4
y = P["q4"]["year"]; ent = set(L[(L.city == "Portland") & (L.room_type == "Entire home/apt")].id)
c = pd.concat(x[x.listing_id.isin(ent) & (x.date.dt.year == y) & x.date.dt.month.between(3, 8)] for x in cal("portland"))
check("Q4", {(r["month_num"], r["available_nights"]) for r in queries.run(db, "q4", **P["q4"])["rows"]}, set(c.groupby(c.date.dt.month).size().items()))
# Q5 + Q6
R = []
for k, c in CITIES.items():
    r = pd.read_csv(f"{D}/{k}/reviews.csv", dtype=str, keep_default_na=False, usecols=["listing_id","date","reviewer_id"]); r["city"] = c; R.append(r)
R = pd.concat(R); R["date"] = pd.to_datetime(R.date); R["listing_id"] = R.listing_id.astype("int64")
dec = R[R.date.dt.month == 12]
check("Q5", {(x["city"], x["year"], x["december_reviews"]) for x in queries.run(db, "q5", **P["q5"])["rows"]},
      {(a, b, n) for (a, b), n in dec.groupby([dec.city, dec.date.dt.year]).size().items()})
R = R[R.reviewer_id != ""]; R["mon"] = R.date.dt.month
g = R.groupby(["reviewer_id", "listing_id"]).agg(n=("mon", "size"), months=("mon", lambda s: set(s)))
g = g[g.n > 1]
exp = {(int(rid), lid) for (rid, lid), row in g.iterrows() if row.months & avail_months.get(lid, set())}
m6 = queries.run(db, "q6", limit=0)["rows"]
check("Q6", {(r["reviewer_id"], r["listing_id"]) for r in m6}, exp)
print("ALL OK" if ok else "SOME CHECKS FAILED")
