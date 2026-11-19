| Query | Params | Rows | ms (best of 3) | Indexes | Keys examined | Docs examined | COLLSCAN |
|---|---|---|---|---|---|---|---|
| Q1 | {"start": "2026-10-14", "city": "Portland"} | 1,819 | 112.4 | city_available_date | 5,870 | 1,819 | no |
| Q2 | {"year": 2026, "month": 10} | 14 | 3327.2 | city_available_date, city_nbhd | 2,177,604 | 1 | no |
| Q3 | {"year": 2026, "month": 10, "city": "Salem"} | 333 | 89.0 | city_nbhd, city_roomtype | 3,597 | 3,597 | no |
| Q4 | {"year": 2027, "city": "Portland"} | 6 | 1477.4 | city_available_date | 390,982 | 0 | no |
| Q5 | {"first_year": 2008, "last_year": 2026} | 63 | 774.7 | date_city | 210,249 | 0 | no |
| Q6 | {"limit": 100} | 100 | 99.4 | most_repeat | 23,694 | 23,694 | no |
