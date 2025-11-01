# fetch_and_store.py
import requests, pandas as pd, sqlite3, os
from datetime import datetime

API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"
RESOURCE_ID = "ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
STATE = "PUNJAB"

OUT_CSV = "punjab_latest.csv"
OUT_DB = "mgnrega_punjab.db"

def fetch_csv():
    url = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
    params = {
        "api-key": API_KEY,
        "format": "csv",
        "filters[state_name]": STATE,
        "limit": 50000
    }
    print("Fetching:", url)
    r = requests.get(url, params=params, timeout=30)
    print("Status:", r.status_code, "len:", len(r.content))
    r.raise_for_status()
    open(OUT_CSV, "wb").write(r.content)
    print("Saved CSV:", OUT_CSV)
    return OUT_CSV

def ingest_csv_to_sqlite(csv_path):
    print("Reading CSV to DataFrame...")
    df = pd.read_csv(csv_path)
    # basic normalization: ensure columns exist we care about, rename common variants
    # We'll keep entire CSV as raw_json but extract a few numeric fields if present.
    # Normalize column names to lowercase
    df.columns = [c.strip() for c in df.columns]
    # Prepare DB
    conn = sqlite3.connect(OUT_DB)
    cur = conn.cursor()
    # create tables
    cur.execute("""CREATE TABLE IF NOT EXISTS districts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT,
        district_name TEXT,
        district_code TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS mgnrega_monthly (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        district_id INTEGER,
        year INTEGER,
        month INTEGER,
        people_employed INTEGER,
        wages_paid REAL,
        works_completed INTEGER,
        avg_days_per_household REAL,
        raw_json TEXT,
        UNIQUE(district_id, year, month)
    )""")
    conn.commit()
    # Upsert districts and monthly rows
    inserted = 0
    for idx, row in df.iterrows():
        # try to find district name fields
        dname = None
        for k in ['district_name','district','districtname','district_name ']:
            if k in row and pd.notna(row[k]):
                dname = str(row[k]).strip()
                break
        if not dname:
            # skip rows without district name
            continue
        # district code if present
        dcode = None
        for k in ['district_code','district_code ']:
            if k in row and pd.notna(row[k]):
                dcode = str(row[k]).strip()
                break
        # upsert district
        cur.execute("SELECT id FROM districts WHERE district_name = ? AND state = ?", (dname, STATE))
        r = cur.fetchone()
        if r:
            district_id = r[0]
        else:
            cur.execute("INSERT INTO districts (state,district_name,district_code) VALUES (?,?,?)", (STATE, dname, dcode))
            district_id = cur.lastrowid
        # extract year/month fields
        year = None
        month = None
        for k in ['year','financial_year','fy']:
            if k in row and pd.notna(row[k]):
                try:
                    year = int(float(row[k]))
                    break
                except:
                    pass
        for k in ['month','mnth']:
            if k in row and pd.notna(row[k]):
                try:
                    month = int(float(row[k]))
                    break
                except:
                    pass
        # numeric fields heuristics
        def getnum(*cands):
            for c in cands:
                if c in row and pd.notna(row[c]):
                    try:
                        return float(row[c])
                    except:
                        try:
                            s = str(row[c]).replace(',','')
                            return float(s)
                        except:
                            pass
            return None
        people = getnum('people_worked','persons','no_of_persons','people_employed')
        wages = getnum('wages_paid','wage_paid','total_wages')
        works = getnum('works_completed','works','no_of_works')
        avg_days = getnum('avg_days','avg_days_household','avg_days_per_household')

        raw_json = row.to_json(force_ascii=False)

        # upsert monthly
        try:
            cur.execute("""INSERT INTO mgnrega_monthly (district_id,year,month,people_employed,wages_paid,works_completed,avg_days_per_household,raw_json)
                VALUES (?,?,?,?,?,?,?,?)
            """, (district_id, year, month,
                    int(people) if people is not None else None,
                    float(wages) if wages is not None else None,
                    int(works) if works is not None else None,
                    float(avg_days) if avg_days is not None else None,
                    raw_json))
            inserted += 1
        except Exception as e:
            # try update on conflict: naive approach
            try:
                cur.execute("""UPDATE mgnrega_monthly SET people_employed=?, wages_paid=?, works_completed=?, avg_days_per_household=?, raw_json=?
                               WHERE district_id=? AND year=? AND month=?""", (
                    int(people) if people is not None else None,
                    float(wages) if wages is not None else None,
                    int(works) if works is not None else None,
                    float(avg_days) if avg_days is not None else None,
                    raw_json, district_id, year, month
                ))
            except Exception as e2:
                pass
    conn.commit()
    conn.close()
    print(f"Ingested rows (attempted inserts): {inserted}")
    return OUT_DB

if __name__ == '__main__':
    csv = fetch_csv()
    db = ingest_csv_to_sqlite(csv)
    print('Done. DB at:', db)
