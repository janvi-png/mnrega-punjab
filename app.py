# app.py - FastAPI backend using sqlite (for Codespaces demo)
from fastapi import FastAPI, HTTPException
import sqlite3, json
from pydantic import BaseModel
from typing import List
app = FastAPI(title="MGNREGA Punjab - Demo")

DB = "mgnrega_punjab.db"

def get_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/health")
def health():
    return {"status":"ok"}

@app.get("/api/districts")
def list_districts():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, district_name FROM districts ORDER BY district_name")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

@app.get("/api/districts/{district_id}/metrics")
def district_metrics(district_id: int, limit: int = 12):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT year, month, people_employed, wages_paid, works_completed, avg_days_per_household
                   FROM mgnrega_monthly WHERE district_id=? ORDER BY year DESC, month DESC LIMIT ?""", (district_id, limit))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

@app.get("/api/locate")
def locate(q: str):
    # simple name match: q is a city/district name
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, district_name FROM districts WHERE lower(district_name) LIKE ? LIMIT 1", (f"%{q.lower()}%",))
    r = cur.fetchone()
    conn.close()
    if not r:
        raise HTTPException(status_code=404, detail="Not found")
    return {"district_id": r["id"], "district_name": r["district_name"]}
