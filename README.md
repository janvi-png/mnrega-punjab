# MGNREGA Punjab - Codespace-ready MVP (CSV + SQLite)

This repo contains a minimal end-to-end MVP to fetch MGNREGA district data for Punjab
from data.gov.in, store it as CSV and in a local SQLite DB, and expose a FastAPI backend.

## What is included
- fetch_and_store.py : downloads CSV and ingests into `mgnrega_punjab.db`
- app.py : FastAPI app exposing simple APIs
- requirements.txt : python deps
- index.html : simple frontend stub

## Run in GitHub Codespaces (recommended)
1. Create a Codespace from this repo.
2. Open the Codespace terminal.
3. Create & activate venv:
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```
4. Install deps:
   ```
   pip install -r requirements.txt
   ```
5. Fetch data and build DB:
   ```
   python fetch_and_store.py
   ```
   This will create `punjab_latest.csv` and `mgnrega_punjab.db`.
6. Run the API:
   ```
   uvicorn app:app --host 0.0.0.0 --port 8000 --reload
   ```
   In Codespaces, forward port 8000 and open in browser.
7. API endpoints:
   - GET /api/districts
   - GET /api/districts/{id}/metrics
   - GET /api/locate?q=<name>

## Notes
- This is an MVP focused on data ingestion + simple API for your demo.
- Later steps: add React frontend, multilingual UI, TTS (browser), SQS+worker, Terraform infra.
