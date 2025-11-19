# Global Youth Education Analytics

A lightweight data science project that analyzes global youth education conditions using World Bank WDI data, serving a Flask API + interactive dashboard.

---

## What It Does
- Cleans WDI CSVs → builds `education_clean.csv` (~250 countries)
- Computes a **Youth Learning Score (0–100)** using weighted metrics
- Generates **education profiles** (e.g., high access & literacy)
- Provides **live Flask APIs** for filtering & insights
- Frontend dashboard with sorting, search, and country comparisons

---

## Setup & Run
```bash
git clone https://github.com/yt987/youth-analytics.git
cd youth-learning-explorer
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python scripts/data_pipeline.py      # build clean CSV + insights
python run.py                        # start dashboard
