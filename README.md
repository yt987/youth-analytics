# Global Youth Education Analytics

A lightweight Flask + pandas dashboard for exploring youth education conditions across countries using the World Bank WDI dataset.

## What it does
- Ingests WDI CSVs, filters key indicators, and builds a clean country-level table.
- Classifies countries into simple **education profiles** using transparent thresholds.
- Exposes JSON APIs for summaries and country lists, then renders an interactive dashboard.

## Repo layout