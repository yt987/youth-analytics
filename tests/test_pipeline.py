# tests/test_pipleline.py
import csv
import os

import pandas as pd
import pytest

# File name intentionally matches the user's requested spelling: test_pipleline.py


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def make_wdi_fixtures(tmp_path):
    raw_dir = tmp_path / "data_raw" / "WDI_CSV"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Minimal WDICSV.csv with 3 years
    wdi_header = [
        "Country Name",
        "Country Code",
        "Indicator Name",
        "Indicator Code",
        "2018",
        "2019",
        "2020",
    ]

    rows = [
        # AAA has strong metrics (expect high_access_literacy)
        ["Aland", "AAA", "Youth literacy rate, 15-24 years, total (%)", "SE.ADT.1524.LT.ZS", "", "96.0", "96.5"],
        ["Aland", "AAA", "Primary education, net enrollment rate (%)", "SE.PRM.NENR", "", "91.0", "91.2"],
        ["Aland", "AAA", "Secondary education, net enrollment rate (%)", "SE.SEC.NENR", "84.0", "86.0", "86.5"],
        ["Aland", "AAA", "Government expenditure on education, total (% of GDP)", "SE.XPD.TOTL.GD.ZS", "4.1", "4.5", "4.6"],

        # BBB missing secondary net but has gross; literacy lower (expect mixed_profile/low_access depending on thresholds)
        ["Bretoria", "BBB", "Youth literacy rate, 15-24 years, total (%)", "SE.ADT.1524.LT.ZS", "88.0", "", "90.0"],
        ["Bretoria", "BBB", "Primary education, net enrollment rate (%)", "SE.PRM.NENR", "89.0", "89.5", ""],
        ["Bretoria", "BBB", "Secondary education, gross enrollment rate (%)", "SE.SEC.ENRR", "", "82.0", "83.0"],
        ["Bretoria", "BBB", "Government expenditure on education, total (% of GDP)", "SE.XPD.TOTL.GD.ZS", "3.9", "", "4.0"],
    ]
    wdi_csv = raw_dir / "WDICSV.csv"
    write_csv(str(wdi_csv), wdi_header, rows)

    # WDICountry.csv meta
    meta_header = ["Country Code", "Short Name", "Region", "Income Group"]
    meta_rows = [
        ["AAA", "Aland", "Europe & Central Asia", "High income"],
        ["BBB", "Bretoria", "East Asia & Pacific", "Upper middle income"],
        # Include an aggregate to ensure it's excluded by the pipeline's meta loader
        ["WLD", "World", "Aggregates", ""],
    ]
    meta_csv = raw_dir / "WDICountry.csv"
    write_csv(str(meta_csv), meta_header, meta_rows)

    return wdi_csv, meta_csv


def test_pipeline_builds_clean_table(tmp_path, monkeypatch):
    wdi_csv, meta_csv = make_wdi_fixtures(tmp_path)

    # Import after we have fixtures
    import importlib
    dp = importlib.import_module("scripts.data_pipeline")

    # Point the pipeline constants at our fixtures and temp output
    out_dir = tmp_path / "data_clean"
    out_csv = out_dir / "education_clean.csv"
    monkeypatch.setattr(dp, "RAW_DIR", str(wdi_csv.parent))
    monkeypatch.setattr(dp, "WDI_CSV", str(wdi_csv))
    monkeypatch.setattr(dp, "COUNTRY_CSV", str(meta_csv))
    monkeypatch.setattr(dp, "OUT_DIR", str(out_dir))
    monkeypatch.setattr(dp, "OUT_CSV", str(out_csv))

    # Run end-to-end
    dp.main()

    assert out_csv.exists(), "education_clean.csv should be created"

    df = pd.read_csv(out_csv)
    # Exact column order as contract
    expected_cols = [
        "country",
        "country_code",
        "youth_literacy_rate",
        "primary_enrollment_rate",
        "secondary_enrollment_rate",
        "gov_education_spending_pct_gdp",
        "latest_year",
        "region",
        "income_group",
        "education_profile",
    ]
    assert list(df.columns) == expected_cols

    # Two countries, no aggregates
    assert len(df) == 2

    # AAA should be high_access_literacy by thresholds in pipeline
    aaa = df[df["country_code"] == "AAA"].iloc[0]
    assert aaa["education_profile"] == "high_access_literacy"
    assert pytest.approx(float(aaa["youth_literacy_rate"]), rel=0, abs=1e-6) == 96.5
    assert int(aaa["latest_year"]) == 2020

    # BBB should end up mixed_profile or low_access depending on thresholds
    bbb = df[df["country_code"] == "BBB"].iloc[0]
    assert bbb["education_profile"] in ("mixed_profile", "low_access_literacy")
    # Secondary value should come from gross fallback
    assert pytest.approx(float(bbb["secondary_enrollment_rate"]), rel=0, abs=1e-6) == 83.0