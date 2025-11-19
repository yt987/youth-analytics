# tests/test_api.py
import pandas as pd
import pytest


def make_df():
    return pd.DataFrame(
        [
            {
                "country": "Aland",
                "country_code": "AAA",
                "youth_literacy_rate": 96.5,
                "primary_enrollment_rate": 91.2,
                "secondary_enrollment_rate": 86.5,
                "gov_education_spending_pct_gdp": 4.6,
                "latest_year": 2020,
                "region": "Europe & Central Asia",
                "income_group": "High income",
                "education_profile": "high_access_literacy",
            },
            {
                "country": "Bretoria",
                "country_code": "BBB",
                "youth_literacy_rate": 90.0,
                "primary_enrollment_rate": 89.5,
                "secondary_enrollment_rate": 83.0,
                "gov_education_spending_pct_gdp": 4.0,
                "latest_year": 2020,
                "region": "East Asia & Pacific",
                "income_group": "Upper middle income",
                "education_profile": "mixed_profile",
            },
        ]
    )


@pytest.fixture()
def app(monkeypatch):
    import app as app_pkg

    # Patch loader to bypass disk CSV
    monkeypatch.setattr(app_pkg, "_load_clean_csv", lambda root_dir: make_df())
    flask_app = app_pkg.create_app()
    flask_app.config.update(TESTING=True)
    yield flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


def test_meta(client):
    r = client.get("/api/meta")
    assert r.status_code == 200
    j = r.get_json()
    assert "regions" in j and "income_groups" in j and "profiles" in j
    assert j["counts"]["countries"] == 2
    assert j["last_year"] == 2020


def test_stats_filters(client):
    # Filter to Europe region (only AAA)
    r = client.get("/api/stats?region=Europe%20%26%20Central%20Asia")
    assert r.status_code == 200
    j = r.get_json()
    assert j["counts"]["countries"] == 1
    assert j["avg"]["literacy"] == 96.5

    # Apply numeric threshold that excludes both -> zero countries but stats should be nulls
    r = client.get("/api/stats?min_literacy=99")
    j = r.get_json()
    assert j["counts"]["countries"] == 0
    assert j["avg"]["literacy"] is None


def test_countries_pagination_and_sort(client):
    # Sort by literacy desc; AAA first
    r = client.get("/api/countries?sort=literacy&order=desc&page=1&per_page=1")
    assert r.status_code == 200
    j = r.get_json()
    assert j["page"] == 1 and j["per_page"] == 1 and j["total"] == 2 and j["pages"] == 2
    assert j["results"][0]["country_code"] == "AAA"

    # Next page should have BBB
    r2 = client.get("/api/countries?sort=literacy&order=desc&page=2&per_page=1")
    j2 = r2.get_json()
    assert j2["results"][0]["country_code"] == "BBB"


def test_country_endpoint(client):
    r = client.get("/api/country/AAA")
    assert r.status_code == 200
    j = r.get_json()
    assert j["country"] == "Aland" and j["country_code"] == "AAA"

    r404 = client.get("/api/country/XXX")
    assert r404.status_code == 404