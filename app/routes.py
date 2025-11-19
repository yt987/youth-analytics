import math
import os
from typing import Tuple
import json
import numpy as np

import pandas as pd
from flask import (
    Blueprint,
    current_app,
    jsonify,
    render_template,
    request,
    send_from_directory,
)



bp = Blueprint("main", __name__)

def _records_nullsafe(df: pd.DataFrame, cols=None):
    """Convert a DataFrame to JSON-ready records with NaN -> None."""
    if cols is not None:
        df = df[cols]
    # pandas writes NaN as null in JSON; json.loads gives Python None
    return json.loads(df.to_json(orient="records"))

@bp.route("/")
def index():
    # Assumes templates/dashboard.html exists in your repo structure
    return render_template("dashboard.html")


def _df_copy() -> pd.DataFrame:
    return current_app.config["DATAFRAME"].copy()


def _split_multi(val: str):
    if val is None or val == "":
        return None
    return [s.strip() for s in val.split(",") if s.strip()]


def _apply_filters(df: pd.DataFrame, args) -> pd.DataFrame:
    regions = _split_multi(args.get("region"))
    incomes = _split_multi(args.get("income_group"))
    profiles = _split_multi(args.get("profile"))

    if regions:
        df = df[df["region"].isin(regions)]
    if incomes:
        df = df[df["income_group"].isin(incomes)]
    if profiles:
        df = df[df["education_profile"].isin(profiles)]

    # Numeric minimum thresholds
    mins = {
        "youth_literacy_rate": args.get("min_literacy"),
        "primary_enrollment_rate": args.get("min_primary"),
        "secondary_enrollment_rate": args.get("min_secondary"),
        "gov_education_spending_pct_gdp": args.get("min_spend"),
    }
    for col, raw in mins.items():
        if raw not in (None, ""):
            try:
                thr = float(raw)
                df = df[df[col].notna() & (df[col] >= thr)]
            except ValueError:
                # Ignore bad inputs; leave df unchanged for that filter
                pass

    return df

def _compute_yls(df: pd.DataFrame) -> pd.Series:
    metrics = [
        "youth_literacy_rate",
        "primary_enrollment_rate",
        "secondary_enrollment_rate",
        "gov_education_spending_pct_gdp",
    ]
    weights = {
        "youth_literacy_rate": 0.4,
        "primary_enrollment_rate": 0.2,
        "secondary_enrollment_rate": 0.2,
        "gov_education_spending_pct_gdp": 0.2,
    }
    z = {}
    for m in metrics:
        s = pd.to_numeric(df[m], errors="coerce")
        mu, sd = s.mean(skipna=True), s.std(skipna=True)
        z[m] = (s - mu) / sd if sd and not pd.isna(sd) else pd.Series(np.zeros(len(s)), index=df.index)
    zdf = pd.DataFrame(z)
    w = pd.Series(weights)
    present = zdf.notna().astype(float)
    row_w = present.mul(w, axis=1).sum(axis=1).replace(0, pd.NA)
    yls_z = zdf.fillna(0).mul(w, axis=1).sum(axis=1).div(row_w)
    zmin, zmax = yls_z.min(skipna=True), yls_z.max(skipna=True)
    if zmax == zmin:
        return pd.Series(np.full(len(yls_z), 50.0), index=df.index).round(1)
    return (100 * (yls_z - zmin) / (zmax - zmin)).round(1)

def _paginate(df: pd.DataFrame, page: int, per_page: int) -> Tuple[pd.DataFrame, int]:
    total = len(df)
    start = max(0, (page - 1) * per_page)
    end = start + per_page
    return df.iloc[start:end], total


@bp.route("/api/meta")
def api_meta():
    df = _df_copy()
    regions = sorted([x for x in df["region"].dropna().unique() if x and x != "nan"])
    incomes = sorted([x for x in df["income_group"].dropna().unique() if x and x != "nan"])
    profiles = sorted([x for x in df["education_profile"].dropna().unique() if x and x != "nan"])
    last_year = pd.to_numeric(df["latest_year"], errors="coerce").dropna()
    last_year = int(last_year.max()) if not last_year.empty else None

    return jsonify(
        {
            "regions": regions,
            "income_groups": incomes,
            "profiles": profiles,
            "indicators": [
                "youth_literacy_rate",
                "primary_enrollment_rate",
                "secondary_enrollment_rate",
                "gov_education_spending_pct_gdp",
            ],
            "last_year": last_year,
            "counts": {"countries": int(len(df))},
        }
    )


@bp.route("/api/stats")
def api_stats():
    df = _apply_filters(_df_copy(), request.args)

    def avg(col):
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        return None if s.empty else round(float(s.mean()), 1)

    return jsonify(
        {
            "avg": {
                "literacy": avg("youth_literacy_rate"),
                "primary": avg("primary_enrollment_rate"),
                "secondary": avg("secondary_enrollment_rate"),
                "spend": avg("gov_education_spending_pct_gdp"),
            },
            "counts": {"countries": int(len(df))},
        }
    )


@bp.route("/api/countries")
def api_countries():
    df = _apply_filters(_df_copy(), request.args)

    # Sorting
    sort_key = request.args.get("sort", "country")
    order = request.args.get("order", "asc")
    sort_map = {
        "country": "country",
        "literacy": "youth_literacy_rate",
        "primary": "primary_enrollment_rate",
        "secondary": "secondary_enrollment_rate",
        "spend": "gov_education_spending_pct_gdp",
        "year": "latest_year",
    }
    sort_col = sort_map.get(sort_key, "country")
    df = df.sort_values(by=sort_col, ascending=(order != "desc"), na_position="last")

    # Pagination
    try:
        page = max(1, int(request.args.get("page", 1)))
        per_page = max(1, min(200, int(request.args.get("per_page", 50))))
    except ValueError:
        page, per_page = 1, 50

    page_df, total = _paginate(df, page, per_page)

    cols = [
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
    results = _records_nullsafe(page_df, cols) 

    return jsonify(
        {
            "results": results,
            "page": page,
            "per_page": per_page,
            "total": int(total),
            "pages": int(math.ceil(total / per_page)) if per_page else 1,
        }
    )


@bp.route("/api/country/<string:iso3>")
def api_country(iso3: str):
    df = _df_copy()
    row = df[df["country_code"].str.upper() == iso3.upper()]
    if row.empty:
        return jsonify({"error": "Not found"}), 404
    ser = row.iloc[0]
    data = {k: (None if pd.isna(v) else v) for k, v in ser.to_dict().items()}
    return jsonify(data)


@bp.route("/api/download/clean")
def api_download_clean():
    root = current_app.config["ROOT_DIR"]
    directory = os.path.join(root, "data_clean")
    filename = "education_clean.csv"
    return send_from_directory(directory, filename, as_attachment=True)

@bp.route("/api/insights")
def api_insights():
    """
    Returns analytics computed by the pipeline (data_clean/insights.json).
    If the file is missing, returns a minimal empty payload.
    """
    root = current_app.config["ROOT_DIR"]
    path = os.path.join(root, "data_clean", "insights.json")
    if not os.path.exists(path):
        return jsonify({"last_year": None, "top_yls": [], "bottom_yls": [], "top_improvers_literacy_5y": [], "correlations": {}})

    import json
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

# add a new endpoint anywhere after api_insights()
@bp.route("/api/insights/live")
def api_insights_live():
    df = _apply_filters(_df_copy(), request.args)
    if df.empty:
        return jsonify(
            {"last_year": None, "top_yls": [], "bottom_yls": [], "top_improvers_literacy_5y": [], "correlations": {}}
        )

    sub = df.copy()
    sub["yls_score"] = _compute_yls(sub)

    top_yls = (
        sub.sort_values("yls_score", ascending=False)
        .head(10)[["country_code", "country", "yls_score"]]
        .to_dict("records")
    )
    bottom_yls = (
        sub.sort_values("yls_score", ascending=True)
        .head(10)[["country_code", "country", "yls_score"]]
        .to_dict("records")
    )

    corr_df = sub[
        ["youth_literacy_rate", "primary_enrollment_rate", "secondary_enrollment_rate", "gov_education_spending_pct_gdp"]
    ].apply(pd.to_numeric, errors="coerce")
    cm = corr_df.corr(method="pearson").round(2)
    correlations = cm.where(pd.notna(cm), None).to_dict()

    improvers = []
    root = current_app.config["ROOT_DIR"]
    path = os.path.join(root, "data_clean", "insights.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            base = json.load(f)
        allowed = set(sub["country_code"].dropna().astype(str))
        base_improvers = base.get("top_improvers_literacy_5y", [])
        improvers = [r for r in base_improvers if r.get("country_code") in allowed][:10]

    last_year = pd.to_numeric(sub["latest_year"], errors="coerce").dropna()
    last_year = int(last_year.max()) if not last_year.empty else None

    return jsonify(
        {
            "last_year": last_year,
            "top_yls": top_yls,
            "bottom_yls": bottom_yls,
            "top_improvers_literacy_5y": improvers,
            "correlations": correlations,
        }
    )