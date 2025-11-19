import os
import json
import pandas as pd
import numpy as np

# Repo paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(ROOT, "data_raw", "WDI_CSV")
WDI_CSV = os.path.join(RAW_DIR, "WDICSV.csv")
COUNTRY_CSV = os.path.join(RAW_DIR, "WDICountry.csv")
OUT_DIR = os.path.join(ROOT, "data_clean")
OUT_CSV = os.path.join(OUT_DIR, "education_clean.csv")
OUT_INSIGHTS = os.path.join(OUT_DIR, "insights.json")

# Indicators
IND = {
    "youth_lit": "SE.ADT.1524.LT.ZS",
    "prim_net": "SE.PRM.NENR",
    "sec_net": "SE.SEC.NENR",
    "sec_gross": "SE.SEC.ENRR",
    "spend": "SE.XPD.TOTL.GD.ZS",
}

# Profile thresholds
LIT_T, PRIM_T, SEC_T, SPEND_T = 95.0, 90.0, 85.0, 4.0


def _is_year(col: str) -> bool:
    s = str(col)
    if s.isdigit() and len(s) == 4:
        return True
    if " [YR" in s:
        head = s.split(" ")[0]
        return head.isdigit() and len(head) == 4
    return False


def _parse_year(label: str):
    s = str(label)
    if " " in s:
        s = s.split(" ")[0]
    try:
        return int(s)
    except Exception:
        return None


def load_wdi_long(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    year_cols = [c for c in df.columns if _is_year(c)]
    long = df.melt(
        id_vars=["Country Name", "Country Code", "Indicator Name", "Indicator Code"],
        value_vars=year_cols,
        var_name="YearLabel",
        value_name="Value",
    )
    long["year"] = long["YearLabel"].map(_parse_year)
    long.drop(columns=["YearLabel"], inplace=True)
    long["Value"] = pd.to_numeric(long["Value"], errors="coerce")
    return long


def load_country_meta(path: str) -> pd.DataFrame:
    meta = pd.read_csv(path, low_memory=False)
    name_col = "Short Name" if "Short Name" in meta.columns else ("TableName" if "TableName" in meta.columns else "Country Name")
    keep = ["Country Code", name_col, "Region", "Income Group"]
    keep = [c for c in keep if c in meta.columns]
    meta = meta[keep].rename(columns={"Country Code": "country_code", name_col: "country", "Region": "region", "Income Group": "income_group"})
    meta = meta[meta["country_code"].astype(str).str.len() == 3]
    if "region" in meta:
        meta = meta[meta["region"].astype(str) != "Aggregates"]
    return meta


def latest_per_indicator(long_df: pd.DataFrame, indicator_code: str) -> pd.DataFrame:
    sub = long_df[(long_df["Indicator Code"] == indicator_code) & (long_df["year"].notna())].copy()
    sub = sub.dropna(subset=["Value"])
    sub = sub.sort_values(["Country Code", "year"])
    idx = sub.groupby("Country Code")["year"].idxmax()
    # NOTE: do NOT return Country Name here to avoid merge suffix collisions
    out = sub.loc[idx, ["Country Code", "year", "Value"]].rename(
        columns={
            "Country Code": "country_code",
            "year": f"{indicator_code}__year",
            "Value": f"{indicator_code}__value",
        }
    )
    return out

def change_over_window(long_df: pd.DataFrame, indicator_code: str, window: int = 5) -> pd.DataFrame:
    """Latest minus value >= window years earlier (nearest earlier), else NaN."""
    sub = long_df[(long_df["Indicator Code"] == indicator_code) & (long_df["year"].notna())].copy()
    sub = sub.dropna(subset=["Value"])
    # latest per country
    latest = sub.sort_values(["Country Code", "year"]).groupby("Country Code").tail(1)
    latest = latest.rename(columns={"Value": "v_latest", "year": "y_latest"})[["Country Code", "v_latest", "y_latest"]]
    # prior per country: nearest value with year <= y_latest - window
    merged = sub.merge(latest, on="Country Code", how="inner")
    merged = merged[merged["year"] <= merged["y_latest"] - window]
    prior = merged.sort_values(["Country Code", "year"]).groupby("Country Code").tail(1)
    out = latest.merge(prior[["Country Code", "Value", "year"]], on="Country Code", how="left")
    out["delta"] = out["v_latest"] - out["Value"]
    return out.rename(columns={"Country Code": "country_code"})[["country_code", "delta"]]


def build_clean_table(wdi_long: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    frames = [
        latest_per_indicator(wdi_long, IND["youth_lit"]),
        latest_per_indicator(wdi_long, IND["prim_net"]),
        latest_per_indicator(wdi_long, IND["sec_net"]),
        latest_per_indicator(wdi_long, IND["sec_gross"]),
        latest_per_indicator(wdi_long, IND["spend"]),
    ]
    from functools import reduce
    wide = reduce(lambda l, r: pd.merge(l, r, on="country_code", how="outer"), frames)
    wide = pd.merge(wide, meta, on="country_code", how="left")

    def colv(code): return f"{code}__value"
    def coly(code): return f"{code}__year"

    lit = pd.to_numeric(wide.get(colv(IND["youth_lit"])), errors="coerce").clip(0, 100)
    prim = pd.to_numeric(wide.get(colv(IND["prim_net"])), errors="coerce").clip(0, 100)
    sec = pd.to_numeric(wide.get(colv(IND["sec_net"])), errors="coerce")
    sec_g = pd.to_numeric(wide.get(colv(IND["sec_gross"])), errors="coerce")
    sec = sec.where(sec.notna(), sec_g).clip(0, 100)
    spend = pd.to_numeric(wide.get(colv(IND["spend"])), errors="coerce").clip(0, 15)

    lit_y = pd.to_numeric(wide.get(coly(IND["youth_lit"])), errors="coerce")
    prim_y = pd.to_numeric(wide.get(coly(IND["prim_net"])), errors="coerce")
    sec_y = pd.to_numeric(wide.get(coly(IND["sec_net"])), errors="coerce")
    sec_y = sec_y.where(sec_y.notna(), pd.to_numeric(wide.get(coly(IND["sec_gross"])), errors="coerce"))
    spend_y = pd.to_numeric(wide.get(coly(IND["spend"])), errors="coerce")

    latest_year = pd.concat([lit_y, prim_y, sec_y, spend_y], axis=1).max(axis=1, skipna=True)

    out = pd.DataFrame({
        "country": wide["country"],
        "country_code": wide["country_code"],
        "youth_literacy_rate": lit.round(1),
        "primary_enrollment_rate": prim.round(1),
        "secondary_enrollment_rate": sec.round(1),
        "gov_education_spending_pct_gdp": spend.round(1),
        "latest_year": latest_year.astype("Int64"),
        "region": wide["region"],
        "income_group": wide["income_group"],
    })

    def profile_row(r):
        hit_lit = pd.notna(r["youth_literacy_rate"]) and r["youth_literacy_rate"] >= LIT_T
        hit_prim = pd.notna(r["primary_enrollment_rate"]) and r["primary_enrollment_rate"] >= PRIM_T
        hit_sec = pd.notna(r["secondary_enrollment_rate"]) and r["secondary_enrollment_rate"] >= SEC_T
        hit_spend = pd.notna(r["gov_education_spending_pct_gdp"]) and r["gov_education_spending_pct_gdp"] >= SPEND_T
        if hit_lit and (hit_prim or hit_sec) and hit_spend:
            return "high_access_literacy"
        hits = sum([hit_lit, hit_prim or hit_sec, hit_spend])
        return "mixed_profile" if hits >= 2 else "low_access_literacy"

    out["education_profile"] = out.apply(profile_row, axis=1)
    out = out.dropna(subset=["country_code"]).reset_index(drop=True)
    return out


def compute_yls(df: pd.DataFrame) -> pd.DataFrame:
    """Youth Learning Score 0–100 with weights and missing-aware normalization."""
    metrics = ["youth_literacy_rate", "primary_enrollment_rate", "secondary_enrollment_rate", "gov_education_spending_pct_gdp"]
    weights = {"youth_literacy_rate": 0.4, "primary_enrollment_rate": 0.2, "secondary_enrollment_rate": 0.2, "gov_education_spending_pct_gdp": 0.2}

    z = {}
    for m in metrics:
        s = pd.to_numeric(df[m], errors="coerce")
        mu, sd = s.mean(skipna=True), s.std(skipna=True)
        z[m] = (s - mu) / sd if sd and not np.isnan(sd) else pd.Series(np.zeros(len(s)))
    z = pd.DataFrame(z)

    # renormalize weights if some metrics missing per row
    w = pd.DataFrame({m: weights[m] for m in metrics}, index=df.index)
    present = z.notna().astype(float)
    row_w = (w * present).sum(axis=1)
    row_w = row_w.replace(0, np.nan)
    yls_z = (z.fillna(0) * w).sum(axis=1) / row_w

    # scale to 0–100
    zmin, zmax = yls_z.min(skipna=True), yls_z.max(skipna=True)
    yls_0_100 = 100 * (yls_z - zmin) / (zmax - zmin) if zmax != zmin else yls_z * 0 + 50
    return pd.DataFrame({"country_code": df["country_code"], "yls_score": yls_0_100.round(1)})


def build_insights(wdi_long: pd.DataFrame, clean_df: pd.DataFrame) -> dict:
    # 5y literacy change
    lit_change = change_over_window(wdi_long, IND["youth_lit"], window=5).rename(columns={"delta": "lit_change_5y"})
    # YLS
    yls = compute_yls(clean_df)
    merged = clean_df.merge(yls, on="country_code", how="left").merge(lit_change, on="country_code", how="left")

    # Top/bottom by YLS
    top_yls = merged.dropna(subset=["yls_score"]).nlargest(10, "yls_score")[["country_code", "country", "yls_score"]].to_dict("records")
    bottom_yls = merged.dropna(subset=["yls_score"]).nsmallest(10, "yls_score")[["country_code", "country", "yls_score"]].to_dict("records")

    # Top improvers by literacy change
    improvers = merged.dropna(subset=["lit_change_5y"]).nlargest(10, "lit_change_5y")[["country_code", "country", "lit_change_5y"]].to_dict("records")

    # Correlations (snapshot)

    corr_df = clean_df[
        ["youth_literacy_rate", "primary_enrollment_rate",
        "secondary_enrollment_rate", "gov_education_spending_pct_gdp"]
    ].apply(pd.to_numeric, errors="coerce")

    corr_mat = corr_df.corr(method="pearson").round(2)
    # Convert NaN -> None so JSON is valid (becomes `null`)
    corr = corr_mat.where(pd.notna(corr_mat), None).to_dict()


    last_year = int(pd.to_numeric(clean_df["latest_year"], errors="coerce").dropna().max()) if not clean_df.empty else None
    return {"last_year": last_year, "top_yls": top_yls, "bottom_yls": bottom_yls, "top_improvers_literacy_5y": improvers, "correlations": corr}


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    wdi_long = load_wdi_long(WDI_CSV)
    meta = load_country_meta(COUNTRY_CSV)
    clean = build_clean_table(wdi_long, meta)

    # Write main CSV (schema unchanged)
    cols = ["country","country_code","youth_literacy_rate","primary_enrollment_rate","secondary_enrollment_rate","gov_education_spending_pct_gdp","latest_year","region","income_group","education_profile"]
    clean = clean[cols]
    clean.to_csv(OUT_CSV, index=False)
    print(f"Wrote {OUT_CSV} with {len(clean)} rows.")

    # Insights JSON for analytics UI
    insights = build_insights(wdi_long, clean)
    with open(OUT_INSIGHTS, "w", encoding="utf-8") as f:
        json.dump(insights, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_INSIGHTS}.")


if __name__ == "__main__":
    main()