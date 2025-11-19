import os
import pandas as pd
from flask import Flask


def _load_clean_csv(root_dir: str) -> pd.DataFrame:
    path = os.path.join(root_dir, "data_clean", "education_clean.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing {path}. Run your data pipeline to create it."
        )
    df = pd.read_csv(path)

    # Expected columns:
    expected = {
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
    }
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"education_clean.csv missing columns: {sorted(missing)}")

    # Normalize dtypes
    for c in [
        "youth_literacy_rate",
        "primary_enrollment_rate",
        "secondary_enrollment_rate",
        "gov_education_spending_pct_gdp",
        "latest_year",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["country", "country_code", "region", "income_group", "education_profile"]:
        df[c] = df[c].astype(str)

    return df


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    app.config["ROOT_DIR"] = root_dir
    app.config["DATAFRAME"] = _load_clean_csv(root_dir)

    from .routes import bp as api_bp
    app.register_blueprint(api_bp)

    return app