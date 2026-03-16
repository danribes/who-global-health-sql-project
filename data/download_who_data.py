"""
Download WHO Global Health Observatory data for SQL final project.
Fetches 4 health indicators + NCD deaths by cause + dimensions from the GHO OData API.

Indicators (health estimates):
  - WHOSIS_000001: Life expectancy at birth (years)
  - WHOSIS_000002: Healthy life expectancy (HALE) at birth (years)
  - WHOSIS_000004: Adult mortality rate (per 1000 population)
  - WHOSIS_000015: Life expectancy at age 60 (years)

Deaths by cause (NCD):
  - SDG_SH_DTH_RNCOM: Number of deaths by NCD type, country, year, sex

Communicable disease deaths:
  - TB_e_mort_exc_tbhiv_num: TB deaths (excl. HIV)
  - MALARIA_EST_DEATHS: Malaria deaths (estimated)
  - HIV_0000000006: HIV/AIDS-related deaths
  - HEPATITIS_HBV_DEATHS_NUM: Hepatitis B deaths
  - HEPATITIS_HCV_DEATHS_NUM: Hepatitis C deaths

Output CSVs saved to data/ folder.

Usage:
    python3 data/download_who_data.py
"""

import csv
import json
import os
import urllib.request
import urllib.error
import time

BASE_URL = "https://ghoapi.azureedge.net/api"
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

INDICATORS = {
    "WHOSIS_000001": "Life expectancy at birth (years)",
    "WHOSIS_000002": "Healthy life expectancy (HALE) at birth (years)",
    "WHOSIS_000004": "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
    "WHOSIS_000015": "Life expectancy at age 60 (years)",
}

PAGE_SIZE = 1000
MAX_RETRIES = 3


def fetch_json(url):
    """Fetch JSON from URL with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def fetch_all_pages(base_url):
    """Fetch all pages from an OData endpoint using $skip pagination."""
    records = []
    skip = 0
    page = 0
    while True:
        page += 1
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}$top={PAGE_SIZE}&$skip={skip}"
        print(f"  Fetching page {page} ({len(records)} records so far)...")
        data = fetch_json(url)
        batch = data.get("value", [])
        if not batch:
            break
        records.extend(batch)
        skip += len(batch)
    return records


def download_indicator_data():
    """Download all 4 indicator datasets and combine into one CSV."""
    all_rows = []

    for code, name in INDICATORS.items():
        print(f"\nDownloading {code}: {name}")
        url = f"{BASE_URL}/{code}"
        records = fetch_all_pages(url)
        print(f"  Total records: {len(records)}")

        for r in records:
            all_rows.append({
                "indicator_code": r.get("IndicatorCode", ""),
                "country_code": r.get("SpatialDim", ""),
                "spatial_dim_type": r.get("SpatialDimType", ""),
                "year": r.get("TimeDim", ""),
                "sex_code": r.get("Dim1", ""),
                "numeric_value": r.get("NumericValue", ""),
                "low": r.get("Low", ""),
                "high": r.get("High", ""),
            })

    output_path = os.path.join(DATA_DIR, "health_estimates_raw.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "indicator_code", "country_code", "spatial_dim_type",
            "year", "sex_code", "numeric_value", "low", "high",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nSaved {len(all_rows)} rows to {output_path}")
    return len(all_rows)


def download_ncd_deaths():
    """Download NCD deaths by cause, country, year, sex."""
    print("\nDownloading SDG_SH_DTH_RNCOM: NCD deaths by cause")
    url = f"{BASE_URL}/SDG_SH_DTH_RNCOM"
    records = fetch_all_pages(url)
    print(f"  Total records: {len(records)}")

    output_path = os.path.join(DATA_DIR, "ncd_deaths_raw.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "indicator_code", "country_code", "spatial_dim_type",
            "year", "sex_code", "cause_code", "numeric_value", "low", "high",
        ])
        writer.writeheader()
        for r in records:
            writer.writerow({
                "indicator_code": r.get("IndicatorCode", ""),
                "country_code": r.get("SpatialDim", ""),
                "spatial_dim_type": r.get("SpatialDimType", ""),
                "year": r.get("TimeDim", ""),
                "sex_code": r.get("Dim1", ""),
                "cause_code": r.get("Dim2", ""),
                "numeric_value": r.get("NumericValue", ""),
                "low": r.get("Low", ""),
                "high": r.get("High", ""),
            })

    print(f"Saved {len(records)} rows to {output_path}")
    return len(records)


COMMUNICABLE_INDICATORS = {
    "TB_e_mort_exc_tbhiv_num": "Tuberculosis",
    "MALARIA_EST_DEATHS": "Malaria",
    "HIV_0000000006": "HIV/AIDS",
    "HEPATITIS_HBV_DEATHS_NUM": "Hepatitis B",
    "HEPATITIS_HCV_DEATHS_NUM": "Hepatitis C",
}


def download_communicable_deaths():
    """Download communicable disease death counts by country/year."""
    all_rows = []

    for code, disease_name in COMMUNICABLE_INDICATORS.items():
        print(f"\nDownloading {code}: {disease_name} deaths")
        url = f"{BASE_URL}/{code}"
        records = fetch_all_pages(url)
        print(f"  Total records: {len(records)}")

        for r in records:
            all_rows.append({
                "indicator_code": r.get("IndicatorCode", ""),
                "disease_name": disease_name,
                "country_code": r.get("SpatialDim", ""),
                "spatial_dim_type": r.get("SpatialDimType", ""),
                "year": r.get("TimeDim", ""),
                "numeric_value": r.get("NumericValue", ""),
                "low": r.get("Low", ""),
                "high": r.get("High", ""),
            })

    output_path = os.path.join(DATA_DIR, "communicable_deaths_raw.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "indicator_code", "disease_name", "country_code",
            "spatial_dim_type", "year", "numeric_value", "low", "high",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nSaved {len(all_rows)} rows to {output_path}")
    return len(all_rows)


def download_countries():
    """Download country dimension data."""
    print("\nDownloading country dimension...")
    url = f"{BASE_URL}/DIMENSION/COUNTRY/DimensionValues"
    records = fetch_all_pages(url)
    print(f"  Total records: {len(records)}")

    output_path = os.path.join(DATA_DIR, "countries.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "country_code", "country_name", "parent_code", "parent_title",
        ])
        writer.writeheader()
        for r in records:
            writer.writerow({
                "country_code": r.get("Code", ""),
                "country_name": r.get("Title", ""),
                "parent_code": r.get("ParentCode", ""),
                "parent_title": r.get("ParentTitle", ""),
            })

    print(f"Saved {len(records)} rows to {output_path}")
    return len(records)


def download_regions():
    """Download region dimension data."""
    print("\nDownloading region dimension...")
    url = f"{BASE_URL}/DIMENSION/REGION/DimensionValues"
    records = fetch_all_pages(url)
    print(f"  Total records: {len(records)}")

    output_path = os.path.join(DATA_DIR, "regions.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "region_code", "region_name",
        ])
        writer.writeheader()
        for r in records:
            writer.writerow({
                "region_code": r.get("Code", ""),
                "region_name": r.get("Title", ""),
            })

    print(f"Saved {len(records)} rows to {output_path}")
    return len(records)


if __name__ == "__main__":
    print("=" * 60)
    print("WHO Global Health Observatory — Data Download")
    print("=" * 60)

    n_facts = download_indicator_data()
    n_ncd = download_ncd_deaths()
    n_comm = download_communicable_deaths()
    n_countries = download_countries()
    n_regions = download_regions()

    print("\n" + "=" * 60)
    print("Download complete!")
    print(f"  Health estimates: {n_facts:,} rows  → data/health_estimates_raw.csv")
    print(f"  NCD deaths:       {n_ncd:,} rows   → data/ncd_deaths_raw.csv")
    print(f"  Comm. deaths:     {n_comm:,} rows  → data/communicable_deaths_raw.csv")
    print(f"  Countries:        {n_countries:,} rows   → data/countries.csv")
    print(f"  Regions:          {n_regions:,} rows    → data/regions.csv")
    print("=" * 60)
