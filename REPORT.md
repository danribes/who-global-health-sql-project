# WHO Global Health Estimates — Project Report

## 1. Introduction

This report documents the full process of building a SQL data warehouse project using data from the World Health Organization (WHO) Global Health Observatory. The project follows a layered architecture (staging → core → semantic) and demonstrates SQL proficiency across schema design, ETL, data quality, analytical queries, and advanced SQL features.

**Motor SQL**: MySQL 8.0
**Data source**: WHO GHO OData API (`https://ghoapi.azureedge.net/api/`)
**Time span covered**: 2000–2024 (varies by indicator)
**Geographic scope**: 228 countries across 6 WHO regions

---

## 2. Dataset Selection

### 2.1 Selection criteria

The project brief required a dataset with:
- At least 3 related tables with real relationships
- At least one usable date field
- At least one numeric metric
- Potential for non-trivial JOINs
- Sufficient "dirtiness" to clean (nulls, formats, duplicates)

### 2.2 Exploration process

The WHO Global Health Observatory was chosen as the data source. The GHO provides an open OData API with no authentication required, returning JSON data that can be paginated using `$top` and `$skip` parameters.

Several candidate datasets were evaluated:

| Dataset | Records | Country-level? | Dimensions | Decision |
|---|---|---|---|---|
| GHE_DALYNUM (DALYs by cause) | ~208K | No — regional only | Country, year, sex, age, cause | Rejected: no country granularity |
| WHOSIS_000001 (Life expectancy) | ~13K | Yes | Country, year, sex | Selected |
| WHOSIS_000002 (HALE) | ~13K | Yes | Country, year, sex | Selected |
| WHOSIS_000004 (Adult mortality) | ~13K | Yes | Country, year, sex | Selected |
| WHOSIS_000015 (Life exp. at 60) | ~13K | Yes | Country, year, sex | Selected |
| SDG_SH_DTH_RNCOM (NCD deaths) | ~46K | Yes | Country, year, sex, cause | Selected |
| TB_e_mort_exc_tbhiv_num (TB deaths) | ~4.7K | Yes | Country, year | Selected |
| MALARIA_EST_DEATHS (Malaria deaths) | ~2.9K | Yes | Country, year | Selected |
| HIV_0000000006 (HIV/AIDS deaths) | ~5.1K | Yes | Country, year | Selected |
| HEPATITIS_HBV_DEATHS_NUM (Hep B) | ~159 | Yes | Country (2022 only) | Selected |
| HEPATITIS_HCV_DEATHS_NUM (Hep C) | ~160 | Yes | Country (2022 only) | Selected |
| MORT_300 (Child mortality causes) | ~207K | Partially | Country, age, child cause | Rejected: narrower scope |
| MDG_0000000020 (TB incidence) | ~5K | Yes | Country, year | Rejected: no death counts |

### 2.3 Rationale for final selection

The initial plan was to use the GHE DALY dataset, which appeared to be the richest option (~208K records with disease-cause hierarchy). However, upon testing the API, the DALY data only contained **regional and global aggregates** — not country-level data — making it unsuitable for country-level analysis.

The project was pivoted to combine **4 health estimate indicators** (WHOSIS_000001, 000002, 000004, 000015) which all share the same structure: country-level, year, sex, with confidence intervals. These were later enriched with:

1. **NCD deaths by cause** (SDG_SH_DTH_RNCOM) — providing disease-level mortality data with 4 cause categories, by country, year, and sex
2. **Communicable disease deaths** (5 separate indicators for TB, Malaria, HIV, Hepatitis B, Hepatitis C) — providing the infectious disease perspective

This three-dataset combination enables the project's central analytical story: the **epidemiological transition** from communicable to non-communicable disease burden.

### 2.4 MySQL engine rationale

MySQL was chosen because:
- The project template was already written in MySQL syntax (DELIMITER $$, STR_TO_DATE, SIGNAL SQLSTATE, etc.)
- MySQL 8.0+ supports all required features: window functions, CTEs, procedures, functions, triggers
- Avoids rewriting all 7 template files for a different engine
- Available via the existing Docker container (`mysql-evolve` on port 3307)

---

## 3. Data Acquisition

### 3.1 Download script

A Python script (`data/download_who_data.py`) was created to automate data download from the WHO GHO OData API. The script:

1. Fetches data from 10 API endpoints using `$top`/`$skip` pagination (the API returns max 1000 records per request and does not provide `@odata.nextLink` for automatic pagination)
2. Extracts relevant fields from the JSON responses
3. Saves results as CSV files in the `data/` directory

### 3.2 API challenges encountered

- **No `@odata.nextLink`**: Unlike standard OData implementations, the WHO API does not return pagination links. Manual `$skip`-based pagination was required.
- **Inconsistent `$filter` support**: OData `$filter` queries for `SpatialDimType eq 'COUNTRY'` returned empty results, requiring all data to be downloaded and filtered during ETL.
- **Mixed spatial types**: Each indicator returns a mix of COUNTRY, REGION, GLOBAL, and WORLDBANKINCOMEGROUP records. Only COUNTRY records are loaded into the core layer.

### 3.3 Data files produced

| File | Description | Rows | Size |
|---|---|---|---|
| `health_estimates_raw.csv` | 4 health indicators (LE, HALE, mortality, LE at 60) | 51,744 | ~4.5 MB |
| `ncd_deaths_raw.csv` | NCD deaths by cause (cardiovascular, cancer, diabetes, respiratory) | 46,560 | ~4.2 MB |
| `communicable_deaths_raw.csv` | Communicable disease deaths (TB, malaria, HIV, hepatitis B/C) | 12,989 | ~0.8 MB |
| `countries.csv` | Country dimension with WHO region mapping | 234 | ~12 KB |
| `regions.csv` | WHO region codes and names | 43 | ~1 KB |

---

## 4. Data Model Design

### 4.1 Layered architecture

The project follows a three-layer architecture:

```
STAGING (raw text)          →    CORE (typed, modeled)       →    SEMANTIC (views)
stg_health_estimates_raw         dim_indicator (4)                vw_health_enriched
stg_ncd_deaths_raw               dim_country (228)               vw_yearly_kpi
stg_communicable_deaths_raw      dim_sex (3)                     vw_region_yearly_kpi
stg_countries_raw                dim_cause (4 NCD)               vw_deaths_enriched
                                 dim_disease (5 communicable)    vw_yearly_deaths_by_cause
                                 fct_health_estimate (48,840)    vw_communicable_enriched
                                 fct_ncd_deaths (43,920)
                                 fct_communicable_deaths (10,909)
```

### 4.2 Staging layer

All staging tables store data as `VARCHAR` (raw text) exactly as received from the CSV files. This preserves the original data and allows validation before type conversion. Each staging table includes `source_file` and `ingested_at` metadata columns for audit purposes.

### 4.3 Core layer — dimensions

| Dimension | PK | Rows | Source | Notes |
|---|---|---|---|---|
| `dim_indicator` | `indicator_code` | 4 | Static INSERT | Health estimate indicator definitions |
| `dim_country` | `country_code` | 228 | `stg_countries_raw` | ISO-3 codes, WHO region mapping |
| `dim_sex` | `sex_code` | 3 | Static INSERT | SEX_BTSX, SEX_MLE, SEX_FMLE |
| `dim_cause` | `cause_code` | 4 | Static INSERT | NCD cause codes (GHE061, GHE080, GHE110, GHE117) |
| `dim_disease` | `disease_code` | 5 | Static INSERT | Communicable disease indicator codes |

Six countries from the staging data were excluded because they lack WHO region assignments: Channel Islands, Hong Kong, Macao, former Serbia and Montenegro, Pristina, and Kosovo. This reduced the country dimension from 234 to 228.

### 4.4 Core layer — facts

| Fact table | Grain | Rows | Dimensions | Metric |
|---|---|---|---|---|
| `fct_health_estimate` | (indicator, country, year, sex) | 48,840 | dim_indicator, dim_country, dim_sex | `metric_value` + CI |
| `fct_ncd_deaths` | (cause, country, year, sex) | 43,920 | dim_cause, dim_country, dim_sex | `death_count` + CI |
| `fct_communicable_deaths` | (disease, country, year) | 10,909 | dim_disease, dim_country | `death_count` + CI |

Note: Communicable disease deaths have **no sex breakdown** — the WHO API provides only total-population figures for TB, malaria, HIV, and hepatitis indicators.

### 4.5 Design decisions

1. **Separate fact tables**: NCD and communicable deaths are kept in separate tables because they have different grains (NCD has sex breakdown, communicable does not) and different source structures. Cross-fact analysis is done via JOINs in the analytical queries.

2. **Separate dim_cause and dim_disease**: NCD causes use WHO GHE cause codes (e.g., `GHECAUSES_GHE110`), while communicable diseases use WHO indicator codes as identifiers (e.g., `TB_e_mort_exc_tbhiv_num`). They could have been unified into a single dimension, but keeping them separate preserves the distinct provenance and avoids forcing a common key scheme.

3. **Two-step INSERT for confidence intervals**: MySQL 8.0 strict mode produces an `Incorrect DECIMAL value` error when `CAST`-ing empty strings to `DECIMAL` inside `INSERT...SELECT` at scale — even when wrapped in `NULLIF`. The workaround is to INSERT the main metrics first (step 1), then UPDATE the confidence intervals separately (step 2) with an explicit `WHERE raw_low <> ''` filter.

4. **Carriage return cleanup**: The country CSV had Windows-style line endings (`\r\n`), which caused `\r` characters to be embedded in `region_name`. MySQL's `TRIM()` does not strip `\r`, so `REPLACE(TRIM(...), '\r', '')` was added to the ETL.

---

## 5. ETL Process

### 5.1 Execution order

| Step | File | Action |
|---|---|---|
| 1 | `01_schema.sql` | Creates database `who_disease_burden`, all staging and core tables |
| 2 | CSV import | `LOAD DATA INFILE` (or DBeaver Import Wizard) into 4 staging tables |
| 3 | `02_load_staging.sql` | Validates row counts, samples, parseability, unique values |
| 4 | `03_transform_core.sql` | Loads dimensions, then 3 fact tables (2-step INSERT + UPDATE for CI) |
| 5 | `04_semantic_views.sql` | Creates 6 business-facing views |
| 6 | `06_quality_checks.sql` | Runs 10 quality validation sections |
| 7 | `05_analysis_queries.sql` | Executes 20 analytical queries |
| 8 | `07_advanced_sql.sql` | Creates functions, procedure, trigger, and runs smoke tests |

### 5.2 Data transformations applied

- **Type casting**: VARCHAR to INT, SMALLINT, DECIMAL, using `CAST()` with validation
- **Whitespace cleaning**: `TRIM()` on all text fields
- **Carriage return removal**: `REPLACE(..., '\r', '')` on country dimension fields
- **Null handling**: Empty strings and 'None' values converted to SQL NULL via `NULLIF()`
- **Spatial filtering**: Only `SpatialDimType = 'COUNTRY'` rows loaded (regional/global aggregates excluded)
- **Referential integrity**: JOIN-based filtering ensures only rows with valid dimension keys are loaded

### 5.3 Row counts after ETL

| Table | Staging rows | Core rows | Filtered out |
|---|---|---|---|
| Health estimates | 51,744 | 48,840 | 2,904 (region/global/income group aggregates) |
| NCD deaths | 46,560 | 43,920 | 2,640 (region/global/income group aggregates) |
| Communicable deaths | 12,989 | 10,909 | 2,080 (region/global/income group aggregates) |
| Countries | 234 | 228 | 6 (missing region assignment) |

---

## 6. Data Quality

### 6.1 Quality checks performed

| # | Check | Result |
|---|---|---|
| 1 | Null values in critical fact columns | 0 nulls across all 3 facts |
| 2 | Orphan foreign keys (country, indicator, sex, cause, disease) | 0 orphans in all tables |
| 3 | Duplicate business keys | 0 duplicates |
| 4 | Year range validation | Health: 2000–2021, NCD: 2000–2019, Comm: varies (2000–2024) |
| 5 | Negative life expectancy values | 0 found |
| 6 | Negative mortality / death count values | 0 found |
| 7 | Life expectancy > 100 years | 0 found |
| 8 | Confidence interval coverage | 75% of health estimates have CI; 100% of NCD deaths; varies for communicable |
| 9 | Inverted confidence intervals (low > high) | 0 found |
| 10 | HALE > Life Expectancy inconsistency | 0 found |

### 6.2 Data quality issue identified

The project brief requires at least 1 data quality issue detected and corrected. The implemented check searches for **inverted confidence intervals** (where `low_ci > high_ci`), which would indicate swapped bounds in the source data. A transactional `UPDATE` swaps the values if any are found:

```sql
START TRANSACTION;
UPDATE fct_health_estimate
SET low_ci = high_ci, high_ci = low_ci
WHERE low_ci IS NOT NULL AND high_ci IS NOT NULL AND low_ci > high_ci;
COMMIT;
```

In the current dataset, no inverted intervals were found, but the detection and correction mechanism is in place.

### 6.3 Hidden data quality issue found during development

A critical data quality issue was discovered during development: **Windows-style carriage returns (`\r`)** embedded in the `region_name` column of `dim_country`. This caused all JOIN-based region filters (e.g., `WHERE region_name = 'Africa'`) to silently return 0 rows, because `'Africa\r' <> 'Africa'`. The issue was invisible in query results because `\r` is a non-printing character.

This was detected by inspecting column values with `HEX()` and fixed by adding `REPLACE(TRIM(...), '\r', '')` to the ETL. This is a realistic example of a production data quality issue — invisible characters from cross-platform CSV transfers.

---

## 7. Analytical Queries

### 7.1 Overview

The project contains 20 analytical queries organized in 4 thematic blocks:

| Block | Queries | Theme |
|---|---|---|
| Q1–Q10 | Health estimates | Life expectancy trends, gender gaps, regional comparisons |
| Q11–Q14 | NCD deaths | Disease burden by cause, cross-fact correlation with life expectancy |
| Q15–Q16 | Communicable deaths | Temporal trends, most affected countries per disease |
| Q17–Q20 | Cross-dataset | NCD vs communicable comparison, epidemiological transition, double burden |

### 7.2 Requirements checklist

| Requirement | Minimum | Actual | Queries |
|---|---|---|---|
| Total queries | 8–12 | 20 | All |
| Temporal aggregations | 2 | 5 | Q1, Q2, Q11, Q15, Q18 |
| CTEs | 2 | 11 | Q4, Q5, Q6, Q8, Q12, Q13, Q16, Q17, Q18, Q19, Q20 |
| Top-N per group | 1 | 3 | Q4, Q12, Q16 |
| Data quality detected/corrected | 1 | 2 | CI inversion check + `\r` cleanup |

### 7.3 Query descriptions and rationale

#### Block 1: Health Estimates (Q1–Q10)

**Q1 — Global life expectancy trend (2000–2021)**
Temporal aggregation showing the average life expectancy across 185 countries per year. This is the foundational trend line for the project. The data reveals a steady increase from 67.0 years (2000) to 72.6 years (2019), followed by a decline to 71.3 years (2021) — likely reflecting the impact of the COVID-19 pandemic.

**Q2 — Year-over-year % change**
Uses `LAG()` window function to compute annual growth rates. Highlights the 2020 (-0.78%) and 2021 (-1.07%) declines — the only negative years in the 22-year series.

**Q3 — Top 10 countries by life expectancy (2021)**
Simple ranking to identify the healthiest countries. Japan leads at 84.5 years, followed by Singapore (83.9) and South Korea (83.8).

**Q4 — Top 3 per WHO region**
Uses `ROW_NUMBER() OVER (PARTITION BY region)` to find the best-performing country in each region. This demonstrates top-N-per-group, a key analytical pattern.

**Q5 — Gender gap by region**
Pivots male vs female life expectancy using `CASE WHEN` in a CTE. The Americas show the largest gender gap (5.73 years), while Eastern Mediterranean has the smallest (3.64 years).

**Q6 — Life expectancy vs adult mortality correlation**
Cross-indicator JOIN between WHOSIS_000001 and WHOSIS_000004. Demonstrates that the two indicators are inversely related — countries with high life expectancy consistently have low adult mortality rates.

**Q7 — Regional improvement (2000 vs 2021)**
Compares the first and last years to measure 21-year improvement. Africa improved the most (+9.28 years, +17.1%), while the Americas showed almost no improvement (+0.23 years).

**Q8 — Mortality concentration (cumulative %)**
Uses cumulative `SUM() OVER (ORDER BY ...)` to show that the top 10 countries by adult mortality account for ~13% of the global total. Demonstrates concentration analysis.

**Q9 — HALE ranking within regions**
Uses `DENSE_RANK()` to position each country within its region by healthy life expectancy. Useful for identifying regional outliers.

**Q10 — Countries with declining life expectancy**
Identifies 23 countries where life expectancy fell between 2000 and 2021. Paraguay (-4.41), Philippines (-3.57), and Peru (-3.46) saw the largest declines — all in the Americas, likely reflecting COVID-19 impact.

#### Block 2: NCD Deaths (Q11–Q14)

**Q11 — NCD death trends by cause (2000–2019)**
Temporal aggregation of the 4 NCD causes. Cardiovascular diseases dominate with ~17.9M deaths in 2019, followed by cancers (~9.3M), chronic respiratory (~4.1M), and diabetes (~2.0M).

**Q12 — Most lethal NCD per country**
Uses `ROW_NUMBER() PARTITION BY country` to find the #1 killer per country. Cardiovascular diseases are the top cause in almost every country, with Japan and the UK being notable exceptions where cancer leads.

**Q13 — NCD death share by region**
Window function calculating each cause's percentage within each region. Eastern Mediterranean has the highest cardiovascular share (64.0%), while South-East Asia has an unusually high chronic respiratory share (21.3%).

**Q14 — Life expectancy vs cardiovascular deaths (cross-fact)**
JOINs `vw_health_enriched` with `vw_deaths_enriched` — connecting the health estimates fact with the NCD deaths fact. Shows that large-population countries (China, India) dominate absolute death counts regardless of life expectancy.

#### Block 3: Communicable Deaths (Q15–Q16)

**Q15 — Communicable disease death trends**
Tracks TB, malaria, HIV, and hepatitis deaths over time. Shows significant progress: HIV/AIDS deaths have declined dramatically since 2005 in many countries.

**Q16 — Top 5 countries per communicable disease**
Uses a CTE to find the latest available year per disease, then ranks countries. Key findings:
- TB: India (300K deaths), Indonesia (118K)
- Malaria: Nigeria (185K), DR Congo (68K)
- HIV/AIDS: South Africa (53K), Mozambique (44K)
- Hepatitis B: Indonesia (61K), Nigeria (46K)
- Hepatitis C: Pakistan (50K), Mexico (12K)

#### Block 4: Cross-Dataset Analysis (Q17–Q20)

**Q17 — NCD vs Communicable by region (2019)**
The central comparative query. Uses UNION ALL across both death fact tables to calculate each region's disease mix. Key finding: Africa is the only region where communicable diseases still represent a significant share (42.5%) of deaths. In Europe, NCDs account for 99.6%.

**Q18 — Epidemiological transition (2000 → 2019)**
Computes the NCD-to-communicable death ratio for 2000 and 2019 per region. Africa's ratio shifted from 0.6 (communicable dominated) to 1.4 (NCD now dominates) — confirming the ongoing epidemiological transition even in the world's poorest region.

**Q19 — Total mortality burden (3-fact JOIN)**
The most complex query in the project. JOINs all 3 fact tables (health estimates + NCD deaths + communicable deaths) to show each country's life expectancy alongside its total mortality burden. Demonstrates advanced multi-fact analysis.

**Q20 — Africa's double burden**
Focuses on African countries that face simultaneously high NCD AND communicable mortality. Nigeria leads with 654K combined deaths (59% communicable). DR Congo, Tanzania, and Niger have roughly 50/50 splits — illustrating the "double burden of disease" where countries must fight infectious diseases while chronic diseases are also rising.

---

## 8. Advanced SQL

### 8.1 Functions

**`fn_safe_pct(p_num, p_den)`** — Calculates a percentage with safe division. Returns NULL instead of raising an error when the denominator is zero or NULL. Used in the analytical queries for calculating regional shares.

**`fn_ci_width(p_low, p_high)`** — Calculates the width of a confidence interval. Returns NULL when either bound is missing. Useful for assessing data uncertainty.

### 8.2 Procedure

**`sp_refresh_core(p_verbose BOOLEAN)`** — Encapsulates the entire ETL pipeline (dimensions + 3 fact tables) in a single stored procedure with:
- Explicit `START TRANSACTION` / `COMMIT`
- `DECLARE EXIT HANDLER FOR SQLEXCEPTION` with `ROLLBACK`
- Optional verbose output showing row counts
- Idempotent execution (TRUNCATEs before loading)

### 8.3 Trigger

**`trg_fct_health_bi_validate`** — BEFORE INSERT trigger on `fct_health_estimate` that validates:
- Life expectancy indicators cannot have negative `metric_value`
- `year_val` must be in range 1900–2100
- Raises `SIGNAL SQLSTATE '45000'` with descriptive message on violation

---

## 9. Key Findings

### 9.1 Life expectancy

- Global average life expectancy increased from **67.0 years (2000) to 72.6 years (2019)**, then declined to **71.3 years (2021)** — a 2-year setback likely driven by COVID-19.
- **Japan** leads the world at 84.5 years; **Lesotho** is the lowest at 50.7 years.
- **Africa** showed the greatest improvement (+9.3 years), while the **Americas** barely improved (+0.2 years) over the full period.
- **23 countries** saw life expectancy decline between 2000 and 2021, predominantly in the Americas.
- Women live longer than men in every WHO region, with the largest gap in the Americas (5.7 years).

### 9.2 Non-communicable diseases

- **Cardiovascular diseases** are the world's #1 killer: ~17.9M deaths in 2019, accounting for 50–64% of NCD deaths depending on region.
- **Cancer** is the #2 cause globally (~9.3M), but is the leading NCD killer in Japan and the UK.
- The **Eastern Mediterranean** has the highest cardiovascular death share (64%), while **South-East Asia** has an unusually high chronic respiratory disease share (21.3%).

### 9.3 Communicable diseases

- **Tuberculosis** remains the deadliest communicable disease with India alone accounting for 300K deaths in 2024.
- **Malaria** is heavily concentrated in Sub-Saharan Africa, with Nigeria alone responsible for 185K deaths.
- **HIV/AIDS** deaths have declined significantly since their peak but remain heavily concentrated in Southern and Eastern Africa.
- **Hepatitis** data is limited (2022 only) but shows Indonesia, Pakistan, and Nigeria as the most affected countries.

### 9.4 Epidemiological transition

- Africa is the **only region** where communicable diseases still represent a large share of mortality (42.5% in 2019).
- All other regions are overwhelmingly dominated by NCDs (93–99.6%).
- Africa's NCD-to-communicable ratio **flipped from 0.6 (2000) to 1.4 (2019)** — meaning NCDs now cause more deaths than infectious diseases even in Africa.
- Several African countries face a **"double burden"**: Nigeria (654K combined deaths), DR Congo (363K), and Tanzania (159K) have roughly equal NCD and communicable mortality.

---

## 10. Limitations

1. **No population data**: Death counts are absolute numbers. Large countries (China, India) always dominate rankings simply due to population size. Per-capita rates would provide more meaningful comparisons.

2. **NCD-only cause breakdown**: The NCD dataset covers only 4 disease categories (cardiovascular, cancer, diabetes, respiratory). Injuries, mental health, and other causes are not included.

3. **No sex breakdown for communicable diseases**: TB, malaria, HIV, and hepatitis data are available only as total-population figures, preventing gender analysis for these diseases.

4. **Uneven temporal coverage**: Health estimates cover 2000–2021, NCD deaths cover 2000–2019, communicable diseases vary (TB/malaria/HIV: 2000–2024, hepatitis: 2022 only). Cross-dataset comparisons are limited to overlapping years.

5. **Modeled estimates**: All WHO GHO values are statistical estimates, not direct registry counts. Countries with weak vital registration systems may have wider uncertainty intervals.

6. **Hepatitis data sparsity**: Hepatitis B and C only have data for a single year (2022, 159–160 rows), limiting their usefulness for trend analysis.

---

## 11. Reproducibility

### 11.1 Prerequisites
- Python 3.x (for data download)
- MySQL 8.0+ (server)
- Internet connection (for WHO API)

### 11.2 Steps
```bash
# 1. Download data
python3 data/download_who_data.py

# 2. Create schema
mysql -u root -p < sql/01_schema.sql

# 3. Import CSVs (via LOAD DATA INFILE or DBeaver Import Wizard)

# 4-8. Execute SQL files in order
mysql -u root -p who_disease_burden < sql/02_load_staging.sql
mysql -u root -p who_disease_burden < sql/03_transform_core.sql
mysql -u root -p who_disease_burden < sql/04_semantic_views.sql
mysql -u root -p who_disease_burden < sql/06_quality_checks.sql
mysql -u root -p who_disease_burden < sql/05_analysis_queries.sql
mysql -u root -p who_disease_burden < sql/07_advanced_sql.sql
```

### 11.3 Final table counts (expected)

| Table | Expected rows |
|---|---|
| dim_country | 228 |
| dim_indicator | 4 |
| dim_sex | 3 |
| dim_cause | 4 |
| dim_disease | 5 |
| fct_health_estimate | ~48,840 |
| fct_ncd_deaths | ~43,920 |
| fct_communicable_deaths | ~10,900 |

---

## 12. Project Structure Summary

```
sql_final_work/
  data/
    download_who_data.py          # Python download script (WHO GHO API)
    health_estimates_raw.csv      # 4 health indicators (~51K rows)
    ncd_deaths_raw.csv            # NCD deaths by cause (~46K rows)
    communicable_deaths_raw.csv   # Communicable disease deaths (~13K rows)
    countries.csv                 # Country dimension (234 rows)
    regions.csv                   # Region dimension (43 rows)
  sql/
    01_schema.sql                 # Database + table definitions
    02_load_staging.sql           # Post-load validation
    03_transform_core.sql         # ETL: staging → core
    04_semantic_views.sql         # 6 semantic views
    05_analysis_queries.sql       # 20 analytical queries
    06_quality_checks.sql         # 10 quality check sections
    07_advanced_sql.sql           # 2 functions + 1 procedure + 1 trigger
  PROJECT_BRIEF.md                # Original project requirements
  README.md                       # Project documentation
  REPORT.md                       # This report
```
