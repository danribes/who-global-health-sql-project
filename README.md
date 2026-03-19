# WHO Global Health Estimates — SQL Final Project

## 1. Introduction

This project builds a complete, reproducible SQL data warehouse using data from the World Health Organization (WHO) Global Health Observatory. It follows a layered architecture (staging → core → semantic) and demonstrates SQL proficiency across schema design, ETL, data quality, analytical queries, and advanced SQL features.

**Motor SQL**: MySQL 8.0+
**Data source**: [WHO GHO OData API](https://ghoapi.azureedge.net/api/)
**Time span covered**: 2000–2024 (varies by indicator)
**Geographic scope**: 228 countries across 6 WHO regions
**Total fact rows**: ~103,700

### Pregunta guia

> *"How has life expectancy and the burden of disease mortality evolved globally? What are the most lethal diseases — both communicable and non-communicable — per country, and how many people die from them?"*

---

## 2. Dataset

**WHO Global Health Observatory (GHO)** — Three complementary datasets:

### 2.1 Health Estimate Indicators

| Indicator Code | Description | Unit |
|---|---|---|
| WHOSIS_000001 | Life expectancy at birth | years |
| WHOSIS_000002 | Healthy life expectancy (HALE) at birth | years |
| WHOSIS_000004 | Adult mortality rate (15-60) | per 1000 population |
| WHOSIS_000015 | Life expectancy at age 60 | years |

### 2.2 NCD Deaths by Cause (SDG_SH_DTH_RNCOM)

| Cause Code | Disease Category |
|---|---|
| GHE061 | Malignant neoplasms (cancers) |
| GHE080 | Diabetes mellitus |
| GHE110 | Cardiovascular diseases |
| GHE117 | Chronic respiratory diseases |

### 2.3 Communicable Disease Deaths

| Indicator Code | Disease |
|---|---|
| TB_e_mort_exc_tbhiv_num | Tuberculosis |
| MALARIA_EST_DEATHS | Malaria |
| HIV_0000000006 | HIV/AIDS |
| HEPATITIS_HBV_DEATHS_NUM | Hepatitis B |
| HEPATITIS_HCV_DEATHS_NUM | Hepatitis C |

### 2.4 Dataset selection process

Several candidate datasets were evaluated from the WHO GHO API:

| Dataset | Records | Country-level? | Decision |
|---|---|---|---|
| GHE_DALYNUM (DALYs by cause) | ~208K | No — regional only | Rejected: no country granularity |
| WHOSIS_000001–000015 (Health estimates) | ~13K each | Yes | Selected |
| SDG_SH_DTH_RNCOM (NCD deaths) | ~46K | Yes | Selected |
| TB / Malaria / HIV / Hepatitis deaths | ~13K combined | Yes | Selected |
| MORT_300 (Child mortality causes) | ~207K | Partially | Rejected: narrower scope |
| MDG_0000000020 (TB incidence) | ~5K | Yes | Rejected: no death counts |

The initial plan was to use the GHE DALY dataset (~208K records with disease-cause hierarchy). However, upon testing the API, the DALY data only contained **regional and global aggregates** — not country-level data. The project was pivoted to combine 4 health estimate indicators with NCD and communicable disease deaths, enabling the project's central analytical story: the **epidemiological transition** from communicable to non-communicable disease burden.

### 2.5 MySQL engine rationale

MySQL was chosen because:
- The project template was already written in MySQL syntax (DELIMITER $$, STR_TO_DATE, SIGNAL SQLSTATE)
- MySQL 8.0+ supports all required features: window functions, CTEs, procedures, functions, triggers
- Available via existing Docker container

---

## 3. Preguntas de negocio

1. How has global life expectancy evolved year by year?
2. Which countries have the highest and lowest life expectancy?
3. How does life expectancy differ by gender across regions?
4. Is there a relationship between life expectancy and adult mortality?
5. Which regions have improved the most over 21 years?
6. Are there countries where life expectancy has declined?
7. What is the most lethal NCD per country?
8. How have NCD deaths evolved over time by cause?
9. What share of NCD deaths does each disease represent per region?
10. Do countries with high cardiovascular mortality have lower life expectancy?
11. How have communicable disease deaths (TB, Malaria, HIV, Hepatitis) evolved?
12. Which countries are most affected by each communicable disease?
13. How do NCD vs communicable death shares compare across WHO regions?
14. Has the NCD-to-communicable death ratio shifted between 2000 and 2019? (epidemiological transition)
15. Which countries have the highest total mortality burden (NCD + communicable combined)?
16. Which African countries face the "double burden" of high NCD AND communicable mortality?

---

## 4. Data Model

### 4.1 Layered architecture

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

### 4.2 Data flow diagram

```
         CSV files (4 files)
              │
              ▼
┌────────────────┐ ┌──────────────┐ ┌────────────────────┐ ┌────────────────┐
│stg_health_     │ │stg_ncd_      │ │stg_communicable_   │ │stg_countries_  │
│estimates_raw   │ │deaths_raw    │ │deaths_raw          │ │raw             │
└───────┬────────┘ └──────┬───────┘ └─────────┬──────────┘ └───────┬────────┘
        │                 │                   │                    │
        └─────────────────┴─── 03_transform ──┴────────────────────┘
                                    │
        ┌───────────────────────────┼──────────────────────────────┐
        ▼                           ▼                              ▼
  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐ ┌───────────┐
  │dim_      │ │dim_cause │ │dim_      │ │dim_sex │ │dim_      │ │dim_disease│
  │indicator │ │(4 NCD    │ │country   │ │(3 rows)│ │indicator │ │(5 commun.)│
  │(4 rows)  │ │ causes)  │ │(228 rows)│ │        │ │(4 rows)  │ │           │
  └────┬─────┘ └────┬─────┘ └────┬─────┘ └───┬────┘ └──────────┘ └─────┬─────┘
       │             │            │           │                         │
       ▼             ▼            ▼           ▼                         ▼
┌────────────────┐ ┌──────────────────┐ ┌───────────────────────┐
│fct_health_     │ │fct_ncd_deaths    │ │fct_communicable_deaths│
│estimate        │ │(~43,900 rows)    │ │(~10,900 rows)         │
│(~48,800 rows)  │ │                  │ │                       │
└───────┬────────┘ └────────┬─────────┘ └──────────┬────────────┘
        │                   │                      │
        ▼                   ▼                      ▼
┌────────────────┐ ┌──────────────────┐ ┌───────────────────────┐
│vw_health_      │ │vw_deaths_        │ │vw_communicable_       │
│  enriched      │ │  enriched        │ │  enriched             │
│vw_yearly_kpi   │ │vw_yearly_deaths_ │ │                       │
│vw_region_      │ │  by_cause        │ │                       │
│  yearly_kpi    │ │                  │ │                       │
└────────────────┘ └──────────────────┘ └───────────────────────┘
```

### 4.3 Dimensions

| Dimension | PK | Rows | Source | Notes |
|---|---|---|---|---|
| `dim_indicator` | `indicator_code` | 4 | Static INSERT | Health estimate indicator definitions |
| `dim_country` | `country_code` | 228 | `stg_countries_raw` | ISO-3 codes, WHO region mapping |
| `dim_sex` | `sex_code` | 3 | Static INSERT | SEX_BTSX, SEX_MLE, SEX_FMLE |
| `dim_cause` | `cause_code` | 4 | Static INSERT | NCD cause codes (GHE061, GHE080, GHE110, GHE117) |
| `dim_disease` | `disease_code` | 5 | Static INSERT | Communicable disease indicator codes |

Six countries from the staging data were excluded because they lack WHO region assignments: Channel Islands, Hong Kong, Macao, former Serbia and Montenegro, Pristina, and Kosovo.

### 4.4 Facts

| Fact table | Grain | Rows | Dimensions | Metric |
|---|---|---|---|---|
| `fct_health_estimate` | (indicator, country, year, sex) | 48,840 | dim_indicator, dim_country, dim_sex | `metric_value` + CI |
| `fct_ncd_deaths` | (cause, country, year, sex) | 43,920 | dim_cause, dim_country, dim_sex | `death_count` + CI |
| `fct_communicable_deaths` | (disease, country, year) | 10,909 | dim_disease, dim_country | `death_count` + CI |

Note: Communicable disease deaths have **no sex breakdown** — the WHO API provides only total-population figures.

### 4.5 Design decisions

1. **Separate fact tables**: NCD and communicable deaths have different grains (NCD has sex breakdown, communicable does not) and different source structures. Cross-fact analysis is done via JOINs in analytical queries.

2. **Separate dim_cause and dim_disease**: NCD causes use WHO GHE cause codes (e.g., `GHECAUSES_GHE110`), while communicable diseases use indicator codes (e.g., `TB_e_mort_exc_tbhiv_num`). Keeping them separate preserves distinct provenance.

3. **Two-step INSERT for confidence intervals**: MySQL 8.0 strict mode produces an `Incorrect DECIMAL value` error when `CAST`-ing empty strings to `DECIMAL` inside `INSERT...SELECT` at scale. The workaround is to INSERT main metrics first, then UPDATE the confidence intervals separately.

4. **Carriage return cleanup**: The country CSV had Windows-style line endings (`\r\n`), causing invisible `\r` characters in `region_name`. MySQL's `TRIM()` does not strip `\r`, so `REPLACE(TRIM(...), '\r', '')` was added to the ETL.

---

## 5. Data Acquisition

### 5.1 Download script

A Python script (`data/download_who_data.py`) automates data download from the WHO GHO OData API:

1. Fetches data from 10 API endpoints using `$top`/`$skip` pagination (the API returns max 1000 records per request and does not provide `@odata.nextLink`)
2. Extracts relevant fields from the JSON responses
3. Saves results as CSV files in `data/`

### 5.2 API challenges encountered

- **No `@odata.nextLink`**: Manual `$skip`-based pagination was required
- **Inconsistent `$filter` support**: OData filters for `SpatialDimType eq 'COUNTRY'` returned empty results; filtering is done during ETL instead
- **Mixed spatial types**: Each indicator returns COUNTRY, REGION, GLOBAL, and WORLDBANKINCOMEGROUP records; only COUNTRY is loaded

### 5.3 Data files produced

| File | Description | Rows |
|---|---|---|
| `health_estimates_raw.csv` | 4 health indicators (LE, HALE, mortality, LE at 60) | 51,744 |
| `ncd_deaths_raw.csv` | NCD deaths by cause (cardiovascular, cancer, diabetes, respiratory) | 46,560 |
| `communicable_deaths_raw.csv` | Communicable disease deaths (TB, malaria, HIV, hepatitis B/C) | 12,989 |
| `countries.csv` | Country dimension with WHO region mapping | 234 |
| `regions.csv` | WHO region codes and names | 43 |

---

## 6. ETL Process

### 6.1 Execution order

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

### 6.2 Data transformations applied

- **Type casting**: VARCHAR → INT, SMALLINT, DECIMAL via `CAST()` with validation
- **Whitespace cleaning**: `TRIM()` on all text fields
- **Carriage return removal**: `REPLACE(..., '\r', '')` on country dimension fields
- **Null handling**: Empty strings and 'None' values → SQL NULL via `NULLIF()`
- **Spatial filtering**: Only `SpatialDimType = 'COUNTRY'` rows loaded
- **Referential integrity**: JOIN-based filtering ensures only valid dimension keys are loaded

### 6.3 Row counts after ETL

| Table | Staging rows | Core rows | Filtered out |
|---|---|---|---|
| Health estimates | 51,744 | 48,840 | 2,904 (region/global/income group) |
| NCD deaths | 46,560 | 43,920 | 2,640 (region/global/income group) |
| Communicable deaths | 12,989 | 10,909 | 2,080 (region/global/income group) |
| Countries | 234 | 228 | 6 (missing region assignment) |

---

## 7. Data Quality

### 7.1 Quality checks performed

| # | Check | Result |
|---|---|---|
| 1 | Null values in critical fact columns | 0 nulls across all 3 facts |
| 2 | Orphan foreign keys (country, indicator, sex, cause, disease) | 0 orphans in all tables |
| 3 | Duplicate business keys | 0 duplicates |
| 4 | Year range validation | Health: 2000–2021, NCD: 2000–2019, Comm: varies (2000–2024) |
| 5 | Negative life expectancy values | 0 found |
| 6 | Negative mortality / death count values | 0 found |
| 7 | Life expectancy > 100 years | 0 found |
| 8 | Confidence interval coverage | 75% of health estimates have CI; 100% of NCD deaths |
| 9 | Inverted confidence intervals (low > high) | 0 found |
| 10 | HALE > Life Expectancy inconsistency | 0 found |

### 7.2 Data quality issue: confidence interval inversion

The implemented check searches for **inverted confidence intervals** (`low_ci > high_ci`), which would indicate swapped bounds in the source data. A transactional `UPDATE` swaps the values if any are found:

```sql
START TRANSACTION;
UPDATE fct_health_estimate
SET low_ci = high_ci, high_ci = low_ci
WHERE low_ci IS NOT NULL AND high_ci IS NOT NULL AND low_ci > high_ci;
COMMIT;
```

### 7.3 Hidden data quality issue: carriage returns

A critical issue was discovered during development: **Windows-style carriage returns (`\r`)** embedded in `dim_country.region_name`. This caused all JOIN-based region filters (e.g., `WHERE region_name = 'Africa'`) to silently return 0 rows, because `'Africa\r' <> 'Africa'`. The issue was invisible in query results because `\r` is a non-printing character.

Detected by inspecting column values with `HEX()` and fixed by adding `REPLACE(TRIM(...), '\r', '')` to the ETL. This is a realistic example of a production data quality issue — invisible characters from cross-platform CSV transfers.

### 7.4 Quality checklist

- [x] Sin nulos en columnas criticas de las 3 facts
- [x] Sin filas huerfanas (integridad referencial garantizada por FK)
- [x] Sin duplicados de clave de negocio
- [x] Rango de anios correcto por indicador
- [x] Sin valores negativos de esperanza de vida ni muertes
- [x] Intervalos de confianza invertidos detectados y corregidos
- [x] Consistencia HALE <= Life Expectancy verificada

---

## 8. Analytical Queries

### 8.1 Overview

The project contains 20 analytical queries organized in 4 thematic blocks:

| Block | Queries | Theme |
|---|---|---|
| Q1–Q10 | Health estimates | Life expectancy trends, gender gaps, regional comparisons |
| Q11–Q14 | NCD deaths | Disease burden by cause, cross-fact correlation with life expectancy |
| Q15–Q16 | Communicable deaths | Temporal trends, most affected countries per disease |
| Q17–Q20 | Cross-dataset | NCD vs communicable comparison, epidemiological transition, double burden |

### 8.2 Requirements checklist

| Requirement | Minimum | Actual | Queries |
|---|---|---|---|
| Total queries | 8–12 | 20 | All |
| Temporal aggregations | 2 | 5 | Q1, Q2, Q11, Q15, Q18 |
| CTEs | 2 | 11 | Q4, Q5, Q6, Q8, Q12, Q13, Q16, Q17, Q18, Q19, Q20 |
| Top-N per group | 1 | 3 | Q4, Q12, Q16 |
| Data quality detected/corrected | 1 | 2 | CI inversion check + `\r` cleanup |

### 8.3 Query descriptions and rationale

#### Block 1: Health Estimates (Q1–Q10)

**Q1 — Global life expectancy trend (2000–2021)**: Temporal aggregation showing the average life expectancy across 185 countries per year. Reveals a steady increase from 67.0 years (2000) to 72.6 years (2019), followed by a decline to 71.3 years (2021) — likely reflecting the impact of COVID-19.

**Q2 — Year-over-year % change**: Uses `LAG()` window function to compute annual growth rates. Highlights the 2020 (-0.78%) and 2021 (-1.07%) declines — the only negative years in the 22-year series.

**Q3 — Top 10 countries by life expectancy (2021)**: Simple ranking. Japan leads at 84.5 years, followed by Singapore (83.9) and South Korea (83.8).

**Q4 — Top 3 per WHO region**: Uses `ROW_NUMBER() OVER (PARTITION BY region)` to find the best-performing country in each region. Demonstrates top-N-per-group.

**Q5 — Gender gap by region**: Pivots male vs female life expectancy using `CASE WHEN` in a CTE. Americas show the largest gap (5.73 years), Eastern Mediterranean the smallest (3.64 years).

**Q6 — Life expectancy vs adult mortality correlation**: Cross-indicator JOIN between WHOSIS_000001 and WHOSIS_000004. Confirms strong inverse relationship.

**Q7 — Regional improvement (2000 vs 2021)**: Africa improved the most (+9.28 years, +17.1%), while the Americas showed almost no improvement (+0.23 years).

**Q8 — Mortality concentration (cumulative %)**: Uses cumulative `SUM() OVER (ORDER BY ...)`. Top 10 countries by adult mortality account for ~13% of the global total.

**Q9 — HALE ranking within regions**: Uses `DENSE_RANK()` to position each country within its region by healthy life expectancy.

**Q10 — Countries with declining life expectancy**: Identifies 23 countries where life expectancy fell between 2000 and 2021. Paraguay (-4.41), Philippines (-3.57), and Peru (-3.46) saw the largest declines.

#### Block 2: NCD Deaths (Q11–Q14)

**Q11 — NCD death trends by cause (2000–2019)**: Cardiovascular diseases dominate with ~17.9M deaths in 2019, followed by cancers (~9.3M), chronic respiratory (~4.1M), and diabetes (~2.0M).

**Q12 — Most lethal NCD per country**: Uses `ROW_NUMBER() PARTITION BY country`. Cardiovascular diseases are the top cause in almost every country; Japan and the UK are exceptions where cancer leads.

**Q13 — NCD death share by region**: Window function for percentage within region. Eastern Mediterranean has the highest cardiovascular share (64.0%), South-East Asia has an unusually high chronic respiratory share (21.3%).

**Q14 — Life expectancy vs cardiovascular deaths (cross-fact)**: JOINs health estimates with NCD deaths. Shows that large-population countries (China, India) dominate absolute counts regardless of life expectancy.

#### Block 3: Communicable Deaths (Q15–Q16)

**Q15 — Communicable disease death trends**: Tracks TB, malaria, HIV, and hepatitis deaths over time. HIV/AIDS deaths have declined dramatically since 2005.

**Q16 — Top 5 countries per communicable disease**: TB: India (300K), Indonesia (118K). Malaria: Nigeria (185K), DR Congo (68K). HIV/AIDS: South Africa (53K), Mozambique (44K). Hepatitis B: Indonesia (61K). Hepatitis C: Pakistan (50K).

#### Block 4: Cross-Dataset Analysis (Q17–Q20)

**Q17 — NCD vs Communicable by region (2019)**: Uses UNION ALL across both death fact tables. Africa is the only region where communicable diseases still represent 42.5% of deaths. In Europe, NCDs account for 99.6%.

**Q18 — Epidemiological transition (2000 → 2019)**: Computes the NCD-to-communicable death ratio. Africa's ratio shifted from 0.6 (communicable dominated) to 1.4 (NCD now dominates).

**Q19 — Total mortality burden (3-fact JOIN)**: The most complex query. JOINs all 3 fact tables to show each country's life expectancy alongside its total mortality burden.

**Q20 — Africa's double burden**: Nigeria leads with 654K combined deaths (59% communicable). DR Congo, Tanzania, and Niger have roughly 50/50 splits — illustrating the "double burden of disease."

---

## 9. Advanced SQL

### 9.1 Functions

**`fn_safe_pct(p_num, p_den)`** — Safe percentage calculation. Returns NULL when denominator is zero or NULL.

**`fn_ci_width(p_low, p_high)`** — Confidence interval width. Returns NULL when either bound is missing.

### 9.2 Procedure

**`sp_refresh_core(p_verbose BOOLEAN)`** — Full ETL pipeline (dimensions + 3 facts) in a single stored procedure with:
- Explicit `START TRANSACTION` / `COMMIT`
- `DECLARE EXIT HANDLER FOR SQLEXCEPTION` with `ROLLBACK`
- Optional verbose output showing row counts
- Idempotent execution (TRUNCATEs before loading)

### 9.3 Trigger

**`trg_fct_health_bi_validate`** — BEFORE INSERT on `fct_health_estimate`:
- Life expectancy indicators cannot have negative `metric_value`
- `year_val` must be in range 1900–2100
- Raises `SIGNAL SQLSTATE '45000'` with descriptive message on violation

| Tipo | Nombre | Descripcion |
|---|---|---|
| FUNCTION | `fn_safe_pct` | Porcentaje seguro (evita division por cero) |
| FUNCTION | `fn_ci_width` | Ancho del intervalo de confianza |
| PROCEDURE | `sp_refresh_core` | Recarga completa staging → core en transaccion (3 facts) |
| TRIGGER | `trg_fct_health_bi_validate` | Valida metric_value y year_val antes de INSERT |

---

## 10. Key Findings

### 10.1 Life expectancy

- Global average life expectancy increased from **67.0 years (2000) to 72.6 years (2019)**, then declined to **71.3 years (2021)** — a 2-year setback likely driven by COVID-19.
- **Japan** leads the world at 84.5 years; **Lesotho** is the lowest at 50.7 years.
- **Africa** showed the greatest improvement (+9.3 years), while the **Americas** barely improved (+0.2 years) over the full period.
- **23 countries** saw life expectancy decline between 2000 and 2021, predominantly in the Americas.
- Women live longer than men in every WHO region, with the largest gap in the Americas (5.7 years).

### 10.2 Non-communicable diseases

- **Cardiovascular diseases** are the world's #1 killer: ~17.9M deaths in 2019, accounting for 50–64% of NCD deaths depending on region.
- **Cancer** is the #2 cause globally (~9.3M), but is the leading NCD killer in Japan and the UK.
- The **Eastern Mediterranean** has the highest cardiovascular death share (64%), while **South-East Asia** has an unusually high chronic respiratory disease share (21.3%).

### 10.3 Communicable diseases

- **Tuberculosis** remains the deadliest communicable disease with India alone accounting for 300K deaths in 2024.
- **Malaria** is heavily concentrated in Sub-Saharan Africa, with Nigeria alone responsible for 185K deaths.
- **HIV/AIDS** deaths have declined significantly since their peak but remain concentrated in Southern and Eastern Africa.
- **Hepatitis** data is limited (2022 only) but shows Indonesia, Pakistan, and Nigeria as the most affected countries.

### 10.4 Epidemiological transition

- Africa is the **only region** where communicable diseases still represent a large share of mortality (42.5% in 2019).
- All other regions are overwhelmingly dominated by NCDs (93–99.6%).
- Africa's NCD-to-communicable ratio **flipped from 0.6 (2000) to 1.4 (2019)** — meaning NCDs now cause more deaths than infectious diseases even in Africa.
- Several African countries face a **"double burden"**: Nigeria (654K combined deaths), DR Congo (363K), and Tanzania (159K) have roughly equal NCD and communicable mortality.

---

## 11. Supuestos

- Solo se cargan filas de tipo `COUNTRY` (se excluyen agregados regionales y globales de la API).
- Los intervalos de confianza (`low_ci`, `high_ci`) pueden ser NULL — la API no los proporciona para todos los indicadores/anios.
- Health estimates cubren 2000–2021; NCD deaths cubren 2000–2019; communicable deaths varian por enfermedad (2000–2024).
- El indicador de mortalidad adulta mide probabilidad de muerte entre 15 y 60 anios por cada 1000 habitantes.
- NCD deaths solo cubre las 4 principales causas de muerte no transmisible.
- Communicable deaths no tienen desglose por sexo (solo totales por pais/anio).
- Hepatitis B y C solo tienen datos para 2022.

## 12. Limitaciones

1. **No hay datos de poblacion**: no se pueden calcular tasas de mortalidad per capita, solo numeros absolutos de muertes. Paises grandes (China, India) dominan los rankings simplemente por tamano poblacional.
2. **NCD-only cause breakdown**: el dataset NCD solo cubre 4 categorias de enfermedades. Lesiones, salud mental y otras causas no estan incluidas.
3. **Sin desglose por sexo para enfermedades comunicables**: TB, malaria, HIV y hepatitis solo tienen datos totales, sin permitir analisis de genero.
4. **Cobertura temporal desigual**: Health estimates cubren 2000–2021, NCD deaths 2000–2019, communicable diseases varian (TB/malaria/HIV: 2000–2024, hepatitis: solo 2022). Comparaciones cross-dataset limitadas a anios solapados.
5. **Datos estimados**: todos los valores del GHO son estimaciones estadisticas modeladas, no registros directos de defuncion.
6. **Hepatitis data sparsity**: Hepatitis B y C solo tienen datos para 2022 (159–160 filas), limitando su utilidad para analisis de tendencias.

---

## 13. Instrucciones de reproduccion

### Prerequisitos
- Docker y Docker Compose
- Python 3.x (para descargar datos)
- DBeaver u otra herramienta SQL (para importar CSV y ejecutar queries)

### Pasos

1. **Levantar MySQL con Docker Compose**:
   ```bash
   docker compose up -d
   ```
   Esto arranca un contenedor MySQL 8.0 accesible en `localhost:3307` (usuario: `root`, password: `root`).

2. **Conectar desde DBeaver** (u otra herramienta):
   - Host: `127.0.0.1`
   - Port: `3307`
   - User: `root`
   - Password: `root`

   O desde la terminal:
   ```bash
   docker exec -it mysql-evolve mysql -uroot -proot
   ```

3. **Descargar datos** (requiere conexion a internet):
   ```bash
   python3 data/download_who_data.py
   ```

4. **Crear schema** — ejecutar en MySQL:
   ```
   sql/01_schema.sql
   ```

5. **Importar CSV a staging** con DBeaver Import Wizard o `LOAD DATA INFILE`:
   - `data/health_estimates_raw.csv` → `stg_health_estimates_raw`
   - `data/ncd_deaths_raw.csv` → `stg_ncd_deaths_raw`
   - `data/communicable_deaths_raw.csv` → `stg_communicable_deaths_raw`
   - `data/countries.csv` → `stg_countries_raw`

6. **Validar staging**: `sql/02_load_staging.sql`

7. **Transformar a core**: `sql/03_transform_core.sql`

8. **Crear vistas semanticas**: `sql/04_semantic_views.sql`

9. **Validar calidad**: `sql/06_quality_checks.sql`

10. **Ejecutar consultas analiticas**: `sql/05_analysis_queries.sql`

11. **SQL avanzado**: `sql/07_advanced_sql.sql`

### Parar el contenedor

```bash
docker compose down
```

Para eliminar tambien los datos persistidos:
```bash
docker compose down -v
```

### Expected table counts

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

## 14. Estructura del repositorio

```
who-global-health-sql-project/
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
  charts/                         # 20 PNG visualizations
  generate_charts.py              # Chart generation script (matplotlib)
  docker-compose.yml              # MySQL 8.0 container (port 3307)
  VISUAL_REPORT.html              # HTML report with all charts
  SLIDE_DECK.html                 # Presentation slides with embedded charts
  COMPLIANCE_REPORT.md            # Requirements compliance verification
  PROJECT_BRIEF.md                # Original project requirements
  README.md                       # This file
```
