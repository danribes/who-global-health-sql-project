/* ============================================================
   WHO Global Health Estimates — 03_transform_core.sql
   Limpieza + carga core desde staging
   ============================================================ */

/* OBJETIVO
   - Transformar datos crudos de staging en tablas core limpias
   - Cargar dimensiones primero, luego la fact
   - Solo cargar filas de tipo COUNTRY (excluir regiones/globales)

   GRANO DE LA FACT
   - Una fila por (indicator_code, country_code, year_val, sex_code)
*/

USE who_disease_burden;

/* ========== Limpiar core ========== */
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE fct_communicable_deaths;
TRUNCATE TABLE fct_ncd_deaths;
TRUNCATE TABLE fct_health_estimate;
TRUNCATE TABLE dim_disease;
TRUNCATE TABLE dim_cause;
TRUNCATE TABLE dim_country;
TRUNCATE TABLE dim_indicator;
TRUNCATE TABLE dim_sex;
SET FOREIGN_KEY_CHECKS = 1;

/* ========== Dimension: sex (tabla estatica) ========== */
INSERT INTO dim_sex (sex_code, sex_name) VALUES
    ('SEX_BTSX', 'Both sexes'),
    ('SEX_MLE',  'Male'),
    ('SEX_FMLE', 'Female');

/* ========== Dimension: indicator (tabla estatica) ========== */
INSERT INTO dim_indicator (indicator_code, indicator_name, unit) VALUES
    ('WHOSIS_000001', 'Life expectancy at birth',          'years'),
    ('WHOSIS_000002', 'Healthy life expectancy (HALE) at birth', 'years'),
    ('WHOSIS_000004', 'Adult mortality rate (15-60)',       'per 1000 population'),
    ('WHOSIS_000015', 'Life expectancy at age 60',         'years');

/* ========== Dimension: country ========== */
/* NOTA: REPLACE(\r) necesario porque los CSV pueden tener line endings Windows */
INSERT INTO dim_country (country_code, country_name, region_code, region_name)
SELECT DISTINCT
    REPLACE(TRIM(raw_country_code), '\r', '')  AS country_code,
    REPLACE(TRIM(raw_country_name), '\r', '')  AS country_name,
    REPLACE(TRIM(raw_region_code),  '\r', '')  AS region_code,
    REPLACE(TRIM(raw_region_name),  '\r', '')  AS region_name
FROM stg_countries_raw
WHERE TRIM(IFNULL(raw_country_code, '')) <> ''
  AND TRIM(IFNULL(raw_region_code, ''))  <> ''
  AND TRIM(IFNULL(raw_region_name, ''))  <> '';

SELECT COUNT(*) AS n_countries FROM dim_country;

/* ========== Fact: health estimates (paso 1 — metricas principales) ========== */
/* NOTA: MySQL 8.0 strict mode produce un error con CAST de cadenas vacias
   a DECIMAL en INSERT...SELECT a gran escala, incluso con NULLIF.
   Solucion: insertar primero sin intervalos de confianza y luego
   actualizar las filas que los tienen en un paso separado.
*/
INSERT INTO fct_health_estimate (
    indicator_code,
    country_code,
    year_val,
    sex_code,
    metric_value
)
SELECT
    TRIM(e.raw_indicator_code)                       AS indicator_code,
    TRIM(e.raw_country_code)                         AS country_code,
    CAST(TRIM(e.raw_year) AS UNSIGNED)               AS year_val,
    TRIM(e.raw_sex_code)                             AS sex_code,
    CAST(TRIM(e.raw_numeric_value) AS DECIMAL(18,6)) AS metric_value
FROM stg_health_estimates_raw e
/* Integridad referencial via JOIN */
JOIN dim_country   dc ON dc.country_code   = TRIM(e.raw_country_code)
JOIN dim_indicator di ON di.indicator_code  = TRIM(e.raw_indicator_code)
JOIN dim_sex       ds ON ds.sex_code        = TRIM(e.raw_sex_code)
/* Solo filas de tipo COUNTRY */
WHERE TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
  /* Validaciones de parseo */
  AND TRIM(e.raw_year) REGEXP '^[0-9]{4}$'
  AND TRIM(e.raw_numeric_value) REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

/* ========== Fact: paso 2 — intervalos de confianza ========== */
UPDATE fct_health_estimate f
JOIN stg_health_estimates_raw e
    ON  TRIM(e.raw_indicator_code) = f.indicator_code
    AND TRIM(e.raw_country_code)   = f.country_code
    AND CAST(TRIM(e.raw_year) AS UNSIGNED) = f.year_val
    AND TRIM(e.raw_sex_code)       = f.sex_code
    AND TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
SET f.low_ci  = CAST(TRIM(e.raw_low)  AS DECIMAL(18,6)),
    f.high_ci = CAST(TRIM(e.raw_high) AS DECIMAL(18,6))
WHERE TRIM(e.raw_low)  <> ''
  AND TRIM(e.raw_high) <> '';

/* ========== Dimension: cause (tabla estatica) ========== */
INSERT INTO dim_cause (cause_code, cause_name) VALUES
    ('GHECAUSES_GHE061', 'Malignant neoplasms (cancers)'),
    ('GHECAUSES_GHE080', 'Diabetes mellitus'),
    ('GHECAUSES_GHE110', 'Cardiovascular diseases'),
    ('GHECAUSES_GHE117', 'Chronic respiratory diseases');

/* ========== Fact: NCD deaths (paso 1 — death counts) ========== */
INSERT INTO fct_ncd_deaths (
    cause_code,
    country_code,
    year_val,
    sex_code,
    death_count
)
SELECT
    TRIM(e.raw_cause_code)                           AS cause_code,
    TRIM(e.raw_country_code)                         AS country_code,
    CAST(TRIM(e.raw_year) AS UNSIGNED)               AS year_val,
    TRIM(e.raw_sex_code)                             AS sex_code,
    CAST(TRIM(e.raw_numeric_value) AS DECIMAL(18,2)) AS death_count
FROM stg_ncd_deaths_raw e
JOIN dim_country dc ON dc.country_code = TRIM(e.raw_country_code)
JOIN dim_cause   ca ON ca.cause_code   = TRIM(e.raw_cause_code)
JOIN dim_sex     ds ON ds.sex_code     = TRIM(e.raw_sex_code)
WHERE TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
  AND TRIM(e.raw_year) REGEXP '^[0-9]{4}$'
  AND TRIM(e.raw_numeric_value) REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

/* ========== NCD deaths: paso 2 — intervalos de confianza ========== */
UPDATE fct_ncd_deaths f
JOIN stg_ncd_deaths_raw e
    ON  TRIM(e.raw_cause_code)    = f.cause_code
    AND TRIM(e.raw_country_code)  = f.country_code
    AND CAST(TRIM(e.raw_year) AS UNSIGNED) = f.year_val
    AND TRIM(e.raw_sex_code)      = f.sex_code
    AND TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
SET f.low_ci  = CAST(TRIM(e.raw_low)  AS DECIMAL(18,2)),
    f.high_ci = CAST(TRIM(e.raw_high) AS DECIMAL(18,2))
WHERE TRIM(e.raw_low)  <> ''
  AND TRIM(e.raw_high) <> '';

/* ========== Dimension: disease (comunicables, tabla estatica) ========== */
INSERT INTO dim_disease (disease_code, disease_name, disease_type) VALUES
    ('TB_e_mort_exc_tbhiv_num',  'Tuberculosis',  'communicable'),
    ('MALARIA_EST_DEATHS',       'Malaria',        'communicable'),
    ('HIV_0000000006',           'HIV/AIDS',        'communicable'),
    ('HEPATITIS_HBV_DEATHS_NUM', 'Hepatitis B',    'communicable'),
    ('HEPATITIS_HCV_DEATHS_NUM', 'Hepatitis C',    'communicable');

/* ========== Fact: communicable deaths (paso 1) ========== */
INSERT INTO fct_communicable_deaths (
    disease_code,
    country_code,
    year_val,
    death_count
)
SELECT
    TRIM(e.raw_indicator_code)                       AS disease_code,
    TRIM(e.raw_country_code)                         AS country_code,
    CAST(TRIM(e.raw_year) AS UNSIGNED)               AS year_val,
    CAST(TRIM(e.raw_numeric_value) AS DECIMAL(18,2)) AS death_count
FROM stg_communicable_deaths_raw e
JOIN dim_country dc ON dc.country_code = TRIM(e.raw_country_code)
JOIN dim_disease dd ON dd.disease_code = TRIM(e.raw_indicator_code)
WHERE TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
  AND TRIM(e.raw_year) REGEXP '^[0-9]{4}$'
  AND TRIM(e.raw_numeric_value) REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

/* ========== Communicable deaths: paso 2 — intervalos de confianza ========== */
UPDATE fct_communicable_deaths f
JOIN stg_communicable_deaths_raw e
    ON  TRIM(e.raw_indicator_code) = f.disease_code
    AND TRIM(e.raw_country_code)   = f.country_code
    AND CAST(TRIM(e.raw_year) AS UNSIGNED) = f.year_val
    AND TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
SET f.low_ci  = CAST(TRIM(e.raw_low)  AS DECIMAL(18,2)),
    f.high_ci = CAST(TRIM(e.raw_high) AS DECIMAL(18,2))
WHERE TRIM(e.raw_low)  <> ''
  AND TRIM(e.raw_high) <> '';

/* ========== Verificacion ========== */
SELECT 'fct_health_estimate' AS tbl, COUNT(*) AS n FROM fct_health_estimate
UNION ALL
SELECT 'fct_ncd_deaths', COUNT(*) FROM fct_ncd_deaths
UNION ALL
SELECT 'fct_communicable_deaths', COUNT(*) FROM fct_communicable_deaths;

SELECT indicator_code, COUNT(*) AS n
FROM fct_health_estimate
GROUP BY indicator_code;

SELECT cause_code, COUNT(*) AS n
FROM fct_ncd_deaths
GROUP BY cause_code;

SELECT disease_code, COUNT(*) AS n
FROM fct_communicable_deaths
GROUP BY disease_code;
