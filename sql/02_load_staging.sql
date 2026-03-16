/* ============================================================
   WHO Global Health Estimates — 02_load_staging.sql
   Validacion post-carga de staging
   ============================================================ */

/* OBJETIVO
   - Verificar que los CSV se importaron correctamente a staging
   - Revisar volumen, muestras y problemas de parseo

   INSTRUCCIONES
   1. Importar CSV a tablas stg_* con DBeaver Import Wizard u otra herramienta:
      - data/health_estimates_raw.csv  →  stg_health_estimates_raw
      - data/ncd_deaths_raw.csv        →  stg_ncd_deaths_raw
      - data/communicable_deaths_raw.csv → stg_communicable_deaths_raw
      - data/countries.csv             →  stg_countries_raw
   2. Ejecutar este archivo para validar la carga

   NOTA: No avances a core hasta que staging tenga sentido
*/

USE who_disease_burden;

/* ---------- 1. Conteo de filas ---------- */
SELECT 'stg_health_estimates_raw' AS table_name, COUNT(*) AS n_rows
FROM stg_health_estimates_raw
UNION ALL
SELECT 'stg_ncd_deaths_raw', COUNT(*)
FROM stg_ncd_deaths_raw
UNION ALL
SELECT 'stg_communicable_deaths_raw', COUNT(*)
FROM stg_communicable_deaths_raw
UNION ALL
SELECT 'stg_countries_raw', COUNT(*)
FROM stg_countries_raw;

/* ---------- 2. Muestras ---------- */
SELECT * FROM stg_health_estimates_raw LIMIT 10;
SELECT * FROM stg_ncd_deaths_raw LIMIT 10;
SELECT * FROM stg_communicable_deaths_raw LIMIT 10;
SELECT * FROM stg_countries_raw LIMIT 10;

/* ---------- 3. Valores unicos clave ---------- */
-- Tipos de dimension espacial (esperamos COUNTRY, REGION, GLOBAL, WORLDBANKINCOMEGROUP)
SELECT raw_spatial_dim_type, COUNT(*) AS n
FROM stg_health_estimates_raw
GROUP BY raw_spatial_dim_type
ORDER BY n DESC;

-- Indicadores presentes
SELECT raw_indicator_code, COUNT(*) AS n
FROM stg_health_estimates_raw
GROUP BY raw_indicator_code;

-- Codigos de sexo
SELECT raw_sex_code, COUNT(*) AS n
FROM stg_health_estimates_raw
GROUP BY raw_sex_code;

-- Rango de anios
SELECT MIN(raw_year) AS min_year, MAX(raw_year) AS max_year
FROM stg_health_estimates_raw;

/* ---------- 4. Checks de parseabilidad ---------- */
SELECT
    SUM(CASE WHEN TRIM(raw_year) NOT REGEXP '^[0-9]{4}$'
             THEN 1 ELSE 0 END) AS bad_year,
    SUM(CASE WHEN TRIM(raw_numeric_value) NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
             THEN 1 ELSE 0 END) AS bad_numeric_value,
    SUM(CASE WHEN TRIM(raw_low) NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
              AND TRIM(IFNULL(raw_low, '')) NOT IN ('', 'None')
             THEN 1 ELSE 0 END) AS bad_low,
    SUM(CASE WHEN TRIM(raw_high) NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
              AND TRIM(IFNULL(raw_high, '')) NOT IN ('', 'None')
             THEN 1 ELSE 0 END) AS bad_high,
    SUM(CASE WHEN TRIM(IFNULL(raw_country_code, '')) = ''
             THEN 1 ELSE 0 END) AS empty_country_code,
    SUM(CASE WHEN TRIM(IFNULL(raw_sex_code, '')) = ''
             THEN 1 ELSE 0 END) AS empty_sex_code
FROM stg_health_estimates_raw;

/* ---------- 5. Checks de parseabilidad — NCD deaths ---------- */
SELECT
    SUM(CASE WHEN TRIM(raw_year) NOT REGEXP '^[0-9]{4}$'
             THEN 1 ELSE 0 END) AS bad_year,
    SUM(CASE WHEN TRIM(raw_numeric_value) NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
             THEN 1 ELSE 0 END) AS bad_numeric_value,
    SUM(CASE WHEN TRIM(IFNULL(raw_cause_code, '')) = ''
             THEN 1 ELSE 0 END) AS empty_cause_code,
    SUM(CASE WHEN TRIM(IFNULL(raw_country_code, '')) = ''
             THEN 1 ELSE 0 END) AS empty_country_code
FROM stg_ncd_deaths_raw;

-- Causas presentes en deaths
SELECT raw_cause_code, COUNT(*) AS n
FROM stg_ncd_deaths_raw
GROUP BY raw_cause_code;

/* ---------- 6. Checks de parseabilidad — communicable deaths ---------- */
SELECT
    SUM(CASE WHEN TRIM(raw_year) NOT REGEXP '^[0-9]{4}$'
             THEN 1 ELSE 0 END) AS bad_year,
    SUM(CASE WHEN TRIM(raw_numeric_value) NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
             THEN 1 ELSE 0 END) AS bad_numeric_value,
    SUM(CASE WHEN TRIM(IFNULL(raw_disease_name, '')) = ''
             THEN 1 ELSE 0 END) AS empty_disease_name
FROM stg_communicable_deaths_raw;

-- Enfermedades presentes
SELECT raw_disease_name, COUNT(*) AS n
FROM stg_communicable_deaths_raw
GROUP BY raw_disease_name;

/* ---------- 7. Paises sin region ---------- */
SELECT raw_country_code, raw_country_name
FROM stg_countries_raw
WHERE TRIM(IFNULL(raw_region_code, '')) = ''
   OR TRIM(IFNULL(raw_region_name, '')) = '';
