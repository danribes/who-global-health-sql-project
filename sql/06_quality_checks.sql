/* ============================================================
   WHO Global Health Estimates — 06_quality_checks.sql
   Checklist de calidad de datos y modelo
   ============================================================ */

/* OBJETIVO
   - Comprobar que los datos core tienen sentido
   - Detectar errores antes de analizar
   - Documentar al menos 1 problema detectado y corregido

   PRINCIPIO: un buen proyecto no solo analiza, tambien valida
*/

USE who_disease_burden;

/* ========== 1. Nulos en columnas criticas de la fact ========== */
SELECT
    SUM(CASE WHEN indicator_code IS NULL THEN 1 ELSE 0 END) AS null_indicator,
    SUM(CASE WHEN country_code   IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN year_val       IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN sex_code       IS NULL THEN 1 ELSE 0 END) AS null_sex,
    SUM(CASE WHEN metric_value   IS NULL THEN 1 ELSE 0 END) AS null_metric
FROM fct_health_estimate;

/* ========== 2. Orfandad de claves foraneas ========== */
-- No deberia haber ninguna fila huerfana gracias a los FK constraints

-- Paises en fact que no existen en dim_country
SELECT COUNT(*) AS orphan_country
FROM fct_health_estimate f
LEFT JOIN dim_country dc ON dc.country_code = f.country_code
WHERE dc.country_code IS NULL;

-- Indicadores en fact que no existen en dim_indicator
SELECT COUNT(*) AS orphan_indicator
FROM fct_health_estimate f
LEFT JOIN dim_indicator di ON di.indicator_code = f.indicator_code
WHERE di.indicator_code IS NULL;

-- Sexos en fact que no existen en dim_sex
SELECT COUNT(*) AS orphan_sex
FROM fct_health_estimate f
LEFT JOIN dim_sex ds ON ds.sex_code = f.sex_code
WHERE ds.sex_code IS NULL;

/* ========== 3. Duplicados de clave de negocio ========== */
-- La clave natural es (indicator_code, country_code, year_val, sex_code)
SELECT
    indicator_code,
    country_code,
    year_val,
    sex_code,
    COUNT(*) AS n_dups
FROM fct_health_estimate
GROUP BY indicator_code, country_code, year_val, sex_code
HAVING COUNT(*) > 1
ORDER BY n_dups DESC
LIMIT 20;

/* ========== 4. Rango de anios ========== */
-- Esperamos 2000-2021
SELECT
    MIN(year_val) AS min_year,
    MAX(year_val) AS max_year,
    COUNT(DISTINCT year_val) AS n_distinct_years
FROM fct_health_estimate;

/* ========== 5. Valores fuera de rango ========== */
-- Esperanza de vida negativa (no deberia existir)
SELECT COUNT(*) AS negative_life_expectancy
FROM fct_health_estimate
WHERE indicator_code IN ('WHOSIS_000001', 'WHOSIS_000002', 'WHOSIS_000015')
  AND metric_value < 0;

-- Mortalidad adulta negativa (no deberia existir)
SELECT COUNT(*) AS negative_mortality
FROM fct_health_estimate
WHERE indicator_code = 'WHOSIS_000004'
  AND metric_value < 0;

-- Esperanza de vida > 100 anios (sospechoso)
SELECT country_code, year_val, sex_code, metric_value
FROM fct_health_estimate
WHERE indicator_code = 'WHOSIS_000001'
  AND metric_value > 100;

/* ========== 6. Cobertura de intervalos de confianza ========== */
-- Cuantas filas tienen intervalo de confianza?
SELECT
    indicator_code,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN low_ci IS NOT NULL AND high_ci IS NOT NULL THEN 1 ELSE 0 END) AS with_ci,
    SUM(CASE WHEN low_ci IS NULL OR high_ci IS NULL THEN 1 ELSE 0 END) AS without_ci
FROM fct_health_estimate
GROUP BY indicator_code;

/* ========== 7. PROBLEMA DETECTADO Y CORREGIDO ========== */
/* Deteccion: intervalos de confianza invertidos (low_ci > high_ci)
   Esto indicaria un error en los datos fuente donde los limites
   inferior y superior estan intercambiados.
*/

-- Paso 1: Detectar el problema
SELECT
    'CI invertido' AS issue,
    COUNT(*) AS n_affected
FROM fct_health_estimate
WHERE low_ci IS NOT NULL
  AND high_ci IS NOT NULL
  AND low_ci > high_ci;

-- Detalle de filas afectadas (si las hay)
SELECT fact_id, indicator_code, country_code, year_val, sex_code,
       metric_value, low_ci, high_ci
FROM fct_health_estimate
WHERE low_ci IS NOT NULL
  AND high_ci IS NOT NULL
  AND low_ci > high_ci
LIMIT 10;

-- Paso 2: Corregir intercambiando los valores
-- (solo ejecutar si el paso anterior devuelve filas)
START TRANSACTION;

UPDATE fct_health_estimate
SET low_ci  = high_ci,
    high_ci = low_ci
WHERE low_ci IS NOT NULL
  AND high_ci IS NOT NULL
  AND low_ci > high_ci;

-- Verificar que ya no hay invertidos
SELECT COUNT(*) AS remaining_inverted
FROM fct_health_estimate
WHERE low_ci IS NOT NULL
  AND high_ci IS NOT NULL
  AND low_ci > high_ci;

COMMIT;

/* ========== 8. Consistencia entre indicadores ========== */
-- HALE no deberia superar la esperanza de vida total
SELECT
    le.country_code,
    le.year_val,
    le.sex_code,
    ROUND(le.metric_value, 1) AS life_expectancy,
    ROUND(hale.metric_value, 1) AS hale,
    ROUND(le.metric_value - hale.metric_value, 1) AS gap
FROM fct_health_estimate le
JOIN fct_health_estimate hale
    ON  hale.country_code = le.country_code
    AND hale.year_val     = le.year_val
    AND hale.sex_code     = le.sex_code
WHERE le.indicator_code = 'WHOSIS_000001'
  AND hale.indicator_code = 'WHOSIS_000002'
  AND hale.metric_value > le.metric_value
LIMIT 10;

/* ========== 9. Checks de calidad — fct_ncd_deaths ========== */

-- 9a) Nulos en columnas criticas
SELECT
    SUM(CASE WHEN cause_code   IS NULL THEN 1 ELSE 0 END) AS null_cause,
    SUM(CASE WHEN country_code IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN year_val     IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN death_count  IS NULL THEN 1 ELSE 0 END) AS null_deaths
FROM fct_ncd_deaths;

-- 9b) Orfandad de claves foraneas
SELECT COUNT(*) AS orphan_cause
FROM fct_ncd_deaths f
LEFT JOIN dim_cause ca ON ca.cause_code = f.cause_code
WHERE ca.cause_code IS NULL;

SELECT COUNT(*) AS orphan_country_deaths
FROM fct_ncd_deaths f
LEFT JOIN dim_country dc ON dc.country_code = f.country_code
WHERE dc.country_code IS NULL;

-- 9c) Duplicados de clave de negocio
SELECT cause_code, country_code, year_val, sex_code, COUNT(*) AS n_dups
FROM fct_ncd_deaths
GROUP BY cause_code, country_code, year_val, sex_code
HAVING COUNT(*) > 1
ORDER BY n_dups DESC
LIMIT 10;

-- 9d) Muertes negativas (no deberia existir)
SELECT COUNT(*) AS negative_deaths
FROM fct_ncd_deaths
WHERE death_count < 0;

-- 9e) Rango de anios
SELECT MIN(year_val) AS min_year, MAX(year_val) AS max_year
FROM fct_ncd_deaths;

-- 9f) CI invertido en deaths
SELECT COUNT(*) AS inverted_ci_deaths
FROM fct_ncd_deaths
WHERE low_ci IS NOT NULL AND high_ci IS NOT NULL AND low_ci > high_ci;

/* ========== 10. Checks de calidad — fct_communicable_deaths ========== */

-- 10a) Nulos en columnas criticas
SELECT
    SUM(CASE WHEN disease_code  IS NULL THEN 1 ELSE 0 END) AS null_disease,
    SUM(CASE WHEN country_code  IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN year_val      IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN death_count   IS NULL THEN 1 ELSE 0 END) AS null_deaths
FROM fct_communicable_deaths;

-- 10b) Orfandad
SELECT COUNT(*) AS orphan_disease
FROM fct_communicable_deaths f
LEFT JOIN dim_disease dd ON dd.disease_code = f.disease_code
WHERE dd.disease_code IS NULL;

SELECT COUNT(*) AS orphan_country_comm
FROM fct_communicable_deaths f
LEFT JOIN dim_country dc ON dc.country_code = f.country_code
WHERE dc.country_code IS NULL;

-- 10c) Duplicados
SELECT disease_code, country_code, year_val, COUNT(*) AS n_dups
FROM fct_communicable_deaths
GROUP BY disease_code, country_code, year_val
HAVING COUNT(*) > 1
LIMIT 10;

-- 10d) Muertes negativas
SELECT COUNT(*) AS negative_comm_deaths
FROM fct_communicable_deaths
WHERE death_count < 0;

-- 10e) Rango de anios
SELECT MIN(year_val) AS min_year, MAX(year_val) AS max_year
FROM fct_communicable_deaths;

-- 10f) CI invertido
SELECT COUNT(*) AS inverted_ci_comm
FROM fct_communicable_deaths
WHERE low_ci IS NOT NULL AND high_ci IS NOT NULL AND low_ci > high_ci;
