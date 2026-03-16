/* ============================================================
   WHO Global Health Estimates — 05_analysis_queries.sql
   Consultas analiticas de negocio
   ============================================================ */

/* PREGUNTA GUIA
   "Como ha evolucionado la esperanza de vida y la carga de mortalidad
    a nivel mundial, por regiones y por sexo, entre 2000 y 2021?
    Cuales son las enfermedades mas mortiferas por pais y como han cambiado?"

   CHECKLIST DE REQUISITOS
   - [x] 8-12 consultas (hay 20)
   - [x] 2+ agregaciones temporales (Q1, Q2, Q11, Q15, Q18)
   - [x] 2+ CTEs (Q4, Q5, Q6, Q8, Q12, Q13, Q16, Q17, Q18, Q19, Q20)
   - [x] 1+ ranking top-N por grupo (Q4, Q12, Q16)
   - [x] 1+ caso de calidad detectado y corregido (ver 06_quality_checks.sql)
*/

USE who_disease_burden;

/* ============================================================
   Q1: Evolucion anual de la esperanza de vida global (2000-2021)
   Tipo: Agregacion temporal
   ============================================================ */
-- Muestra la tendencia global del indicador principal a lo largo del tiempo
SELECT
    year_val,
    ROUND(AVG(metric_value), 2) AS avg_life_expectancy,
    COUNT(DISTINCT country_code) AS n_countries
FROM vw_health_enriched
WHERE indicator_code = 'WHOSIS_000001'
  AND sex_name = 'Both sexes'
GROUP BY year_val
ORDER BY year_val;

/* ============================================================
   Q2: Cambio interanual (%) de la esperanza de vida global
   Tipo: Agregacion temporal + window function LAG()
   ============================================================ */
-- Calcula el crecimiento porcentual anio a anio
SELECT
    year_val,
    avg_le,
    LAG(avg_le) OVER (ORDER BY year_val) AS prev_year_le,
    ROUND(
        100.0 * (avg_le - LAG(avg_le) OVER (ORDER BY year_val))
        / NULLIF(LAG(avg_le) OVER (ORDER BY year_val), 0),
        3
    ) AS yoy_change_pct
FROM (
    SELECT
        year_val,
        ROUND(AVG(metric_value), 4) AS avg_le
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND sex_name = 'Both sexes'
    GROUP BY year_val
) yearly
ORDER BY year_val;

/* ============================================================
   Q3: Top 10 paises con mayor esperanza de vida (ultimo anio)
   Tipo: Top-N simple
   ============================================================ */
-- Ranking global de paises por esperanza de vida en 2021
SELECT
    country_name,
    region_name,
    ROUND(metric_value, 1) AS life_expectancy_2021
FROM vw_health_enriched
WHERE indicator_code = 'WHOSIS_000001'
  AND sex_name = 'Both sexes'
  AND year_val = 2021
ORDER BY metric_value DESC
LIMIT 10;

/* ============================================================
   Q4: Top 3 paises por esperanza de vida en cada region WHO
   Tipo: CTE + ROW_NUMBER() PARTITION BY (top-N por grupo)
   ============================================================ */
-- Para cada region, muestra los 3 paises con mayor esperanza de vida
WITH ranked AS (
    SELECT
        region_name,
        country_name,
        ROUND(metric_value, 1) AS life_expectancy,
        ROW_NUMBER() OVER (
            PARTITION BY region_name
            ORDER BY metric_value DESC
        ) AS rn
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND sex_name = 'Both sexes'
      AND year_val = 2021
)
SELECT region_name, country_name, life_expectancy, rn
FROM ranked
WHERE rn <= 3
ORDER BY region_name, rn;

/* ============================================================
   Q5: Brecha de genero en esperanza de vida por region
   Tipo: CTE + pivot manual (CASE)
   ============================================================ */
-- Compara esperanza de vida masculina vs femenina por region
WITH gender_data AS (
    SELECT
        region_name,
        sex_name,
        ROUND(AVG(metric_value), 2) AS avg_le
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND year_val = 2021
      AND sex_name <> 'Both sexes'
    GROUP BY region_name, sex_name
)
SELECT
    region_name,
    MAX(CASE WHEN sex_name = 'Female' THEN avg_le END) AS female_le,
    MAX(CASE WHEN sex_name = 'Male'   THEN avg_le END) AS male_le,
    ROUND(
        MAX(CASE WHEN sex_name = 'Female' THEN avg_le END)
      - MAX(CASE WHEN sex_name = 'Male'   THEN avg_le END),
        2
    ) AS gender_gap
FROM gender_data
GROUP BY region_name
ORDER BY gender_gap DESC;

/* ============================================================
   Q6: Correlacion entre esperanza de vida y mortalidad adulta
   Tipo: CTE + self-join entre indicadores
   ============================================================ */
-- Compara esperanza de vida vs tasa de mortalidad por pais (2021)
WITH le AS (
    SELECT country_code, country_name, region_name,
           ROUND(metric_value, 1) AS life_expectancy
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND sex_name = 'Both sexes'
      AND year_val = 2021
),
mort AS (
    SELECT country_code,
           ROUND(metric_value, 1) AS adult_mortality
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000004'
      AND sex_name = 'Both sexes'
      AND year_val = 2021
)
SELECT
    le.country_name,
    le.region_name,
    le.life_expectancy,
    mort.adult_mortality
FROM le
JOIN mort ON mort.country_code = le.country_code
ORDER BY le.life_expectancy DESC
LIMIT 20;

/* ============================================================
   Q7: Regiones con mayor mejora en esperanza de vida (2000 vs 2021)
   Tipo: Comparacion temporal con subqueries
   ============================================================ */
-- Que regiones han mejorado mas su esperanza de vida en 21 anios?
SELECT
    r2021.region_name,
    r2000.avg_le  AS avg_le_2000,
    r2021.avg_le  AS avg_le_2021,
    ROUND(r2021.avg_le - r2000.avg_le, 2) AS improvement,
    ROUND(100.0 * (r2021.avg_le - r2000.avg_le) / NULLIF(r2000.avg_le, 0), 2) AS improvement_pct
FROM (
    SELECT region_name, ROUND(AVG(metric_value), 2) AS avg_le
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND sex_name = 'Both sexes'
      AND year_val = 2021
    GROUP BY region_name
) r2021
JOIN (
    SELECT region_name, ROUND(AVG(metric_value), 2) AS avg_le
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND sex_name = 'Both sexes'
      AND year_val = 2000
    GROUP BY region_name
) r2000 ON r2000.region_name = r2021.region_name
ORDER BY improvement DESC;

/* ============================================================
   Q8: Concentracion de la mortalidad — top 10 paises acumulan que %?
   Tipo: CTE + window function (SUM acumulativo)
   ============================================================ */
-- Que porcentaje de la mortalidad adulta global concentran los 10 peores paises?
WITH country_mort AS (
    SELECT
        country_name,
        region_name,
        ROUND(metric_value, 1) AS adult_mortality,
        ROW_NUMBER() OVER (ORDER BY metric_value DESC) AS rn,
        ROUND(
            100.0 * metric_value
            / NULLIF(SUM(metric_value) OVER (), 0),
            2
        ) AS pct_of_total,
        ROUND(
            100.0 * SUM(metric_value) OVER (ORDER BY metric_value DESC)
            / NULLIF(SUM(metric_value) OVER (), 0),
            2
        ) AS cumulative_pct
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000004'
      AND sex_name = 'Both sexes'
      AND year_val = 2021
)
SELECT country_name, region_name, adult_mortality,
       pct_of_total, cumulative_pct
FROM country_mort
WHERE rn <= 10;

/* ============================================================
   Q9: Ranking de paises por HALE dentro de cada region (2021)
   Tipo: DENSE_RANK() window function
   ============================================================ */
-- Posicion de cada pais en su region segun esperanza de vida saludable
SELECT
    region_name,
    country_name,
    ROUND(metric_value, 1) AS hale_2021,
    DENSE_RANK() OVER (
        PARTITION BY region_name
        ORDER BY metric_value DESC
    ) AS region_rank
FROM vw_health_enriched
WHERE indicator_code = 'WHOSIS_000002'
  AND sex_name = 'Both sexes'
  AND year_val = 2021
ORDER BY region_name, region_rank;

/* ============================================================
   Q10: Paises con esperanza de vida en descenso (tendencia negativa)
   Tipo: Deteccion de tendencia (primer vs ultimo anio)
   ============================================================ */
-- Identifica paises donde la esperanza de vida bajo entre 2000 y 2021
SELECT
    e2021.country_name,
    e2021.region_name,
    ROUND(e2000.metric_value, 1) AS le_2000,
    ROUND(e2021.metric_value, 1) AS le_2021,
    ROUND(e2021.metric_value - e2000.metric_value, 2) AS le_change
FROM vw_health_enriched e2021
JOIN vw_health_enriched e2000
    ON  e2000.country_code    = e2021.country_code
    AND e2000.indicator_code  = e2021.indicator_code
    AND e2000.sex_code        = e2021.sex_code
WHERE e2021.indicator_code = 'WHOSIS_000001'
  AND e2021.sex_name = 'Both sexes'
  AND e2021.year_val = 2021
  AND e2000.year_val = 2000
  AND e2021.metric_value < e2000.metric_value
ORDER BY le_change ASC;

/* ============================================================
   ============================================================
   QUERIES Q11-Q14: NCD DEATHS — DISEASE BURDEN ANALYSIS
   ============================================================
   ============================================================ */

/* ============================================================
   Q11: Evolucion anual de muertes globales por causa NCD (2000-2019)
   Tipo: Agregacion temporal
   ============================================================ */
-- Como han cambiado las muertes por cada causa NCD a lo largo del tiempo?
SELECT
    year_val,
    cause_name,
    ROUND(SUM(death_count), 0) AS total_deaths
FROM vw_deaths_enriched
WHERE sex_name = 'Both sexes'
GROUP BY year_val, cause_name
ORDER BY year_val, total_deaths DESC;

/* ============================================================
   Q12: Causa mas mortifera por pais (top-1 por grupo, ultimo anio)
   Tipo: CTE + ROW_NUMBER() PARTITION BY
   ============================================================ */
-- Cual es la enfermedad NCD que mas mata en cada pais?
WITH ranked_causes AS (
    SELECT
        country_name,
        region_name,
        cause_name,
        ROUND(death_count, 0) AS deaths_2019,
        ROW_NUMBER() OVER (
            PARTITION BY country_code
            ORDER BY death_count DESC
        ) AS rn
    FROM vw_deaths_enriched
    WHERE sex_name = 'Both sexes'
      AND year_val = 2019
)
SELECT country_name, region_name, cause_name, deaths_2019
FROM ranked_causes
WHERE rn = 1
ORDER BY deaths_2019 DESC
LIMIT 30;

/* ============================================================
   Q13: Distribucion porcentual de muertes NCD por causa y region
   Tipo: CTE + window function (% del total)
   ============================================================ */
-- Que porcentaje de las muertes NCD corresponde a cada causa en cada region?
WITH region_cause AS (
    SELECT
        region_name,
        cause_name,
        ROUND(SUM(death_count), 0) AS total_deaths
    FROM vw_deaths_enriched
    WHERE sex_name = 'Both sexes'
      AND year_val = 2019
    GROUP BY region_name, cause_name
)
SELECT
    region_name,
    cause_name,
    total_deaths,
    ROUND(
        100.0 * total_deaths
        / NULLIF(SUM(total_deaths) OVER (PARTITION BY region_name), 0),
        1
    ) AS pct_of_region
FROM region_cause
ORDER BY region_name, pct_of_region DESC;

/* ============================================================
   Q14: Relacion entre esperanza de vida y muertes cardiovasculares
   Tipo: Cross-fact JOIN entre fct_health_estimate y fct_ncd_deaths
   ============================================================ */
-- Paises con alta mortalidad cardiovascular tienen menor esperanza de vida?
SELECT
    le.country_name,
    le.region_name,
    ROUND(le.metric_value, 1) AS life_expectancy,
    ROUND(d.death_count, 0)   AS cardiovascular_deaths
FROM vw_health_enriched le
JOIN vw_deaths_enriched d
    ON  d.country_code = le.country_code
    AND d.year_val     = le.year_val
    AND d.sex_code     = le.sex_code
WHERE le.indicator_code = 'WHOSIS_000001'
  AND le.sex_name = 'Both sexes'
  AND le.year_val = 2019
  AND d.cause_name = 'Cardiovascular diseases'
ORDER BY d.death_count DESC
LIMIT 20;

/* ============================================================
   ============================================================
   QUERIES Q15-Q16: COMMUNICABLE DISEASE DEATHS
   ============================================================
   ============================================================ */

/* ============================================================
   Q15: Evolucion anual de muertes por enfermedades comunicables
   Tipo: Agregacion temporal
   ============================================================ */
-- Como han evolucionado las muertes por TB, Malaria, HIV, Hepatitis?
SELECT
    year_val,
    disease_name,
    ROUND(SUM(death_count), 0) AS total_deaths,
    COUNT(DISTINCT country_code) AS n_countries
FROM vw_communicable_enriched
GROUP BY year_val, disease_name
ORDER BY year_val, total_deaths DESC;

/* ============================================================
   Q16: Top 5 paises por enfermedad comunicable (ultimo anio disponible)
   Tipo: CTE + ROW_NUMBER() PARTITION BY (top-N por grupo)
   ============================================================ */
-- Cuales son los paises mas afectados por cada enfermedad comunicable?
WITH latest AS (
    SELECT
        disease_code,
        MAX(year_val) AS max_year
    FROM fct_communicable_deaths
    GROUP BY disease_code
),
ranked AS (
    SELECT
        dd.disease_name,
        dc.country_name,
        dc.region_name,
        f.year_val,
        ROUND(f.death_count, 0) AS deaths,
        ROW_NUMBER() OVER (
            PARTITION BY f.disease_code
            ORDER BY f.death_count DESC
        ) AS rn
    FROM fct_communicable_deaths f
    JOIN dim_disease dd ON dd.disease_code = f.disease_code
    JOIN dim_country dc ON dc.country_code = f.country_code
    JOIN latest l ON l.disease_code = f.disease_code AND l.max_year = f.year_val
)
SELECT disease_name, country_name, region_name, year_val, deaths
FROM ranked
WHERE rn <= 5
ORDER BY disease_name, rn;

/* ============================================================
   ============================================================
   QUERIES Q17-Q20: CROSS-DATASET ANALYSIS
   NCD vs Communicable — Epidemiological Transition
   ============================================================
   ============================================================ */

/* ============================================================
   Q17: NCD vs Communicable deaths by WHO region (2019)
   Tipo: Cross-fact UNION + agregacion por region
   ============================================================ */
-- Que regiones siguen dominadas por enfermedades comunicables vs NCDs?
-- Usamos 2019 como anio comun entre ambos datasets
WITH ncd_region AS (
    SELECT
        dc.region_name,
        'NCD' AS disease_type,
        ROUND(SUM(f.death_count), 0) AS total_deaths
    FROM fct_ncd_deaths f
    JOIN dim_country dc ON dc.country_code = f.country_code
    WHERE f.sex_code = 'SEX_BTSX'
      AND f.year_val = 2019
    GROUP BY dc.region_name
),
comm_region AS (
    SELECT
        dc.region_name,
        'Communicable' AS disease_type,
        ROUND(SUM(f.death_count), 0) AS total_deaths
    FROM fct_communicable_deaths f
    JOIN dim_country dc ON dc.country_code = f.country_code
    WHERE f.year_val = 2019
    GROUP BY dc.region_name
),
combined AS (
    SELECT * FROM ncd_region
    UNION ALL
    SELECT * FROM comm_region
)
SELECT
    region_name,
    disease_type,
    total_deaths,
    ROUND(
        100.0 * total_deaths
        / NULLIF(SUM(total_deaths) OVER (PARTITION BY region_name), 0),
        1
    ) AS pct_of_region
FROM combined
ORDER BY region_name, disease_type;

/* ============================================================
   Q18: Transicion epidemiologica — como ha cambiado el ratio
        NCD/Comunicable por region entre 2000 y 2019?
   Tipo: CTE + comparacion temporal entre dos facts
   ============================================================ */
-- Las regiones estan transitando de enfermedades infecciosas a cronicas?
WITH deaths_by_type AS (
    SELECT
        dc.region_name,
        f.year_val,
        'NCD' AS dtype,
        ROUND(SUM(f.death_count), 0) AS deaths
    FROM fct_ncd_deaths f
    JOIN dim_country dc ON dc.country_code = f.country_code
    WHERE f.sex_code = 'SEX_BTSX'
      AND f.year_val IN (2000, 2019)
    GROUP BY dc.region_name, f.year_val

    UNION ALL

    SELECT
        dc.region_name,
        f.year_val,
        'Communicable' AS dtype,
        ROUND(SUM(f.death_count), 0) AS deaths
    FROM fct_communicable_deaths f
    JOIN dim_country dc ON dc.country_code = f.country_code
    WHERE f.year_val IN (2000, 2019)
    GROUP BY dc.region_name, f.year_val
),
pivoted AS (
    SELECT
        region_name,
        year_val,
        SUM(CASE WHEN dtype = 'NCD' THEN deaths ELSE 0 END) AS ncd_deaths,
        SUM(CASE WHEN dtype = 'Communicable' THEN deaths ELSE 0 END) AS comm_deaths
    FROM deaths_by_type
    GROUP BY region_name, year_val
)
SELECT
    region_name,
    year_val,
    ncd_deaths,
    comm_deaths,
    ROUND(ncd_deaths / NULLIF(comm_deaths, 0), 1) AS ncd_to_comm_ratio
FROM pivoted
ORDER BY region_name, year_val;

/* ============================================================
   Q19: Esperanza de vida vs mortalidad total (NCD + comunicable)
   Tipo: Cross-fact JOIN entre las 3 facts
   ============================================================ */
-- Paises con mayor carga total de mortalidad tienen menor esperanza de vida?
WITH le AS (
    SELECT country_code, country_name, region_name,
           ROUND(metric_value, 1) AS life_expectancy
    FROM vw_health_enriched
    WHERE indicator_code = 'WHOSIS_000001'
      AND sex_name = 'Both sexes'
      AND year_val = 2019
),
ncd AS (
    SELECT country_code, ROUND(SUM(death_count), 0) AS ncd_deaths
    FROM fct_ncd_deaths
    WHERE sex_code = 'SEX_BTSX' AND year_val = 2019
    GROUP BY country_code
),
comm AS (
    SELECT country_code, ROUND(SUM(death_count), 0) AS comm_deaths
    FROM fct_communicable_deaths
    WHERE year_val = 2019
    GROUP BY country_code
)
SELECT
    le.country_name,
    le.region_name,
    le.life_expectancy,
    COALESCE(ncd.ncd_deaths, 0)   AS ncd_deaths,
    COALESCE(comm.comm_deaths, 0) AS communicable_deaths,
    COALESCE(ncd.ncd_deaths, 0) + COALESCE(comm.comm_deaths, 0) AS total_deaths
FROM le
LEFT JOIN ncd  ON ncd.country_code  = le.country_code
LEFT JOIN comm ON comm.country_code = le.country_code
ORDER BY total_deaths DESC
LIMIT 25;

/* ============================================================
   Q20: Doble carga en Africa — paises con alta mortalidad
        tanto NCD como comunicable
   Tipo: CTE + cross-fact + filtrado multidimensional
   ============================================================ */
-- Que paises africanos enfrentan la "doble carga" de enfermedad?
WITH african_ncd AS (
    SELECT
        f.country_code,
        dc.country_name,
        ROUND(SUM(f.death_count), 0) AS ncd_deaths
    FROM fct_ncd_deaths f
    JOIN dim_country dc ON dc.country_code = f.country_code
    WHERE dc.region_name = 'Africa'
      AND f.sex_code = 'SEX_BTSX'
      AND f.year_val = 2019
    GROUP BY f.country_code, dc.country_name
),
african_comm AS (
    SELECT
        f.country_code,
        ROUND(SUM(f.death_count), 0) AS comm_deaths
    FROM fct_communicable_deaths f
    JOIN dim_country dc ON dc.country_code = f.country_code
    WHERE dc.region_name = 'Africa'
      AND f.year_val = 2019
    GROUP BY f.country_code
)
SELECT
    n.country_name,
    n.ncd_deaths,
    COALESCE(c.comm_deaths, 0) AS communicable_deaths,
    n.ncd_deaths + COALESCE(c.comm_deaths, 0) AS total_deaths,
    ROUND(
        100.0 * COALESCE(c.comm_deaths, 0)
        / NULLIF(n.ncd_deaths + COALESCE(c.comm_deaths, 0), 0),
        1
    ) AS pct_communicable
FROM african_ncd n
LEFT JOIN african_comm c ON c.country_code = n.country_code
WHERE COALESCE(c.comm_deaths, 0) > 0
ORDER BY total_deaths DESC
LIMIT 20;
