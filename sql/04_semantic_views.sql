/* ============================================================
   WHO Global Health Estimates — 04_semantic_views.sql
   Capa semantica: vistas de negocio reutilizables
   ============================================================ */

/* OBJETIVO
   - Crear vistas que enriquezcan los datos core
   - Facilitar el analisis posterior con nombres claros
   - Al menos 2 vistas de negocio (requisito del proyecto)
   - Vistas para health estimates Y NCD deaths
*/

USE who_disease_burden;

/* ========== Vista 1: Datos enriquecidos (base) ========== */
-- Une la fact con todas las dimensiones para tener una vista completa
CREATE OR REPLACE VIEW vw_health_enriched AS
SELECT
    f.fact_id,
    f.indicator_code,
    di.indicator_name,
    di.unit,
    f.country_code,
    dc.country_name,
    dc.region_code,
    dc.region_name,
    f.year_val,
    f.sex_code,
    ds.sex_name,
    f.metric_value,
    f.low_ci,
    f.high_ci,
    /* Ancho del intervalo de confianza */
    CASE
        WHEN f.high_ci IS NOT NULL AND f.low_ci IS NOT NULL
        THEN ROUND(f.high_ci - f.low_ci, 4)
        ELSE NULL
    END AS ci_width
FROM fct_health_estimate f
JOIN dim_indicator di ON di.indicator_code = f.indicator_code
JOIN dim_country   dc ON dc.country_code   = f.country_code
JOIN dim_sex       ds ON ds.sex_code       = f.sex_code;

/* ========== Vista 2: KPIs anuales por indicador ========== */
-- Agregacion temporal global: promedio, min, max por indicador y anio
CREATE OR REPLACE VIEW vw_yearly_kpi AS
SELECT
    indicator_code,
    indicator_name,
    unit,
    year_val,
    sex_name,
    COUNT(*)                       AS n_countries,
    ROUND(AVG(metric_value), 2)    AS avg_value,
    ROUND(MIN(metric_value), 2)    AS min_value,
    ROUND(MAX(metric_value), 2)    AS max_value,
    ROUND(STDDEV(metric_value), 2) AS stddev_value
FROM vw_health_enriched
GROUP BY indicator_code, indicator_name, unit, year_val, sex_name;

/* ========== Vista 3: KPIs por region con cuota ========== */
-- Promedio por region y anio, con % de participacion regional
CREATE OR REPLACE VIEW vw_region_yearly_kpi AS
SELECT
    t.indicator_code,
    t.indicator_name,
    t.year_val,
    t.region_name,
    t.n_countries,
    t.avg_value,
    ROUND(
        100 * t.avg_value
        / NULLIF(SUM(t.avg_value) OVER (PARTITION BY t.indicator_code, t.year_val), 0),
        2
    ) AS region_share_pct
FROM (
    SELECT
        indicator_code,
        indicator_name,
        year_val,
        region_name,
        COUNT(DISTINCT country_code) AS n_countries,
        ROUND(AVG(metric_value), 2)  AS avg_value
    FROM vw_health_enriched
    WHERE sex_name = 'Both sexes'
    GROUP BY indicator_code, indicator_name, year_val, region_name
) t;

/* ========== Vista 4: NCD Deaths enriquecida ========== */
-- Une la fact de muertes con todas sus dimensiones
CREATE OR REPLACE VIEW vw_deaths_enriched AS
SELECT
    f.fact_id,
    f.cause_code,
    ca.cause_name,
    f.country_code,
    dc.country_name,
    dc.region_code,
    dc.region_name,
    f.year_val,
    f.sex_code,
    ds.sex_name,
    f.death_count,
    f.low_ci,
    f.high_ci
FROM fct_ncd_deaths f
JOIN dim_cause   ca ON ca.cause_code   = f.cause_code
JOIN dim_country dc ON dc.country_code = f.country_code
JOIN dim_sex     ds ON ds.sex_code     = f.sex_code;

/* ========== Vista 5: Muertes anuales por causa (global) ========== */
CREATE OR REPLACE VIEW vw_yearly_deaths_by_cause AS
SELECT
    cause_code,
    cause_name,
    year_val,
    SUM(death_count)                    AS total_deaths,
    COUNT(DISTINCT country_code)        AS n_countries,
    ROUND(AVG(death_count), 0)          AS avg_deaths_per_country
FROM vw_deaths_enriched
WHERE sex_name = 'Both sexes'
GROUP BY cause_code, cause_name, year_val;

/* ========== Vista 6: Communicable deaths enriquecida ========== */
CREATE OR REPLACE VIEW vw_communicable_enriched AS
SELECT
    f.fact_id,
    f.disease_code,
    dd.disease_name,
    f.country_code,
    dc.country_name,
    dc.region_code,
    dc.region_name,
    f.year_val,
    f.death_count,
    f.low_ci,
    f.high_ci
FROM fct_communicable_deaths f
JOIN dim_disease dd ON dd.disease_code = f.disease_code
JOIN dim_country dc ON dc.country_code = f.country_code;
