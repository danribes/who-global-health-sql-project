/* ============================================================
   WHO Global Health Estimates — 07_advanced_sql.sql
   SQL avanzado: FUNCTION, PROCEDURE, TRIGGER
   Motor: MySQL
   ============================================================ */

/* OBJETIVO
   - Demostrar uso de SQL avanzado mas alla del SELECT basico
   - Incluir FUNCTION, PROCEDURE y TRIGGER compatibles con MySQL
   - Incluir smoke tests para verificar que funcionan
*/

USE who_disease_burden;

/* ========== FUNCTION: fn_safe_pct ========== */
/* Calcula un porcentaje de forma segura (evita division por cero).
   Uso: SELECT fn_safe_pct(25, 200) → 12.50
*/

DELIMITER $$

DROP FUNCTION IF EXISTS fn_safe_pct$$
CREATE FUNCTION fn_safe_pct(p_num DECIMAL(18,6), p_den DECIMAL(18,6))
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    IF p_den IS NULL OR p_den = 0 THEN
        RETURN NULL;
    END IF;
    RETURN ROUND(100 * p_num / p_den, 2);
END$$

DELIMITER ;

/* ========== FUNCTION: fn_ci_width ========== */
/* Calcula el ancho del intervalo de confianza.
   Si no hay IC, devuelve NULL.
   Uso: SELECT fn_ci_width(70.5, 72.3) → 1.80
*/

DELIMITER $$

DROP FUNCTION IF EXISTS fn_ci_width$$
CREATE FUNCTION fn_ci_width(p_low DECIMAL(18,6), p_high DECIMAL(18,6))
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    IF p_low IS NULL OR p_high IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN ROUND(p_high - p_low, 2);
END$$

DELIMITER ;

/* ========== PROCEDURE: sp_refresh_core ========== */
/* Re-ejecuta la carga completa de staging a core dentro de una transaccion.
   Parametro: p_verbose (BOOLEAN) - si TRUE, muestra conteos intermedios.
*/

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_refresh_core$$
CREATE PROCEDURE sp_refresh_core(IN p_verbose BOOLEAN)
BEGIN
    DECLARE v_n_countries BIGINT DEFAULT 0;
    DECLARE v_n_facts     BIGINT DEFAULT 0;
    DECLARE v_n_deaths    BIGINT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Error en sp_refresh_core: transaccion revertida';
    END;

    START TRANSACTION;

    /* Limpiar core */
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

    /* Cargar dimensiones estaticas */
    INSERT INTO dim_sex (sex_code, sex_name) VALUES
        ('SEX_BTSX', 'Both sexes'),
        ('SEX_MLE',  'Male'),
        ('SEX_FMLE', 'Female');

    INSERT INTO dim_indicator (indicator_code, indicator_name, unit) VALUES
        ('WHOSIS_000001', 'Life expectancy at birth',                'years'),
        ('WHOSIS_000002', 'Healthy life expectancy (HALE) at birth', 'years'),
        ('WHOSIS_000004', 'Adult mortality rate (15-60)',             'per 1000 population'),
        ('WHOSIS_000015', 'Life expectancy at age 60',               'years');

    /* Cargar dim_country */
    INSERT INTO dim_country (country_code, country_name, region_code, region_name)
    SELECT DISTINCT
        REPLACE(TRIM(raw_country_code), '\r', ''),
        REPLACE(TRIM(raw_country_name), '\r', ''),
        REPLACE(TRIM(raw_region_code),  '\r', ''),
        REPLACE(TRIM(raw_region_name),  '\r', '')
    FROM stg_countries_raw
    WHERE TRIM(IFNULL(raw_country_code, '')) <> ''
      AND TRIM(IFNULL(raw_region_code, ''))  <> ''
      AND TRIM(IFNULL(raw_region_name, ''))  <> '';

    SET v_n_countries = (SELECT COUNT(*) FROM dim_country);

    /* Cargar fact — paso 1: metricas principales */
    INSERT INTO fct_health_estimate (
        indicator_code, country_code, year_val, sex_code, metric_value
    )
    SELECT
        TRIM(e.raw_indicator_code),
        TRIM(e.raw_country_code),
        CAST(TRIM(e.raw_year) AS UNSIGNED),
        TRIM(e.raw_sex_code),
        CAST(TRIM(e.raw_numeric_value) AS DECIMAL(18,6))
    FROM stg_health_estimates_raw e
    JOIN dim_country   dc ON dc.country_code   = TRIM(e.raw_country_code)
    JOIN dim_indicator di ON di.indicator_code  = TRIM(e.raw_indicator_code)
    JOIN dim_sex       ds ON ds.sex_code        = TRIM(e.raw_sex_code)
    WHERE TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
      AND TRIM(e.raw_year) REGEXP '^[0-9]{4}$'
      AND TRIM(e.raw_numeric_value) REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

    /* Cargar fact — paso 2: intervalos de confianza */
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

    /* Cargar dim_cause */
    INSERT INTO dim_cause (cause_code, cause_name) VALUES
        ('GHECAUSES_GHE061', 'Malignant neoplasms (cancers)'),
        ('GHECAUSES_GHE080', 'Diabetes mellitus'),
        ('GHECAUSES_GHE110', 'Cardiovascular diseases'),
        ('GHECAUSES_GHE117', 'Chronic respiratory diseases');

    /* Cargar fct_ncd_deaths — paso 1 */
    INSERT INTO fct_ncd_deaths (cause_code, country_code, year_val, sex_code, death_count)
    SELECT
        TRIM(e.raw_cause_code),
        TRIM(e.raw_country_code),
        CAST(TRIM(e.raw_year) AS UNSIGNED),
        TRIM(e.raw_sex_code),
        CAST(TRIM(e.raw_numeric_value) AS DECIMAL(18,2))
    FROM stg_ncd_deaths_raw e
    JOIN dim_country dc ON dc.country_code = TRIM(e.raw_country_code)
    JOIN dim_cause   ca ON ca.cause_code   = TRIM(e.raw_cause_code)
    JOIN dim_sex     ds ON ds.sex_code     = TRIM(e.raw_sex_code)
    WHERE TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
      AND TRIM(e.raw_year) REGEXP '^[0-9]{4}$'
      AND TRIM(e.raw_numeric_value) REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

    /* Cargar fct_ncd_deaths — paso 2: intervalos de confianza */
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

    /* Cargar dim_disease */
    INSERT INTO dim_disease (disease_code, disease_name, disease_type) VALUES
        ('TB_e_mort_exc_tbhiv_num',  'Tuberculosis',  'communicable'),
        ('MALARIA_EST_DEATHS',       'Malaria',        'communicable'),
        ('HIV_0000000006',           'HIV/AIDS',        'communicable'),
        ('HEPATITIS_HBV_DEATHS_NUM', 'Hepatitis B',    'communicable'),
        ('HEPATITIS_HCV_DEATHS_NUM', 'Hepatitis C',    'communicable');

    /* Cargar fct_communicable_deaths — paso 1 */
    INSERT INTO fct_communicable_deaths (disease_code, country_code, year_val, death_count)
    SELECT
        TRIM(e.raw_indicator_code),
        TRIM(e.raw_country_code),
        CAST(TRIM(e.raw_year) AS UNSIGNED),
        CAST(TRIM(e.raw_numeric_value) AS DECIMAL(18,2))
    FROM stg_communicable_deaths_raw e
    JOIN dim_country dc ON dc.country_code = TRIM(e.raw_country_code)
    JOIN dim_disease dd ON dd.disease_code = TRIM(e.raw_indicator_code)
    WHERE TRIM(e.raw_spatial_dim_type) = 'COUNTRY'
      AND TRIM(e.raw_year) REGEXP '^[0-9]{4}$'
      AND TRIM(e.raw_numeric_value) REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

    /* Cargar fct_communicable_deaths — paso 2: CI */
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

    SET v_n_facts = (SELECT COUNT(*) FROM fct_health_estimate);
    SET v_n_deaths = (SELECT COUNT(*) FROM fct_ncd_deaths)
                   + (SELECT COUNT(*) FROM fct_communicable_deaths);

    COMMIT;

    IF p_verbose THEN
        SELECT v_n_countries AS countries_loaded, v_n_facts AS facts_loaded, v_n_deaths AS deaths_loaded;
    END IF;
END$$

DELIMITER ;

/* ========== TRIGGER: Validacion BEFORE INSERT ========== */
/* Valida que:
   - metric_value no sea negativo para indicadores de esperanza de vida
   - year_val este en rango valido (1900-2100)
*/

DELIMITER $$

DROP TRIGGER IF EXISTS trg_fct_health_bi_validate$$
CREATE TRIGGER trg_fct_health_bi_validate
BEFORE INSERT ON fct_health_estimate
FOR EACH ROW
BEGIN
    /* Esperanza de vida no puede ser negativa */
    IF NEW.indicator_code IN ('WHOSIS_000001', 'WHOSIS_000002', 'WHOSIS_000015')
       AND NEW.metric_value < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'metric_value no puede ser negativo para indicadores de esperanza de vida';
    END IF;

    /* Anio fuera de rango razonable */
    IF NEW.year_val < 1900 OR NEW.year_val > 2100 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'year_val fuera de rango (1900-2100)';
    END IF;
END$$

DELIMITER ;

/* ========== SMOKE TESTS ========== */

-- Test 1: fn_safe_pct
SELECT
    fn_safe_pct(25, 200)   AS pct_normal,    -- 12.50
    fn_safe_pct(100, 0)    AS pct_div_zero,  -- NULL
    fn_safe_pct(NULL, 100) AS pct_null_num;  -- NULL

-- Test 2: fn_ci_width
SELECT
    fn_ci_width(70.5, 72.3) AS ci_normal,   -- 1.80
    fn_ci_width(NULL, 72.3)  AS ci_null;    -- NULL

-- Test 3: sp_refresh_core (verbose mode)
-- NOTA: esto recarga toda la fact, solo ejecutar si se quiere resetear
-- CALL sp_refresh_core(TRUE);

-- Test 4: Verificar trigger — intentar insertar un valor invalido
-- (descomentar para probar, dara error esperado)
-- INSERT INTO fct_health_estimate (indicator_code, country_code, year_val, sex_code, metric_value)
-- VALUES ('WHOSIS_000001', 'ESP', 2021, 'SEX_BTSX', -5.0);

-- Test 5: Usar fn_safe_pct con datos reales
SELECT
    dc.region_name,
    ROUND(AVG(f.metric_value), 2) AS avg_le,
    fn_safe_pct(
        AVG(f.metric_value),
        (SELECT AVG(metric_value) FROM fct_health_estimate
         WHERE indicator_code = 'WHOSIS_000001' AND sex_code = 'SEX_BTSX' AND year_val = 2021)
    ) AS pct_of_global_avg
FROM fct_health_estimate f
JOIN dim_country dc ON dc.country_code = f.country_code
WHERE f.indicator_code = 'WHOSIS_000001'
  AND f.sex_code = 'SEX_BTSX'
  AND f.year_val = 2021
GROUP BY dc.region_name
ORDER BY avg_le DESC;
