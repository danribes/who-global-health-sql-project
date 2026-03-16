/* ============================================================
   WHO Global Health Estimates — 01_schema.sql
   Database schema: staging (raw text) + core (typed, modeled)
   Motor: MySQL
   ============================================================ */

/* OBJETIVO
   - Crear la base de datos y todas las tablas
   - Staging: datos crudos importados desde CSV (todo VARCHAR)
   - Core: tablas limpias con tipos correctos y claves foraneas

   GRANOS
   - fct_health_estimate: una fila por (indicador, pais, anio, sexo)
   - fct_ncd_deaths: una fila por (causa NCD, pais, anio, sexo)
   - fct_communicable_deaths: una fila por (enfermedad, pais, anio)
*/

CREATE DATABASE IF NOT EXISTS who_disease_burden
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE who_disease_burden;

/* ========== STAGING (raw, todo texto) ========== */

DROP TABLE IF EXISTS stg_health_estimates_raw;
DROP TABLE IF EXISTS stg_ncd_deaths_raw;
DROP TABLE IF EXISTS stg_communicable_deaths_raw;
DROP TABLE IF EXISTS stg_countries_raw;

CREATE TABLE stg_health_estimates_raw (
    raw_indicator_code   VARCHAR(50),
    raw_country_code     VARCHAR(20),
    raw_spatial_dim_type VARCHAR(50),
    raw_year             VARCHAR(20),
    raw_sex_code         VARCHAR(20),
    raw_numeric_value    VARCHAR(50),
    raw_low              VARCHAR(50),
    raw_high             VARCHAR(50),
    source_file          VARCHAR(120) DEFAULT 'health_estimates_raw.csv',
    ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_ncd_deaths_raw (
    raw_indicator_code   VARCHAR(50),
    raw_country_code     VARCHAR(20),
    raw_spatial_dim_type VARCHAR(50),
    raw_year             VARCHAR(20),
    raw_sex_code         VARCHAR(20),
    raw_cause_code       VARCHAR(50),
    raw_numeric_value    VARCHAR(50),
    raw_low              VARCHAR(50),
    raw_high             VARCHAR(50),
    source_file          VARCHAR(120) DEFAULT 'ncd_deaths_raw.csv',
    ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_communicable_deaths_raw (
    raw_indicator_code   VARCHAR(50),
    raw_disease_name     VARCHAR(100),
    raw_country_code     VARCHAR(20),
    raw_spatial_dim_type VARCHAR(50),
    raw_year             VARCHAR(20),
    raw_numeric_value    VARCHAR(50),
    raw_low              VARCHAR(50),
    raw_high             VARCHAR(50),
    source_file          VARCHAR(120) DEFAULT 'communicable_deaths_raw.csv',
    ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_countries_raw (
    raw_country_code  VARCHAR(20),
    raw_country_name  VARCHAR(200),
    raw_region_code   VARCHAR(20),
    raw_region_name   VARCHAR(100),
    source_file       VARCHAR(120) DEFAULT 'countries.csv',
    ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/* ========== CORE (clean, typed) ========== */

DROP TABLE IF EXISTS fct_communicable_deaths;
DROP TABLE IF EXISTS fct_ncd_deaths;
DROP TABLE IF EXISTS fct_health_estimate;
DROP TABLE IF EXISTS dim_disease;
DROP TABLE IF EXISTS dim_cause;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_indicator;
DROP TABLE IF EXISTS dim_sex;

/* --- Dimension: Country --- */
CREATE TABLE dim_country (
    country_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(200) NOT NULL,
    region_code  VARCHAR(10) NOT NULL,
    region_name  VARCHAR(100) NOT NULL
);

/* --- Dimension: Indicator --- */
CREATE TABLE dim_indicator (
    indicator_code VARCHAR(20) PRIMARY KEY,
    indicator_name VARCHAR(200) NOT NULL,
    unit           VARCHAR(50) NOT NULL
);

/* --- Dimension: Sex --- */
CREATE TABLE dim_sex (
    sex_code VARCHAR(10) PRIMARY KEY,
    sex_name VARCHAR(20) NOT NULL
);

/* --- Dimension: NCD Cause of Death --- */
CREATE TABLE dim_cause (
    cause_code VARCHAR(30) PRIMARY KEY,
    cause_name VARCHAR(100) NOT NULL
);

/* --- Dimension: Communicable Disease --- */
CREATE TABLE dim_disease (
    disease_code VARCHAR(50) PRIMARY KEY,
    disease_name VARCHAR(100) NOT NULL,
    disease_type VARCHAR(20) NOT NULL DEFAULT 'communicable'
);

/* --- Fact: Health Estimates --- */
CREATE TABLE fct_health_estimate (
    fact_id        BIGINT PRIMARY KEY AUTO_INCREMENT,
    indicator_code VARCHAR(20)    NOT NULL,
    country_code   VARCHAR(10)    NOT NULL,
    year_val       SMALLINT       NOT NULL,
    sex_code       VARCHAR(10)    NOT NULL,
    metric_value   DECIMAL(18,6)  NOT NULL,
    low_ci         DECIMAL(18,6)  NULL,
    high_ci        DECIMAL(18,6)  NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (indicator_code) REFERENCES dim_indicator(indicator_code),
    FOREIGN KEY (country_code)   REFERENCES dim_country(country_code),
    FOREIGN KEY (sex_code)       REFERENCES dim_sex(sex_code),

    INDEX idx_year       (year_val),
    INDEX idx_country    (country_code),
    INDEX idx_indicator  (indicator_code),
    INDEX idx_sex        (sex_code)
);

/* --- Fact: NCD Deaths by Cause --- */
CREATE TABLE fct_ncd_deaths (
    fact_id      BIGINT PRIMARY KEY AUTO_INCREMENT,
    cause_code   VARCHAR(30)    NOT NULL,
    country_code VARCHAR(10)    NOT NULL,
    year_val     SMALLINT       NOT NULL,
    sex_code     VARCHAR(10)    NOT NULL,
    death_count  DECIMAL(18,2)  NOT NULL,
    low_ci       DECIMAL(18,2)  NULL,
    high_ci      DECIMAL(18,2)  NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (cause_code)   REFERENCES dim_cause(cause_code),
    FOREIGN KEY (country_code) REFERENCES dim_country(country_code),
    FOREIGN KEY (sex_code)     REFERENCES dim_sex(sex_code),

    INDEX idx_deaths_year    (year_val),
    INDEX idx_deaths_country (country_code),
    INDEX idx_deaths_cause   (cause_code),
    INDEX idx_deaths_sex     (sex_code)
);

/* --- Fact: Communicable Disease Deaths --- */
CREATE TABLE fct_communicable_deaths (
    fact_id       BIGINT PRIMARY KEY AUTO_INCREMENT,
    disease_code  VARCHAR(50)    NOT NULL,
    country_code  VARCHAR(10)    NOT NULL,
    year_val      SMALLINT       NOT NULL,
    death_count   DECIMAL(18,2)  NOT NULL,
    low_ci        DECIMAL(18,2)  NULL,
    high_ci       DECIMAL(18,2)  NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (disease_code)  REFERENCES dim_disease(disease_code),
    FOREIGN KEY (country_code)  REFERENCES dim_country(country_code),

    INDEX idx_comm_year     (year_val),
    INDEX idx_comm_country  (country_code),
    INDEX idx_comm_disease  (disease_code)
);
