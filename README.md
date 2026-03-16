# WHO Global Health Estimates — SQL Final Project

## Motor SQL
**MySQL 8.0+**

## Dataset
**WHO Global Health Observatory (GHO)** — Three complementary datasets:

### 1. Health Estimate Indicators
| Indicator Code | Description | Unit |
|---|---|---|
| WHOSIS_000001 | Life expectancy at birth | years |
| WHOSIS_000002 | Healthy life expectancy (HALE) at birth | years |
| WHOSIS_000004 | Adult mortality rate (15-60) | per 1000 population |
| WHOSIS_000015 | Life expectancy at age 60 | years |

### 2. NCD Deaths by Cause (SDG_SH_DTH_RNCOM)
| Cause Code | Disease Category |
|---|---|
| GHE061 | Malignant neoplasms (cancers) |
| GHE080 | Diabetes mellitus |
| GHE110 | Cardiovascular diseases |
| GHE117 | Chronic respiratory diseases |

### 3. Communicable Disease Deaths
| Indicator Code | Disease |
|---|---|
| TB_e_mort_exc_tbhiv_num | Tuberculosis |
| MALARIA_EST_DEATHS | Malaria |
| HIV_0000000006 | HIV/AIDS |
| HEPATITIS_HBV_DEATHS_NUM | Hepatitis B |
| HEPATITIS_HCV_DEATHS_NUM | Hepatitis C |

**Source**: [WHO GHO OData API](https://ghoapi.azureedge.net/api/)
**Volume**: ~51,700 + ~46,500 + ~13,000 raw rows → **~103,700 country-level fact rows**
**Coverage**: 228 countries, 6 WHO regions, years 2000–2024 (varies by indicator), 3 sex categories (NCD), total population (communicable)

## Pregunta guia
> *"How has life expectancy and the burden of disease mortality evolved globally? What are the most lethal diseases — both communicable and non-communicable — per country, and how many people die from them?"*

## Preguntas de negocio
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

## Modelo de datos

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

**Fact grains**:
- `fct_health_estimate`: One row per (indicator, country, year, sex)
- `fct_ncd_deaths`: One row per (NCD cause, country, year, sex)
- `fct_communicable_deaths`: One row per (disease, country, year)

## Estructura del repositorio
```
sql_final_work/
  data/
    download_who_data.py          # Script Python para descargar datos de la API
    health_estimates_raw.csv      # Datos crudos de indicadores (~51K rows)
    ncd_deaths_raw.csv            # Muertes NCD por causa (~46K rows)
    communicable_deaths_raw.csv   # Muertes comunicables (~13K rows)
    countries.csv                 # Dimension de paises (234 rows)
    regions.csv                   # Dimension de regiones (43 rows)
  sql/
    01_schema.sql                 # Definicion de tablas (staging + core)
    02_load_staging.sql           # Validacion post-carga
    03_transform_core.sql         # ETL staging → core
    04_semantic_views.sql         # 6 vistas de negocio
    05_analysis_queries.sql       # 20 consultas analiticas
    06_quality_checks.sql         # Checklist de calidad
    07_advanced_sql.sql           # FUNCTION, PROCEDURE, TRIGGER
  PROJECT_BRIEF.md
  README.md
```

## Instrucciones de reproduccion

### Prerequisitos
- Python 3.x (para descargar datos)
- MySQL 8.0+ (servidor activo)
- DBeaver u otra herramienta SQL (para importar CSV)

### Pasos

1. **Descargar datos** (requiere conexion a internet):
   ```bash
   python3 data/download_who_data.py
   ```

2. **Crear schema** — ejecutar en MySQL:
   ```
   sql/01_schema.sql
   ```

3. **Importar CSV a staging** con DBeaver Import Wizard:
   - `data/health_estimates_raw.csv` → `stg_health_estimates_raw`
   - `data/ncd_deaths_raw.csv` → `stg_ncd_deaths_raw`
   - `data/communicable_deaths_raw.csv` → `stg_communicable_deaths_raw`
   - `data/countries.csv` → `stg_countries_raw`

4. **Validar staging**:
   ```
   sql/02_load_staging.sql
   ```

5. **Transformar a core**:
   ```
   sql/03_transform_core.sql
   ```

6. **Crear vistas semanticas**:
   ```
   sql/04_semantic_views.sql
   ```

7. **Validar calidad**:
   ```
   sql/06_quality_checks.sql
   ```

8. **Ejecutar consultas analiticas**:
   ```
   sql/05_analysis_queries.sql
   ```

9. **SQL avanzado** (funciones, procedimiento, trigger):
   ```
   sql/07_advanced_sql.sql
   ```

## Supuestos
- Solo se cargan filas de tipo `COUNTRY` (se excluyen agregados regionales y globales de la API).
- Los intervalos de confianza (`low_ci`, `high_ci`) pueden ser NULL — la API no los proporciona para todos los indicadores/anios.
- Health estimates cubren 2000–2021; NCD deaths cubren 2000–2019; communicable deaths varian por enfermedad (2000–2024).
- El indicador de mortalidad adulta mide probabilidad de muerte entre 15 y 60 anios por cada 1000 habitantes.
- NCD deaths solo cubre las 4 principales causas de muerte no transmisible.
- Communicable deaths no tienen desglose por sexo (solo totales por pais/anio).
- Hepatitis B y C solo tienen datos para 2022.

## Limitaciones
- **No hay datos de poblacion**: no se pueden calcular tasas de mortalidad per capita, solo numeros absolutos de muertes.
- **Granularidad temporal**: solo anual (no mensual ni trimestral).
- **Sin desglose por edad** en estos indicadores (solo por sexo para NCD, ni sexo para communicable).
- **Cobertura desigual**: Hepatitis tiene solo 1 anio (2022); TB/Malaria/HIV tienen 20+ anios.
- **Datos estimados**: los valores de la OMS son estimaciones modeladas, no registros directos.

## Checklist de calidad
- [x] Sin nulos en columnas criticas de las 3 facts
- [x] Sin filas huerfanas (integridad referencial garantizada por FK)
- [x] Sin duplicados de clave de negocio
- [x] Rango de anios correcto por indicador
- [x] Sin valores negativos de esperanza de vida ni muertes
- [x] Intervalos de confianza invertidos detectados y corregidos
- [x] Consistencia HALE <= Life Expectancy verificada

## SQL avanzado implementado
| Tipo | Nombre | Descripcion |
|---|---|---|
| FUNCTION | `fn_safe_pct` | Porcentaje seguro (evita division por cero) |
| FUNCTION | `fn_ci_width` | Ancho del intervalo de confianza |
| PROCEDURE | `sp_refresh_core` | Recarga completa staging → core en transaccion (3 facts) |
| TRIGGER | `trg_fct_health_bi_validate` | Valida metric_value y year_val antes de INSERT |
