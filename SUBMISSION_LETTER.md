# Project Submission — SQL Final Project

**To:** Nicolás Stambolsky
**From:** Dan Ribes
**Date:** 16 March 2026
**Subject:** SQL Final Project — WHO Global Health Estimates

---

Dear Nicolás,

I am pleased to submit my SQL final project for your review. The project is available in the following GitHub repository:

**https://github.com/danribes/who-global-health-sql-project**

## Project Summary

The project builds a reproducible SQL data warehouse using data from the **World Health Organization (WHO) Global Health Observatory**. It analyses life expectancy trends, non-communicable disease (NCD) mortality, and communicable disease deaths across 228 countries over the period 2000–2024.

The central research question is: *"How has life expectancy and the burden of disease mortality evolved globally? What are the most lethal diseases — both communicable and non-communicable — per country, and how many people die from them?"*

## Technical Details

- **SQL Engine:** MySQL 8.0+
- **Data Source:** WHO GHO OData API (open, no authentication required)
- **Total Data Volume:** ~103,700 country-level fact rows across 3 fact tables

## What is Included

| Deliverable | Description |
|---|---|
| `sql/01_schema.sql` – `07_advanced_sql.sql` | 7 SQL scripts, numbered in execution order |
| `data/` | Raw CSV files + Python download script for reproducibility |
| `README.md` | Comprehensive documentation (dataset, model, ETL, queries, findings, limitations, reproduction instructions) |
| `COMPLIANCE_REPORT.md` | Verification that all project requirements are met |
| `VISUAL_REPORT.html` + `charts/` | Visual report with 20 charts, one per analytical query |

## Key Highlights

- **3-layer architecture**: 4 staging tables → 8 core tables (5 dimensions + 3 facts) → 6 semantic views
- **20 analytical queries** (8–12 required), including CTEs, window functions, cross-fact JOINs, and UNION ALL across heterogeneous datasets
- **10 data quality check sections** with documented detection and correction of issues
- **Advanced SQL**: 2 functions, 1 stored procedure with transactional error handling, 1 BEFORE INSERT trigger
- **Cross-dataset analysis** comparing NCD and communicable disease deaths to demonstrate the global epidemiological transition

## Reproduction Instructions

Full step-by-step instructions are provided in the README. In summary:

1. Run `python3 data/download_who_data.py` to fetch data from the WHO API
2. Execute the 7 SQL scripts in order (01–07) in MySQL

All data files are included in the repository, so the project can also be reproduced without re-downloading from the API.

---

Please do not hesitate to contact me if you have any questions or need any clarification about the project.

Kind regards,

Dan Ribes
