"""
Generate visual charts for all 20 analytical queries.
Produces individual PNGs in charts/ and an HTML report (VISUAL_REPORT.html).

Usage:
    python3 generate_charts.py
"""

import os
import pymysql
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# --- Config ---
DB_HOST = "127.0.0.1"
DB_PORT = 3307
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "who_disease_burden"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHART_DIR = os.path.join(BASE_DIR, "charts")
os.makedirs(CHART_DIR, exist_ok=True)

sns.set_theme(style="whitegrid", font_scale=1.1)
PALETTE = sns.color_palette("Set2", 10)


def get_conn():
    return pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER,
                           password=DB_PASS, database=DB_NAME)


def query_df(sql):
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def save(fig, name):
    path = os.path.join(CHART_DIR, f"{name}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {name}.png")
    return path


# ============================================================
# CHARTS
# ============================================================

charts = []  # (filename, title, description)


def q01():
    """Q1: Global life expectancy trend"""
    df = query_df("""
        SELECT year_val, ROUND(AVG(metric_value),2) AS avg_le
        FROM vw_health_enriched
        WHERE indicator_code='WHOSIS_000001' AND sex_name='Both sexes'
        GROUP BY year_val ORDER BY year_val
    """)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["year_val"], df["avg_le"], marker="o", linewidth=2.5, color=PALETTE[0])
    ax.fill_between(df["year_val"], df["avg_le"], alpha=0.15, color=PALETTE[0])
    ax.set_title("Q1: Global Average Life Expectancy at Birth (2000–2021)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Life Expectancy (years)")
    ax.set_ylim(65, 74)
    for yr in [2019, 2020, 2021]:
        row = df[df["year_val"] == yr].iloc[0]
        ax.annotate(f'{row["avg_le"]:.1f}', (yr, row["avg_le"]),
                    textcoords="offset points", xytext=(0, 12), ha="center", fontsize=9)
    charts.append(("q01", "Global Life Expectancy Trend",
                   "Steady increase from 67.0 (2000) to 72.6 (2019), then a COVID-19-driven decline to 71.3 (2021)."))
    return save(fig, "q01")


def q02():
    """Q2: Year-over-year % change"""
    df = query_df("""
        SELECT year_val, avg_le,
               LAG(avg_le) OVER (ORDER BY year_val) AS prev,
               ROUND(100.0*(avg_le - LAG(avg_le) OVER (ORDER BY year_val))
                     / NULLIF(LAG(avg_le) OVER (ORDER BY year_val),0), 3) AS yoy
        FROM (SELECT year_val, ROUND(AVG(metric_value),4) AS avg_le
              FROM vw_health_enriched
              WHERE indicator_code='WHOSIS_000001' AND sex_name='Both sexes'
              GROUP BY year_val) t ORDER BY year_val
    """)
    df = df.dropna(subset=["yoy"])
    colors = [PALETTE[3] if v < 0 else PALETTE[0] for v in df["yoy"]]
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(df["year_val"], df["yoy"], color=colors, edgecolor="white", linewidth=0.5)
    ax.axhline(0, color="grey", linewidth=0.8)
    ax.set_title("Q2: Year-over-Year Change in Global Life Expectancy (%)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Change (%)")
    charts.append(("q02", "Year-over-Year Life Expectancy Change",
                   "2020 and 2021 are the only years with negative growth — a clear COVID-19 signature."))
    return save(fig, "q02")


def q03():
    """Q3: Top 10 countries by life expectancy"""
    df = query_df("""
        SELECT country_name, ROUND(metric_value,1) AS le
        FROM vw_health_enriched
        WHERE indicator_code='WHOSIS_000001' AND sex_name='Both sexes' AND year_val=2021
        ORDER BY metric_value DESC LIMIT 10
    """)
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(df["country_name"][::-1], df["le"][::-1], color=PALETTE[1])
    ax.set_title("Q3: Top 10 Countries by Life Expectancy (2021)")
    ax.set_xlabel("Life Expectancy (years)")
    ax.set_xlim(80, 86)
    for bar in bars:
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                f'{bar.get_width():.1f}', va="center", fontsize=9)
    charts.append(("q03", "Top 10 Countries by Life Expectancy",
                   "Japan leads at 84.5 years, followed by Singapore and South Korea."))
    return save(fig, "q03")


def q04():
    """Q4: Top 3 per WHO region"""
    df = query_df("""
        WITH ranked AS (
            SELECT region_name, country_name, ROUND(metric_value,1) AS le,
                   ROW_NUMBER() OVER (PARTITION BY region_name ORDER BY metric_value DESC) AS rn
            FROM vw_health_enriched
            WHERE indicator_code='WHOSIS_000001' AND sex_name='Both sexes' AND year_val=2021
        ) SELECT * FROM ranked WHERE rn <= 3 ORDER BY region_name, rn
    """)
    fig, ax = plt.subplots(figsize=(14, 6))
    regions = df["region_name"].unique()
    x = range(len(regions))
    width = 0.25
    for i in range(3):
        subset = df[df["rn"] == i + 1]
        bars = ax.bar([p + i * width for p in x], subset["le"].values, width,
                      label=f"Rank {i+1}", color=PALETTE[i])
        for bar, name in zip(bars, subset["country_name"].values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                    name, ha="center", va="bottom", fontsize=7, rotation=45)
    ax.set_xticks([p + width for p in x])
    ax.set_xticklabels(regions, rotation=20, ha="right")
    ax.set_ylabel("Life Expectancy (years)")
    ax.set_title("Q4: Top 3 Countries per WHO Region — Life Expectancy (2021)")
    ax.legend()
    ax.set_ylim(60, 90)
    charts.append(("q04", "Top 3 per WHO Region",
                   "Switzerland leads Europe, Japan leads Western Pacific, Algeria leads Africa."))
    return save(fig, "q04")


def q05():
    """Q5: Gender gap by region"""
    df = query_df("""
        WITH g AS (
            SELECT region_name, sex_name, ROUND(AVG(metric_value),2) AS avg_le
            FROM vw_health_enriched
            WHERE indicator_code='WHOSIS_000001' AND year_val=2021 AND sex_name<>'Both sexes'
            GROUP BY region_name, sex_name
        )
        SELECT region_name,
               MAX(CASE WHEN sex_name='Female' THEN avg_le END) AS female_le,
               MAX(CASE WHEN sex_name='Male' THEN avg_le END) AS male_le
        FROM g GROUP BY region_name ORDER BY female_le - male_le DESC
    """)
    fig, ax = plt.subplots(figsize=(10, 5))
    y = range(len(df))
    ax.barh(y, df["female_le"], height=0.4, label="Female", color=PALETTE[4], align="center")
    ax.barh([i + 0.4 for i in y], df["male_le"], height=0.4, label="Male", color=PALETTE[0], align="center")
    ax.set_yticks([i + 0.2 for i in y])
    ax.set_yticklabels(df["region_name"])
    ax.set_xlabel("Life Expectancy (years)")
    ax.set_title("Q5: Gender Gap in Life Expectancy by WHO Region (2021)")
    ax.legend()
    for i, row in df.iterrows():
        gap = row["female_le"] - row["male_le"]
        ax.text(row["female_le"] + 0.2, i, f"gap: {gap:.1f}y", va="center", fontsize=8)
    charts.append(("q05", "Gender Gap in Life Expectancy",
                   "Women outlive men in every region. Americas have the widest gap (5.7 years)."))
    return save(fig, "q05")


def q06():
    """Q6: Life expectancy vs adult mortality scatter"""
    df = query_df("""
        SELECT le.country_name, le.region_name,
               ROUND(le.metric_value,1) AS life_expectancy,
               ROUND(mort.metric_value,1) AS adult_mortality
        FROM vw_health_enriched le
        JOIN vw_health_enriched mort ON mort.country_code=le.country_code
             AND mort.year_val=le.year_val AND mort.sex_code=le.sex_code
        WHERE le.indicator_code='WHOSIS_000001' AND mort.indicator_code='WHOSIS_000004'
          AND le.sex_name='Both sexes' AND le.year_val=2021
    """)
    fig, ax = plt.subplots(figsize=(10, 7))
    regions = df["region_name"].unique()
    for i, reg in enumerate(sorted(regions)):
        sub = df[df["region_name"] == reg]
        ax.scatter(sub["life_expectancy"], sub["adult_mortality"],
                   label=reg, alpha=0.7, s=40, color=PALETTE[i % len(PALETTE)])
    ax.set_xlabel("Life Expectancy (years)")
    ax.set_ylabel("Adult Mortality Rate (per 1000)")
    ax.set_title("Q6: Life Expectancy vs Adult Mortality by Country (2021)")
    ax.legend(fontsize=8, loc="upper right")
    charts.append(("q06", "Life Expectancy vs Adult Mortality",
                   "Strong inverse correlation — countries with high life expectancy have low adult mortality."))
    return save(fig, "q06")


def q07():
    """Q7: Regional improvement 2000 vs 2021"""
    df = query_df("""
        SELECT r2021.region_name,
               r2000.avg_le AS le_2000, r2021.avg_le AS le_2021,
               ROUND(r2021.avg_le - r2000.avg_le, 2) AS improvement
        FROM (SELECT region_name, ROUND(AVG(metric_value),2) AS avg_le
              FROM vw_health_enriched WHERE indicator_code='WHOSIS_000001'
              AND sex_name='Both sexes' AND year_val=2021 GROUP BY region_name) r2021
        JOIN (SELECT region_name, ROUND(AVG(metric_value),2) AS avg_le
              FROM vw_health_enriched WHERE indicator_code='WHOSIS_000001'
              AND sex_name='Both sexes' AND year_val=2000 GROUP BY region_name) r2000
        ON r2000.region_name=r2021.region_name ORDER BY improvement DESC
    """)
    fig, ax = plt.subplots(figsize=(10, 5))
    colors = [PALETTE[0] if v > 2 else PALETTE[3] for v in df["improvement"]]
    ax.barh(df["region_name"][::-1], df["improvement"][::-1], color=colors[::-1])
    ax.set_xlabel("Improvement in Life Expectancy (years)")
    ax.set_title("Q7: Life Expectancy Improvement by Region (2000 → 2021)")
    for i, (v, r) in enumerate(zip(df["improvement"][::-1], df["region_name"][::-1])):
        ax.text(v + 0.1, i, f"+{v:.1f}", va="center", fontsize=9)
    charts.append(("q07", "Regional Improvement (2000 vs 2021)",
                   "Africa improved the most (+9.3 years). Americas barely changed (+0.2 years)."))
    return save(fig, "q07")


def q08():
    """Q8: Mortality concentration"""
    df = query_df("""
        WITH cm AS (
            SELECT country_name, ROUND(metric_value,1) AS mort,
                   ROW_NUMBER() OVER (ORDER BY metric_value DESC) AS rn,
                   ROUND(100.0*SUM(metric_value) OVER (ORDER BY metric_value DESC)
                         /NULLIF(SUM(metric_value) OVER (),0),2) AS cum_pct
            FROM vw_health_enriched
            WHERE indicator_code='WHOSIS_000004' AND sex_name='Both sexes' AND year_val=2021
        ) SELECT * FROM cm WHERE rn <= 20
    """)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(range(len(df)), df["mort"], color=PALETTE[3], alpha=0.7, label="Mortality rate")
    ax2 = ax.twinx()
    ax2.plot(range(len(df)), df["cum_pct"], color=PALETTE[0], marker="o", linewidth=2, label="Cumulative %")
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df["country_name"], rotation=60, ha="right", fontsize=8)
    ax.set_ylabel("Adult Mortality Rate (per 1000)")
    ax2.set_ylabel("Cumulative % of Global Total")
    ax.set_title("Q8: Mortality Concentration — Top 20 Countries (2021)")
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    charts.append(("q08", "Mortality Concentration",
                   "The 20 countries with highest adult mortality account for ~22% of the global total."))
    return save(fig, "q08")


def q09():
    """Q9: HALE by region (boxplot)"""
    df = query_df("""
        SELECT region_name, ROUND(metric_value,1) AS hale
        FROM vw_health_enriched
        WHERE indicator_code='WHOSIS_000002' AND sex_name='Both sexes' AND year_val=2021
    """)
    fig, ax = plt.subplots(figsize=(12, 5))
    order = df.groupby("region_name")["hale"].median().sort_values(ascending=False).index.tolist()
    sns.boxplot(data=df, x="region_name", y="hale", order=order, palette="Set2", ax=ax)
    ax.set_title("Q9: Healthy Life Expectancy (HALE) Distribution by Region (2021)")
    ax.set_xlabel("")
    ax.set_ylabel("HALE (years)")
    ax.tick_params(axis="x", rotation=20)
    charts.append(("q09", "HALE Distribution by Region",
                   "Europe has the highest median HALE; Africa shows the widest spread and lowest values."))
    return save(fig, "q09")


def q10():
    """Q10: Countries with declining life expectancy"""
    df = query_df("""
        SELECT e2021.country_name, e2021.region_name,
               ROUND(e2000.metric_value,1) AS le_2000,
               ROUND(e2021.metric_value,1) AS le_2021,
               ROUND(e2021.metric_value - e2000.metric_value,2) AS le_change
        FROM vw_health_enriched e2021
        JOIN vw_health_enriched e2000
            ON e2000.country_code=e2021.country_code
            AND e2000.indicator_code=e2021.indicator_code
            AND e2000.sex_code=e2021.sex_code
        WHERE e2021.indicator_code='WHOSIS_000001' AND e2021.sex_name='Both sexes'
          AND e2021.year_val=2021 AND e2000.year_val=2000
          AND e2021.metric_value < e2000.metric_value
        ORDER BY le_change ASC LIMIT 15
    """)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(df["country_name"][::-1], df["le_change"][::-1], color=PALETTE[3])
    ax.set_xlabel("Change in Life Expectancy (years)")
    ax.set_title("Q10: Countries with Declining Life Expectancy (2000 → 2021)")
    ax.axvline(0, color="grey", linewidth=0.8)
    for i, v in enumerate(df["le_change"][::-1]):
        ax.text(v - 0.15, i, f"{v:.1f}", va="center", ha="right", fontsize=8, color="white")
    charts.append(("q10", "Countries with Declining Life Expectancy",
                   "23 countries saw declines, led by Paraguay (-4.4), Philippines (-3.6), Peru (-3.5) — mostly in the Americas."))
    return save(fig, "q10")


def q11():
    """Q11: NCD death trends by cause"""
    df = query_df("""
        SELECT year_val, cause_name, ROUND(SUM(death_count),0) AS total
        FROM vw_deaths_enriched WHERE sex_name='Both sexes'
        GROUP BY year_val, cause_name ORDER BY year_val
    """)
    fig, ax = plt.subplots(figsize=(12, 6))
    for i, cause in enumerate(df["cause_name"].unique()):
        sub = df[df["cause_name"] == cause]
        ax.plot(sub["year_val"], sub["total"]/1e6, marker=".", linewidth=2, label=cause, color=PALETTE[i])
    ax.set_title("Q11: Global NCD Deaths by Cause (2000–2019)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Deaths (millions)")
    ax.legend(fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1fM"))
    charts.append(("q11", "NCD Death Trends by Cause",
                   "Cardiovascular diseases dominate at ~17.9M deaths/year. All NCD causes are rising."))
    return save(fig, "q11")


def q12():
    """Q12: Most lethal NCD per country (top 20)"""
    df = query_df("""
        WITH ranked AS (
            SELECT country_name, region_name, cause_name,
                   ROUND(death_count,0) AS deaths,
                   ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY death_count DESC) AS rn
            FROM vw_deaths_enriched WHERE sex_name='Both sexes' AND year_val=2019
        ) SELECT * FROM ranked WHERE rn=1 ORDER BY deaths DESC LIMIT 20
    """)
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = [PALETTE[0] if c == "Cardiovascular diseases" else PALETTE[1] for c in df["cause_name"]]
    ax.barh(df["country_name"][::-1], df["deaths"][::-1] / 1e6, color=colors[::-1])
    ax.set_xlabel("Deaths (millions)")
    ax.set_title("Q12: Most Lethal NCD per Country — Top 20 (2019)")
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=PALETTE[0], label="Cardiovascular"),
                       Patch(facecolor=PALETTE[1], label="Cancer")]
    ax.legend(handles=legend_elements, loc="lower right")
    charts.append(("q12", "Most Lethal NCD per Country",
                   "Cardiovascular diseases are the #1 NCD killer in almost every country. Japan and France are exceptions (cancer leads)."))
    return save(fig, "q12")


def q13():
    """Q13: NCD death share by region"""
    df = query_df("""
        WITH rc AS (
            SELECT region_name, cause_name, ROUND(SUM(death_count),0) AS total
            FROM vw_deaths_enriched WHERE sex_name='Both sexes' AND year_val=2019
            GROUP BY region_name, cause_name
        ) SELECT region_name, cause_name, total,
          ROUND(100.0*total/NULLIF(SUM(total) OVER (PARTITION BY region_name),0),1) AS pct
        FROM rc ORDER BY region_name
    """)
    fig, ax = plt.subplots(figsize=(12, 6))
    pivot = df.pivot(index="region_name", columns="cause_name", values="pct").fillna(0)
    pivot.plot(kind="bar", stacked=True, ax=ax, color=PALETTE[:4])
    ax.set_title("Q13: NCD Death Distribution by Cause per Region (2019)")
    ax.set_ylabel("Share of NCD Deaths (%)")
    ax.set_xlabel("")
    ax.tick_params(axis="x", rotation=20)
    ax.legend(fontsize=8, loc="upper right")
    charts.append(("q13", "NCD Death Share by Region",
                   "Eastern Mediterranean has the highest cardiovascular share (64%). South-East Asia has unusually high respiratory disease share (21%)."))
    return save(fig, "q13")


def q14():
    """Q14: Life expectancy vs cardiovascular deaths"""
    df = query_df("""
        SELECT le.country_name, le.region_name,
               ROUND(le.metric_value,1) AS life_expectancy,
               ROUND(d.death_count,0) AS cv_deaths
        FROM vw_health_enriched le
        JOIN vw_deaths_enriched d ON d.country_code=le.country_code
             AND d.year_val=le.year_val AND d.sex_code=le.sex_code
        WHERE le.indicator_code='WHOSIS_000001' AND le.sex_name='Both sexes'
          AND le.year_val=2019 AND d.cause_name='Cardiovascular diseases'
    """)
    fig, ax = plt.subplots(figsize=(10, 7))
    regions = sorted(df["region_name"].unique())
    for i, reg in enumerate(regions):
        sub = df[df["region_name"] == reg]
        ax.scatter(sub["life_expectancy"], sub["cv_deaths"]/1e6, label=reg,
                   alpha=0.7, s=40, color=PALETTE[i % len(PALETTE)])
    ax.set_xlabel("Life Expectancy (years)")
    ax.set_ylabel("Cardiovascular Deaths (millions)")
    ax.set_title("Q14: Life Expectancy vs Cardiovascular Deaths (2019)")
    ax.legend(fontsize=8)
    for _, row in df.nlargest(5, "cv_deaths").iterrows():
        ax.annotate(row["country_name"], (row["life_expectancy"], row["cv_deaths"]/1e6),
                    fontsize=7, alpha=0.8)
    charts.append(("q14", "Life Expectancy vs Cardiovascular Deaths",
                   "Absolute death counts are driven by population size — China and India dominate regardless of life expectancy."))
    return save(fig, "q14")


def q15():
    """Q15: Communicable disease death trends"""
    df = query_df("""
        SELECT year_val, disease_name,
               ROUND(SUM(death_count),0) AS total_deaths
        FROM vw_communicable_enriched
        GROUP BY year_val, disease_name ORDER BY year_val
    """)
    fig, ax = plt.subplots(figsize=(12, 6))
    for i, disease in enumerate(sorted(df["disease_name"].unique())):
        sub = df[df["disease_name"] == disease]
        ax.plot(sub["year_val"], sub["total_deaths"]/1e6, marker=".", linewidth=2,
                label=disease, color=PALETTE[i])
    ax.set_title("Q15: Global Communicable Disease Deaths Over Time")
    ax.set_xlabel("Year")
    ax.set_ylabel("Deaths (millions)")
    ax.legend(fontsize=9)
    charts.append(("q15", "Communicable Disease Death Trends",
                   "TB remains the deadliest communicable disease. HIV/AIDS deaths have declined significantly since ~2005. Hepatitis data only available for 2022."))
    return save(fig, "q15")


def q16():
    """Q16: Top 5 countries per communicable disease"""
    df = query_df("""
        WITH latest AS (
            SELECT disease_code, MAX(year_val) AS max_year
            FROM fct_communicable_deaths GROUP BY disease_code
        ),
        ranked AS (
            SELECT dd.disease_name, dc.country_name, f.year_val,
                   ROUND(f.death_count,0) AS deaths,
                   ROW_NUMBER() OVER (PARTITION BY f.disease_code ORDER BY f.death_count DESC) AS rn
            FROM fct_communicable_deaths f
            JOIN dim_disease dd ON dd.disease_code=f.disease_code
            JOIN dim_country dc ON dc.country_code=f.country_code
            JOIN latest l ON l.disease_code=f.disease_code AND l.max_year=f.year_val
        ) SELECT * FROM ranked WHERE rn<=5 ORDER BY disease_name, rn
    """)
    diseases = df["disease_name"].unique()
    n = len(diseases)
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    axes = axes.flatten()
    for i, disease in enumerate(sorted(diseases)):
        sub = df[df["disease_name"] == disease]
        axes[i].barh(sub["country_name"][::-1], sub["deaths"][::-1] / 1e3, color=PALETTE[i])
        axes[i].set_title(disease, fontsize=10, fontweight="bold")
        axes[i].set_xlabel("Deaths (thousands)")
    for j in range(n, len(axes)):
        axes[j].set_visible(False)
    fig.suptitle("Q16: Top 5 Countries per Communicable Disease (Latest Year)", fontsize=13, y=1.01)
    fig.tight_layout()
    charts.append(("q16", "Top 5 Countries per Communicable Disease",
                   "India leads in TB (300K), Nigeria in Malaria (185K), South Africa in HIV/AIDS (53K)."))
    return save(fig, "q16")


def q17():
    """Q17: NCD vs Communicable by region"""
    df = query_df("""
        WITH ncd AS (
            SELECT dc.region_name, 'NCD' AS dtype, ROUND(SUM(f.death_count),0) AS deaths
            FROM fct_ncd_deaths f JOIN dim_country dc ON dc.country_code=f.country_code
            WHERE f.sex_code='SEX_BTSX' AND f.year_val=2019 GROUP BY dc.region_name
        ), comm AS (
            SELECT dc.region_name, 'Communicable' AS dtype, ROUND(SUM(f.death_count),0) AS deaths
            FROM fct_communicable_deaths f JOIN dim_country dc ON dc.country_code=f.country_code
            WHERE f.year_val=2019 GROUP BY dc.region_name
        ) SELECT * FROM ncd UNION ALL SELECT * FROM comm
    """)
    pivot = df.pivot(index="region_name", columns="dtype", values="deaths").fillna(0)
    pivot["total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("total", ascending=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(pivot.index, pivot["NCD"]/1e6, label="NCD", color=PALETTE[0])
    ax.barh(pivot.index, pivot["Communicable"]/1e6, left=pivot["NCD"]/1e6,
            label="Communicable", color=PALETTE[3])
    ax.set_xlabel("Deaths (millions)")
    ax.set_title("Q17: NCD vs Communicable Disease Deaths by Region (2019)")
    ax.legend()
    for i, (region, row) in enumerate(pivot.iterrows()):
        pct_comm = 100 * row["Communicable"] / row["total"] if row["total"] > 0 else 0
        if pct_comm > 2:
            ax.text(row["total"]/1e6 + 0.1, i, f"{pct_comm:.0f}% comm.", va="center", fontsize=8)
    charts.append(("q17", "NCD vs Communicable Deaths by Region",
                   "Africa is the only region where communicable diseases still represent 42.5% of deaths. Europe: 99.6% NCD."))
    return save(fig, "q17")


def q18():
    """Q18: Epidemiological transition"""
    df = query_df("""
        WITH d AS (
            SELECT dc.region_name, f.year_val, 'NCD' AS dtype, ROUND(SUM(f.death_count),0) AS deaths
            FROM fct_ncd_deaths f JOIN dim_country dc ON dc.country_code=f.country_code
            WHERE f.sex_code='SEX_BTSX' AND f.year_val IN (2000,2019) GROUP BY dc.region_name, f.year_val
            UNION ALL
            SELECT dc.region_name, f.year_val, 'Communicable', ROUND(SUM(f.death_count),0)
            FROM fct_communicable_deaths f JOIN dim_country dc ON dc.country_code=f.country_code
            WHERE f.year_val IN (2000,2019) GROUP BY dc.region_name, f.year_val
        ),
        p AS (
            SELECT region_name, year_val,
                   SUM(CASE WHEN dtype='NCD' THEN deaths ELSE 0 END) AS ncd,
                   SUM(CASE WHEN dtype='Communicable' THEN deaths ELSE 0 END) AS comm
            FROM d GROUP BY region_name, year_val
        )
        SELECT *, ROUND(ncd/NULLIF(comm,0),1) AS ratio FROM p ORDER BY region_name, year_val
    """)
    regions = df["region_name"].unique()
    fig, ax = plt.subplots(figsize=(12, 6))
    x = range(len(regions))
    r2000 = df[df["year_val"] == 2000].set_index("region_name")["ratio"]
    r2019 = df[df["year_val"] == 2019].set_index("region_name")["ratio"]
    width = 0.35
    ax.bar([i - width/2 for i in x], [r2000.get(r, 0) for r in regions], width,
           label="2000", color=PALETTE[3], alpha=0.8)
    ax.bar([i + width/2 for i in x], [r2019.get(r, 0) for r in regions], width,
           label="2019", color=PALETTE[0], alpha=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels(regions, rotation=20, ha="right")
    ax.set_ylabel("NCD-to-Communicable Ratio")
    ax.set_title("Q18: Epidemiological Transition — NCD/Communicable Death Ratio (2000 vs 2019)")
    ax.axhline(1, color="red", linestyle="--", linewidth=0.8, label="Ratio = 1 (equal)")
    ax.legend(fontsize=9)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mticker.ScalarFormatter())
    charts.append(("q18", "Epidemiological Transition",
                   "Africa's ratio flipped from 0.6 (communicable dominated) to 1.4 (NCD dominated). All regions shifted further toward NCD."))
    return save(fig, "q18")


def q19():
    """Q19: Total mortality burden (3-fact)"""
    df = query_df("""
        WITH le AS (
            SELECT country_code, country_name, region_name, ROUND(metric_value,1) AS life_expectancy
            FROM vw_health_enriched
            WHERE indicator_code='WHOSIS_000001' AND sex_name='Both sexes' AND year_val=2019
        ),
        ncd AS (SELECT country_code, ROUND(SUM(death_count),0) AS ncd_deaths
                FROM fct_ncd_deaths WHERE sex_code='SEX_BTSX' AND year_val=2019 GROUP BY country_code),
        comm AS (SELECT country_code, ROUND(SUM(death_count),0) AS comm_deaths
                 FROM fct_communicable_deaths WHERE year_val=2019 GROUP BY country_code)
        SELECT le.country_name, le.region_name, le.life_expectancy,
               COALESCE(ncd.ncd_deaths,0) AS ncd, COALESCE(comm.comm_deaths,0) AS comm,
               COALESCE(ncd.ncd_deaths,0)+COALESCE(comm.comm_deaths,0) AS total
        FROM le LEFT JOIN ncd ON ncd.country_code=le.country_code
        LEFT JOIN comm ON comm.country_code=le.country_code
        ORDER BY total DESC LIMIT 20
    """)
    fig, ax = plt.subplots(figsize=(12, 6))
    y = range(len(df))
    ax.barh(y, df["ncd"][::-1]/1e6, label="NCD", color=PALETTE[0])
    ax.barh(y, df["comm"][::-1]/1e6, left=df["ncd"][::-1]/1e6, label="Communicable", color=PALETTE[3])
    ax.set_yticks(list(y))
    ax.set_yticklabels(df["country_name"][::-1], fontsize=8)
    ax.set_xlabel("Total Deaths (millions)")
    ax.set_title("Q19: Total Mortality Burden — Top 20 Countries (2019)")
    ax.legend()
    for i, (_, row) in enumerate(df[::-1].iterrows()):
        ax.text((row["ncd"] + row["comm"])/1e6 + 0.05, i,
                f'LE: {row["life_expectancy"]}y', va="center", fontsize=7)
    charts.append(("q19", "Total Mortality Burden (3-Fact JOIN)",
                   "China and India dominate absolute counts. Life expectancy is annotated to show that large burden doesn't always mean low LE."))
    return save(fig, "q19")


def q20():
    """Q20: Africa's double burden"""
    df = query_df("""
        WITH an AS (
            SELECT f.country_code, dc.country_name, ROUND(SUM(f.death_count),0) AS ncd_deaths
            FROM fct_ncd_deaths f JOIN dim_country dc ON dc.country_code=f.country_code
            WHERE dc.region_name='Africa' AND f.sex_code='SEX_BTSX' AND f.year_val=2019
            GROUP BY f.country_code, dc.country_name
        ),
        ac AS (
            SELECT f.country_code, ROUND(SUM(f.death_count),0) AS comm_deaths
            FROM fct_communicable_deaths f JOIN dim_country dc ON dc.country_code=f.country_code
            WHERE dc.region_name='Africa' AND f.year_val=2019
            GROUP BY f.country_code
        )
        SELECT n.country_name, n.ncd_deaths, COALESCE(c.comm_deaths,0) AS comm_deaths,
               n.ncd_deaths+COALESCE(c.comm_deaths,0) AS total
        FROM an n LEFT JOIN ac c ON c.country_code=n.country_code
        WHERE COALESCE(c.comm_deaths,0) > 0
        ORDER BY total DESC LIMIT 15
    """)
    fig, ax = plt.subplots(figsize=(12, 6))
    y = range(len(df))
    ax.barh(y, df["ncd_deaths"][::-1]/1e3, label="NCD", color=PALETTE[0])
    ax.barh(y, df["comm_deaths"][::-1]/1e3, left=df["ncd_deaths"][::-1]/1e3,
            label="Communicable", color=PALETTE[3])
    ax.set_yticks(list(y))
    ax.set_yticklabels(df["country_name"][::-1], fontsize=9)
    ax.set_xlabel("Deaths (thousands)")
    ax.set_title("Q20: Africa's Double Burden — NCD + Communicable Deaths (2019)")
    ax.legend()
    for i, (_, row) in enumerate(df[::-1].iterrows()):
        pct = 100 * row["comm_deaths"] / row["total"] if row["total"] > 0 else 0
        ax.text((row["ncd_deaths"] + row["comm_deaths"])/1e3 + 2, i,
                f"{pct:.0f}% comm.", va="center", fontsize=8)
    charts.append(("q20", "Africa's Double Burden",
                   "Nigeria faces 654K combined deaths (59% communicable). DR Congo, Tanzania, Niger have ~50/50 splits."))
    return save(fig, "q20")


# ============================================================
# HTML Report
# ============================================================

def generate_html():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>WHO Global Health Estimates — Visual Report</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; max-width: 1100px; margin: 0 auto;
         padding: 20px; background: #fafafa; color: #333; }
  h1 { color: #1a5276; border-bottom: 3px solid #1a5276; padding-bottom: 10px; }
  h2 { color: #2c3e50; margin-top: 40px; }
  .chart-block { background: white; border-radius: 8px; padding: 20px; margin: 20px 0;
                 box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .chart-block img { width: 100%; border-radius: 4px; }
  .chart-block p { color: #555; font-size: 15px; line-height: 1.6; margin-top: 10px; }
  .chart-block .tag { display: inline-block; background: #eaf2f8; color: #1a5276;
                      padding: 2px 8px; border-radius: 4px; font-size: 12px; margin-bottom: 8px; }
  .summary { background: #eaf2f8; padding: 15px 20px; border-radius: 8px; margin: 20px 0; }
  .summary ul { margin: 5px 0; }
</style>
</head>
<body>
<h1>WHO Global Health Estimates — Visual Report</h1>
<div class="summary">
<p><strong>Source:</strong> WHO Global Health Observatory (GHO) OData API</p>
<p><strong>Coverage:</strong> 228 countries · 6 WHO regions · 2000–2024</p>
<p><strong>Data:</strong> 103,669 fact rows across 3 fact tables (health estimates, NCD deaths, communicable deaths)</p>
<ul>
  <li><strong>20 analytical queries</strong> covering life expectancy, NCD mortality, communicable disease deaths, and cross-dataset epidemiological analysis</li>
</ul>
</div>
"""
    sections = {
        "Health Estimates (Q1–Q10)": [c for c in charts if c[0] in [f"q{i:02d}" for i in range(1, 11)]],
        "NCD Deaths (Q11–Q14)": [c for c in charts if c[0] in [f"q{i:02d}" for i in range(11, 15)]],
        "Communicable Deaths (Q15–Q16)": [c for c in charts if c[0] in ["q15", "q16"]],
        "Cross-Dataset Analysis (Q17–Q20)": [c for c in charts if c[0] in [f"q{i:02d}" for i in range(17, 21)]],
    }
    for section, items in sections.items():
        html += f'<h2>{section}</h2>\n'
        for fname, title, desc in items:
            html += f"""<div class="chart-block">
<span class="tag">{fname.upper()}</span>
<h3>{title}</h3>
<img src="charts/{fname}.png" alt="{title}">
<p>{desc}</p>
</div>\n"""

    html += """
<div class="summary" style="margin-top: 40px;">
<h3>Key Takeaways</h3>
<ul>
  <li>Global life expectancy rose from 67.0 to 72.6 years (2000–2019), then fell to 71.3 (2021) due to COVID-19.</li>
  <li>Cardiovascular diseases are the world's #1 killer at ~17.9M deaths/year (2019).</li>
  <li>Africa is the only region where communicable diseases still represent &gt;40% of mortality.</li>
  <li>Africa's NCD-to-communicable ratio flipped from 0.6 (2000) to 1.4 (2019) — the epidemiological transition is underway everywhere.</li>
  <li>Nigeria, DR Congo, and Tanzania face the worst "double burden" of simultaneous NCD and communicable disease mortality.</li>
</ul>
</div>
</body></html>"""

    path = os.path.join(BASE_DIR, "VISUAL_REPORT.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nHTML report saved to {path}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("Generating charts...")
    funcs = [q01, q02, q03, q04, q05, q06, q07, q08, q09, q10,
             q11, q12, q13, q14, q15, q16, q17, q18, q19, q20]
    for fn in funcs:
        print(f"\n{fn.__doc__}")
        fn()

    generate_html()
    print(f"\nDone! {len(charts)} charts generated in charts/")
