import sqlite3
import os
import requests
import pandas as pd
import io
import matplotlib.pyplot as plt
import seaborn as sns
from adjustText import adjust_text

API_KEY = os.environ["CENSUS_API_KEY"]
HOME = os.path.expanduser("~")
DB_PATH = os.path.join(HOME, "tx-voter-eligibility.db")
CHART_PATH = os.path.join(HOME, "tx-voter-scatter.png")

url = "http://api.census.gov/data/2023/acs/acs5"
params = {
    "get": "NAME,B01001_001E",
    "for": "county:*",
    "in": "state:48",
    "key": API_KEY
}

response = requests.get(url, params=params)
data = response.json()

cvap_params = {
    "get": "NAME,B29001_001E",
    "for": "county:*",
    "in": "state:48",
    "key": API_KEY
}

cvap_response = requests.get(url, params=cvap_params)
cvap_data = cvap_response.json()

cvap_lookup = {}
for row in cvap_data[1:]:
    fips = row[2] + row[3]
    cvap = int(row[1])
    cvap_lookup[fips] = cvap

vera_url = "https://raw.githubusercontent.com/vera-institute/incarceration-trends/refs/heads/main/incarceration_trends_county.csv"
df = pd.read_csv(vera_url)
tx = df[df["state_abbr"] == "TX"].copy().query("year <= 2022")

sos_url = "https://www.sos.state.tx.us/elections/historical/jan2023.shtml"
sos_response = requests.get(sos_url)
sos_tables = pd.read_html(io.StringIO(sos_response.text))
vr = sos_tables[0]

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS counties (
        fips        TEXT PRIMARY KEY,
        county_name TEXT,
        population  INTEGER,
        cvap        INTEGER    
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS releases (
        fips                   TEXT,
        year                   INTEGER,
        total_jail_discharges  INTEGER,
        total_prison_pop       INTEGER                   
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS supervision_current (
        fips         TEXT,
        year         INTEGER,
        on_parole    INTEGER,
        on_probation INTEGER
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS voter_registration (
       fips                 TEXT,
       year                 INTEGER,
       registered_voters    INTEGER,
       UNIQUE(fips, year)        
    )
""")

for row in data[1:]:
    name = row[0]
    population = int(row[1])
    fips = row[2] + row[3]
    cursor.execute(
        "INSERT OR IGNORE INTO counties (fips, county_name, population) VALUES (?, ?, ?)",
        (fips, name, population)
    )

for fips, cvap in cvap_lookup.items():
    conn.execute(
        "UPDATE counties SET cvap = ? WHERE fips = ?",
        (cvap, fips)
    )

for _, row in tx.iterrows():
    fips = str(int(row["county_fips"])).zfill(5)
    year = int(row["year"])
    discharges = None if pd.isna(row["total_jail_discharges"]) else int(row["total_jail_discharges"])
    prison_pop = None if pd.isna(row["total_prison_pop"]) else int(row["total_prison_pop"])
    cursor.execute(
        "INSERT OR IGNORE INTO releases (fips, year, total_jail_discharges, total_prison_pop) VALUES (?, ?, ?, ?)",
        (fips, year, discharges, prison_pop)
    )

county_name_to_fips = {}
for row in conn.execute("SELECT fips, county_name FROM counties"):
    name = row[1].replace(" County, Texas", "").upper().replace(" ", "")
    county_name_to_fips[name] = row[0]

for _, row in vr.iterrows():
    name = str(row["County Name"]).upper().strip().replace(" ", "")
    fips = county_name_to_fips.get(name)
    if fips is None:
        continue
    registered = int(str(row["Voter Registration"]).replace(",", ""))
    cursor.execute(
        "INSERT OR IGNORE INTO voter_registration (fips, year, registered_voters) VALUES (?, ?, ?)",
        (fips, 2023, registered)
    )

conn.commit()

print("\n=== DATA VALIDATION ===")

checks = {}

checks["counties_count"] = conn.execute("SELECT COUNT(*) FROM counties").fetchone()[0]
checks["releases_count"] = conn.execute("SELECT COUNT(*) FROM releases").fetchone()[0]
checks["voter_reg_count"] = conn.execute("SELECT COUNT(*) FROM voter_registration").fetchone()[0]

print(f"Counties loaded:       {checks['counties_count']} (expected 254)")
print(f"Release records:       {checks['releases_count']}")
print(f"Voter reg records:     {checks['voter_reg_count']} (expected 253-254)")

bad_fips = conn.execute("""
    SELECT COUNT(*) FROM counties
    WHERE LENGTH(fips) != 5 OR fips NOT LIKE '48%'
""").fetchone()[0]
print(f"Malformed FIPS codes:  {bad_fips} (expected 0)")

bad_cvap = conn.execute("""
    SELECT COUNT(*) FROM counties
    WHERE cvap > population
""").fetchone()[0]
print(f"CVAP > population:     {bad_cvap} (expected 0)")

null_discharges = conn.execute("""
    SELECT ROUND(100.0 * SUM(CASE WHEN total_jail_discharges IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
    FROM releases
""").fetchone()[0]
print(f"NULL jail discharges:  {null_discharges}%")

dupes = conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT fips FROM counties GROUP BY fips HAVING COUNT(*) > 1
    )
""").fetchone()[0]
print(f"Duplicate FIPS:        {dupes} (expected 0)")

print("=== END VALIDATION ===\n")

query = """
    WITH jail_history AS (
        SELECT fips, SUM(total_jail_discharges) AS total_discharges
        FROM releases
        GROUP BY fips
    ),
    registration_gap AS (
        SELECT
            c.fips,
            c.county_name,
            c.cvap,
            vr.registered_voters,
            jh.total_discharges,
            ROUND(vr.registered_voters * 100.0 / c.cvap, 1) AS registration_rate
        FROM counties c
        JOIN voter_registration vr ON c.fips = vr.fips
        JOIN jail_history jh ON c.fips = jh.fips
    )
    SELECT county_name, cvap, registered_voters, registration_rate, total_discharges
    FROM registration_gap
    ORDER BY registration_rate ASC
    LIMIT 10
"""

results = cursor.execute(query).fetchall()
for row in results:
    print(row)

conn.close()

query_all = """
    WITH jail_history AS (
        SELECT fips, SUM(total_jail_discharges) AS total_discharges
        FROM releases
        GROUP BY fips
    ),
    registration_gap AS (
        SELECT
            c.county_name,
            c.cvap,
            vr.registered_voters,
            jh.total_discharges,
            ROUND(vr.registered_voters * 100.0 / c.cvap, 1) AS registration_rate
        FROM counties c
        JOIN voter_registration vr ON c.fips = vr.fips
        JOIN jail_history jh ON c.fips = jh.fips
    )
    SELECT county_name, registration_rate, total_discharges
    FROM registration_gap
"""

conn2 = sqlite3.connect(DB_PATH)
plot_df = pd.read_sql_query(query_all, conn2)
conn2.close()

prison_counties = {
    "Anderson County, Texas", "Angelina County, Texas", "Bee County, Texas",
    "Bexar County, Texas", "Bowie County, Texas", "Brazoria County, Texas",
    "Brazos County, Texas", "Brown County, Texas", "Burnet County, Texas",
    "Caldwell County, Texas", "Cherokee County, Texas", "Childress County, Texas",
    "Coryell County, Texas", "Dallas County, Texas", "Dawson County, Texas",
    "DeWitt County, Texas", "Duvall County, Texas", "El Paso County, Texas",
    "Falls County, Texas", "Fort Bend County, Texas", "Freestone County, Texas",
    "Frio County, Texas", "Galveston County, Texas", "Garza County, Texas",
    "Gray County, Texas", "Grimes County, Texas", "Hale County, Texas",
    "Harris County, Texas", "Hartley County, Texas", "Hays County, Texas",
    "Hidalgo County, Texas", "Houston County, Texas", "Jack County, Texas",
    "Jasper County, Texas", "Jefferson County, Texas", "Johnson County, Texas",
    "Jones County, Texas", "Karnes County, Texas", "La Salle County, Texas",
    "Liberty County, Texas", "Lubbock County, Texas", "Madison County, Texas",
    "Medina County, Texas", "Mitchell County, Texas", "Pecos County, Texas",
    "Polk County, Texas", "Potter County, Texas", "Rusk County, Texas",
    "San Saba County, Texas", "Scurry County, Texas", "Stephens County, Texas",
    "Swisher County, Texas", "Travis County, Texas", "Tyler County, Texas",
    "Walker County, Texas", "Willacy County, Texas", "Williamson County, Texas",
    "Wise County, Texas", "Wichita County, Texas", "Wood County, Texas",
}

plot_df["has_prison"] = plot_df["county_name"].isin(prison_counties)


plt.figure(figsize=(12, 7))
ax = sns.scatterplot(
    data=plot_df[~plot_df["has_prison"]],
    x="total_discharges",
    y="registration_rate",
    color="steelblue",
    alpha=0.6,
    label="No TDCJ unit"
)
sns.scatterplot(
    data=plot_df[plot_df["has_prison"]],
    x="total_discharges",
    y="registration_rate",
    color="red",
    alpha=0.7,
    ax=ax,
    label="Has TDCJ prison unit"
)
ax.legend(loc="upper left", fontsize=8)


ax.set_xscale("log")
ax.set_ylim(50, 150)

texts = []
for _, row in plot_df.iterrows():
    if row["registration_rate"] < 68 or row["total_discharges"] > 200000:
        label = row["county_name"].replace(" County, Texas", "")
        t = ax.text(row["total_discharges"], row["registration_rate"],
                    label, fontsize=7)
        texts.append(t)

adjust_text(texts, arrowprops=dict(arrowstyle="-", color="gray", lw=0.5))
ax.axhline(y=100, color="red", linestyle="--", linewidth=1, alpha=0.7)
ax.text(ax.get_xlim()[0], 101, "100% = all eligible voters registered (data anomaly above this line)",
        fontsize=7, color="red", alpha=0.8)


plt.title("Voter Registration Rate vs. Jail Discharges by Texas County")
plt.xlabel("Total Jail Discharges Since 2000")
plt.ylabel("Voter Registration Rate (% of CVAP) from 2023")
sources = (
    "Sources: U.S. Census Bureau ACS 5-Year Estimates 2023 (population, CVAP) | "
    "Vera Institute Incarceration Trends 2000–2022 (jail discharges) | "
    "Texas Secretary of State Voter Registration Jan 2023 | "
    "TDCJ Unit Directory (prison county classification)"
)
plt.figtext(0.5, -0.03, sources, ha="center", fontsize=6, color="gray", wrap=True)
plt.tight_layout()
plt.savefig(CHART_PATH, dpi=150, bbox_inches="tight")
print("Chart saved.")
