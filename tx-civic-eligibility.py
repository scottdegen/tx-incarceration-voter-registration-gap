import sqlite3
import os
import ast
import requests
import pandas as pd
import io
import matplotlib.pyplot as plt
import seaborn as sns
import anthropic
from adjustText import adjust_text

API_KEY = os.environ["CENSUS_API_KEY"]
HOME = os.path.expanduser("~")
DB_PATH = os.path.join(HOME, "tx-voter-eligibility.db")
CHART_PATH = os.path.join(HOME, "tx-voter-scatter.png")

# --- LLM-based TDCJ prison county extraction ---
# Instead of hardcoding county names, we fetch the live TDCJ unit directory
# and use Claude to extract structured data from the HTML. This keeps the
# prison county list up-to-date without manual maintenance.
client = anthropic.Anthropic()

tdcj_response = requests.get("https://www.tdcj.texas.gov/unit_directory/")
raw_text = tdcj_response.text

message = client.messages.create(
    model="claude-opus-4-7",
    max_tokens=1024,
    messages=[{
        "role": "user",
        "content": f"Extract all prison unit names and their Texas counties from this HTML. Return ONLY a Python list of tuples with no explanation, no markdown, no code fences: [(unit_name, county_name), ...]. HTML: {raw_text[:10000]}"
    }]
)

# ast.literal_eval safely parses the LLM's string output into a Python object
# without using exec() — avoids arbitrary code execution risk
raw_pairs = ast.literal_eval(message.content[0].text.strip())
prison_counties = {f"{county} County, Texas" for _, county in raw_pairs}

# --- Census ACS 5-year estimates (2023 vintage) ---
# Using 2023 to align with 2023 voter registration data from Texas SOS.
# B01001_001E = total population, B29001_001E = citizen voting age population (CVAP)
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

# Build a FIPS → CVAP lookup before opening the DB connection.
# Census returns state and county codes separately (row[2], row[3]);
# concatenating them gives the standard 5-digit FIPS.
cvap_lookup = {}
for row in cvap_data[1:]:
    fips = row[2] + row[3]
    cvap = int(row[1])
    cvap_lookup[fips] = cvap

# --- Vera Institute incarceration trends ---
# Filtered to Texas and capped at 2022 — Vera's most recent complete year.
# Years beyond 2022 exist in the dataset but have empty values.
vera_url = "https://raw.githubusercontent.com/vera-institute/incarceration-trends/refs/heads/main/incarceration_trends_county.csv"
df = pd.read_csv(vera_url)
tx = df[df["state_abbr"] == "TX"].copy().query("year <= 2022")

# --- Texas SOS voter registration (January 2023) ---
# pd.read_html expects a file-like object; io.StringIO wraps the raw HTML
# string so pandas can parse it without writing to disk.
sos_url = "https://www.sos.state.tx.us/elections/historical/jan2023.shtml"
sos_response = requests.get(sos_url)
sos_tables = pd.read_html(io.StringIO(sos_response.text))
vr = sos_tables[0]

# --- Database setup ---
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

# UNIQUE(fips, year) constraint is what makes INSERT OR IGNORE work as a
# deduplication guard — without it, re-running the script would append rows.
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
    # Vera stores county_fips as a float (e.g. 48001.0); zfill(5) ensures
    # small counties aren't truncated to 4 digits after int conversion.
    fips = str(int(row["county_fips"])).zfill(5)
    year = int(row["year"])
    # NaN values from Vera must be converted to None so SQLite stores NULL
    # rather than raising a ValueError on int() conversion.
    discharges = None if pd.isna(row["total_jail_discharges"]) else int(row["total_jail_discharges"])
    prison_pop = None if pd.isna(row["total_prison_pop"]) else int(row["total_prison_pop"])
    cursor.execute(
        "INSERT OR IGNORE INTO releases (fips, year, total_jail_discharges, total_prison_pop) VALUES (?, ?, ?, ?)",
        (fips, year, discharges, prison_pop)
    )

# Build a name → FIPS lookup from the counties table to join SOS data,
# which has county names but no FIPS codes.
# Spaces are stripped from both sides because SOS uses "LASALLE" while
# Census stores "La Salle" — normalizing removes the mismatch.
county_name_to_fips = {}
for row in conn.execute("SELECT fips, county_name FROM counties"):
    name = row[1].replace(" County, Texas", "").upper().replace(" ", "")
    county_name_to_fips[name] = row[0]

for _, row in vr.iterrows():
    name = str(row["County Name"]).upper().strip().replace(" ", "")
    fips = county_name_to_fips.get(name)
    if fips is None:
        # Skips the statewide total row at the bottom of the SOS table
        continue
    registered = int(str(row["Voter Registration"]).replace(",", ""))
    cursor.execute(
        "INSERT OR IGNORE INTO voter_registration (fips, year, registered_voters) VALUES (?, ?, ?)",
        (fips, 2023, registered)
    )

conn.commit()

# --- Data validation ---
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

high_null_counties = conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT fips,
               100.0 * SUM(CASE WHEN total_jail_discharges IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_rate
        FROM releases
        GROUP BY fips
        HAVING null_rate > 50
    )
""").fetchone()[0]
print(f"Counties >50% NULL discharges: {high_null_counties}")

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
    null_rates AS (
        SELECT fips,
               100.0 * SUM(CASE WHEN total_jail_discharges IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_rate
        FROM releases
        GROUP BY fips
    ),
    registration_gap AS (
        SELECT
            c.county_name,
            c.cvap,
            vr.registered_voters,
            jh.total_discharges,
            nr.null_rate,
            ROUND(vr.registered_voters * 100.0 / c.cvap, 1) AS registration_rate
        FROM counties c
        JOIN voter_registration vr ON c.fips = vr.fips
        JOIN jail_history jh ON c.fips = jh.fips
        JOIN null_rates nr ON c.fips = nr.fips
    )
    SELECT county_name, registration_rate, total_discharges, null_rate
    FROM registration_gap
"""

# Reopen connection after conn.close() above — fresh read-only query for plotting
conn2 = sqlite3.connect(DB_PATH)
plot_df = pd.read_sql_query(query_all, conn2)
conn2.close()

plot_df["has_prison"] = plot_df["county_name"].isin(prison_counties)
plot_df["sparse_data"] = plot_df["null_rate"] > 50

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
sns.scatterplot(
    data=plot_df[plot_df["sparse_data"]],
    x="total_discharges",
    y="registration_rate",
    color="black",
    alpha=0.9,
    marker="x",
    s=60,
    ax=ax,
    label=">50% missing discharge data"
)
ax.legend(loc="upper left", fontsize=8)

ax.set_xscale("log")
# Y-axis capped at 50-150 to exclude extreme outliers (e.g. Kenedy County at
# 610%) caused by Census undercounting CVAP in very small rural counties.
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
