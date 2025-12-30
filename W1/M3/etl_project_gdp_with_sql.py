import json
import sqlite3
from datetime import datetime
import pycountry
import pycountry_convert as pc

# logging function
def log_message(message):
    now = datetime.now()
    time_stamp = now.strftime("%Y-%B-%d-%H-%M-%S")
    with open("etl_project_log.txt", "a") as f:
        f.write(f"{time_stamp}, {message}\n")

# country -> region mapping
def country_to_region(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        country_alpha2 = country.alpha_2
        continent_name = pc.country_alpha2_to_continent_code(country_alpha2)
        continent_dict = {
            'AF': 'Africa',
            'AS': 'Asia',
            'EU': 'Europe',
            'NA': 'North America',
            'SA': 'South America',
            'OC': 'Oceania',
            'AN': 'Antarctica'
        }
        return continent_dict.get(continent_name, "Unknown")
    except Exception:
        return "Unknown"

def load_json_to_db(json_path: str, db_path: str, table_name: str):
    log_message("INFO: Loading to DB started")

    # read JSON file
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    log_message(f"DEBUG: Loaded {len(data)} records")

    # connect DB
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # create table
    cur.execute(f"""
    create table if not exists {table_name} (
        Country TEXT PRIMARY KEY,
        GDP_USD_billion REAL
    )
    """)
    conn.commit()
    log_message(f"DEBUG: created table name: {table_name}")

    # remove original data
    cur.execute(f"delete from {table_name}")
    conn.commit()

    # add/rewrite data
    for row in data:
        cur.execute(f"""
        insert into {table_name} (Country, GDP_USD_billion) values (?, ?)
        """, (row['Country'], row['GDP_USD_billion']))
    conn.commit()
    log_message(f"DEBUG: Inserted {len(data)} records into table {table_name}")
    log_message("INFO: Loading to DB finished")

    print("GDP >= 100B USD")
    cur.execute(f"select * from {table_name} where GDP_USD_billion >= 100")
    for r in cur.fetchall():
        print(r)

    print("\nTop5 GDP average by Region")
    for row in data:
        row['Region'] = country_to_region(row['Country'])

    cur.execute("""
    CREATE TEMP TABLE temp_region_table (
        Country text,
        GDP_USD_billion real,
        Region text
    )
    """)

    for row in data:
        cur.execute("""
        insert into temp_region_table (Country, GDP_USD_billion, Region) values (?, ?, ?)
        """, (row['Country'], row['GDP_USD_billion'], row['Region']))
    cur.execute("""
    SELECT Region, AVG(GDP_USD_billion) AS avg_top5
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS rn
        FROM temp_region_table
    ) sub
    WHERE rn <= 5
    GROUP BY Region
    """)

    for region, avg_top5 in cur.fetchall():
        print(f"{region}: {avg_top5:.2f}B USD")
        
    conn.close()

if __name__ == "__main__":
    json_path = "Countries_by_GDP.json"
    db_path = "World_Economies.db"
    table_name = "Countries_by_GDP"
    load_json_to_db(json_path, db_path, table_name)