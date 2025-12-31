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
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    log_message(f"DEBUG: dropped table if exists: {table_name}")
    cur.execute(f"""
    create table {table_name} (
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
    
    conn.close()

def query(db_path: str, table_name: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    #1. GDP >= 100B USD
    print("\n===== GDP >= 100B USD Countries =====")
    cur.execute(f"""
        SELECT Country, GDP_USD_billion
        FROM {table_name}
        WHERE GDP_USD_billion >= 100
        ORDER BY GDP_USD_billion DESC
    """)

    rows = cur.fetchall()
    for country, gdp in rows:
        print(f"{country}: {gdp}B USD")

    #2. Top 5 GDP average by Region
    print("\n===== Region-wise Top 5 GDP Average =====")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Countries_with_Region (
            Country TEXT PRIMARY KEY,
            Region TEXT,
            GDP_USD_billion REAL
        )
    """)
    conn.commit()
    
    cur.execute("DELETE FROM Countries_with_Region")
    conn.commit()

    # Country → Region 변환 후 삽입
    cur.execute(f"SELECT Country, GDP_USD_billion FROM {table_name}")
    countries = cur.fetchall()

    for country, gdp in countries:
        try:
            country_alpha2 = pycountry.countries.lookup(country).alpha_2
            continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
            region = pc.convert_continent_code_to_continent_name(continent_code)
        except:
            region = "Others"

        cur.execute("""
            INSERT INTO Countries_with_Region (Country, Region, GDP_USD_billion)
            VALUES (?, ?, ?)
        """, (country, region, gdp))

    conn.commit()

    # Region별 Top5 GDP 평균 계산
    cur.execute("""
        SELECT Region, AVG(GDP_USD_billion) as avg_gdp
        FROM (
            SELECT Country, Region, GDP_USD_billion,
                   ROW_NUMBER() OVER (
                       PARTITION BY Region
                       ORDER BY GDP_USD_billion DESC
                   ) as rn
            FROM Countries_with_Region
        )
        WHERE rn <= 5
        GROUP BY Region
        ORDER BY avg_gdp DESC
    """)

    results = cur.fetchall()
    for region, avg_gdp in results:
        print(f"{region}: {avg_gdp:.2f}B USD")

    conn.close()
    


if __name__ == "__main__":
    json_path = "Countries_by_GDP.json"
    db_path = "World_Economies.db"
    table_name = "Countries_by_GDP"
    
    load_json_to_db(json_path, db_path, table_name)
    query(db_path, table_name)
    