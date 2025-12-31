from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import json

# logging function
def log_message(message):
    now = datetime.now()
    time_stamp = now.strftime("%Y-%B-%d-%H-%M-%S")
    with open("etl_project_log.txt", "a") as f:
        f.write(f"{time_stamp}, {message}\n")

# Extraction
def extract_system(url: str, json_path: str):
    """
    Extract raw data from url
    Returns dataframe including:
    - Country
    - GDP
    Returns caption: used for unit detection
    """
    log_message("INFO: Extract started")

    # request html of url
    # check Robot policy, User-Agent: https://www.whatismybrowser.com/detect/what-is-my-user-agent/
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    html = requests.get(url, headers=headers).text
    log_message("DEBUG: Requested page fetched")
    
    # parsing, find all table tags
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    if not tables:
        log_message("ERROR: No tables found")
    else:
        log_message(f"DEBUG: Found {len(tables)} tables")
    
    target_table = None
    target_caption = ""
    for table in tables:
        caption_element = table.find("caption")
        caption = caption_element.get_text(strip=True) if caption_element else "No caption"
        log_message(f"DEBUG: Caption - {caption}")
        if caption and "GDP" in caption:
            target_table = table
            target_caption = caption
            log_message("DEBUG: GDP table found")
            break

    if target_table is None:
        log_message("ERROR: GDP table NOT found")
        raise ValueError("GDP table not found")

    data = []

    tbody = target_table.find("tbody")
    rows = tbody.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        # ignore none data rows
        if len(cols) < 2:
            continue

        country = cols[0].get_text(strip=True)
        if country.lower() == "world": # skip World row
            continue

        # Extract IMF latest year
        gdp = cols[1].get_text(strip=True)
        data.append({
            "Country": country,
            "GDP": gdp
        })

    raw_json = {
        "caption": target_caption,
        "data": data
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, ensure_ascii=False, indent=4)

    log_message(f"INFO: Extract finished → saved to {json_path}")


def transform_system(raw_data: str, transform_data: str) -> pd.DataFrame:
    """
    Transform raw data (based on unit described on caption)
    Returns dataframe including:
    - Country
    - GDP
    Transform:
    - Country name cleaning
    - remove NaN
    - Convert GDP to Billion USD, float
    - Sort by GDP descending
    """
    log_message("INFO: Transform started")

    with open(raw_data, "r", encoding="utf-8") as f:
        raw_json = json.load(f)

    caption = raw_json.get("caption", "")
    df = pd.DataFrame(raw_json["data"])

    # Country name cleaning (remove [])
    df['Country'] = df['Country'].apply(lambda x: re.sub(r"\[.*?\]", "", x).strip())
    log_message("DEBUG: clean Country name")

    # remove ',' and convert to float
    df['GDP'] = df['GDP'].str.replace(',','').str.strip()
    # NaN data process
    df['GDP'] = pd.to_numeric(df['GDP'], errors='coerce')
    df = df.dropna(subset=['GDP'])
    log_message("DEBUG: convert to float and remove NaN")
    
    # million to billion
    caption_lower = caption.lower() if caption else ""
    if "million" in caption_lower:
        log_message("DEBUG: unit-million")
        df['GDP_USD_billion'] = (df['GDP'] / 1000).round(2)
    elif "billion" in caption_lower:
        log_message("DEBUG: unit-billion")
        df['GDP_USD_billion'] = df['GDP'].round(2)
    else:
        log_message("DEBUG: unit-none")
        df['GDP_USD_billion'] = (df['GDP'] / 1000000).round(2)

    output = df[["Country", "GDP_USD_billion"]].to_dict(orient="records")
    with open(transform_data, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    log_message(f"INFO: Transform finished → saved to {transform_data}")


def load_system(prev_data: str, json_path: str):
    """
    Load data into database
    """
    log_message("INFO: Load started")

    with open(prev_data, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    log_message(f"INFO: Load finished → saved to {json_path}")

if __name__ ==  "__main__":
    url =  "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    raw_data = "Countries_by_GDP_raw.json"
    transform_data = "Countries_by_GDP_trans.json"
    json_path = "Countries_by_GDP.json"
    
    extract_system(url, raw_data)
    transform_system(raw_data, transform_data)
    load_system(transform_data, json_path)