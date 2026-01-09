import logging
import os
from typing import Any, Dict, List, Tuple

import pandas as pd

from car_pipeline.db.mysql import get_mysql_conn, load_settings

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
DATA_DIR = os.path.join(ROOT, "data")
MASTER_PATH = os.path.join(DATA_DIR, "master", "car_price_master.csv")


DDL = [
    """
    CREATE DATABASE IF NOT EXISTS {db}
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """,
    """
    CREATE TABLE IF NOT EXISTS {db}.car_listings (
      listing_id        BIGINT PRIMARY KEY,
      listing_url       VARCHAR(500),
      brand             VARCHAR(120),
      model             VARCHAR(120),
      year              INT,
      mileage           INT,
      fuel              VARCHAR(60),
      color             VARCHAR(60),
      region_v2         INT,
      region_name       VARCHAR(120),
      location_lat      DECIMAL(10,7),
      location_lng      DECIMAL(10,7),
      first_seen        DATETIME NOT NULL,
      last_seen         DATETIME NOT NULL,
      last_price        BIGINT,
      source            VARCHAR(60) NOT NULL,
      updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_brand_model (brand, model),
      INDEX idx_region (region_name),
      INDEX idx_year (year)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS {db}.price_history (
      id          BIGINT AUTO_INCREMENT PRIMARY KEY,
      listing_id  BIGINT NOT NULL,
      price       BIGINT,
      crawl_date  DATETIME NOT NULL,
      source      VARCHAR(60) NOT NULL,
      UNIQUE KEY uq_listing_crawl (listing_id, crawl_date),
      INDEX idx_listing_date (listing_id, crawl_date),
      CONSTRAINT fk_price_listing FOREIGN KEY (listing_id)
        REFERENCES {db}.car_listings(listing_id)
        ON DELETE CASCADE
    ) ENGINE=InnoDB;
    """,
]


def setup_logger() -> logging.Logger:
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    logger = logging.getLogger("load_to_mysql")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(os.path.join(ROOT, "logs", "load_to_mysql.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def to_int(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


def to_float(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return float(x)
    except Exception:
        return None


def ensure_schema(logger: logging.Logger) -> None:
    settings = load_settings()
    db = settings["mysql"]["database"]

    conn = get_mysql_conn(database=None)
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt.format(db=db))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Ensured schema database=%s", db)


def main() -> None:
    logger = setup_logger()

    if not os.path.exists(MASTER_PATH):
        logger.info("Master file not found: %s", MASTER_PATH)
        return

    ensure_schema(logger)

    df = pd.read_csv(MASTER_PATH, encoding="utf-8-sig")
    logger.info("Read master=%s rows=%s", MASTER_PATH, len(df))

    # required cols
    required = [
        "listing_id", "listing_url", "brand", "model", "year", "mileage", "fuel", "color",
        "region_v2", "region_name", "location_lat", "location_lng", "crawl_date", "price", "source"
    ]
    for c in required:
        if c not in df.columns:
            df[c] = None

    df["listing_id"] = df["listing_id"].apply(to_int)
    df = df[df["listing_id"].notna()]

    df["year"] = df["year"].apply(to_int)
    df["mileage"] = df["mileage"].apply(to_int)
    df["region_v2"] = df["region_v2"].apply(to_int)
    df["price"] = df["price"].apply(to_int)
    df["location_lat"] = df["location_lat"].apply(to_float)
    df["location_lng"] = df["location_lng"].apply(to_float)
    df["crawl_date"] = pd.to_datetime(df["crawl_date"], errors="coerce")

    df = df[df["crawl_date"].notna()]

    # prepare rows
    listings_rows: List[Tuple[Any, ...]] = []
    history_rows: List[Tuple[Any, ...]] = []

    for _, r in df.iterrows():
        listing_id = int(r["listing_id"])
        crawl_date = r["crawl_date"].to_pydatetime()
        price = r["price"] if pd.notna(r["price"]) else None

        listings_rows.append(
            (
                listing_id,
                str(r.get("listing_url") or ""),
                str(r.get("brand") or ""),
                str(r.get("model") or ""),
                r.get("year"),
                r.get("mileage"),
                str(r.get("fuel") or ""),
                str(r.get("color") or ""),
                r.get("region_v2"),
                str(r.get("region_name") or ""),
                r.get("location_lat"),
                r.get("location_lng"),
                crawl_date,  # first_seen candidate
                crawl_date,  # last_seen
                price,
                str(r.get("source") or "chotot"),
            )
        )

        history_rows.append((listing_id, price, crawl_date, str(r.get("source") or "chotot")))

    conn = get_mysql_conn()
    cur = conn.cursor()

    upsert_sql = """
    INSERT INTO car_listings(
      listing_id, listing_url, brand, model, year, mileage, fuel, color,
      region_v2, region_name, location_lat, location_lng,
      first_seen, last_seen, last_price, source
    ) VALUES (
      %s,%s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
      listing_url = VALUES(listing_url),
      brand = VALUES(brand),
      model = VALUES(model),
      year = VALUES(year),
      mileage = VALUES(mileage),
      fuel = VALUES(fuel),
      color = VALUES(color),
      region_v2 = VALUES(region_v2),
      region_name = VALUES(region_name),
      location_lat = VALUES(location_lat),
      location_lng = VALUES(location_lng),
      last_seen = GREATEST(last_seen, VALUES(last_seen)),
      first_seen = LEAST(first_seen, VALUES(first_seen)),
      last_price = VALUES(last_price),
      source = VALUES(source);
    """

    hist_sql = """
    INSERT INTO price_history(listing_id, price, crawl_date, source)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      price = VALUES(price),
      source = VALUES(source);
    """

    # executemany
    cur.executemany(upsert_sql, listings_rows)
    upserted = cur.rowcount

    cur.executemany(hist_sql, history_rows)
    hist_upserted = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    logger.info("MYSQL upsert car_listings rowcount=%s", upserted)
    logger.info("MYSQL upsert price_history rowcount=%s", hist_upserted)


if __name__ == "__main__":
    main()
