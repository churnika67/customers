import os
import sys
import psycopg2
from psycopg2 import extras, errors
import csv
from pathlib import Path
import time

from utils import get_db_url

csv.field_size_limit(sys.maxsize)
LOCK_TIMEOUT = os.getenv("DB_LOCK_TIMEOUT", "5s")
STATEMENT_TIMEOUT = os.getenv("DB_STATEMENT_TIMEOUT", "300s")
CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))

DROP_TABLES_SQL = [
    "DROP TABLE IF EXISTS OrderDetail CASCADE",
    "DROP TABLE IF EXISTS Product CASCADE",
    "DROP TABLE IF EXISTS ProductCategory CASCADE",
    "DROP TABLE IF EXISTS Customer CASCADE",
    "DROP TABLE IF EXISTS Country CASCADE",
    "DROP TABLE IF EXISTS Region CASCADE",
    "DROP TABLE IF EXISTS stage_orderdetail CASCADE",
    "DROP TABLE IF EXISTS stage_product CASCADE",
    "DROP TABLE IF EXISTS stage_product_category CASCADE",
    "DROP TABLE IF EXISTS stage_customer CASCADE",
    "DROP TABLE IF EXISTS stage_country CASCADE",
    "DROP TABLE IF EXISTS stage_region CASCADE"
]

CREATE_TABLE_SQL = """
-- Staging tables
CREATE TABLE IF NOT EXISTS stage_region (
    Region TEXT
);

CREATE TABLE IF NOT EXISTS stage_country (
    Country TEXT,
    Region TEXT
);

CREATE TABLE IF NOT EXISTS stage_customer (
    Name TEXT,
    Address TEXT,
    City TEXT,
    Country TEXT,
    Region TEXT,
    ProductName TEXT
);

CREATE TABLE IF NOT EXISTS stage_product_category (
    ProductCategory TEXT,
    ProductCategoryDescription TEXT
);

CREATE TABLE IF NOT EXISTS stage_product (
    ProductName TEXT,
    ProductUnitPrice REAL,
    ProductCategory TEXT
);

CREATE TABLE IF NOT EXISTS stage_orderdetail (
    CustomerName TEXT,
    ProductName TEXT,
    OrderDate TEXT,
    QuantityOrdered INTEGER
);

-- Core tables
CREATE TABLE IF NOT EXISTS Region (
    RegionID SERIAL PRIMARY KEY,
    Region TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Country (
    CountryID SERIAL PRIMARY KEY,
    Country TEXT NOT NULL,
    RegionID INTEGER NOT NULL REFERENCES Region(RegionID),
    UNIQUE (Country)
);

CREATE TABLE IF NOT EXISTS Customer (
    CustomerID SERIAL PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Address TEXT NOT NULL,
    City TEXT NOT NULL,
    CountryID INTEGER NOT NULL REFERENCES Country(CountryID)
);

CREATE TABLE IF NOT EXISTS ProductCategory (
    ProductCategoryID SERIAL PRIMARY KEY,
    ProductCategory TEXT NOT NULL,
    ProductCategoryDescription TEXT,
    UNIQUE (ProductCategory)
);

CREATE TABLE IF NOT EXISTS Product (
    ProductID SERIAL PRIMARY KEY,
    ProductName TEXT NOT NULL,
    ProductUnitPrice REAL NOT NULL,
    ProductCategoryID INTEGER NOT NULL REFERENCES ProductCategory(ProductCategoryID),
    UNIQUE (ProductName)
);

CREATE TABLE IF NOT EXISTS OrderDetail (
    OrderID SERIAL PRIMARY KEY,
    CustomerID INTEGER NOT NULL REFERENCES Customer(CustomerID),
    ProductID INTEGER NOT NULL REFERENCES Product(ProductID),
    OrderDate DATE NOT NULL,
    QuantityOrdered INTEGER NOT NULL
);
"""

FILES = {
    "data": {
        "filename": "data.csv",
        "batch_size": 5000,
        "stage_table": "stage_customer"
    }
}

EXPECTED_COLUMNS = {
    "data": [
        "Name",
        "Address",
        "City",
        "Country",
        "Region",
        "ProductName"
    ]
}


def get_connection(db_url):
    conn = psycopg2.connect(db_url, connect_timeout=CONNECT_TIMEOUT)
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = %s;", (LOCK_TIMEOUT,))
        cur.execute("SET statement_timeout = %s;", (STATEMENT_TIMEOUT,))
    conn.commit()
    return conn


def drop_existing_tables(conn):
    with conn.cursor() as cur:
        for stmt in DROP_TABLES_SQL:
            try:
                cur.execute(stmt)
                conn.commit()
            except errors.LockNotAvailable:
                conn.rollback()
                print(f"Skipped drop (table busy): {stmt}")
            except Exception:
                conn.rollback()
                raise
    print("Finished dropping existing tables")


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("Tables created successfully")


def load_tsv_to_stage(conn, filepath, stage_table, expected_columns, batch_size=5000, delimiter="\t"):
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {filepath}")

    with path.open("r", encoding="utf-8-sig") as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter=delimiter)
        missing = sorted(set(expected_columns) - set(csv_reader.fieldnames))
        if missing:
            raise ValueError(f"{filepath} missing expected columns: {missing}")

        placeholders = ", ".join(["%s"] * len(expected_columns))
        sql = f"INSERT INTO {stage_table} ({', '.join(expected_columns)}) VALUES ({placeholders})"
        rows, total_count = [], 0
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {stage_table}")
        conn.commit()
        print(f"Cleaned up rows from {stage_table}")

        for row in csv_reader:
            rows.append([row.get(c, None) for c in expected_columns])
            if len(rows) == batch_size:
                extras.execute_batch(cursor, sql, rows)
                conn.commit()
                total_count += len(rows)
                rows = []
                print(f"Inserted {total_count:,} rows...")

        if rows:
            extras.execute_batch(cursor, sql, rows)
            conn.commit()
            total_count += len(rows)
            print(f"Inserted final {len(rows):,} rows; total: {total_count:,}")

        cursor.close()
        print(f"Finished loading data into {stage_table}")


def load_all_staging(conn):
    for name, meta in FILES.items():
        filename = meta["filename"]
        if not Path(filename).exists():
            print(f"Skipping {filename} (file not found)")
            continue
        stage_table = meta.get("stage_table", f"stage_{name}")
        load_tsv_to_stage(
            conn,
            filename,
            stage_table,
            EXPECTED_COLUMNS[name],
            meta.get("batch_size", 5000),
            meta.get("delimiter", "\t"),
        )


def build_dimensions(conn):
    cur = conn.cursor()

    # Region
    cur.execute("""
        INSERT INTO Region(Region)
        SELECT DISTINCT Region FROM stage_customer
        WHERE Region IS NOT NULL AND Region <> ''
        ON CONFLICT (Region) DO NOTHING;
    """)

    # Country
    cur.execute("""
        INSERT INTO Country(Country, RegionID)
        SELECT DISTINCT s.Country, r.RegionID
        FROM stage_customer s
        JOIN Region r ON s.Region = r.Region
        WHERE s.Country IS NOT NULL AND s.Country <> ''
        ON CONFLICT (Country) DO NOTHING;
    """)

    # ProductCategory - using distinct product names’ first tokens (mock logic)
    cur.execute("""
        INSERT INTO ProductCategory(ProductCategory, ProductCategoryDescription)
        SELECT DISTINCT LEFT(ProductName, 5), 'Auto-generated'
        FROM stage_customer
        WHERE ProductName IS NOT NULL AND ProductName <> ''
        ON CONFLICT (ProductCategory) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    print("Dimension tables populated")


def load_entities(conn):
    cur = conn.cursor()

    # Customer
    cur.execute("""
        INSERT INTO Customer(FirstName, LastName, Address, City, CountryID)
        SELECT
            SPLIT_PART(Name, ' ', 1),
            COALESCE(NULLIF(SPLIT_PART(Name, ' ', 2), ''), 'Unknown'),
            Address,
            City,
            c.CountryID
        FROM stage_customer s
        JOIN Country c ON s.Country = c.Country
        ON CONFLICT DO NOTHING;
    """)

    # Product
    cur.execute("""
        INSERT INTO Product(ProductName, ProductUnitPrice, ProductCategoryID)
        SELECT DISTINCT
            UNNEST(STRING_TO_ARRAY(ProductName, ';')) AS Product,
            ROUND((random() * 100 + 1)::numeric, 2) AS UnitPrice,
            1
        FROM stage_customer
        ON CONFLICT (ProductName) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    print("Entity tables populated")


def build_facts(conn):
    cur = conn.cursor()

    # OrderDetail
    cur.execute("""
        INSERT INTO OrderDetail(CustomerID, ProductID, OrderDate, QuantityOrdered)
        SELECT
            c.CustomerID,
            p.ProductID,
            CURRENT_DATE,
            FLOOR(random() * 10 + 1)
        FROM stage_customer s
        JOIN Customer c ON SPLIT_PART(s.Name, ' ', 1) = c.FirstName
        JOIN Product p ON p.ProductName = ANY(STRING_TO_ARRAY(s.ProductName, ';'))
        ON CONFLICT DO NOTHING;
    """)

    conn.commit()
    cur.close()
    print("Fact tables populated")


if __name__ == "__main__":
    DATABASE_URL = get_db_url()

    print("Creating tables...")
    conn = get_connection(DATABASE_URL)
    drop_existing_tables(conn)
    create_tables(conn)
    conn.close()
    print()

    print("Loading staging data...")
    start_time = time.monotonic()
    conn = get_connection(DATABASE_URL)
    load_all_staging(conn)
    conn.close()
    end_time = time.monotonic()
    print(f"Staging data loaded. Elapsed: {end_time - start_time:.2f}s\n")

    print("Building dimensions...")
    conn = get_connection(DATABASE_URL)
    build_dimensions(conn)
    conn.close()

    print("Loading entities...")
    conn = get_connection(DATABASE_URL)
    load_entities(conn)
    conn.close()

    print("Building facts...")
    conn = get_connection(DATABASE_URL)
    build_facts(conn)
    conn.close()

    print("\n✅ Database migration complete!")
