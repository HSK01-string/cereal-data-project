"""
load.py

Load curated cereals data into MySQL using SQLAlchemy.

Responsibilities:
- Connect to MySQL using a DB URL (env or CLI)
- Create normalized schema if not exists (manufacturers, cereals, nutrition)
- Upsert (insert or update) manufacturer records
- Upsert cereals and nutrition records in a transaction
- Provide CLI flags for dry-run, input path, batch size, and log level

Security:
- Reads DB credentials from environment variables by default: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
- Alternatively accepts a full SQLAlchemy DB URL via --db-url

Usage examples:
  # set env vars (do not commit .env)
  export DB_USER=cereals_user
  export DB_PASSWORD=secret
  export DB_HOST=localhost
  export DB_PORT=3306
  export DB_NAME=cereals_db

  # run loader against a curated CSV
  python src/load.py --input data/processed/cereals_curated_20251009_102136.csv --dry-run

  # run actual load
  python src/load.py --input data/processed/cereals_curated_20251009_102136.csv

Notes:
- This script is designed for small-to-medium datasets used in a portfolio project.
- For very large datasets, consider bulk-loading strategies (LOAD DATA INFILE, chunked COPY, or file-based stages).
"""

from __future__ import annotations
import os
import sys
import csv
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData, Column, Integer, String, Enum, Float, Numeric, ForeignKey
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -------------------- CONFIG & LOGGER --------------------
logger = logging.getLogger("loader")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)

DEFAULT_DB_PORT = os.getenv("DB_PORT", "3306")

# -------------------- DB SCHEMA DDL (SQLAlchemy Meta) --------------------
# We create three tables: manufacturers, cereals, nutrition

def get_engine(db_url: Optional[str] = None) -> Engine:
    """Create SQLAlchemy engine from DB URL or environment variables."""
    if db_url:
        url = db_url
    else:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", DEFAULT_DB_PORT)
        db = os.getenv("DB_NAME", "cereals_db")
        if not user or not password:
            raise ValueError("DB credentials missing. Set DB_USER and DB_PASSWORD environment variables or pass --db-url.")
        url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    logger.debug("Using DB URL: %s", url)
    engine = create_engine(url, pool_pre_ping=True)
    return engine


def create_schema(engine: Engine) -> None:
    """Create tables if they do not exist. Idempotent."""
    meta = MetaData()
    manufacturers = Table(
        "manufacturers", meta,
        Column("mfr_code", String(1), primary_key=True),
        Column("name", String(255), nullable=True),
        Column("created_at", String(64), nullable=False)
    )

    cereals = Table(
        "cereals", meta,
        Column("cereal_id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(255), nullable=False, unique=True),
        Column("mfr_code", String(1), nullable=False),
        Column("type", Enum('C', 'H'), nullable=False, server_default='C'),
        Column("shelf", Integer, nullable=True),
        Column("weight", Numeric(5,2), nullable=True),
        Column("cups", Numeric(5,3), nullable=True),
        Column("rating", Numeric(8,4), nullable=True),
        Column("created_at", String(64), nullable=False)
    )

    nutrition = Table(
        "nutrition", meta,
        Column("cereal_id", Integer, ForeignKey("cereals.cereal_id", ondelete='CASCADE'), primary_key=True),
        Column("calories", Integer, nullable=True),
        Column("protein", Integer, nullable=True),
        Column("fat", Integer, nullable=True),
        Column("sodium", Integer, nullable=True),
        Column("fiber", Float, nullable=True),
        Column("carbo", Float, nullable=True),
        Column("sugars", Integer, nullable=True),
        Column("potass", Integer, nullable=True),
        Column("vitamins", Integer, nullable=True)
    )

    meta.create_all(engine)
    logger.info("Ensured tables exist: manufacturers, cereals, nutrition")


# -------------------- UPSERT HELPERS --------------------

def upsert_manufacturers(conn, df: pd.DataFrame) -> int:
    """Upsert manufacturer codes from df into manufacturers table.
    Returns number of upserts attempted.
    """
    if "mfr" not in df.columns:
        logger.warning("No 'mfr' column in input; skipping manufacturers upsert")
        return 0
    codes = sorted(df['mfr'].astype(str).str.strip().unique())
    rows = 0
    for code in codes:
        if not code:
            continue
        now = datetime.utcnow().isoformat()
        # Use INSERT ... ON DUPLICATE KEY UPDATE pattern for MySQL
        stmt = text(
            "INSERT INTO manufacturers (mfr_code, name, created_at) VALUES (:code, NULL, :now) "
            "ON DUPLICATE KEY UPDATE created_at = :now"
        )
        conn.execute(stmt, {"code": code, "now": now})
        rows += 1
    logger.info("Upserted %d manufacturer codes", rows)
    return rows


def upsert_cereals_and_nutrition(conn, df: pd.DataFrame, batch_size: int = 500) -> Tuple[int,int]:
    """Upsert cereals and nutrition.
    Strategy (simple, robust for small datasets):
      - For each row, try to find existing cereal by name.
      - If exists, update cereals table and nutrition table via cereal_id.
      - If not exists, insert into cereals, fetch cereal_id, then insert nutrition.
    Returns (rows_processed, rows_skipped)
    """
    processed = 0
    skipped = 0
    # Define mapping of columns expected for cereals and nutrition
    cereal_cols = ["name","mfr","type","shelf","weight","cups","rating"]
    nutrition_cols = ["calories","protein","fat","sodium","fiber","carbo","sugars","potass","vitamins"]

    for idx, row in df.iterrows():
        processed += 1
        try:
            # find existing cereal by name (unique)
            res = conn.execute(text("SELECT cereal_id FROM cereals WHERE name = :name"), {"name": row['name']}).fetchone()
            now = datetime.utcnow().isoformat()
            if res:
                cereal_id = int(res[0])
                # update cereals
                update_stmt = text(
                    "UPDATE cereals SET mfr_code=:mfr, type=:type, shelf=:shelf, weight=:weight, cups=:cups, rating=:rating WHERE cereal_id=:cereal_id"
                )
                conn.execute(update_stmt, {
                    "mfr": row.get('mfr'),
                    "type": row.get('type'),
                    "shelf": int(row['shelf']) if pd.notna(row.get('shelf')) else None,
                    "weight": float(row['weight']) if pd.notna(row.get('weight')) else None,
                    "cups": float(row['cups']) if pd.notna(row.get('cups')) else None,
                    "rating": float(row['rating']) if pd.notna(row.get('rating')) else None,
                    "cereal_id": cereal_id
                })
                # upsert nutrition by cereal_id
                # try update first
                update_nut = text(
                    "UPDATE nutrition SET calories=:calories, protein=:protein, fat=:fat, sodium=:sodium, "
                    "fiber=:fiber, carbo=:carbo, sugars=:sugars, potass=:potass, vitamins=:vitamins WHERE cereal_id=:cereal_id"
                )
                result = conn.execute(update_nut, {**{k: (int(row[k]) if pd.notna(row.get(k)) else None) for k in ['calories','protein','fat','sodium','sugars','potass','vitamins']},
                                                    "fiber": float(row['fiber']) if pd.notna(row.get('fiber')) else None,
                                                    "carbo": float(row['carbo']) if pd.notna(row.get('carbo')) else None,
                                                    "cereal_id": cereal_id})
                # if no rows updated, insert
                if result.rowcount == 0:
                    insert_nut = text(
                        "INSERT INTO nutrition (cereal_id, calories, protein, fat, sodium, fiber, carbo, sugars, potass, vitamins) "
                        "VALUES (:cereal_id, :calories, :protein, :fat, :sodium, :fiber, :carbo, :sugars, :potass, :vitamins)"
                    )
                    conn.execute(insert_nut, {
                        "cereal_id": cereal_id,
                        "calories": int(row['calories']) if pd.notna(row.get('calories')) else None,
                        "protein": int(row['protein']) if pd.notna(row.get('protein')) else None,
                        "fat": int(row['fat']) if pd.notna(row.get('fat')) else None,
                        "sodium": int(row['sodium']) if pd.notna(row.get('sodium')) else None,
                        "fiber": float(row['fiber']) if pd.notna(row.get('fiber')) else None,
                        "carbo": float(row['carbo']) if pd.notna(row.get('carbo')) else None,
                        "sugars": int(row['sugars']) if pd.notna(row.get('sugars')) else None,
                        "potass": int(row['potass']) if pd.notna(row.get('potass')) else None,
                        "vitamins": int(row['vitamins']) if pd.notna(row.get('vitamins')) else None
                    })
            else:
                # insert cereal
                insert_cereal = text(
                    "INSERT INTO cereals (name, mfr_code, type, shelf, weight, cups, rating, created_at) "
                    "VALUES (:name, :mfr, :type, :shelf, :weight, :cups, :rating, :now)"
                )
                res_ins = conn.execute(insert_cereal, {
                    "name": row['name'],
                    "mfr": row.get('mfr'),
                    "type": row.get('type') if pd.notna(row.get('type')) else 'C',
                    "shelf": int(row['shelf']) if pd.notna(row.get('shelf')) else None,
                    "weight": float(row['weight']) if pd.notna(row.get('weight')) else None,
                    "cups": float(row['cups']) if pd.notna(row.get('cups')) else None,
                    "rating": float(row['rating']) if pd.notna(row.get('rating')) else None,
                    "now": now
                })
                # fetch last inserted id
                cereal_id = int(res_ins.lastrowid)
                # insert nutrition
                insert_nut = text(
                    "INSERT INTO nutrition (cereal_id, calories, protein, fat, sodium, fiber, carbo, sugars, potass, vitamins) "
                    "VALUES (:cereal_id, :calories, :protein, :fat, :sodium, :fiber, :carbo, :sugars, :potass, :vitamins)"
                )
                conn.execute(insert_nut, {
                    "cereal_id": cereal_id,
                    "calories": int(row['calories']) if pd.notna(row.get('calories')) else None,
                    "protein": int(row['protein']) if pd.notna(row.get('protein')) else None,
                    "fat": int(row['fat']) if pd.notna(row.get('fat')) else None,
                    "sodium": int(row['sodium']) if pd.notna(row.get('sodium')) else None,
                    "fiber": float(row['fiber']) if pd.notna(row.get('fiber')) else None,
                    "carbo": float(row['carbo']) if pd.notna(row.get('carbo')) else None,
                    "sugars": int(row['sugars']) if pd.notna(row.get('sugars')) else None,
                    "potass": int(row['potass']) if pd.notna(row.get('potass')) else None,
                    "vitamins": int(row['vitamins']) if pd.notna(row.get('vitamins')) else None
                })
        except Exception as exc:
            logger.exception("Failed to upsert row index %s name=%s: %s", idx, row.get('name'), exc)
            skipped += 1
            continue
    logger.info("Processed %d rows, skipped %d", processed, skipped)
    return processed, skipped


# -------------------- MAIN --------------------

def main(input_path: str, db_url: Optional[str] = None, dry_run: bool = False, batch_size: int = 500, log_level: str = "INFO") -> int:
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    input_p = Path(input_path)
    if not input_p.exists():
        logger.error("Input file not found: %s", input_p)
        return 2

    # read curated file via pandas
    logger.info("Reading curated input: %s", input_p)
    if input_p.suffix.lower() in [".parquet", ".pq"]:
        df = pd.read_parquet(input_p)
    else:
        df = pd.read_csv(input_p)

    # minimal column checks
    expected = set(["name","mfr","type","calories","protein","fat","sodium","fiber","carbo","sugars","potass","vitamins","shelf","weight","cups","rating"])
    missing = expected - set(df.columns)
    if missing:
        logger.warning("Input is missing expected columns: %s", missing)

    if dry_run:
        logger.info("Dry run: would process %d rows", len(df))
        return 0

    # connect to DB and create schema
    try:
        engine = get_engine(db_url)
    except Exception as exc:
        logger.exception("Failed to construct DB engine: %s", exc)
        return 3

    try:
        with engine.begin() as conn:
            create_schema(engine)
            # upsert manufacturers
            upsert_manufacturers(conn, df)
            # upsert cereals & nutrition
            processed, skipped = upsert_cereals_and_nutrition(conn, df, batch_size=batch_size)
    except SQLAlchemyError as exc:
        logger.exception("DB error during load: %s", exc)
        return 4
    except Exception as exc:
        logger.exception("Unexpected error during load: %s", exc)
        return 5

    logger.info("Load complete. Processed=%d Skipped=%d", processed, skipped)
    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser("load.py - load curated cereals into MySQL")
    parser.add_argument("--input", "-i", required=True, help="Path to curated CSV/Parquet file")
    parser.add_argument("--db-url", help="Optional full SQLAlchemy DB URL (overrides env vars)")
    parser.add_argument("--dry-run", action="store_true", help="Do everything except write to DB")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for iterative operations")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = parser.parse_args()

    rc = main(args.input, db_url=args.db_url, dry_run=args.dry_run, batch_size=args.batch_size, log_level=args.log_level)
    sys.exit(rc)
