"""
transform.py

Transforms cereals staging data into a curated, analysis-ready dataset.

Responsibilities:
- Read staging artifact (CSV or Parquet) produced by extract step
- Normalize column names and categorical codes
- Coerce numeric columns (safe conversion with NaN for invalids)
- Treat dataset placeholders (e.g., -1) as NaN for selected numeric columns
- Deduplicate on (name, mfr) keeping the highest rating
- Basic validation and summary report
- Write curated output to data/processed/<prefix>_YYYYMMDD_HHMMSS.parquet (or CSV)

Usage (example):
    python src/transform.py --input data/staging/cereal_20251008_120000.parquet \
                           --output-dir data/processed --format parquet --dedupe
"""
from __future__ import annotations
import sys
import os
from pathlib import Path
from datetime import datetime
import json
import logging
from typing import Tuple, Dict, Any, Optional

import pandas as pd

# -------- CONFIG / CONSTANTS ----------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
DEFAULT_INPUT = PROJECT_ROOT / "data" / "staging"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
NUMERIC_COLUMNS = [
    "calories","protein","fat","sodium","fiber","carbohydrates",
    "sugars","potassium","vitamins","shelf","weight","cups","rating"
]
REQUIRED_COLUMNS = [
    "name","mfr","type"
] + NUMERIC_COLUMNS
PLACEHOLDER_NEGATIVE_ONE = ["potassium"]  # treat -1 in these columns as missing

# -------- LOGGER ----------------------------
logger = logging.getLogger("transform")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)

# --------- UTILS ----------------------------
def _resolve_input_path(input_path: Optional[str]) -> Path:
    """Resolve input path; if directory provided, attempt to pick latest file inside."""
    if input_path is None:
        raise ValueError("input_path must be provided")
    p = Path(input_path)
    if p.is_dir():
        # choose latest modified file in directory
        files = sorted([f for f in p.iterdir() if f.is_file()], key=lambda x: x.stat().st_mtime, reverse=True)
        if not files:
            raise FileNotFoundError(f"No files found in staging dir: {p}")
        return files[0]
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")
    return p


def read_staging(path: Path, nrows: Optional[int] = None) -> pd.DataFrame:
    """Read CSV or Parquet staging file into a DataFrame."""
    logger.info("Reading staging file: %s", path)
    if path.suffix.lower() in [".parquet", ".pq"]:
        df = pd.read_parquet(path)
    else:
        # CSV or other comma-separated file
        df = pd.read_csv(path, low_memory=False, nrows=nrows)
    logger.info("Read %d rows and %d columns", len(df), len(df.columns))
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: strip, lower, remove surrounding whitespace."""
    df = df.rename(columns=lambda c: str(c).strip())
    # we keep case of letters but ensure consistent spelling; not forcing lowercase to preserve original mapping
    return df


def coerce_numeric_columns(df: pd.DataFrame, numeric_cols=NUMERIC_COLUMNS) -> pd.DataFrame:
    """Coerce numeric columns safely; invalid parsing -> NaN."""
    for col in numeric_cols:
        if col in df.columns:
            # Remove stray characters, then convert
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def treat_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """Convert dataset-specific placeholder values (like -1) to NaN for some columns."""
    for col in PLACEHOLDER_NEGATIVE_ONE:
        if col in df.columns:
            df.loc[df[col] == -1, col] = pd.NA
    return df


def normalize_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize small categorical codes: mfr -> uppercase single char, type -> 'C' or 'H'."""
    if "mfr" in df.columns:
        df["mfr"] = df["mfr"].astype(str).str.strip().str.upper().str[0]
    if "type" in df.columns:
        # coerce type to single uppercase char; default to 'C' if unknown
        df["type"] = df["type"].astype(str).str.strip().str.upper().str[0].replace({None: "C"})
        df["type"] = df["type"].where(df["type"].isin(["C", "H"]), other="C")
    return df


def create_traceability(df: pd.DataFrame) -> pd.DataFrame:
    """Add a source_row_id (hash) for traceability and dedupe reference."""
    if "source_row_id" not in df.columns:
        # create a compact hash of the row's name + mfr to track provenance
        def _row_hash(row):
            base = f"{row.get('name','')}-{row.get('mfr','')}"
            return abs(hash(base))  # deterministic per run; if you want stronger, use hashlib
        df["source_row_id"] = df.apply(_row_hash, axis=1)
    return df


def deduplicate(df: pd.DataFrame, subset=("name", "mfr"), keep_by="rating") -> Tuple[pd.DataFrame,int]:
    """
    Deduplicate rows based on subset columns.
    Keep the row with the highest 'keep_by' value when duplicates exist.
    Returns (deduped_df, removed_count).
    """
    if not all(c in df.columns for c in subset):
        logger.warning("deduplicate: one of the subset columns not present; skipping dedup.")
        return df, 0

    # sort so that highest rating comes first
    if keep_by in df.columns:
        df_sorted = df.sort_values(by=list(subset) + [keep_by], ascending=[True]*len(subset) + [False])
    else:
        df_sorted = df.sort_values(by=list(subset))
    before = len(df_sorted)
    df_dedup = df_sorted.drop_duplicates(subset=list(subset), keep="first").reset_index(drop=True)
    after = len(df_dedup)
    removed = before - after
    logger.info("Deduplication removed %d rows (from %d to %d)", removed, before, after)
    return df_dedup, removed


def validate_post_transform(df: pd.DataFrame) -> Dict[str, Any]:
    """Run a few sanity checks and return a small report dict."""
    report = {"rows": len(df), "columns": list(df.columns), "missing_required": [], "numeric_nulls": {}}
    # required column presence
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    report["missing_required"] = missing
    # numeric null counts
    for c in NUMERIC_COLUMNS:
        if c in df.columns:
            report["numeric_nulls"][c] = int(df[c].isna().sum())
    # simple bounds check on calories and rating
    if "calories" in df.columns:
        if df["calories"].min(skipna=True) < 0:
            report.setdefault("warnings", []).append("calories_has_negative")
    if "rating" in df.columns:
        # rating should typically be >0
        if df["rating"].isna().all():
            report.setdefault("errors", []).append("rating_all_missing")
    return report


def write_curated(df: pd.DataFrame, output_dir: Path, fmt: str = "parquet", prefix: str = "cereals_curated") -> Path:
    """Write curated DataFrame to output_dir with timestamped filename. Returns Path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if fmt == "parquet":
        out_path = output_dir / f"{prefix}_{ts}.parquet"
        # write atomically to temp and rename
        tmp = out_path.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=False)
        tmp.rename(out_path)
    else:
        out_path = output_dir / f"{prefix}_{ts}.csv"
        tmp = out_path.with_suffix(".csv.tmp")
        df.to_csv(tmp, index=False)
        tmp.rename(out_path)
    logger.info("Wrote curated file to: %s", out_path)
    return out_path


def write_report(report: Dict[str, Any], output_dir: Path, prefix: str = "transform_report") -> Path:
    """Write JSON report to output_dir and return path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{prefix}_{ts}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    logger.info("Wrote transform report to: %s", path)
    return path


# -------- MAIN ORCHESTRATION ----------------
def transform_staging_to_curated(input_path: str,
                                 output_dir: str = None,
                                 fmt: str = "parquet",
                                 dedupe_flag: bool = True,
                                 nrows: Optional[int] = None) -> int:
    """Orchestrate transform step. Returns 0 on success, non-zero on error."""
    try:
        input_p = _resolve_input_path(input_path)
    except Exception as exc:
        logger.error("Failed to resolve input path: %s", exc)
        return 2

    try:
        df = read_staging(input_p, nrows=nrows)
    except Exception as exc:
        logger.error("Failed to read staging file: %s", exc)
        return 3

    # Normalize and clean
    df = normalize_column_names(df)
    df = coerce_numeric_columns(df)
    df = treat_placeholders(df)
    df = normalize_categorical(df)
    df = create_traceability(df)

    # Deduplicate if requested
    removed = 0
    if dedupe_flag:
        df, removed = deduplicate(df, subset=("name", "mfr"), keep_by="rating")

    # Validate
    report = validate_post_transform(df)
    report["deduplicated_rows"] = int(removed)
    report["input_file"] = str(input_p)
    report["run_timestamp"] = datetime.utcnow().isoformat()

    # If required columns missing -> treat as error
    if report.get("missing_required"):
        logger.error("Missing required columns after transform: %s", report["missing_required"])
        write_report(report, Path(output_dir or DEFAULT_OUTPUT_DIR))
        return 4

    # Write curated artifact
    try:
        out_path = write_curated(df, Path(output_dir or DEFAULT_OUTPUT_DIR), fmt=fmt)
    except Exception as exc:
        logger.exception("Failed to write curated output: %s", exc)
        report["write_error"] = str(exc)
        write_report(report, Path(output_dir or DEFAULT_OUTPUT_DIR))
        return 5

    # finalize report
    report["rows_written"] = len(df)
    report["output_file"] = str(out_path)
    write_report(report, Path(output_dir or DEFAULT_OUTPUT_DIR))
    logger.info("Transform completed successfully. Rows: %d", len(df))
    return 0


# -------- CLI ENTRYPOINT --------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("transform.py - cereals staging -> curated")
    parser.add_argument("--input", "-i", help="Input staging file or directory", required=True)
    parser.add_argument("--output-dir", "-o", help="Output directory for curated files",
                        default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--format", "-f", choices=["parquet", "csv"], default="parquet",
                        help="Output format")
    parser.add_argument("--no-dedupe", action="store_true", help="Disable deduplication step")
    parser.add_argument("--nrows", type=int, help="Read only nrows (for quick tests)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log_level))

    rc = transform_staging_to_curated(
        input_path=args.input,
        output_dir=args.output_dir,
        fmt=args.format,
        dedupe_flag=not args.no_dedupe,
        nrows=args.nrows
    )
    sys.exit(rc)
