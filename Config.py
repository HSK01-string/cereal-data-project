# extract_config_and_pipeline.py
"""
Extract + stage cereals CSV -> staging (parquet/csv)
Safe-by-default: validations, lightweight cleaning, timestamped output, metadata.
"""

from __future__ import annotations
import sys
import os
import json
import tempfile
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, Any, Optional

import logging
import pandas as pd

# --------------------------
# 1) Configuration / constants
# --------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if file is at root
DEFAULT_RAW_PATH = PROJECT_ROOT / "data" / "raw" / "Cereals.csv"
DEFAULT_STAGING_DIR = PROJECT_ROOT / "data" / "staging"
ALLOWED_OUTPUT_FORMATS = ["parquet", "csv"]
DEFAULT_OUTPUT_FORMAT = "parquet"

# Keep numeric names consistent with your transform step if you later join them
NUMERIC_COLUMNS = [
    "calories", "protein", "fat", "sodium", "fiber", "carbohydrate", "sugars",
    "potassium", "vitamins", "shelf", "weight", "cups", "rating"
]

REQUIRED_COLUMNS = [
    "name", "mfr", "type"
] + NUMERIC_COLUMNS

# Small thresholds or rules for validation (soft checks)
VALIDATION_RULES = {
    "calories_max": 2000,
    "sugars_min": -10,   # allow placeholders like -1 (we will coerce)
    "rating_min": 0,
    "rating_max": 1000
}

# ------------------------
# 2) logger setup
# ------------------------
logger = logging.getLogger("extract")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)


# ----------------------------
# 3) Utility functions
# ----------------------------
def resolve_paths(input_path: Optional[str] = None, staging_dir: Optional[str] = None
                 ) -> Tuple[Path, Path]:
    """Return (raw_input_path, staging_dir_path) as Path objects."""
    raw_path = Path(input_path) if input_path else DEFAULT_RAW_PATH
    staging = Path(staging_dir) if staging_dir else DEFAULT_STAGING_DIR
    # staging directory is created by writer later; ensure parent exists
    if not raw_path.exists():
        logger.warning("Raw input path does not exist: %s", raw_path)
    return raw_path.resolve(), staging.resolve()


def check_file_exists(path: Path) -> bool:
    """Return True if file exists and is readable."""
    if not path.exists():
        logger.error("File not found: %s", path)
        return False
    if not os.access(path, os.R_OK):
        logger.error("File not readable: %s", path)
        return False
    return True


def read_csv(path: Path, nrows: Optional[int] = None, encoding: str = "utf-8") -> pd.DataFrame:
    """Read CSV into DataFrame with safe options."""
    logger.info("Reading CSV: %s", path)
    # pandas CSV engines: 'c' (fast, default) or 'python' (slower). We'll use default.
    df = pd.read_csv(path, low_memory=False, nrows=nrows, encoding=encoding)
    logger.info("Read rows=%d cols=%d", len(df), len(df.columns))
    return df


def validate_schema(df: pd.DataFrame,
                    required_columns: list = REQUIRED_COLUMNS,
                    rules: dict = VALIDATION_RULES,
                    strict: bool = False) -> Dict[str, Any]:
    """Basic validation: presence, dtype issues, ranges, duplicates."""
    report: Dict[str, Any] = {"ok": True, "missing_columns": [], "dtype_issues": {}, "range_warnings": {}, "duplicate_count": 0}
    # missing columns
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        report["missing_columns"] = missing
        report["ok"] = False

    # duplicates by (name, mfr) if both present
    if "name" in df.columns and "mfr" in df.columns:
        dup_count = int(df.duplicated(subset=["name", "mfr"]).sum())
        report["duplicate_count"] = dup_count

    # numeric columns: check non-numeric entries and ranges
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            # count values that cannot be coerced to numeric
            non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
            n_non_numeric = int(non_numeric.sum())
            if n_non_numeric:
                report["dtype_issues"][col] = n_non_numeric
                if strict:
                    report["ok"] = False

            # simple range checks
            try:
                numeric = pd.to_numeric(df[col], errors="coerce")
                if col == "calories" and numeric.min(skipna=True) < 0:
                    report["range_warnings"].setdefault(col, []).append("negative_values")
                if col == "rating":
                    rmin = rules.get("rating_min")
                    rmax = rules.get("rating_max")
                    if rmin is not None and numeric.min(skipna=True) < rmin:
                        report["range_warnings"].setdefault(col, []).append("below_min")
                    if rmax is not None and numeric.max(skipna=True) > rmax:
                        report["range_warnings"].setdefault(col, []).append("above_max")
            except Exception:
                # defensive
                report["dtype_issues"].setdefault(col, "check_failed")

    if report["range_warnings"] and strict:
        report["ok"] = False

    return report


def basic_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Lightweight cleaning: normalize column names, trim strings, coerce numeric, add id."""
    # normalize column names to lowercase/no surrounding whitespace
    df = df.rename(columns=lambda c: str(c).strip().lower())

    # strip whitespace in object columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # canonicalize some columns if present
    if "mfr" in df.columns:
        df["mfr"] = df["mfr"].astype(str).str.upper().str[0]

    if "type" in df.columns:
        df["type"] = df["type"].astype(str).str.upper().str[0]
        df["type"] = df["type"].where(df["type"].isin(["C", "H"]), other="C")

    # coerce numeric columns listed above
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # treat placeholder -1 for potass (if present)
    if "potass" in df.columns:
        df.loc[df["potass"] == -1, "potass"] = pd.NA
    # add a simple trace id if not present
    if "source_row_id" not in df.columns:
        import hashlib

        def row_hash(r):
            base = f"{r.get('name', '')}|{r.get('mfr', '')}"
            return hashlib.md5(base.encode("utf-8")).hexdigest()[:10]

        df["source_row_id"] = df.apply(row_hash, axis=1)

    return df


def write_staging(df: pd.DataFrame, staging_dir: Path, output_format: str = DEFAULT_OUTPUT_FORMAT,
                  prefix: str = "cereal", overwrite: bool = False) -> Path:
    """Write DataFrame to staging directory; return final path."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if output_format == "parquet":
        target = staging_dir / f"{prefix}_{ts}.parquet"
        tmp = staging_dir / f"{prefix}_{ts}.parquet.tmp"
        # prefer pyarrow if installed, else rely on default engine
        df.to_parquet(tmp, index=False)
        if target.exists() and not overwrite:
            raise FileExistsError(target)
        tmp.rename(target)
    else:
        target = staging_dir / f"{prefix}_{ts}.csv"
        tmp = staging_dir / f"{prefix}_{ts}.csv.tmp"
        df.to_csv(tmp, index=False)
        if target.exists() and not overwrite:
            raise FileExistsError(target)
        tmp.rename(target)

    logger.info("Wrote staging file: %s", target)
    return target


def emit_metadata(stats: Dict[str, Any], meta_path: Path) -> None:
    """Write metadata JSON to meta_path"""
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, indent=2, default=str)
    logger.info("Wrote metadata: %s", meta_path)


# ---------------------------
# 4) Main orchestration
# ---------------------------
def extract_and_transform_cereal_data(input_path: str = None,
                                      staging_dir: str = None,
                                      output_format: str = DEFAULT_OUTPUT_FORMAT,
                                      overwrite: bool = False,
                                      dry_run: bool = False,
                                      strict: bool = False) -> int:
    logger.info("Starting extract_and_transform_cereal_data")
    raw_path, staging = resolve_paths(input_path, staging_dir)

    if not check_file_exists(raw_path):
        logger.error("Input file check failed: %s", raw_path)
        return 2

    try:
        df = read_csv(raw_path)
    except Exception as e:
        logger.exception("Failed to read CSV: %s", e)
        return 3

    report = validate_schema(df, strict=strict)
    logger.info("Validation report: %s", report)
    if not report.get("ok", False):
        logger.error("Validation failed (strict=%s). Aborting.", strict)
        return 4

    df_clean = basic_cleanup(df)

    if dry_run:
        logger.info("Dry-run enabled; skipping write. Rows after clean: %d", len(df_clean))
        return 0

    try:
        out_path = write_staging(df_clean, staging, output_format, overwrite=overwrite)
    except FileExistsError as e:
        logger.error("Output exists and overwrite=False: %s", e)
        return 6
    except Exception as e:
        logger.exception("Failed to write staging: %s", e)
        return 5

    meta = {
        "rows_read": len(df),
        "rows_written": len(df_clean),
        "input_file": str(raw_path),
        "output_file": str(out_path),
        "run_timestamp": datetime.utcnow().isoformat()
    }
    meta_path = out_path.with_suffix(out_path.suffix + ".meta.json")
    emit_metadata(meta, meta_path)
    logger.info("Pipeline completed successfully.")
    return 0


# ---------------------------
# 5) CLI entrypoint
# ---------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract and stage Cereals dataset")
    parser.add_argument("--input", "-i", help="Path to raw cereals CSV", default=str(DEFAULT_RAW_PATH))
    parser.add_argument("--staging-dir", "-s", help="Staging directory", default=str(DEFAULT_STAGING_DIR))
    parser.add_argument("--format", "-f", choices=ALLOWED_OUTPUT_FORMATS, default=DEFAULT_OUTPUT_FORMAT)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite staging file if exists")
    parser.add_argument("--dry-run", action="store_true", help="Run checks but do not write outputs")
    parser.add_argument("--strict", action="store_true", help="Treat validation warnings as errors")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    rc = extract_and_transform_cereal_data(
        input_path=args.input,
        staging_dir=args.staging_dir,
        output_format=args.format,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        strict=args.strict
    )
    sys.exit(rc)
