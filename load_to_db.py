import pandas as pd
from sqlalchemy import create_engine, text, String, Date
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from pathlib import Path
import uuid
import time
import traceback
from datetime import datetime
import re

# ────────── DB CONFIG ──────────
DB_URI = "postgresql+psycopg://postgres:1234@localhost:5432/healthcare_db"
engine = create_engine(DB_URI)
DATA_PATH = Path("C:/Users/irahman2/Documents/dataset_gen/data_raw/output_1")
LOG_PATH = Path("etl_errors.txt")

# ────────── TABLE CONFIGS ──────────
tables = [
    "patients", "encounters", "observations",
    "conditions", "medications", "procedures"
]

uuid_cols = {
    "patients": ["id"],
    "encounters": ["id", "patient"],
    "observations": ["patient", "encounter"],
    "conditions": ["patient", "encounter"],
    "medications": ["patient", "encounter"],
    "procedures": ["patient", "encounter"],
}

date_cols = {
    "patients": ["birthdate", "deathdate"],
    "encounters": ["date"],
    "observations": ["date"],
    "conditions": ["start", "stop"],
    "medications": ["start", "stop"],
    "procedures": ["date"],
}

dtype_map = {
    "id": PG_UUID(as_uuid=True),
    "patient": PG_UUID(as_uuid=True),
    "encounter": PG_UUID(as_uuid=True),
    "birthdate": Date(),
    "deathdate": Date(),
    "date": Date(),
    "start": Date(),
    "stop": Date(),
}

UUID_RE = re.compile(r'^[0-9a-fA-F-]{36}$')

def safe_uuid(val):
    if pd.isna(val) or not isinstance(val, str):
        return None
    val = val.strip()
    return uuid.UUID(val) if UUID_RE.match(val) else None

def log_error(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n\n")

# ────────── LOAD LOOP ──────────
start_time = time.time()
log_error("==== ETL RUN START ====\n")

conn = engine.connect()

for tbl in tables:
    try:
        with conn.begin():
            print(f"\n🧩 Loading {tbl}...")

            try:
                conn.execute(text(f"TRUNCATE TABLE healthcare_demo.{tbl} CASCADE;"))
            except Exception as e:
                log_error(f"⚠️ Could not truncate {tbl}: {e}")

            csv_path = DATA_PATH / f"{tbl}.csv"
            if not csv_path.exists():
                msg = f"{tbl}.csv not found, skipping."
                print(f"⚠️ {msg}")
                log_error(msg)
                continue

            df = pd.read_csv(csv_path, on_bad_lines="skip", dtype=str)
            df.columns = [c.strip().lower() for c in df.columns]
            print(f"→ {len(df)} rows read from {csv_path.name}")

            # UUID conversion
            for col in uuid_cols.get(tbl, []):
                if col in df.columns:
                    df[col] = df[col].apply(safe_uuid)

            # Drop invalid or missing IDs
            if "id" in df.columns:
                before = len(df)
                bad_rows = df[df["id"].isnull()]
                df = df[df["id"].notnull()]
                dropped = before - len(df)
                if dropped > 0:
                    bad_path = DATA_PATH / f"bad_rows_{tbl}.csv"
                    bad_rows.to_csv(bad_path, index=False)
                    log_error(f"⚠️ Dropped {dropped} invalid ID rows from {tbl}. Saved to {bad_path.name}")

            # Referential integrity checks
            if "patient" in df.columns and tbl != "patients":
                valid_patients = pd.read_sql("SELECT id FROM healthcare_demo.patients", conn)
                valid_patients_set = set(valid_patients["id"])
                before = len(df)
                df = df[df["patient"].isin(valid_patients_set)]
                dropped = before - len(df)
                if dropped > 0:
                    log_error(f"⚠️ Dropped {dropped} records from {tbl} due to missing patient references.")

            if "encounter" in df.columns and tbl not in ["patients", "encounters"]:
                valid_encounters = pd.read_sql("SELECT id FROM healthcare_demo.encounters", conn)
                valid_encounters_set = set(valid_encounters["id"])
                before = len(df)
                df = df[df["encounter"].isin(valid_encounters_set)]
                dropped = before - len(df)
                if dropped > 0:
                    log_error(f"⚠️ Dropped {dropped} records from {tbl} due to missing encounter references.")

            # Date conversions
            for col in date_cols.get(tbl, []):
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

            df = df.where(pd.notnull(df), None)

            df.to_sql(
                tbl,
                conn,
                schema="healthcare_demo",
                if_exists="append",
                index=False,
                dtype={c: dtype_map.get(c, String()) for c in df.columns},
                chunksize=1000,
                method="multi",
            )

            print(f"✅ {tbl} loaded ({len(df)} rows)")
    except Exception as e:
        err = traceback.format_exc(limit=3)
        print(f"❌ Error loading {tbl}: {e}")
        log_error(f"Error in table {tbl}:\n{e}\n{err}")

conn.close()

elapsed = round(time.time() - start_time, 2)
summary = f"🎯 ETL complete in {elapsed} seconds.\n"
print(f"\n{summary}")
log_error(summary)
log_error("==== ETL RUN END ====\n\n")
