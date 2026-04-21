"""
sql_to_tsv_stream.py

- Uses psycopg2 and a server-side named cursor to stream large results.
- Writes TSV incrementally with a configurable fetch size.

Requirements:
    pip install psycopg2
"""

import csv
import logging
import os
import sys
import uuid
from pathlib import Path

import dotenv
import psycopg2
from psycopg2.extras import DictCursor

dotenv.load_dotenv()


def setup_logging(log_file: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="a"),
        ],
    )


def get_sql_files(source_dir: Path):
    return sorted(
        [
            p
            for p in source_dir.iterdir()
            if p.is_file() and p.suffix.lower() == ".sql"
        ]
    )

def write_rows_tsv_stream(cur, out_path: Path, fetch_size: int):
    """
    cur: a server-side cursor that has already executed a query
    Fetches rows in chunks and writes them to out_path as TSV.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        # Column names
        columns = (
            [desc.name for desc in cur.description]
            if cur.description is not None
            else []
        )
        if columns:
            writer.writerow(columns)
        else:
            # no result columns -> nothing to write
            return

        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            # rows are dict-like (DictCursor) if created appropriately
            for row in rows:
                # If row is a psycopg2.extras.DictRow, indexing by column name works.
                writer.writerow([row.get(col) for col in columns])

def run_sql_file_stream(conn, sql_text: str, out_path: Path, fetch_size: int):
    """
    Creates a server-side cursor with a unique name, executes sql_text,
    and streams results to TSV. Rolls back on error to close the transaction cleanly.
    """
    # Use a unique name for the server-side cursor
    cursor_name = "c_" + uuid.uuid4().hex
    # psycopg2 will create a named cursor if you pass name param
    with conn.cursor(name=cursor_name, cursor_factory=DictCursor) as cur:
        # Execute inside a transaction (server-side cursor requires it)
        cur.itersize = fetch_size  # hints psycopg2 for fetchmany performance
        cur.execute(sql_text)
        cur.fetchmany(size=0)
        if cur.description is None:
            # No rows to fetch (e.g., DDL/DML). Create an empty file or skip as desired.
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("", encoding="utf-8")
            return
        write_rows_tsv_stream(cur, out_path, fetch_size)


def main():
    source_dir = Path(os.environ.get("SOURCE_DIR"))
    dest_dir = Path(os.environ.get("DEST_DIR"))
    log_file = Path(os.environ.get("LOG_FILE"))

    setup_logging(log_file)

    if not source_dir.exists() or not source_dir.is_dir():
        logging.error(
            "Source directory does not exist or is not a directory: %s",
            source_dir,
        )
        sys.exit(2)

    sql_files = get_sql_files(source_dir)
    if not sql_files:
        logging.info("No .sql files found in %s", source_dir)
        return

    schema = os.environ.get("DB_SCHEMA")

    conn_info = dict(
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USERNAME"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
        sslmode="require",
        options=f"-c search_path={schema}",
    )

    try:
        conn = psycopg2.connect(**conn_info)
    except Exception as e:
        logging.exception("Failed to connect to database: %s", e)
        sys.exit(3)

    # Ensure we are not in autocommit mode because server-side cursors require a transaction
    conn.autocommit = False

    # Try to set read-only transaction by default
    try:
        with conn.cursor() as startup_cur:
            startup_cur.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;"
            )
    except Exception:
        # not fatal
        pass

    errors = 0
    for sql_path in sql_files:
        logging.info("Processing %s", sql_path.name)
        try:
            sql_text = sql_path.read_text(encoding="utf-8")
        except Exception as e:
            logging.exception("Failed to read %s: %s", sql_path, e)
            errors += 1
            continue

        out_filename = sql_path.stem + ".tsv"
        out_path = dest_dir / out_filename

        try:
            # Each file executes in its own transaction so the server-side cursor is closed when we commit/rollback
            run_sql_file_stream(
                conn, sql_text, out_path, int(os.environ.get("FETCH_SIZE"))
            )
            if True:
                conn.commit()
            else:
                # rollback to end the read-only transaction and free resources (server-side cursor will be closed)
                conn.rollback()
            logging.info("Wrote results to %s", out_path)
        except Exception as e:
            logging.exception("Failed executing %s: %s", sql_path.name, e)
            try:
                conn.rollback()
            except Exception:
                pass
            errors += 1

    conn.close()
    logging.info(
        "Completed. %d file(s) processed, %d error(s).", len(sql_files), errors
    )

if __name__ == "__main__":
    main()
