import duckdb
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def query_duckdb_from_gcs(
    gcs_db_path: str,
    key_id: str,
    secret: str,
    query: str,
    alias: str = "remote_db",
    output_csv_path: str = None
):
    """
    Attach and query a DuckDB database stored in GCS (read-only), with logs and timing.

    Parameters:
    - gcs_db_path: str → GCS URI (e.g., 'gs://bucket-name/path/to/file.duckdb')
    - key_id: str → HMAC key ID
    - secret: str → HMAC secret
    - query: str → SQL query to execute (use alias.table_name if needed)
    - alias: str → Alias for the attached DuckDB database

    Returns:
    - Pandas DataFrame with query result
    """
    logging.info("Connecting to DuckDB...")
    t0 = time.time()
    con = duckdb.connect()
    logging.info(f"Connected to DuckDB (Time taken: {time.time() - t0:.2f}s)")

    logging.info("Installing and loading httpfs extension...")
    t1 = time.time()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    logging.info(f"httpfs extension loaded (Time taken: {time.time() - t1:.2f}s)")

    logging.info("Configuring GCS credentials...")
    t2 = time.time()
    con.execute(f"""
        CREATE OR REPLACE SECRET gcs_secret (
            TYPE GCS,
            KEY_ID '{key_id}',
            SECRET '{secret}'
        );
    """)
    con.execute(f"""
        SET s3_region='auto';
        SET s3_access_key_id='{key_id}';
        SET s3_secret_access_key='{secret}';
    """)
    logging.info(f"GCS credentials configured (Time taken: {time.time() - t2:.2f}s)")

    logging.info(f"Attaching database: {gcs_db_path} as alias: {alias}...")
    t3 = time.time()
    con.execute(f"ATTACH '{gcs_db_path}' AS {alias} (READ_ONLY);")
    logging.info(f"Database attached (Time taken: {time.time() - t3:.2f}s)")

    logging.info("Executing query...")
    logging.debug(f"Query: {query}")
    t4 = time.time()
    df = con.execute(query).fetchdf()
    logging.info(f"Query executed (Time taken: {time.time() - t4:.2f}s)")

    total_time = time.time() - t0
    logging.info(f"Total time taken: {total_time:.2f}s")

     # Save to CSV if path is provided
    if output_csv_path:
        df.to_csv(output_csv_path, index=False)
        logging.info(f"Result saved to {output_csv_path}")

    return df
