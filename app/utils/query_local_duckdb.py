import duckdb
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def query_local_duckdb(
    db_path: str,
    query: str,
    output_csv_path: str = None
):
    """
    Query a local DuckDB database with logging and optional CSV export.

    Parameters:
    - db_path: str → Path to the .duckdb file (e.g., './warehouse.duckdb')
    - query: str → SQL query string
    - output_csv_path: str → Optional path to save the results as CSV

    Returns:
    - Pandas DataFrame with query results
    """
    logging.info(f"Connecting to local DuckDB database: {db_path}")
    t0 = time.time()
    con = duckdb.connect(db_path, read_only=True)
    logging.info(f"Connected (Time taken: {time.time() - t0:.2f}s)")

    logging.info("Executing query...")
    logging.debug(f"Query: {query}")
    t1 = time.time()
    df = con.execute(query).fetchdf()
    logging.info(f"Query executed (Time taken: {time.time() - t1:.2f}s)")

    if output_csv_path:
        df.to_csv(output_csv_path, index=False)
        logging.info(f"Result saved to {output_csv_path}")

    logging.info(f"Total time taken: {time.time() - t0:.2f}s")
    return df
