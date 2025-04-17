import duckdb
import logging
import gc
from typing import List, Dict, Generator, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DuckDBQueryService")

class DuckDBQueryService:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = duckdb.connect(self.db_path)

    def build_where_clause(self, filters: Dict[str, any]) -> str:
        if not filters:
            return ""
        clauses = []
        for key, value in filters.items():
            if "__" in key:
                col, op = key.split("__", 1)
                if op == "gte":
                    clauses.append(f"{col} >= '{value}'")
                elif op == "lte":
                    clauses.append(f"{col} <= '{value}'")
                elif op == "gt":
                    clauses.append(f"{col} > '{value}'")
                elif op == "lt":
                    clauses.append(f"{col} < '{value}'")
                elif op == "ne":
                    clauses.append(f"{col} != '{value}'")
            else:
                clauses.append(f"{key} = '{value}'")
        return "WHERE " + " AND ".join(clauses)

    def generate_csv_stream(
        self,
        base_query: str,
        filters: Dict[str, any] = None,
        required_columns: Optional[List[str]] = None
    ) -> Optional[Generator[str, None, None]]:
        try:
            where_clause = self.build_where_clause(filters)
            final_query = f"{base_query.strip()} {where_clause};"
            logger.info(f"Executing query:\n{final_query}")

            df = self.con.execute(final_query).fetchdf()

            if df.empty:
                logger.info("No data found.")
                return None

            if required_columns:
                existing_columns = [col for col in required_columns if col in df.columns]
                missing = set(required_columns) - set(existing_columns)
                if missing:
                    logger.warning(f"Missing columns skipped: {missing}")
                df = df[existing_columns]

            logger.info(f"Fetched {len(df)} rows.")

            def stream(df_stream, cols):
                try:
                    yield ','.join(cols) + '\n'
                    for row in df_stream.itertuples(index=False, name=None):
                        yield ','.join(map(str, row)) + '\n'
                finally:
                    df_stream.drop(df_stream.index, inplace=True)
                    del df_stream
                    gc.collect()
                    logger.info("Cleaned up DataFrame after streaming.")

            return stream(df, df.columns.tolist())

        except Exception as e:
            logger.error(f"Error during DuckDB query: {e}")
            return None
