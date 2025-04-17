import logging
from typing import Optional, Generator, List
from app.utils.duckdb_quey_service import DuckDBQueryService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
DUCKDB_PATH = "warehouse.duckdb"  # Path to your local DuckDB database

class ReportService:
    logger = logging.getLogger("ReportService")

    # def __init__(self, db_path: str):
    #     self.duckdb_service = DuckDBQueryService(db_path)

    def get_total_learning_hours_csv_stream(
        start_date: str,
        end_date: str,
        mdo_id: str,
        required_columns: Optional[List[str]] = None
    ) -> Optional[Generator[str, None, None]]:
        duckdb_service = DuckDBQueryService(DUCKDB_PATH)
        try:
            base_query = """
                SELECT 
                    ud.user_id,
                    ud.full_name,
                    ue.content_id,
                    ue.enrolled_on,
                    ue.first_completed_on,
                    ue.last_completed_on,
                    ue.certificate_generated,
                    c.content_name,
                    c.content_duration
                FROM user_detail ud
                INNER JOIN user_enrolments ue ON ud.user_id = ue.user_id
                INNER JOIN content c ON ue.content_id = c.content_id
            """

            filters = {
                "ud.mdo_id": mdo_id,
                "ue.enrolled_on__gte": start_date,
                "ue.enrolled_on__lte": end_date
            }

            ReportService.logger.info("Generating report CSV stream...")
            return duckdb_service.generate_csv_stream(
                base_query=base_query,
                filters=filters,
                required_columns=required_columns
            )

        except Exception as e:
            ReportService.logger.error(f"Error in report generation: {e}")
            return None
