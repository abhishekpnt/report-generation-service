import logging
from cryptography.fernet import Fernet
import pandas as pd
from app.models.report_model import ReportData
from app.services.fetch_data import DataFetcher
from constants import USER_DETAILS_TABLE, CONTENT_TABLE, USER_ENROLMENTS_TABLE
import gc


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class ReportService:
    logger = logging.getLogger(__name__)



    @staticmethod
    def get_total_learning_hours_csv_stream(start_date, end_date, mdo_id, required_columns=None):
        try:
            fetcher = DataFetcher()

            # Fetch filtered user data
            user_df = fetcher.fetch_data_as_dataframe(
                USER_DETAILS_TABLE,
                {"mdo_id": mdo_id},
                columns=["user_id", "mdo_id", "full_name"]
            )

            if user_df.empty:
                ReportService.logger.info("No users found for given mdo_id.")
                return None

            user_ids = user_df["user_id"].tolist()
            ReportService.logger.info(f"Fetched {len(user_ids)} users.")

            # Fetch filtered enrollment data
            enrollment_filters = {
                "enrolled_on__gte": start_date,
                "enrolled_on__lte": end_date
            }

            enrollment_df = fetcher.fetch_data_as_dataframe(
                USER_ENROLMENTS_TABLE,
                enrollment_filters,
                columns=["user_id", "certificate_generated", "content_id", "enrolled_on", "first_completed_on", "last_completed_on"]
            )

            if enrollment_df.empty:
                ReportService.logger.info("No enrollment data found for the given date range.")
                return None

            # Filter enrollment to only matching user_ids
            enrollment_df = enrollment_df[enrollment_df["user_id"].isin(user_ids)]
            if enrollment_df.empty:
                ReportService.logger.info("No enrollments matched the filtered user IDs.")
                return None

            # Fetch content data (consider filtering by content_id list if needed)
            content_df = fetcher.fetch_data_as_dataframe(
                CONTENT_TABLE,
                columns=["content_id", "content_duration", "content_name"]
            )

            if content_df.empty:
                ReportService.logger.info("No content data found.")
                return None

            # Merge all three datasets
            merged_df = (
                user_df
                .merge(enrollment_df, on="user_id", how="inner")
                .merge(content_df, on="content_id", how="inner")
            )

            if merged_df.empty:
                ReportService.logger.info("Merged dataset is empty.")
                return None

            # Filter columns if specified
            if required_columns:
                existing_columns = [col for col in required_columns if col in merged_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                merged_df = merged_df[existing_columns]

            def generate_csv_stream(df, cols):
                try:
                    yield ','.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        yield ','.join(map(str, row)) + '\n'
                finally:
                    # Safe cleanup after generator is fully consumed
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")

            ReportService.logger.info(f"CSV stream generated with {len(merged_df)} rows.")

            # Explicit cleanup of DataFrames
            user_df.drop(user_df.index, inplace=True)
            enrollment_df.drop(enrollment_df.index, inplace=True)
            content_df.drop(content_df.index, inplace=True)

            del user_df, enrollment_df, content_df
            user_df = enrollment_df = content_df = None
            gc.collect()

            # Return CSV content without closing the stream
            return generate_csv_stream(merged_df, existing_columns)

        except MemoryError as me:
            ReportService.logger.error("MemoryError encountered. Consider processing data in smaller chunks.")
            raise
        except Exception as e:
            ReportService.logger.error(f"Error generating CSV stream: {e}")
            return None
