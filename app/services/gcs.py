if __name__ == "__main__":
    from duckdb_gs_util import query_duckdb_from_gcs

    KEY_ID = ""
    SECRET = ""
    GCS_PATH = ""

    # Query for user enrolments
    query = """
    SELECT 
        ue.user_id,
        ud.mdo_id,
        ud.full_name,
        ue.content_id,
        c.content_name,
        c.content_type,
        CAST(ue.enrolled_on AS TIMESTAMP) AS enrolled_on,
        ue.first_completed_on,
        ue.certificate_id
    FROM remote_db.user_enrolments ue
    JOIN remote_db.user_detail ud ON ue.user_id = ud.user_id
    JOIN remote_db.content c ON ue.content_id = c.content_id
    WHERE 
        ud.mdo_id IN ('0136040744089436167049')
        AND CAST(ue.enrolled_on AS TIMESTAMP) >= TIMESTAMP '2025-01-01'
        AND CAST(ue.enrolled_on AS TIMESTAMP) <= TIMESTAMP '2025-01-02';
    """

    df = query_duckdb_from_gcs(
        gcs_db_path=GCS_PATH,
        key_id=KEY_ID,
        secret=SECRET,
        query=query,
        output_csv_path="query_output.csv"
    )

    print(df)
