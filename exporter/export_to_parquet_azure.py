import os

import duckdb
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from loguru import logger


def export_sqlite_to_parquet_and_upload():
    """Export SQLite tables to Parquet and upload to Azure"""

    logger.info("Starting SQLite to Parquet export and Azure upload process")

    logger.info("Loading environment variables from .env file")
    load_dotenv()

    logger.info("Initializing DuckDB connection")
    conn = duckdb.connect()
    logger.info("Installing SQLite extension")
    conn.execute("INSTALL sqlite;")
    logger.info("Loading SQLite extension")
    conn.execute("LOAD sqlite;")
    logger.info("Setting SQLite options")
    conn.execute("SET sqlite_all_varchar=true;")

    # SQLite database path
    db_path = "/app/src/services/BAG_parser/output/bag.sqlite"
    logger.info(f"Using SQLite database at: {db_path}")

    # Azure connection string from environment variable
    logger.info("Getting Azure Storage connection string")
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connect_str:
        logger.error(
            "Azure Storage connection string not found in environment variables"
        )
        raise ValueError(
            "Azure Storage connection string not found in environment variables"
        )

    logger.info("Creating Azure blob service client")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "development"
    blob_prefix = "bag/"
    logger.info(f"Using container: {container_name} with prefix: {blob_prefix}")
    container_client = blob_service_client.get_container_client(container_name)

    logger.info("Getting list of tables from SQLite database")
    tables = conn.execute(f"""
        SELECT name FROM sqlite_scan('{db_path}', 'sqlite_master')
        WHERE type='table'
        AND name NOT LIKE 'sqlite_%'
    """).fetchall()
    logger.info(f"Found {len(tables)} tables to process")

    # Process each table
    for table in tables:
        table_name = table[0]
        logger.info(f"Starting processing of table: {table_name}")

        logger.info(f"Creating temporary table for {table_name}")
        conn.execute(f"""
            CREATE TABLE temp_table AS
            SELECT * FROM sqlite_scan('{db_path}', '{table_name}');
        """)

        # Convert numeric columns back to proper types
        logger.info(f"Converting data types for {table_name}")
        schema = conn.execute("DESCRIBE temp_table").fetchall()
        for col in schema:
            col_name = col[0]
            # Convert numeric columns
            if col_name in ["rd_x", "rd_y", "latitude", "longitude", "oppervlakte"]:
                conn.execute(f"""
                    ALTER TABLE temp_table
                    ALTER COLUMN {col_name} TYPE DOUBLE
                    USING CASE
                        WHEN {col_name} = '' THEN NULL
                        ELSE CAST({col_name} AS DOUBLE)
                    END
                """)
            # Convert integer columns
            elif col_name in ["huisnummer", "bouwjaar"]:
                conn.execute(f"""
                    ALTER TABLE temp_table
                    ALTER COLUMN {col_name} TYPE INTEGER
                    USING CASE
                        WHEN {col_name} = '' THEN NULL
                        ELSE CAST({col_name} AS INTEGER)
                    END
                """)

        logger.info(f"Converting {table_name} to Arrow format")
        arrow_table = conn.execute("SELECT * FROM temp_table").arrow()

        # Local parquet file path
        parquet_path = f"/tmp/{table_name}.parquet"
        logger.info(f"Writing {table_name} to parquet file: {parquet_path}")
        pq.write_table(arrow_table, parquet_path)

        logger.info(f"Uploading {table_name}.parquet to Azure")
        with open(parquet_path, "rb") as data:
            blob_client = container_client.get_blob_client(
                f"{blob_prefix}{table_name}.parquet"
            )
            blob_client.upload_blob(data, overwrite=True)

        logger.info(f"Cleaning up temporary files for {table_name}")
        os.remove(parquet_path)
        conn.execute("DROP TABLE temp_table")

        logger.info(f"Completed processing table: {table_name}")

    conn.close()
    logger.info("Export and upload process completed successfully")


if __name__ == "__main__":
    export_sqlite_to_parquet_and_upload()
