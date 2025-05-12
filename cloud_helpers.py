# cloud_helpers.py

import os
import traceback
from google.cloud import storage
from googleapiclient import discovery
from google.auth import default
import mysql.connector

# Configuration (you can later move these to environment variables)
GCS_BUCKET = "hackathon25-bucket"
GCS_SUBFOLDER = "PremTables"
DB_CONNECTION_NAME = 'hackathon25-459214:us-central1:hackathon-mysql'
DB_USER = 'admin'
DB_PASSWORD = 'admin-hackathon'
DB_NAME = 'Hackathon'
PROJECT_ID = 'hackathon25-459214'
INSTANCE_ID = 'hackathon-mysql'

def upload_to_gcs(local_path, filename):
    gcs_path = f"{GCS_SUBFOLDER}/{filename}"
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        return gcs_path
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Failed to upload to GCS: {e}")

def prepare_mysql_table(table_name):
    try:
        connection = mysql.connector.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            unix_socket=f"/cloudsql/{DB_CONNECTION_NAME}",
            connection_timeout=10
        )
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                ID VARCHAR(20) NOT NULL,
                Premium DECIMAL(10, 2),
                PRIMARY KEY (ID)
            );
        """)
        connection.commit()
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        traceback.print_exc()
        raise RuntimeError(f"Failed to prepare MySQL table: {err}")

def cloudsql_import(gcs_path, table_name):
    credentials, _ = default()
    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    body = {
        "importContext": {
            "fileType": "CSV",
            "uri": f"gs://{GCS_BUCKET}/{gcs_path}",
            "database": DB_NAME,
            "csvImportOptions": {
                "table": table_name,
                "columns": ["ID", "Premium"]
            }
        }
    }

    try:
        request = service.instances().import_(
            project=PROJECT_ID,
            instance=INSTANCE_ID,
            body=body
        )
        return request.execute()
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Cloud SQL import failed: {e}")
