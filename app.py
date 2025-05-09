from flask import Flask, request, render_template_string
from google.cloud import storage
import mysql.connector
import os

app = Flask(__name__)

# Configure your bucket name and MySQL credentials
GCS_BUCKET = "hackathon25-bucket"
GCS_SUBFOLDER = "PremTables"
DB_CONNECTION_NAME = 'hack25-463119576432:us-central1:hackathon-db'
DB_USER = 'admin'
DB_PASSWORD = 'admin-hackathon'
DB_NAME = 'Hackathon'

UPLOAD_FORM = """
<!doctype html>
<title>Upload CSV</title>
<h1>Upload a CSV File</h1>
<form method=post enctype=multipart/form-data action="/upload">
  <input type=file name=file>
  <input type=submit value=Upload>
</form>
"""

@app.route('/', methods=['GET'])
def index():
    return render_template_string(UPLOAD_FORM)

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return 'No file uploaded', 400

    file = request.files['file']
    filename = file.filename
    table_name = os.path.splitext(filename)[0].replace('-', '_')
    local_path = f"/tmp/{filename}"
    file.save(local_path)

    # Upload to GCS subfolder
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"{GCS_SUBFOLDER}/{filename}")
        blob.upload_from_filename(local_path)
    except Exception as e:
        return f'File saved locally, but failed to upload to GCS: {e}', 500

    # Connect to MySQL via Cloud SQL Auth Proxy socket
    try:
        connection = mysql.connector.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            unix_socket=f"/cloudsql/{DB_CONNECTION_NAME}",
            connection_timeout=10
        )
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        cursor.execute(f"""
            CREATE TABLE `{table_name}` (
                ID VARCHAR(20) NOT NULL,
                Premium DECIMAL(10, 2),
                PRIMARY KEY (ID)
            );
        """)
        connection.commit()
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        return f'Failed to prepare MySQL table: {err}', 500

    return f'File {filename} uploaded to GCS folder "{GCS_SUBFOLDER}" and MySQL table `{table_name}` was reset with columns ID and Premium.'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
