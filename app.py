from flask import Flask, request, jsonify
from google.cloud import storage
import os
import mysql.connector

app = Flask(__name__)

# Set environment variables or use a secrets manager in production
GCS_BUCKET = os.environ.get('GCS_BUCKET_NAME')
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
DB_HOST = os.environ.get('MYSQL_HOST')
DB_USER = os.environ.get('MYSQL_USER')
DB_PASSWORD = os.environ.get('MYSQL_PASSWORD')
DB_NAME = os.environ.get('MYSQL_DATABASE')

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    filename = file.filename

    # Save to GCS
    client = storage.Client()
    bucket = client.get_bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_file(file)

    # Create MySQL table with same name as file (excluding extension)
    table_name = os.path.splitext(filename)[0].replace('-', '_')

    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    cursor = connection.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data JSON
        )
    """)
    connection.commit()
    cursor.close()
    connection.close()

    # Simulate Push API with dummy response for now
    return jsonify({
        'message': f'File {filename} uploaded to GCS and table `{table_name}` created in MySQL.',
        'gcs_path': f'gs://{GCS_BUCKET}/{filename}'
    }), 200

if __name__ == '__main__':
    app.run(debug=True)
