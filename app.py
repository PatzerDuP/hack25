from flask import Flask, request, jsonify
from google.cloud import storage
import os
import mysql.connector

app = Flask(__name__)

# Hardcoded configuration for proof of concept
GCS_BUCKET = 'hackathon25-bucket'
PROJECT_ID = 'hackathon25-459214'
DB_HOST = '34.42.130.81'  # e.g., '127.0.0.1' or Cloud SQL IP
DB_USER = 'admin'
DB_PASSWORD = 'admin-hackathon'
DB_NAME = 'Hackathon'

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    filename = file.filename

    # Only confirm upload to container and DB connectivity for now
    try:
        # Attempt MySQL connection
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        return jsonify({'error': f'Database connection failed: {err}'}), 500

    return jsonify({
        'message': f'File {filename} received and DB connection successful.'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
