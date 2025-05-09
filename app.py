from flask import Flask, request, render_template_string
from google.cloud import storage
import os

app = Flask(__name__)

# Configure your bucket name here
GCS_BUCKET = "hackathon25-bucket"

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
    local_path = f"/tmp/{filename}"
    file.save(local_path)

    # Upload to GCS
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(filename)
        blob.upload_from_filename(local_path)
    except Exception as e:
        return f'File saved locally, but failed to upload to GCS: {e}', 500

    return f'File {filename} uploaded successfully to container and GCS.'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
