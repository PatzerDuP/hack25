# main.py

from flask import Flask, request, render_template_string
import os
from cloud_helpers import upload_to_gcs, prepare_mysql_table, cloudsql_import

app = Flask(__name__)

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

    try:
        gcs_path = upload_to_gcs(local_path, filename)
    except Exception as e:
        return str(e), 500

    try:
        prepare_mysql_table(table_name)
    except Exception as e:
        return str(e), 500

    try:
        response = cloudsql_import(gcs_path, table_name)
    except Exception as e:
        return str(e), 500

    return f'File {filename} uploaded to GCS and imported into MySQL table {table_name}.'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
