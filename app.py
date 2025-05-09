from flask import Flask, request, render_template_string

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
    file.save(f"/tmp/{filename}")  # save to container's local storage

    return f'File {filename} uploaded successfully to container.'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
