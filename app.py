from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    filename = file.filename
    file.save(f"/tmp/{filename}")  # save to container's local storage

    return jsonify({
        'message': f'File {filename} uploaded successfully to container.'
    }), 200

@app.route('/', methods=['GET'])
def index():
    return 'Flask app is running. Use POST /upload to upload a file.'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
