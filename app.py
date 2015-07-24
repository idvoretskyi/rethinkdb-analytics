from flask import Flask, send_from_directory
app = Flask(__name__, static_url_path='')

@app.route("/")
def index():
    return app.send_static_file('index.html')

@app.route('/results/<path:path>')
def send_results(path):
    return send_from_directory('results', path)

if __name__ == "__main__":
    app.run()
