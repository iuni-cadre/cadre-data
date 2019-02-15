from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to the Cadre Data API.'


@app.route('/status')
def status():
    return jsonify({'Status': 'The API is running'})


if __name__ == '__main__':
    app.run()