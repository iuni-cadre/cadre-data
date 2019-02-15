from flask import Flask, jsonify, render_template

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to the Cadre Data API.'


@app.route('/status')
def status():
    # this route should return the status of the data API.
    return jsonify({'Status': 'The API is running if the API is running.'}), 200


@app.route('/<path:fallback>')
@app.route('/api/<path:fallback>')
def api_fallback(fallback):
    # this route should catch all api calls that aren't actually endpoints
    return jsonify({'error': 'Unknown Endpoint'}), 404


if __name__ == '__main__':
    app.run()