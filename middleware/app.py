from flask import Flask, jsonify, render_template

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to the Cadre Data API.'


@app.route('/status')
def status():
    # this route should return the status of the data API.
    return jsonify({'Status': 'The API is running if the API is running.'}), 404


@app.route('/api/<path:fallback>')
def api_fallback(fallback):
    # this route should catch all api calls that aren't actually endpoints
    return jsonify({'error': 'Unknown Endpoint'}), 404


@app.route('/<path:fallback>')
def fallback(fallback):
    # this function is defining what to display during a fallback
    return render_template("fallback.html")


if __name__ == '__main__':
    app.run()