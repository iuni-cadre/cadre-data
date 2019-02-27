import logging.config
import os
import sys

from flask import jsonify, Blueprint

blueprint = Blueprint('cadre_data', __name__)
logger = logging.getLogger('cadre_data')

abspath = os.path.abspath(os.path.dirname(__file__))
middleware = os.path.dirname(abspath)
cadre = os.path.dirname(middleware)
sys.path.append(cadre)


@blueprint.route('/api/data')
def welcome():
    return 'Welcome to the Cadre Data API.'


@blueprint.route('/api/data/status')
def status():
    # this route should return the status of the data API.
    return jsonify({'Status': 'The API is running if the API is running.'}), 200


@blueprint.route('/<path:fallback>')
@blueprint.route('/api/<path:fallback>')
def api_fallback(fallback):
    # this route should catch all api calls that aren't actually endpoints
    return jsonify({'error': 'Unknown Endpoint'}), 404