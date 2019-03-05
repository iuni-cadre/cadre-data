import json
import logging.config
import os
import sys
from os import path, remove

from flask import Flask
from flask_cors import CORS

from flask_graphql import GraphQLView


abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
middleware = cadre + '/middleware'
sys.path.append(cadre)

import util.config_reader
from middleware.views.schema import Query, schema

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = util.config_reader.get_app_secret()

view_func = GraphQLView.as_view(
    'graphql', schema=schema, graphiql=True)

app.add_url_rule('/graphql', view_func=view_func)

# If applicable, delete the existing log file to generate a fresh log file during each execution
logfile_path = abspath + "/cadre_data_logging.log"
if path.isfile(logfile_path):
    remove(logfile_path)

log_conf = abspath + '/logging-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')

from .views import wos, mag, cadre_data
app.register_blueprint(wos.blueprint)
app.register_blueprint(mag.blueprint)
app.register_blueprint(cadre_data.blueprint)
