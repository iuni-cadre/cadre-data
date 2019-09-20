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
from middleware.views.schema import wos_schema
from middleware.views.schema_mag import mag_schema

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = util.config_reader.get_app_secret()

view_func = GraphQLView.as_view(
    '/api/data/wos-graphql/publication', schema=wos_schema, graphiql=True)

app.add_url_rule('/api/data/wos-graphql/publication', view_func=view_func)

view_func1 = GraphQLView.as_view(
    '/api/data/mag-graphql/publication', schema=mag_schema, graphiql=True)

app.add_url_rule('/api/data/mag-graphql/publication', view_func=view_func)

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

from .views import cadre_data, wos_sqs
app.register_blueprint(cadre_data.blueprint)
app.register_blueprint(wos_sqs.blueprint)
