from flask import Flask, jsonify, render_template
import os, sys
import psycopg2

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
sys.path.append(cadre)

import util.config_reader

app = Flask(__name__)
app.config['SECRET_KEY'] = util.config_reader.get_app_secret()

connection = psycopg2.connect(user=util.config_reader.get_cadre_db_username(),
                              password=util.config_reader.get_cadre_db_pwd(),
                              host=util.config_reader.get_cadre_db_hostname(),
                              port=util.config_reader.get_cadre_db_port(),
                              database=util.config_reader.get_cadre_db_name())
cursor = connection.cursor()


@app.route('/database')
def connect_database():
    try:
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")
        # Print PostgreSQL version
        cursor.execute("SELECT user, current_database();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return jsonify({'You are connected to': connection.get_dsn_parameters()}), 200
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # Closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")


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