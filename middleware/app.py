from flask import Flask, jsonify, render_template
from flask_cors import CORS
import os, sys
import psycopg2
import collections
import json

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
sys.path.append(cadre)

import util.config_reader

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = util.config_reader.get_app_secret()

connection = psycopg2.connect(user=util.config_reader.get_cadre_db_username(),
                              password=util.config_reader.get_cadre_db_pwd(),
                              host=util.config_reader.get_cadre_db_hostname(),
                              port=util.config_reader.get_cadre_db_port(),
                              database=util.config_reader.get_cadre_db_name())


@app.route('/database')
def connect_database():
    try:
        cursor = connection.cursor()
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
           # if (connection):
        cursor.close()
           # connection.close()
        print("PostgreSQL connection is closed")


@app.route('/database/<string:year>')
def show_post(year):
    try:
        cursor = connection.cursor()
        # call stored procedure
        cursor.callproc('show_wos_summary', [year, ])
        result = cursor.fetchall()
        # Convert query results to objects of key-value pairs
        objects_list = []
        for row in result:
            d = collections.OrderedDict()
            d['ID'] = row[0]
            d['FileNumber'] = row[1]
            d['CollID'] = row[2]
            d['PubYear'] = row[3]
            d['Season'] = row[4]
            d['PubMonth'] = row[5]
            d['PubDay'] = row[6]
            d['CoverDate'] = row[7]
            d['EDate'] = row[8]
            d['Vol'] = row[9]
            d['Issue'] = row[10]
            d['VolIss'] = row[11]
            d['Supplement'] = row[12]
            d['SpecialIssue'] = row[13]
            d['PartNo'] = row[14]
            d['PubType'] = row[15]
            d['Medium'] = row[16]
            d['Model'] = row[17]
            d['Indicator'] = row[18]
            d['Inpi'] = row[19]
            d['IsArchive'] = row[20]
            d['City'] = row[21]
            d['Country'] = row[22]
            d['HasAbstract'] = row[23]
            d['SortDate'] = row[24]
            d['TitleCount'] = row[25]
            d['NameCount'] = row[26]
            d['DocTypeCount'] = row[27]
            d['ConferenceCount'] = row[28]
            d['LanguageCount'] = row[29]
            d['NormalizedLanguageCount'] = row[30]
            d['NormalizedDocTypeCount'] = row[31]
            d['DescriptiveRefCount'] = row[32]
            d['ReferenceCount'] = row[33]
            d['AddressCount'] = row[34]
            d['HeadingsCount'] = row[35]
            d['SubHeadingsCount'] = row[36]
            d['SubjectsCount'] = row[37]
            d['FundAck'] = row[38]
            d['GrantsCount'] = row[39]
            d['GrantsComplete'] = row[40]
            d['KeywordCount'] = row[41]
            d['AbstractCount'] = row[42]
            d['ItemCollId'] = row[43]
            d['ItemIds'] = row[44]
            d['ItemIdsAvail'] = row[45]
            d['BibId'] = row[46]
            d['BibPageCount'] = row[47]
            d['BibPageCountType'] = row[48]
            d['ReviewedLanguageCount'] = row[49]
            d['ReviewedAuthorCount'] = row[50]
            d['ReviewedYear'] = row[51]
            d['KeywordsPlusCount'] = row[52]
            d['BookChapters'] = row[53]
            d['BookPages'] = row[54]
            d['BookNotesCount'] = row[55]
            d['ChapterListCount'] = row[56]
            d['ContributorCount'] = row[57]
            objects_list.append(d)
        return json.dumps(objects_list), 200
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # Closing database connection.
            # if (connection):
        cursor.close()
            # connection.close()
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
