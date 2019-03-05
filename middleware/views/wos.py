import traceback

from flask import jsonify, request, Blueprint
import requests
import os, sys
import psycopg2
import collections
import json
import logging.config

blueprint = Blueprint('wos', __name__)
logger = logging.getLogger('wos')

abspath = os.path.abspath(os.path.dirname(__file__))
middleware = os.path.dirname(abspath)
cadre = os.path.dirname(middleware)
util = cadre + '/util'
sys.path.append(cadre)

from util.db_util import connection_pool
import util.config_reader


@blueprint.route('/api/data/wos/status', methods=['GET'])
def wos_status():
    try:
        auth_token = request.headers.get('auth-token')
        username = request.headers.get('auth-username')
        validata_token_args = {
            'username': username
        }
        headers = {
            'auth-token': auth_token,
            'Content-Type': 'application/json'
        }
        validate_token_response = requests.post(util.config_reader.get_cadre_token_ep(),
                                                data=json.dumps(validata_token_args),
                                                headers=headers,
                                                verify=False)
        status_code = validate_token_response.status_code
        if status_code == 200:
            role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            logger.info('User authorized !!!')
            for role in roles:
                if 'wos' in role:
                    role_found = True
            if role_found:
                # Use getconn() method to Get Connection from connection pool
                connection = connection_pool.getconn()
                cursor = connection.cursor()
                # Print PostgreSQL Connection properties
                print(connection.get_dsn_parameters(), "\n")
                # Print PostgreSQL version
                cursor.execute("SELECT user, current_database();")
                record = cursor.fetchone()
                print("You are connected to - ", record, "\n")
                return jsonify({'You are connected to': connection.get_dsn_parameters()}), 200
            else:
                logger.info('User has guest role. He does not have access to WOS database.. '
                            'Please login with BTAA member institution, if you are part of it..')
                return jsonify({'error': 'User is not authorized to access data in WOS'}), 405
        elif status_code == 401:
            logger.error('User is not authorized to access this endpoint !!!')
            return jsonify({'error': 'User is not authorized to access this endpoint'}), 401
        elif status_code == 500:
            logger.error('Unable to contact login server to validate the token !!!')
            return jsonify({'error': 'Unable to contact login server to validate the token'}), 500
        else:
            logger.error('Something went wrong. Contact admins !!! ')
            return jsonify({'error': 'Something went wrong. Contact admins'}), 500
    except (Exception, psycopg2.Error) as error:
        traceback.print_tb(error.__traceback__)
        logger.error('Error while connecting to PostgreSQL')
        return jsonify({'error': str(error)}), 500
    finally:
        # Closing database connection.
        cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        connection_pool.putconn(connection)
        print("PostgreSQL connection pool is closed")


@blueprint.route('/api/data/wos/publications/<string:year>', methods=['GET'])
def get_publications_per_year(year):
    try:
        auth_token = request.headers.get('auth-token')
        username = request.headers.get('auth-username')
        validata_token_args = {
            'username': username
        }
        headers = {
            'auth-token': auth_token,
            'Content-Type': 'application/json'
        }
        validate_token_response = requests.post(util.config_reader.get_cadre_token_ep(),
                                                data=json.dumps(validata_token_args),
                                                headers=headers,
                                                verify=False)
        status_code = validate_token_response.status_code
        if status_code == 200:
            role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            logger.info('User authorized !!!')
            for role in roles:
                if 'wos' in role:
                    role_found = True
            if role_found:
                logger.info('User has wos role')
                # Use getconn() method to Get Connection from connection pool
                connection = connection_pool.getconn()
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
            else:
                logger.info('User has guest role. He does not have access to WOS database.. '
                            'Please login with BTAA member institution, if you are part of it..')
                return jsonify({'error': 'User is not authorized to access data in WOS'}), 405
        elif status_code == 401:
            logger.error('User is not authorized to access this endpoint !!!')
            return jsonify({'error': 'User is not authorized to access this endpoint'}), 401
        elif status_code == 500:
            logger.error('Unable to contact login server to validate the token !!!')
            return jsonify({'error': 'Unable to contact login server to validate the token'}), 500
        else:
            logger.error('Something went wrong. Contact admins !!! ')
            return jsonify({'error': 'Something went wrong. Contact admins'}), 500
    except (Exception, psycopg2.Error) as error:
        traceback.print_tb(error.__traceback__)
        logger.error('Error while connecting to PostgreSQL')
        return jsonify({'error': str(error)}), 500
    finally:
        # Closing database connection.
        cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        connection_pool.putconn(connection)
        print("PostgreSQL connection pool is closed")


@blueprint.route('/api/data/wos/test-sns', methods=['GET'])
def test_aws_sns():

    logger.info('message sent !!!')