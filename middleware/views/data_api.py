import traceback
import uuid

import requests
import os, sys
import psycopg2
import json
import logging.config
import boto3
from datetime import date

from flask import Blueprint, jsonify, request

blueprint = Blueprint('wos_sqs', __name__)
logger = logging.getLogger('wos_sqs')

abspath = os.path.abspath(os.path.dirname(__file__))
middleware = os.path.dirname(abspath)
cadre = os.path.dirname(middleware)
util = cadre + '/util'
sys.path.append(cadre)

import util.config_reader
from util.db_util import cadre_meta_connection_pool
from util.db_util import wos_connection_pool
from util.db_util import mag_connection_pool


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def generate_wos_query(output_filter_string, filters):
    interface_query = 'SELECT ' + output_filter_string + ' FROM wos_core.interface_table WHERE '
    value_array = []
    for item in filters:
        logger.info(item)
        field = item['field']
        value = item['value']
        operation = item['operation']
        if field == 'year':
            if value is not None:
                interface_query += ' year=%s ' + operation
                value = value.strip()
                logger.info('Year: ' + value)
                value_array.append(str(value))
        elif field == 'journal_name':
            if value is not None:
                interface_query += ' journal_tsv @@ to_tsquery (%s) ' + operation
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Journal Name: ' + value)
                value_array.append(value)
        elif field == 'authors_full_name':
            if value is not None:
                interface_query += ' authors_full_name iLIKE %s ' + operation
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Authors Full Name: ' + value)
                value_array.append(value)
        elif field == 'title':
            if value is not None:
                interface_query += ' title_tsv @@ to_tsquery (%s) ' + operation
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Title: ' + value)
                value_array.append(value)

    interface_query = interface_query + ' LIMIT 10'
    logger.info("Query: " + interface_query)
    logger.info(value_array)
    return interface_query, value_array


def generate_mag_query(output_filter_string, query_json):
    value_array = []
    interface_query = 'SELECT ' + output_filter_string + ' FROM mag_core.interface_table WHERE'
    for item in query_json:
        logger.info(item)
        field = item['field']
        value = item['value']
        operand = item['operation']
        if field == 'year':
            if value is not None:
                interface_query += ' year=%s ' + operand
                value = value.strip()
                logger.info('Year: ' + value)
                value_array.append(str(value))
        elif field == 'journal_display_name':
            if value is not None:
                interface_query += ' journal_display_name iLIKE %s  ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Journals Name: ' + value)
                value_array.append(value)
        elif field == 'authors_display_name':
            if value is not None:
                interface_query += ' authors_display_name iLIKE %s ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Authors Name: ' + value)
                value_array.append(value)
        elif field == 'paper_title':
            if value is not None:
                interface_query += ' paper_title_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Paper Title: ' + value)
                value_array.append(value)
        elif field == 'paper_abstract':
            if value is not None:
                interface_query += ' paper_abstract_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Paper Abstract: ' + value)
                value_array.append(value)
        elif field == 'doi':
            if value is not None:
                interface_query += ' doi iLIKE %s ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('DOI: ' + value)
                value_array.append(value)
        elif field == 'conference_display_name':
            if value is not None:
                interface_query += ' conference_display_name iLIKE %s ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Conference Display Name: ' + value)
                value_array.append(value)

    interface_query = interface_query + ' LIMIT 10'
    logger.info("Query: " + interface_query)
    return interface_query, value_array


# For the preview queries: call the database directly,
# Preview query for graph queries will be same as sql query with citation counts
@blueprint.route('/api/data/publications-sync', methods=['POST'])
def submit_query_preview():
    try:
        request_json = request.get_json()
        dataset = request_json['dataset']
        filters = request_json['filters']
        output_fields = request_json['output']
        auth_token = request.headers.get('auth-token')
        username = request.headers.get('auth-username')
        output_filters_single = []
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
        wos_connection = wos_connection_pool.getconn()
        wos_cursor = wos_connection.cursor()
        mag_connection = mag_connection_pool.getconn()
        mag_cursor = mag_connection.cursor()
        status_code = validate_token_response.status_code
        if status_code == 200:
            wos_role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            user_id = response_json['user_id']
            network_query_type = 'other'
            degree = 0
            for role in roles:
                if 'wos' in role:
                    wos_role_found = True
            logger.info(output_fields)
            for output_filed in output_fields:
                type = output_filed['type']
                if type == 'single':
                    field = output_filed['field']
                    if field == 'wos_id':
                        output_filters_single.append('id')
                    elif field == 'references':
                        output_filters_single.append("\\'references\\'")
                    else:
                        output_filters_single.append(field)
            # check if network option is selected
            for output_filed in output_fields:
                type = output_filed['type']
                if type == 'network':
                    network_query_type = output_filed['field']
                    degree = int(output_filed['degree'])

            output_filter_string = ",".join(output_filters_single)
            if dataset == 'wos':
                if wos_role_found:
                    logger.info('User has wos role')
                    if network_query_type == 'references':
                        output_filters_single.append('reference_count')
                        output_filter_string = ",".join(output_filters_single)
                    interface_query, value_array = generate_wos_query(output_filter_string, filters)
                    value_tuple = tuple(value_array)
                    wos_cursor.execute(interface_query, value_tuple)
                    if wos_cursor.rowcount == 0:
                        logger.info('The value of the row count is zero.')
                    response = []
                    if wos_cursor.rowcount > 0:
                        results = wos_cursor.fetchall()
                        for result in results:
                            paper_response = {}
                            for i in range(len(output_filters_single)):
                                result_json = {output_filters_single[i]: result[i]}
                                paper_response.update(result_json)
                            response.append(paper_response)
                    return jsonify(response), 200
                else:
                    logger.error("User does not have access to WOS dataset..")
                    return jsonify({'error': 'User does not have access to WOS dataset'}, 401)
            else:
                if network_query_type == 'citations':
                    output_filters_single.append('paper_citation_count')
                    output_filter_string = ",".join(output_filters_single)
                interface_query, value_array = generate_mag_query(output_filter_string, filters)
                value_tuple = tuple(value_array)
                logger.info(interface_query)
                mag_cursor.execute(interface_query, value_tuple)
                if mag_cursor.rowcount == 0:
                    logger.info('The value of the row count is zero.')
                response = []
                if mag_cursor.rowcount > 0:
                    results = mag_cursor.fetchall()
                    for result in results:
                        paper_response = {}
                        for i in range(len(output_filters_single)):
                            result_json = {output_filters_single[i]: result[i]}
                            paper_response.update(result_json)
                        response.append(paper_response)
                return jsonify(response), 200
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
        logger.error('Error while connecting to cadre meta database. Error is ' + str(error))
        return jsonify({'error': str(error)}), 500
    finally:
        # Closing database connections.
        wos_cursor.close()
        mag_cursor.close()
        wos_connection_pool.putconn(wos_connection)
        mag_connection_pool.putconn(mag_connection)


@blueprint.route('/api/data/publications-async', methods=['POST'])
def submit_query():
    try:
        request_json = request.get_json()
        auth_token = request.headers.get('auth-token')
        username = request.headers.get('auth-username')
        connection = cadre_meta_connection_pool.getconn()
        cursor = connection.cursor()
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
            sqs_client = boto3.client('sqs',
                    aws_access_key_id=util.config_reader.get_aws_access_key(),
                    aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                    region_name=util.config_reader.get_aws_region())

            queue_url = util.config_reader.get_aws_queue_url()

            role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            user_id = response_json['user_id']
            dataset = request_json['dataset']
            logger.info('User authorized !!!')

            for role in roles:
                if 'wos' in role:
                    role_found = True
            job_id = str(uuid.uuid4())
            logger.info(job_id)
            request_json['job_id'] = job_id
            request_json['username'] = username
            query_in_string = json.dumps(request_json)
            logger.info(query_in_string)
            if dataset == 'wos':
                if role_found:
                    logger.info('User has wos role')
                    sqs_response = sqs_client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=query_in_string,
                        MessageGroupId='cadre'
                    )
                    logger.info(sqs_response)
                    if 'MessageId' in sqs_response:
                        message_id = sqs_response['MessageId']
                        logger.info(message_id)
                        # save job information to meta database
                        insert_q = "INSERT INTO user_job(job_id, user_id, message_id,job_status, type, dataset, started_on) VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp())"

                        data = (job_id, user_id, message_id,  'SUBMITTED', 'QUERY', 'WOS')
                        logger.info(data)
                        cursor.execute(insert_q, data)
                        connection.commit()

                        return jsonify({'message_id': message_id,
                                        'job_id': job_id}, 200)
                    else:
                        logger.error("Error while publishing to sqs")
                        return jsonify({'error': 'error while publishing to SQS'}, 500)
                else:
                    logger.info('User has guest role. He does not have access to WOS database.. '
                            'Please login with BTAA member institution, if you are part of it..')
                    return jsonify({'error': 'User is not authorized to access data in WOS'}), 401
            else:
                sqs_response = sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=query_in_string,
                    MessageGroupId='cadre'
                )
                logger.info(sqs_response)
                if 'MessageId' in sqs_response:
                    message_id = sqs_response['MessageId']
                    logger.info(message_id)
                    # save job information to meta database
                    insert_q = "INSERT INTO user_job(job_id, user_id, message_id,job_status, type, dataset, started_on) VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp())"

                    data = (job_id, user_id, message_id, 'SUBMITTED', 'QUERY', 'MAG')
                    logger.info(data)
                    cursor.execute(insert_q, data)
                    connection.commit()

                    return jsonify({'message_id': message_id,
                                    'job_id': job_id}, 200)
                else:
                    logger.error("Error while publishing to sqs")
                    return jsonify({'error': 'error while publishing to SQS'}, 500)
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
        logger.error('Error while connecting to cadre meta database. Error is ' + str(error))
        return jsonify({'error': str(error)}), 500
    finally:
        # Closing database connection.
        cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        cadre_meta_connection_pool.putconn(connection)
        print("PostgreSQL connection pool is closed")
