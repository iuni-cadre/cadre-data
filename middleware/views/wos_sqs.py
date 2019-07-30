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
from util.db_util import mag_graph_driver


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
        elif field == 'journalsName':
            if value is not None:
                interface_query += ' journal_tsv @@ to_tsquery (%s) ' + operation
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Journals Name: ' + value)
                value_array.append(value)
        elif field == 'authorsFullName':
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


def generate_wos_query_for_graph(output_filter_string, filters):
    interface_query = 'SELECT ' + output_filter_string + ' FROM wos_core.interface_table WHERE '
    for item in filters:
        if 'value' in item:
            value = item['value']
        if 'operator' in item:
            operand = item['operand']
        if 'field' in item:
            field = item['field']
            if field == 'year':
                if value is not None:
                    value = value.strip()
                    if len(value) == 4 and value.isdigit():
                        value = "'{}'".format(value)
                        print("Year: " + value)
                        interface_query += ' year={} '.format(value) + operand
                        # years.append(value)
                        # year_operands.append(operand)
            elif field == 'journalsName':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Journals Name: " + value)
                    interface_query += ' journal_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # journals.append(value)
                    # journal_operands.append(operand)
            elif field == 'authorsFullName':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Authors Full Name: " + value)
                    interface_query += ' authors_full_name iLIKE {} '.format(value) + operand
                    # authors.append(value)
            elif field == 'title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Title: " + value)
                    interface_query += ' title_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # authors.append(value)

    interface_query = interface_query + ' LIMIT 10'
    print("Query: " + interface_query)
    return interface_query


def generate_mag_query(output_filter_string, query_json):
    value_array = []
    interface_query = 'SELECT ' + output_filter_string + ' FROM mag_core.mag_interface_table WHERE '
    for item in query_json:
        logger.info(item)
        field = item['field']
        value = item['value']
        operand = item['operand']
        if field == 'year':
            if value is not None:
                interface_query += ' year=%s ' + operand
                value = value.strip()
                logger.info('Year: ' + value)
                value_array.append(str(value))
        elif field == 'journalsName':
            if value is not None:
                interface_query += ' journal_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Journals Name: ' + value)
                value_array.append(value)
        elif field == 'authorsName':
            if value is not None:
                interface_query += ' authors_full_name iLIKE %s ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Authors Name: ' + value)
                value_array.append(value)
        elif field == 'paperTitle':
            if value is not None:
                interface_query += ' title_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Paper Title: ' + value)
                value_array.append(value)
        elif field == 'bookTitle':
            if value is not None:
                interface_query += ' title_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Book Title: ' + value)
                value_array.append(value)
        elif field == 'doi':
            if value is not None:
                interface_query += ' title_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('DOI: ' + value)
                value_array.append(value)
        elif field == 'conferenceDisplayName':
            if value is not None:
                interface_query += ' title_tsv @@ to_tsquery (%s) ' + operand
                value = value.strip()
                value = value.replace(' ', '%')
                value = '%' + value.upper() + '%'
                logger.info('Field: ' + value)
                value_array.append(value)

    interface_query = interface_query + ' LIMIT 10'
    print("Query: " + interface_query)
    return interface_query, value_array


def generate_mag_query_graph(output_filter_string, filters):
    interface_query = 'SELECT ' + output_filter_string + ' FROM mag_core.mag_interface_table WHERE '
    for item in filters:
        if 'value' in item:
            value = item['value']
        if 'operand' in item:
            operand = item['operand']
        if 'field' in item:
            field = item['field']
            if field == 'year':
                if value is not None:
                    value = value.strip()
                    if len(value) == 4 and value.isdigit():
                        value = "'{}'".format(value)
                        print("Year: " + value)
                        interface_query += ' year={} '.format(value) + operand
                        # years.append(value)
                        # year_operands.append(operand)
            elif field == 'journalsName':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Journals Name: " + value)
                    interface_query += ' journal_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # journals.append(value)
                    # journal_operands.append(operand)
            elif field == 'authorsFullName':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Authors Full Name: " + value)
                    interface_query += ' authors_full_name iLIKE {} '.format(value) + operand
                    # authors.append(value)
            elif field == 'title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Title: " + value)
                    interface_query += ' title_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # authors.append(value)

    interface_query = interface_query + ' LIMIT 10'
    print("Query: " + interface_query)
    return interface_query

# For the preview queries: call the database directly,
# For preview, we will restrict degree to 1 or 2
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
        wos_connection = cadre_meta_connection_pool.getconn()
        wos_cursor = wos_connection.cursor()
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
            wos_role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            user_id = response_json['user_id']
            network_query_type = 'other'
            degree = 0
            for role in roles:
                if 'wos' in role:
                    wos_role_found = True
            for output_filed in output_fields:
                type = output_filed['type']
                if type == 'single':
                    field = output_filed['field']
                    output_filters_single.append(field)
                else:
                    network_query_type = output_filed['field']
                    degree = int(output_filed['degree'])
                    output_filters_single.append('paper_id')
            output_filter_string = ",".join(output_filters_single)
            if dataset == 'wos':
                if wos_role_found:
                    logger.info('User has wos role')
                    if network_query_type == 'citation':
                        interface_query = generate_wos_query_for_graph(output_filter_string, filters)
                    else:
                        wos_connection = wos_connection_pool.getconn()
                        wos_cursor = wos_connection.cursor()
                        interface_query, value_array = generate_wos_query(output_filter_string, filters)
                        value_tuple = tuple(value_array)
                        wos_cursor.execute(interface_query, value_tuple)
                        if wos_cursor.rowcount == 0:
                            logger.info('The value of the row count is zero.')
                        if wos_cursor.rowcount > 0:
                            results = wos_cursor.fetchall()
                            response = []
                            for result in results:
                                for i in range(len(output_filters_single)):
                                    result_json = {output_filters_single[i]: result[i]}
                                    response.append(result_json)
                            return jsonify(json.loads(response)), 200
                else:
                    logger.error("User does not have access to WOS dataset..")
                    return jsonify({'error': 'User does not have access to WOS dataset'}, 401)
            else:
                if network_query_type == 'citation':
                    interface_query = generate_mag_query_graph(output_filter_string, filters)
                    with mag_graph_driver.session() as session:
                        neo4j_query = "CALL apoc.load.jdbc('postgresql_url'," \
                                      " ' " + interface_query + "') YIELD row MATCH (n:paper)<-[*2]-(m:paper)" \
                                                                " WHERE n.paper_id = row.paper_id RETURN n, m"
                        result = session.run(neo4j_query)
                        logger.info(result)
                else:
                    mag_connection = mag_connection_pool.getconn()
                    mag_cursor = mag_connection.cursor()
                    interface_query, value_array = generate_mag_query(output_filter_string, request_json)
                    value_tuple = tuple(value_array)
                    mag_cursor.execute(interface_query, value_tuple)
                    if mag_cursor.rowcount == 0:
                        logger.info('The value of the row count is zero.')
                    if mag_cursor.rowcount > 0:
                        results = mag_cursor.fetchall()
                        response = []
                        for result in results:
                            for i in range(len(output_filters_single)):
                                result_json = {output_filters_single[i]: result[i]}
                                response.append(result_json)
                        return jsonify(json.loads(response)), 200
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
        mag_graph_driver.close()
        wos_connection_pool.putconn(wos_connection)
        mag_connection_pool.putconn(mag_connection)


@blueprint.route('/api/data/publications-async', methods=['POST'])
def submit_query():
    try:
        q = request.json.get('q')
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

            queue_url = 'https://sqs.us-east-2.amazonaws.com/799597216943/cadre-job-queue.fifo'

            role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            user_id = response_json['user_id']
            logger.info('User authorized !!!')

            for role in roles:
                if 'wos' in role:
                    role_found = True
            if role_found:
                logger.info('User has wos role')
                # auto generated job id
                job_id = str(uuid.uuid4())
                logger.info(job_id)
                s3_job_dir = username + '/'
                s3_client = boto3.resource('s3',
                                           aws_access_key_id=util.config_reader.get_aws_access_key(),
                                           aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                           region_name=util.config_reader.get_aws_region())
                root_bucket_name = util.config_reader.get_aws_s3_root()
                root_bucket = s3_client.Bucket(root_bucket_name)
                bucket_job_id = root_bucket_name + '/' + s3_job_dir
                s3_location = 's3://' + bucket_job_id
                logger.info(s3_location)
                root_bucket.put_object(Bucket=root_bucket_name, Key=s3_job_dir)
                q.append({'job_id': job_id})
                q.append({'s3_location': s3_location})
                q.append({'username': username})
                query_in_string = json.dumps(q)
                logger.info(query_in_string)
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
                    insert_q = "INSERT INTO user_job(j_id, user_id, sns_message_id, s3_location,job_status, created_on) VALUES (%s,%s,%s,%s,%s,clock_timestamp())"

                    data = (job_id, user_id, message_id, s3_location, 'SUBMITTED')
                    logger.info(data)
                    cursor.execute(insert_q, data)
                    connection.commit()

                    return jsonify({'message_id': message_id,
                                    'job_id': job_id,
                                    's3_location': s3_location}, 200)
                else:
                    logger.error("Error while publishing to sqs")
                    return jsonify({'error': 'error while publishing to SQS'}, 500)
            else:
                logger.info('User has guest role. He does not have access to WOS database.. '
                        'Please login with BTAA member institution, if you are part of it..')
                return jsonify({'error': 'User is not authorized to access data in WOS'}), 401
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


@blueprint.route('/api/data/job-status/<string:job_id>', methods=['POST'])
def job_status(job_id):
    try:
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
            role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            logger.info('User authorized !!!')

            for role in roles:
                if 'wos' in role:
                    role_found = True
            if role_found:
                logger.info('User has wos role')
                # get job information to meta database
                select_q = "SELECT job_status, last_updated from user_job WHERE j_id=%s"
                data = (job_id,)
                cursor.execute(select_q, data)
                if cursor.rowcount > 0:
                    job_info = cursor.fetchone()
                    job_json = {
                        'job_status': job_info[0],
                        'last_updated_time': job_info[1]
                    }
                    job_response = json.dumps(job_json, cls=DateEncoder)
                    return jsonify(json.loads(job_response), 200)
                else:
                    logger.error("Invalid Job Id provided. Please check..")
                    return jsonify({"Error": "Invalid Job Id"}, 400)
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
        logger.error('Error while connecting to cadre meta database. Error is ' + str(error))
        return jsonify({'error': str(error)}), 500
    finally:
        # Closing database connection.
        cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        cadre_meta_connection_pool.putconn(connection)
        print("PostgreSQL connection pool is closed")
