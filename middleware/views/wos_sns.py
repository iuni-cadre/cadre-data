import traceback
import uuid

import requests
import os, sys
import psycopg2
import json
import logging.config
import boto3
from boto3 import Session

from flask import Blueprint, jsonify, request

blueprint = Blueprint('wos_sns', __name__)
logger = logging.getLogger('wos_sns')

abspath = os.path.abspath(os.path.dirname(__file__))
middleware = os.path.dirname(abspath)
cadre = os.path.dirname(middleware)
util = cadre + '/util'
sys.path.append(cadre)

import util.config_reader
from util.db_util import cadre_meta_connection_pool


@blueprint.route('/api/data/wos/publications-async', methods=['POST'])
def submit_query():
    try:
        q = request.json.get('q')
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
            sns_client = boto3.client('sns',
                    aws_access_key_id=util.config_reader.get_aws_access_key(),
                    aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                    region_name=util.config_reader.get_aws_region())



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
                s3_job_dir = job_id + '/'
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
                query_in_string = json.dumps(q)
                logger.info(query_in_string)
                sns_response = sns_client.publish(
                    TopicArn=util.config_reader.get_aws_sns_wos_topic(),
                    Message=query_in_string,
                    MessageStructure='string'
                )
                logger.info(sns_response)
                if 'MessageId' in sns_response:
                    message_id = sns_response['MessageId']
                    logger.info(message_id)
                    # save job information to meta database
                    connection = cadre_meta_connection_pool.getconn()
                    cursor = connection.cursor()
                    insert_q = "INSERT INTO user_job(j_id, user_id, sns_message_id, s3_location,job_status, created_on) VALUES (%s,%s,%s,%s,%s,clock_timestamp())"

                    data = (job_id, user_id, message_id, s3_location, 'SUBMITTED')
                    logger.info(data)
                    cursor.execute(insert_q, data)
                    connection.commit()

                    return jsonify({'message_id': message_id,
                                    'job_id': job_id,
                                    's3_location': s3_location}, 200)
                else:
                    logger.error("Error while publishing to sns")
                    return jsonify({'error': 'error while publishing to SNS'}, 500)
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
