import traceback

import requests
import os, sys
import psycopg2
import json
import logging.config
import boto3

from flask import Blueprint, jsonify, request

blueprint = Blueprint('wos', __name__)
logger = logging.getLogger('wos_sns')

abspath = os.path.abspath(os.path.dirname(__file__))
middleware = os.path.dirname(abspath)
cadre = os.path.dirname(middleware)
util = cadre + '/util'
sys.path.append(cadre)

import util.config_reader


@blueprint.route('/api/data/wos/publications-async', methods=['GET'])
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
            client = boto3.client('sns')

            role_found = False
            response_json = validate_token_response.json()
            roles = response_json['roles']
            logger.info('User authorized !!!')

            for role in roles:
                if 'wos' in role:
                    role_found = True
            if role_found:
                logger.info('User has wos role')
                logger.info(q)
                query_json = {"default": q}

                response = client.publish(
                    TopicArn='arn:aws:sns:us-east-2:799597216943:cadre-wos',
                    Message=query_json,
                    MessageStructure='json'
                )
                logger.info(response)
                message_id = response['message_id']
                return jsonify({'message_id': message_id}, 200)
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
        logger.error('Error while connecting to AWS SNS. Error is ' + str(error))
        return jsonify({'error': str(error)}), 500
