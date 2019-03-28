import logging
import os
import traceback

import psycopg2
import sys

import requests
from flask import jsonify
from graphene import ObjectType, String, ID, List, Field, Int, relay
import json
import graphene

logger = logging.getLogger('schema')

abspath = os.path.abspath(os.path.dirname(__file__))
middleware = os.path.dirname(abspath)
cadre = os.path.dirname(middleware)
util = cadre + '/util'
sys.path.append(cadre)

from util.db_util import connection_pool
import util.config_reader


class User(ObjectType):
    user_name = String()
    token = String()


class WOSInterfacetable(ObjectType):
    wos_id = String()
    title = String()
    type = String()
    year = String()
    number = String()
    issue = String()
    pages = String()
    id_pubmed = String()
    doi = String()
    authors_full_name = String()
    authors_id_orcid = String()
    authors_id_dais = String()
    authors_id_research = String()
    authors_id_lang = String()
    authors_prefix = String()
    authors_first_name = String()
    authors_middle_name = String()
    authors_last_name = String()
    authors_suffix = String()
    authors_initials = String()
    authors_display_names = String()
    authors_wos_name = String()
    authors_emails = String()
    references = String()
    journals_name = String()
    journals_name_abbrev = String()
    journals_name_iso = String()
    journals_type = String()
    journals_issn = String()
    abstract_text = String()


class WOSFields(graphene.ObjectType):
    year = String(required=True)
    journal = String(required=True)


class MAG(ObjectType):
    id = ID()
    requester = Field(User)
    year = Int()


class Query(graphene.ObjectType):
    node = relay.Node.Field()
    wos = List(WOSInterfacetable, q=graphene.String())

    def resolve_wos(self, info, **args):
        try:
            q = args.get('q')
            connection = connection_pool.getconn()
            cursor = connection.cursor()
            auth_token = info.context.environ['HTTP_AUTH_TOKEN']
            username = info.context.environ['HTTP_AUTH_USERNAME']
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
                    logger.info(q)
                    query_json = json.loads(q)
                    value_array = []
                    interface_query = 'SELECT wos_id, title, type, year, number, issue, pages, id_pubmed, doi, ' \
                                      'authors_full_name, authors_id_orcid, authors_id_dais, authors_id_research, authors_id_lang, ' \
                                      'authors_prefix, authors_first_name, authors_middle_name, authors_last_name, authors_suffix, authors_initials, ' \
                                      'authors_display_names, authors_wos_name, authors_emails, journals_name, journals_name_abbrev, ' \
                                      'journals_name_iso, journals_type, journals_issn, abstract_text, "references" FROM interface_table WHERE '
                    for item in query_json:
                        logger.info(item)
                        field = item['field']
                        value = item['value']
                        operand = item['operand']
                        if field == 'year':
                            if value is not None:
                                interface_query += ' year=%s ' + operand
                                # years.append(value)
                                value_array.append(value)
                                # year_operands.append(operand)
                        elif field == 'journalsName':
                            if value is not None:
                                interface_query += ' journals_name=%s ' + operand
                                # journals.append(value)
                                value_array.append(value)
                                # journal_operands.append(operand)
                        elif field == 'wosId':
                            if value is not None:
                                interface_query += ' wos_id=%s ' + operand
                                # wos_ids.append(value)
                                value_array.append(value)
                                # wos_id_operands.append(operand)
                        elif field == 'author':
                            if value is not None:
                                interface_query += ' authors_display_names=%s ' + operand
                                # authors.append(value)
                                value_array.append(value)
                                # author_operands.append(operand)

                    # year_dict.update({'year': years, 'operands': year_operands})
                    # journals_dict.update({'journals': journals, 'operands': journal_operands})
                    # authors_dict.update({'authors': journals, 'operands': author_operands})
                    # wosids_dict.update({'wos_ids': wos_ids, 'operands': wos_id_operands})

                    logger.info(interface_query)
                    logger.info(value_array)
                    cursor.execute(interface_query, value_array)
                    result = cursor.fetchall()
                    # Convert query results to objects of key-value pairs
                    objects_list = []
                    for row in result:
                        wos = WOSInterfacetable()
                        wos.wos_id = row[0]
                        wos.title = row[1]
                        wos.type = row[2]
                        wos.year = row[3]
                        wos.number = row[4]
                        wos.issue = row[5]
                        wos.pages = row[6]
                        wos.id_pubmed = row[7]
                        wos.doi = row[8]
                        wos.authors_full_name = row[9]
                        wos.authors_id_orcid = row[10]
                        wos.authors_id_dais = row[11]
                        wos.authors_id_research = row[12]
                        wos.authors_id_lang = row[13]
                        wos.authors_prefix = row[14]
                        wos.authors_first_name = row[15]
                        wos.authors_middle_name = row[16]
                        wos.authors_last_name = row[17]
                        wos.authors_suffix = row[18]
                        wos.authors_initials = row[19]
                        wos.authors_display_names = row[20]
                        wos.authors_wos_name = row[21]
                        wos.authors_emails = row[22]
                        wos.references = row[23]
                        wos.journals_name = row[24]
                        wos.journals_name_abbrev = row[25]
                        wos.journals_name_iso = row[26]
                        wos.journals_type = row[27]
                        wos.journals_issn = row[28]
                        wos.abstract_text = row[29]
                        objects_list.append(wos)
                    return objects_list
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
            logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
            return jsonify({'error': str(error)}), 500
        finally:
            # Closing database connection.
            if connection:
                cursor.close()
                connection_pool.putconn(connection)
                logger.info("PostgreSQL connection pool is closed")


schema = graphene.Schema(query=Query, types=[WOSInterfacetable])

# class Query(graphene.ObjectType):
#     requests = List(DataRequest, id=Int(required=True))
#     wos = relay.Node.Field(WOS)
#
#
# schema = graphene.Schema(query=Query)




