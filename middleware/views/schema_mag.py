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

from util.db_util import mag_connection_pool
import util.config_reader


class MAGInterfacetable(ObjectType):
    paper_id = String()
    author_id = String()
    author_sequence_number = String()
    authors_display_name = String()
    authors_last_known_affiliation_id = String()
    journal_id = String()
    conference_series_id = String()
    conference_instance_id = String()
    paper_reference_id = String()
    recommended_paper_id = String()
    field_of_study_id = String()
    doi = String()
    doc_type = String()
    paper_title = String()
    original_title = String()
    book_title = String()
    year = String()
    date = String()
    paper_publisher = String()
    issue = String()
    paper_first_page = String()
    paper_last_page = String()
    paper_reference_count = String()
    paper_citation_count = String()
    paper_estimated_citation = String()
    conference_display_name = String()
    journal_display_name = String()
    journal_issn = String()
    journal_publisher = String()


class Query(graphene.ObjectType):
    node = relay.Node.Field()
    mag = List(MAGInterfacetable, q=graphene.String())

    def resolve_mag(self, info, **args):
        try:
            q = args.get('q')
            connection = mag_connection_pool.getconn()
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
                logger.info(q)
                query_json = json.loads(q)
                value_array = []
                interface_query = 'SELECT paper_id, author_id, author_sequence_number, authors_display_name, authors_last_known_affiliation_id, journal_id, conference_series_id, ' \
                                  'conference_instance_id, paper_reference_id, recommended_paper_id, field_of_study_id, doi, ' \
                                  'doc_type, paper_title, original_title, book_title, year, ' \
                                  'date, paper_publisher, issue, paper_first_page, paper_last_page, paper_reference_count, paper_citation_count, paper_estimated_citation, ' \
                                  'conference_display_name, journal_display_name, journal_issn, journal_publisher FROM mag_core.mag_interface_table WHERE '
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
                            # years.append(value)
                            # year_operands.append(operand)
                    elif field == 'journalsName':
                        if value is not None:
                            interface_query += ' journal_tsv @@ to_tsquery (%s) ' + operand
                            value = value.strip()
                            value = value.replace(' ', '%')
                            value = '%' + value.upper() + '%'
                            logger.info('Journals Name: ' + value)
                            value_array.append(value)
                            # journals.append(value)
                            # journal_operands.append(operand)
                    elif field == 'authorsName':
                        if value is not None:
                            interface_query += ' authors_full_name iLIKE %s ' + operand
                            value = value.strip()
                            value = value.replace(' ', '%')
                            value = '%' + value.upper() + '%'
                            logger.info('Authors Name: ' + value)
                            value_array.append(value)
                            # authors.append(value)
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



                # year_dict.update({'year': years, 'operands': year_operands})
                # journals_dict.update({'journals': journals, 'operands': journal_operands})
                # authors_dict.update({'authors': journals, 'operands': author_operands})
                # wosids_dict.update({'wos_ids': wos_ids, 'operands': wos_id_operands})

                interface_query += ' LIMIT 10'
                logger.info(interface_query)
                logger.info(value_array)
                cursor.execute(interface_query, value_array)
                objects_list = []
                if cursor.rowcount == 0:
                    logger.info('The value of the row count is zero.')
                if cursor.rowcount > 0:
                    logger.info(str(cursor.rowcount))
                    result = cursor.fetchall()
                    logger.info(len(result))
                    # Convert query results to objects of key-value pairs
                    for row in result:
                        mag = MAGInterfacetable()
                        mag.paper_id = row[0]
                        mag.author_id = row[1]
                        mag.author_sequence_number = row[2]
                        mag.authors_display_name = row[3]
                        mag.authors_last_known_affiliation_id = row[4]
                        mag.journal_id = row[5]
                        mag.conference_series_id = row[6]
                        mag.conference_instance_id = row[7]
                        mag.paper_reference_id = row[8]
                        mag.recommended_paper_id = row[9]
                        mag.field_of_study_id = row[10]
                        mag.doi = row[11]
                        mag.doc_type = row[12]
                        mag.paper_title = row[13]
                        mag.original_title = row[14]
                        mag.book_title = row[15]
                        mag.year = row[16]
                        mag.date = row[17]
                        mag.paper_publisher = row[18]
                        mag.issue = row[19]
                        mag.paper_first_page = row[20]
                        mag.paper_last_page = row[21]
                        mag.paper_reference_count = row[22]
                        mag.paper_citation_count = row[23]
                        mag.paper_estimated_citation = row[24]
                        mag.conference_display_name = row[25]
                        mag.journal_display_name = row[26]
                        mag.journal_issn = row[27]
                        mag.journal_publisher = row[28]
                        objects_list.append(mag)
                return objects_list
            elif status_code == 401:
                logger.error('User is not authorized to access this endpoint !!!')
                raise Exception('User is not authorized to access this endpoint !')
            elif status_code == 500:
                logger.error('Unable to contact login server to validate the token !!!')
                raise Exception('Unable to contact login server to validate the token !')
            else:
                logger.error('Something went wrong. Contact admins !!! ')
                raise Exception('Something went wrong. Contact admins !')
        except (Exception, psycopg2.Error) as error:
            traceback.print_tb(error.__traceback__)
            logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
            raise Exception(str(error))
        finally:
            # Closing database connection.
            if connection:
                cursor.close()
                mag_connection_pool.putconn(connection)
                logger.info("PostgreSQL connection pool is closed")


mag_schema = graphene.Schema(query=Query, types=[MAGInterfacetable])