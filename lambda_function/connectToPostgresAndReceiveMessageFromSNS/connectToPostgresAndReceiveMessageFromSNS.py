from __future__ import print_function
import psycopg2
from psycopg2 import pool
import json
import os, sys
import collections
import traceback
import select
import boto3

import rds_config
import rds_job_config

# rds settings
rds_host = rds_config.db_host
db_username = rds_config.db_username
db_password = rds_config.db_password
db_name = rds_config.db_name
port = rds_config.port
aws_access_key_id = rds_config.aws_access_key_id
aws_secret_access_key = rds_config.aws_secret_access_key
region_name = rds_config.region_name

# job database settings
rds_host1 = rds_job_config.db_host
db_username1 = rds_job_config.db_username
db_password1 = rds_job_config.db_password
db_name1 = rds_job_config.db_name

print('Loading function')

# Establishing the connection with Amazon RDS

try:
    connection_pool = pool.SimpleConnectionPool(1,
                                                20,
                                                host=rds_host,
                                                database=db_name,
                                                user=db_username,
                                                password=db_password,
                                                port=5432)

except psycopg2.Error as e:
    print(e)
    sys.exit()

print("SUCCESS: Connection to RDS Postgres instance succeeded")

# Establishing the connection with the job database

try:
    connection_pool1 = pool.SimpleConnectionPool(1,
                                                 20,
                                                 host=rds_host1,
                                                 database=db_name1,
                                                 user=db_username1,
                                                 password=db_password1,
                                                 port=5432)

except psycopg2.Error as e:
    print(e)
    sys.exit()

print("SUCCESS: Connection to Job Database succeeded")


def lambda_handler(event, context):
    """
    This function checks the status of the Postgres RDS instance
    """
    connection = connection_pool.getconn()
    cursor = connection.cursor()

    # Retrieving the message from SNS
    message = event['Records'][0]['Sns']['Message']
    messageId = event['Records'][0]['Sns']['MessageId']

    print("Message ID: " + json.dumps(messageId))
    print("Message: " + message)
    query_json = json.loads(message)

    print(query_json)

    # extract the job id from the message
    for item in query_json:
        if 'job_id' in item:
            job_id = item['job_id']

    # Updating the job status in the job database as running

    # Use getconn() method to Get Connection from connection pool from the job database
    connection1 = connection_pool1.getconn()
    cursor1 = connection1.cursor()
    print("Job ID: " + job_id)
    updateStatement = "UPDATE user_job SET job_status = 'RUNNING', last_updated = CURRENT_TIMESTAMP WHERE j_id = (%s)"
    # Execute the SQL Query
    cursor1.execute(updateStatement, (job_id,))
    print(connection1.get_dsn_parameters())
    connection1.commit()

    # Closing the Job database connection.
    cursor1.close()
    # Use this method to release the connection object and send back to the connection pool
    connection_pool1.putconn(connection1)
    print("PostgreSQL connection pool for the Job Database is closed")

    # Generating the Query that needs to run on the RDS

    value_array = []
    interface_query = 'SELECT wos_id, year, number, issue, pages, authors_full_name, authors_id_orcid, ' \
                      'authors_id_dais, authors_id_research, authors_prefix, authors_first_name, authors_middle_name, ' \
                      'authors_last_name, authors_suffix, authors_initials, authors_display_name, authors_wos_name, ' \
                      'authors_id_lang, authors_email, "references", issn, doi, title, journal_name, journal_abbrev, ' \
                      'journal_iso, abstract_paragraphs FROM interface_table WHERE '
    for item in query_json:
        if 'field' in item:
            field = item['field']
        if 'value' in item:
            value = item['value']
        if 'operand' in item:
            operand = item['operand']
        if 's3_location' in item:
            print("S3 Location: " + item['s3_location'])
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
                    # author_operands.append(operand)

        # year_dict.update({'year': years, 'operands': year_operands})
        # journals_dict.update({'journals': journals, 'operands': journal_operands})
        # authors_dict.update({'authors': journals, 'operands': author_operands})
        # wosids_dict.update({'wos_ids': wos_ids, 'operands': wos_id_operands})
    interface_query = interface_query + 'LIMIT' + ' ' + '9999'
    print("Query: " + interface_query)

    print("Job ID: " + job_id)

    try:
        output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
        with open('/tmp/resultsfile', 'w') as f:
            cursor.copy_expert(output_query, f)

        s3_client = boto3.resource('s3',
                                   aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key,
                                   region_name=region_name)
        root_bucket_name = "cadre-query-result"
        bucket_job_id = '{}/'.format(job_id)
        print("Bucket Job ID: " + bucket_job_id)
        s3_location = 's3://' + bucket_job_id
        s3_client.meta.client.upload_file('/tmp/resultsfile', root_bucket_name, bucket_job_id + 'result.csv')
    except:
        # Updating the job status in the job database as failed

        # Use getconn() method to Get Connection from connection pool from the job database
        connection2 = connection_pool1.getconn()
        cursor2 = connection2.cursor()
        print("Job ID: " + job_id)
        updateStatement = "UPDATE user_job SET job_status = 'FAILED', last_updated = CURRENT_TIMESTAMP WHERE j_id = (%s)"
        # Execute the SQL Query
        cursor2.execute(updateStatement, (job_id,))
        print(connection2.get_dsn_parameters())
        connection2.commit()

        # Closing the Job database connection.
        cursor2.close()
        # Use this method to release the connection object and send back to the connection pool
        connection_pool1.putconn(connection2)
        print("PostgreSQL connection pool for the Job Database is closed")
        return print("ERROR: Cannot execute cursor.\n{}".format(
            traceback.format_exc()))

    # Closing the RDS database connection.
    cursor.close()
    # Use this method to release the connection object and send back to the connection pool
    connection_pool.putconn(connection)
    print("PostgreSQL connection pool for the RDS database is closed")

    # Updating the job database after the job is finished

    # Use getconn() method to Get Connection from connection pool
    connection3 = connection_pool1.getconn()
    cursor3 = connection3.cursor()
    print("Job ID: " + job_id)
    updateStatement = "UPDATE user_job SET job_status = 'COMPLETED', last_updated = CURRENT_TIMESTAMP WHERE j_id = (%s)"
    # Execute the SQL Query
    cursor3.execute(updateStatement, (job_id,))
    print(connection3.get_dsn_parameters())
    connection3.commit()

    # Closing the Job database connection.
    cursor3.close()
    # Use this method to release the connection object and send back to the connection pool
    connection_pool1.putconn(connection3)
    print("PostgreSQL connection pool for the Job Database is closed")

    return {
        'statusCode': 200,
        'body': message
    }
