import collections
import logging
import os
import traceback

import psycopg2
import sys

import requests
from flask import jsonify
from graphene import ObjectType, String, Boolean, ID, List, Field, Int, relay
import json
import graphene
from collections import namedtuple

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


class WOS(ObjectType):
    ID = ID()
    FileNumber = Int()
    CollID = String()
    PubYear = String()
    Season = String()
    PubMonth = String()
    PubDay = String()
    CoverDate = String()
    EDate = String()
    Vol = String()
    Issue = String()
    VolIss = String()
    Supplement = String()
    SpecialIssue = String()
    PartNo = String()
    PubType = String()
    Medium = String()
    Model = String()
    Indicator = String()
    Inpi = String()
    IsArchive = String()
    City = String()
    Country = String()
    HasAbstract = String()
    SortDate = String()
    TitleCount = String()
    NameCount = String()
    DocTypeCount = String()
    ConferenceCount = String()
    LanguageCount = String()
    NormalizedLanguageCount = String()
    NormalizedDocTypeCount = String()
    DescriptiveRefCount = String()
    ReferenceCount = String()
    AddressCount = String()
    HeadingsCount = String()
    SubHeadingsCount = String()
    SubjectsCount = String()
    FundAck = String()
    GrantsCount = String()
    GrantsComplete = String()
    KeywordCount = String()
    AbstractCount = String()
    ItemCollId = String()
    ItemIds = String()
    ItemIdsAvail = String()
    BibId = String()
    BibPageCount = String()
    BibPageCountType = String()
    ReviewedLanguageCount = String()
    ReviewedAuthorCount = String()
    ReviewedYear = String()
    KeywordsPlusCount = String()
    BookChapters = String()
    BookPages = String()
    BookNotesCount = String()
    ChapterListCount = String()
    ContributorCount = String()


class MAG(ObjectType):
    id = ID()
    requester = Field(User)
    year = Int()


class Query(graphene.ObjectType):
    wos = List(WOS, year=Int(required=True))

    def resolve_wos(self, info, year):
        try:
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
                    yearstring = str(year)
                    # call stored procedure
                    cursor.callproc('show_wos_summary', [yearstring, ])
                    result = cursor.fetchall()
                    # Convert query results to objects of key-value pairs
                    objects_list = []
                    for row in result:
                        wos = WOS()
                        wos.ID = row[0]
                        wos.FileNumber = row[1]
                        wos.CollID = row[2]
                        wos.PubYear = row[3]
                        wos.Season = row[4]
                        wos.PubMonth = row[5]
                        wos.PubDay = row[6]
                        wos.CoverDate = row[7]
                        wos.EDate = row[8]
                        wos.Vol = row[9]
                        wos.Issue = row[10]
                        wos.VolIss = row[11]
                        wos.Supplement = row[12]
                        wos.SpecialIssue = row[13]
                        wos.PartNo = row[14]
                        wos.PubType = row[15]
                        wos.Medium = row[16]
                        wos.Model = row[17]
                        wos.Indicator = row[18]
                        wos.Inpi = row[19]
                        wos.IsArchive = row[20]
                        wos.City = row[21]
                        wos.Country = row[22]
                        wos.HasAbstract = row[23]
                        wos.SortDate = row[24]
                        wos.TitleCount = row[25]
                        wos.NameCount = row[26]
                        wos.DocTypeCount = row[27]
                        wos.ConferenceCount = row[28]
                        wos.LanguageCount = row[29]
                        wos.NormalizedLanguageCount = row[30]
                        wos.NormalizedDocTypeCount = row[31]
                        wos.DescriptiveRefCount = row[32]
                        wos.ReferenceCount = row[33]
                        wos.AddressCount = row[34]
                        wos.HeadingsCount = row[35]
                        wos.SubHeadingsCount = row[36]
                        wos.SubjectsCount = row[37]
                        wos.FundAck = row[38]
                        wos.GrantsCount = row[39]
                        wos.GrantsComplete = row[40]
                        wos.KeywordCount = row[41]
                        wos.AbstractCount = row[42]
                        wos.ItemCollId = row[43]
                        wos.ItemIds = row[44]
                        wos.ItemIdsAvail = row[45]
                        wos.BibId = row[46]
                        wos.BibPageCount = row[47]
                        wos.BibPageCountType = row[48]
                        wos.ReviewedLanguageCount = row[49]
                        wos.ReviewedAuthorCount = row[50]
                        wos.ReviewedYear = row[51]
                        wos.KeywordsPlusCount = row[52]
                        wos.BookChapters = row[53]
                        wos.BookPages = row[54]
                        wos.BookNotesCount = row[55]
                        wos.ChapterListCount = row[56]
                        wos.ContributorCount = row[57]
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


schema = graphene.Schema(query=Query)

# class Query(graphene.ObjectType):
#     requests = List(DataRequest, id=Int(required=True))
#     wos = relay.Node.Field(WOS)
#
#
# schema = graphene.Schema(query=Query)




