import logging
import os
import sys

from psycopg2 import pool

abspath = os.path.abspath(os.path.dirname(__file__))
parent = os.path.dirname(abspath)
util = parent + '/util'
sys.path.append(parent)

import util.config_reader

logger = logging.getLogger(__name__)


connection_pool = pool.SimpleConnectionPool(1,
                                            20,
                                            host=util.config_reader.get_cadre_db_hostname(),
                                            database=util.config_reader.get_cadre_db_name(),
                                            user=util.config_reader.get_cadre_db_username(),
                                            password=util.config_reader.get_cadre_db_pwd(),
                                            port=util.config_reader.get_cadre_db_port())

if connection_pool:
    logger.info("Connection pool created successfully")
