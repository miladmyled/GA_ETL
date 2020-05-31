#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################################
### Customized for Python 3.x !! ###
####################################

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta

import socket
import time
import sys

import sqlalchemy
from utils import logger
from ga_functions import category
from ga_functions import active_users
from utils import es_engine, ga_engine, db_engine
from config.config import elastic_configs

VIEW_ID = 'ga:26751439'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_SupplyCategory_PageView'
INDX = 'ga_supplycat'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

es = es_engine.init_engine(elastic_configs['ES_ADDRESS'])
doc = logger.create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


# Database Connection
try:
    cnxn = db_engine.init_engine_alchemy(DB_NAME)
    doc = logger.create_log('DB Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to DB!")
    es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
except Exception as e:
    doc = logger.create_log('DB Connection', 'Nack', hostname=socket.gethostname(), text=str(e))
    es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
    sys.exit()


def validation(analytics):
    sql_maxdate = 'SELECT MAX ([date]) AS "Max Date" FROM {}.dbo.{};'.format(DB_NAME, TABLE_NAME)
    last_insert = pd.read_sql(sql_maxdate, cnxn).iloc[0][0]

    if last_insert is None:
        ref_date = datetime.datetime.strptime('2019-03-21', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
        sql_lastbatch = "SELECT PK FROM {}.dbo.{}" \
                        " WHERE [date] = '{}'".format(DB_NAME, TABLE_NAME, last_insert)
        last_len_DB = len(cnxn.execute(sql_lastbatch).fetchall())
        last_len_GA = len(active_users.fetch_data_daily(VIEW_ID, analytics, last_insert.strftime('%Y-%m-%d'), 'web'))
        if (last_len_GA - last_len_DB) > 0.001 * last_len_GA:
            doc = logger.create_log('DB/GA Consistency', 'Nack', hostname=socket.gethostname(),
                                    text='Corrupted Last Insert, truncate the last batch!',
                                    server_len=last_len_GA, database_len=last_len_DB)
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
            sys.exit()
    return ref_date


def main():
    analytics = ga_engine.initialize_analyticsreporting('web')
    limit_date = datetime.datetime.now().date()
    ref_date = validation(analytics)


    for i in range((limit_date - ref_date).days - 1):
        step_time = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
        data = category.fetch_data(VIEW_ID, analytics, step_time, 'events')
        data.columns = ['supply_category', 'date', 'page_view', 'unique_page_view']

        data['date'] = pd.to_datetime(data['date'])
        data['supply_category'] = data['supply_category'].str.slice(0, 300 - 5)
        data['supply_category'].replace('(not set)', sqlalchemy.sql.null(), inplace=True)

        data.rename(columns={'supply_category': 'supplyCategory',
                             'page_view': 'pageView',
                             'unique_page_view': 'uniquePageView'
                             }, inplace=True)

        try:
            data.to_sql(TABLE_NAME, cnxn, method="multi", if_exists='append', index=False, chunksize=10)
            doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
                                    'Successful Insert', server_len=len(data.index),
                                    database_len=len(data.index))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
        except Exception as e:
            doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


if __name__ == '__main__':
    main()
