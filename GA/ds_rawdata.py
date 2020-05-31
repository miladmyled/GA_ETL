#!/usr/bin/env python
# -*- coding: utf-8 -*-


####################################
### Customized for Python 3.x !! ###
####################################

import pandas as pd
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta

import argparse
import pyodbc
from sqlalchemy import create_engine, text
import numpy as np
import pyodbc

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

from utils import es_engine, ga_engine, db_engine
from elasticsearch import Elasticsearch
from datetime import date, timezone

import os

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta

import socket
import time
import sys

from utils import jalali
from utils import logger
from ga_functions import faq
from ga_functions import rawdata
from utils import es_engine, ga_engine, db_engine
from config.config import elastic_configs

VIEW_ID = 'ga:128493183'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_RawData_DS_V2'
INDX = 'ga_rawdata_digistyle'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

es = es_engine.init_engine(elastic_configs['ES_ADDRESS'])
doc = logger.create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)

# Database Connection
try:
    cnxn = db_engine.init_engine(DB_NAME)
    cursor = cnxn.cursor()
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
        ref_date = datetime.datetime.strptime('2019-04-25', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
        sql_lastbatch = "SELECT PK FROM {}.dbo.{}" \
                        " WHERE [date] = '{}'".format(DB_NAME, TABLE_NAME, last_insert)
        last_len_DB = len(cnxn.execute(sql_lastbatch).fetchall())
        last_len_GA = len(rawdata.fetch_data(VIEW_ID, analytics, last_insert.strftime('%Y-%m-%d'), 'trash'))
        if (last_len_GA - last_len_DB) > 0.001 * last_len_GA:
            doc = logger.create_log('DB/GA Consistency', 'Nack', hostname=socket.gethostname(),
                                    text='Corrupted Last Insert, truncate the last batch!',
                                    server_len=last_len_GA, database_len=last_len_DB)
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
            sys.exit()
    return ref_date


def main():
    analytics = ga_engine.initialize_analyticsreporting('ds-web')
    limit_date = datetime.datetime.now().date()
    ref_date = validation(analytics)


    for i in range((limit_date - ref_date).days - 1):
        step_time = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
        total_df = rawdata.fetch_data(VIEW_ID, analytics, step_time, 'trash')
        total_df['ga:adContent'].replace('(not set)', '', inplace=True)
        total_df['ga:campaign'].replace('(not set)', '', inplace=True)
        total_df['ga:keyword'].replace('(not set)', '', inplace=True)
        #total_df.columns = ['adContent', 'campaign', 'date', 'deviceCategory', 'goal12Completions',
        #                'keyword', 'medium', 'sessions', 'source', 'users']
        total_df = total_df.rename(columns={'ga:adContent': 'adContent', 'ga:campaign': 'campaign',
                                            'ga:date': 'date', 'ga:deviceCategory': 'deviceCategory',
                                            'ga:transactions': 'goal12Completions', 'ga:keyword': 'keyword',
                                            'ga:medium': 'medium', 'ga:sessions': 'sessions',
                                            'ga:source': 'source', 'ga:users': 'users'
                                            })


        total_df['date'] = pd.to_datetime(total_df['date'])

        total_df['adContent'] = total_df['adContent'].str.strip()
        total_df['campaign'] = total_df['campaign'].str.strip()
        total_df['deviceCategory'] = total_df['deviceCategory'].str.strip()
        total_df['keyword'] = total_df['keyword'].str.strip()
        total_df['medium'] = total_df['medium'].str.strip()
        total_df['source'] = total_df['source'].str.strip()

        total_df['adContent'] = total_df['adContent'].str.slice(0, 500-10)
        total_df['campaign'] = total_df['campaign'].str.slice(0, 500-10)
        total_df['deviceCategory'] = total_df['deviceCategory'].str.slice(0, 100-10)
        total_df['keyword'] = total_df['keyword'].str.slice(0, 500-10)
        total_df['medium'] = total_df['medium'].str.slice(0, 100-10)
        total_df['source'] = total_df['source'].str.slice(0, 100-10)


        try:
            cursor.fast_executemany = True
            sql_comm = '''INSERT INTO [{}].[dbo].[{}]
            ([adContent],[campaign],[date],[deviceCategory],[goal12Completions],[keyword],
            [medium],[sessions],[source],[users]) VALUES (?,?,?,?,?,?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
            cursor.executemany(sql_comm,total_df.values.tolist())
            cursor.commit()
            doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
                                    'Successful Insert', server_len= len(total_df.index),
                                    database_len= len(total_df.index))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
        except pyodbc.Error as e:
            doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
            sys.exit()


if __name__ == '__main__':
    main()

