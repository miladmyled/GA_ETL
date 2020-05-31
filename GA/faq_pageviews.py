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

from utils import jalali
from utils import logger
from ga_functions import faq
from ga_functions import active_users
from utils import es_engine, ga_engine, db_engine
from config.config import elastic_configs

VIEW_ID = 'ga:26751439'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_DK_FAQ_PageView'
INDX = 'ga_faq_pageview'

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
        data = faq.fetch_data(VIEW_ID, analytics, step_time, 'pageview')

        data.columns = ['date', 'pagePath', 'pageViews']
        data['pagePath'] = data['pagePath'].str.slice(0, 300)
        data['date'] = pd.to_datetime(data['date'])

        try:
            cursor.fast_executemany = True
            sql_comm = '''INSERT INTO [{}].[dbo].[{}]
            ([date],[pagePath], [pageViews])
             VALUES (?,?,?)'''.format(DB_NAME, TABLE_NAME)
            cursor.executemany(sql_comm, data.values.tolist())
            cursor.commit()
            doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
                                    'Successful Insert', server_len=len(data.index),
                                    database_len=len(data.index))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
        except Exception as e:
            doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)

        time.sleep(2)


if __name__ == '__main__':
    main()
