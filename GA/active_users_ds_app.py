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
from ga_functions import active_users
from utils import es_engine, ga_engine, db_engine
from config.config import elastic_configs

VIEW_ID = 'ga:151572876'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_ActiveUsers_DS_App'
INDX = 'ga_activeusers_ds_app'

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
        ref_date = datetime.datetime.strptime('2017-06-28', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
        sql_lastbatch = "SELECT PK FROM {}.dbo.{}" \
                        " WHERE [date] = '{}'".format(DB_NAME, TABLE_NAME, last_insert)
        last_len_DB = len(cnxn.execute(sql_lastbatch).fetchall())
        last_len_GA = len(active_users.fetch_data_daily(VIEW_ID, analytics, last_insert.strftime('%Y-%m-%d'), 'app'))
        if (last_len_GA - last_len_DB) > 0.001 * last_len_GA:
            doc = logger.create_log('DB/GA Consistency', 'Nack', hostname=socket.gethostname(),
                                    text='Corrupted Last Insert, truncate the last batch!',
                                    server_len=last_len_GA, database_len=last_len_DB)
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
            sys.exit()
    return ref_date


def main():
    analytics = ga_engine.initialize_analyticsreporting('ds-app')
    limit_date = datetime.datetime.now().date()
    ref_date = validation(analytics)

    for i in range((limit_date - ref_date).days - 1):
        step_time = ref_date + relativedelta(days=+i)
        year, month = jalali.Gregorian(step_time).persian_tuple()[0:2]
        custom_start = jalali.Persian(year, month, 1).gregorian_datetime()
        df_part1 = active_users.fetch_data_daily(VIEW_ID, analytics, step_time.strftime('%Y-%m-%d'), 'app')
        df_part1.columns = ['date', 'sessions', 'dailyUsers']
        df_part2 = active_users.fetch_data_monthly(VIEW_ID, analytics, step_time.replace(day=1).strftime('%Y-%m-%d'),
                                                   step_time.strftime('%Y-%m-%d'), 'app')
        df_part2.columns = ['month', 'monthlyUsers']
        df_part3 = active_users.fetch_data_custom_wrapper(VIEW_ID, analytics, custom_start,
                                                          step_time, 'monthlyUsersJalali', 'app')
        df_part4 = active_users.fetch_data_custom_wrapper(VIEW_ID, analytics,
                                                          step_time + relativedelta(days=-29), step_time,
                                                          '30DaysWindow', 'app')

        df_part1['date'] = pd.to_datetime(df_part1['date'])
        total_df = pd.concat([df_part1, df_part2, df_part3, df_part4], axis=1)
        total_df.drop(['month'], axis=1, inplace=True)


        try:
            cursor.fast_executemany = True
            sql_comm = '''INSERT INTO [{}].[dbo].[{}]
            ([date],[sessions],[dailyUsers],[monthlyUsers],[monthlyUsersJalali],[30DaysWindow])
             VALUES (?,?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
            cursor.executemany(sql_comm, total_df.values.tolist())
            cursor.commit()
            doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
                             'Successful Insert', server_len=len(total_df.index),
                             database_len=len(total_df.index))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
        except Exception as e:
            doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
            es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)

        time.sleep(2)


if __name__ == '__main__':
    main()
