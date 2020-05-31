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
from ga_functions import banner
from ga_functions import active_users
from utils import es_engine, ga_engine, db_engine , db_log_error
from config.config import elastic_configs

VIEW_ID = 'ga:26751439'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_Banner_Tracking'
INDX = 'ga_banner'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

es = es_engine.init_engine(elastic_configs['ES_ADDRESS'])
doc = logger.create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


# Database Connection
try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()
    conalch = db_engine.init_engine_alchemy()
    doc = logger.create_log('DB Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to DB!")
    es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
except Exception as e:
    print(e)


def main():
    try:
        analytics = ga_engine.initialize_analyticsreporting('web')
    except Exception as e:
        time.sleep(30)
        try:
            analytics = ga_engine.initialize_analyticsreporting('web')
        except Exception as e:
            db_log_error.log_error('Python_banners' , e)


    for i in range(4):
        try:
            step_time = (datetime.datetime.now().date() + relativedelta(days=-i)).strftime('%Y-%m-%d')
            data = banner.fetch_data(VIEW_ID, analytics, step_time, 'events')
            data.columns = ['date', 'new_customers', 'ctr', 'clicks',
                            'promotion_creative', 'promotion_id', 'promotion_name',
                            'promotion_position', 'impression', 'GMV', 'orders']

            data['date'] = pd.to_datetime(data['date'])
            data['promotion_name'].replace('(not set)', sqlalchemy.sql.null(), inplace=True)
            data['promotion_creative'].replace('(not set)', sqlalchemy.sql.null(), inplace=True)
            data['promotion_id'].replace('(not set)', sqlalchemy.sql.null(), inplace=True)

            data.rename(columns={'promotion_name': 'promotionName',
                                 'promotion_creative': 'promotionCreative',
                                 'promotion_id': 'promotionID',
                                 'promotion_position': 'promotionPosition',
                                 'new_customers': 'newCustomers',
                                 }, inplace=True)
            ordered_cols = ['date','promotionID','promotionCreative','promotionName','promotionPosition','newCustomers','impression','clicks','orders','ctr','GMV']
            data['GMV'] = round(data['GMV'],10)
            data['ctr'] = round(data['ctr'],10)
            data = data[ordered_cols]
            data = data[~data["promotionPosition"].str.contains('"')]
            data['promotionName'] = data['promotionName'].str.strip()
            data['promotionPosition'] = data['promotionPosition'].str.strip()
            data['promotionName'] = data['promotionName'].str.slice(0, 200 - 10)
            data['promotionPosition'] = data['promotionPosition'].str.slice(0, 200 - 10)
        except Exception as e:
            db_log_error.log_error('Python_banners' , e)
        try:
            delete_sql = "DELETE FROM [DB_Marketing].dbo.GA_Banner_Tracking WHERE date = '" + str(step_time) + "'"
            cursor.execute(delete_sql)
            cursor.commit()
            data.to_sql(TABLE_NAME, conalch, method="multi", if_exists='append', index=False, chunksize=10)
            time.sleep(1)
            print('done ' + str(step_time))
        except Exception as e:
            db_log_error.log_error('Python_banners' , e)


if __name__ == '__main__':
    main()
