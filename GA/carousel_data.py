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

from utils import logger
from utils import path_parser
from ga_functions import carousel
from utils import es_engine, ga_engine, db_engine
from config.config import elastic_configs

#Desktop_ID = 'ga:164962980'
#MobileWeb_ID = 'ga:164937152'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_DK_Carousels'
INDX = 'ga_carousel_perf'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

es = es_engine.init_engine(elastic_configs['ES_ADDRESS'])
doc = logger.create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


# Database Connection
try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()
    doc = logger.create_log('DB Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to DB!")
    # es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
except Exception as e:
    print(str(e))
    # doc = logger.create_log('DB Connection', 'Nack', hostname=socket.gethostname(), text=str(e))
    # es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
    # sys.exit()

view_id = sys.argv[1]
data_type = 'Web' if view_id == 'ga:164962980' else 'Mobile Web'
today_date = datetime.datetime.now().date()
# delete_sql = "DELETE FROM [DB_Marketing].[dbo].[GA_DK_Carousels] WHERE date >= CAST(DATEADD(DAY , -5 , GETDATE()) AS DATE) AND source = '" + data_type + "'"
# cursor.execute(delete_sql)
# cursor.commit()

def main():
    analytics = ga_engine.initialize_analyticsreporting('web')
    # ref_date = validation(analytics)
    # ref_date = datetime.datetime.strptime('2019-07-01', '%Y-%m-%d').date()
    ptrns = ['BRAND', 'CMP', 'HOME' , 'LANDING', 'PDP', 'PLP', 'PROFILE', 'SEARCH', 'INCREDIBLE', 'THANKYOU']

    for i in range(6):
        step_time = today_date + relativedelta(days=-i-3)
        for ptrn in ptrns:
                total_df = carousel.fetch_data(view_id, analytics, step_time.strftime('%Y-%m-%d') , ptrn)
                if total_df.empty:
                    time.sleep(2)
                    continue
                else:
                    total_df.columns = ['date' , 'pagepath'  , 'product_addtocarts' , 'carousel_clicks', 'carousel_name', 'carousel_revenue' , 'product_uniquepurchases']
                    total_df['pagepath'] = total_df['pagepath'].map(lambda x: x.replace('?', '/'))
                    total_df['date'] = pd.to_datetime(total_df['date'])
                    total_df['source'] = data_type
                    total_df = total_df[['date','source' , 'carousel_name' , 'carousel_clicks' , 'product_addtocarts' , 'product_uniquepurchases' , 'carousel_revenue']]
                    total_df['carousel_name'] = total_df['carousel_name'].str.strip()
                    total_df['carousel_name'] = total_df['carousel_name'].str.slice(0, 200 - 10)


                try:
                    print(total_df)
                    # cursor.fast_executemany = True
                    # sql_comm = '''INSERT INTO [{}].[dbo].[{}]([date],[source],[carousel_name],[carousel_clicks],[product_addtocarts],[product_uniquepurchases],[carousel_revenue])
                    #                 VALUES (?,?,?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
                    # cursor.executemany(sql_comm, total_df.values.tolist())
                    # cursor.commit()
                    # doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
                    #                  'Successful Insert', server_len=len(total_df.index),
                    #                  database_len=len(total_df.index))
                    # es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
                    print('done' + ' ' + str(step_time) + '**' + str(ptrn) + ' for ' + data_type)
                    time.sleep(2)
                except Exception as e:
                    doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
                    es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


if __name__ == '__main__':
    main()
