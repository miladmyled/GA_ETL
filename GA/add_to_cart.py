#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################################
### Customized for Python 3.x !! ###
####################################

import urllib
import json
import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta

import socket
import time
import sys

from utils import logger
from ga_functions import active_users
from ga_functions import cart
from utils import es_engine, ga_engine, db_engine
from utils import path_parser
from config import config
from config.config import elastic_configs

VIEW_ID = 'ga:26751439'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_Add2Cart_PagePath'
INDX = 'ga_add2cart'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

es = es_engine.init_engine(elastic_configs['ES_ADDRESS'])
doc = logger.create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


# Database Connection
try:
    cnxn = db_engine.init_engine_alchemy(DB_NAME)
    # cursor = cnxn.cursor()
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
        ref_date = datetime.datetime.strptime('2019-05-30', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
    return ref_date


def str_to_dict(json_str):
    try:
        return json.loads(json_str)
    except:
        return None


def main():
    analytics = ga_engine.initialize_analyticsreporting('web')
    limit_date = datetime.datetime.now().date()
    ref_date = validation(analytics)
    # ref_date = datetime.datetime.strptime('2019-06-05', '%Y-%m-%d').date()

    ptrns = ['/search/', '/promotion-page/', '/product-list/', '/cart/',
             '/brand/', '/dkp-', '/landing-page/', '/landings/', '/main/',
             '/profile/', 'adro.co/', 'homepage', 'mobile-homepage', 'outsource']

    types = {'/search/': 'search', '/promotion-page/': 'promotion',
             '/product-list/': 'product-list', '/cart/': 'cart',
             '/brand/': 'brand', '/dkp-': 'product', '/landing-page/': 'landing-page',
             '/landings/': 'landings', '/main/': 'main', 'homepage': 'homepage',
             'mobile-homepage': 'mobile-homepage', '/profile/':'profile', 'adro.co/': 'adro',
             'outsource': 'outsource'}

    for i in range((limit_date - ref_date).days - 1):
        step_time = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
        for ptrn in ptrns[:-1]:
            print(ptrn)
            if ptrn == 'homepage':
                data = cart.fetch_data(VIEW_ID, analytics, step_time, 'https://www.digikala.com/')
            elif ptrn == 'mobile-homepage':
                data = cart.fetch_data(VIEW_ID, analytics, step_time, 'https://mobile.digikala.com/')
            else:
                data = cart.fetch_data(VIEW_ID, analytics, step_time, ptrn)
            data.rename(columns={'ga:dimension5': 'total', 'ga:date': 'date', 'ga:hits':'hits'}, inplace=True)

            data['total'] = data['total'].map(lambda x: str_to_dict(x))
            data = data.dropna(subset=['total'])
            attributes = data['total'].apply(pd.Series)
            data = data.join(attributes)
            data.drop(['total'], axis=1, inplace=True)
            data.rename(columns={'page-path': 'pagepath', 'referrer-path': 'refpath'}, inplace=True)

            # eliminate hits due to the referrer data ...
            if ptrn == 'homepage':
                data = data.query('pagepath == "https://www.digikala.com/" or '
                                  'pagepath == "https://www.digikala.com/?ref=nav_logo"')
            elif ptrn == 'mobile-homepage':
                data = data.query('pagepath == "https://mobile.digikala.com/" or '
                                  'pagepath == "https://mobile.digikala.com/?ref=nav_logo"')
            else:
                data = data[data['pagepath'].str.contains(ptrn) == True]

            data[['pagepath','pagetype']] = path_parser.column_pattern_retriever(data, 'pagepath', ptrn, types[ptrn])
            data['reftype'] = np.nan

            if data.empty:
                continue
            for p in ptrns:
                if p == 'homepage' or p == 'mobile-homepage':
                    sub_data = data.query('refpath == "https://www.digikala.com/" or '
                                          'refpath == "https://www.digikala.com/?ref=nav_logo" or '
                                          'refpath == "https://mobile.digikala.com/?ref=nav_logo" or '
                                          'refpath == "https://mobile.digikala.com/"')
                else:
                    sub_data = data[data['refpath'].str.contains(p) == True]

                if sub_data.empty:
                    continue
                sub_data[['refpath','reftype']] = path_parser.column_pattern_retriever(sub_data, 'refpath', p, types[p])
                data.update(sub_data)
            data['refpath'] = data['refpath'].map(lambda x: 'google' if x.startswith('https://www.google.') else (
                'bing' if x.startswith('https://www.bing.') else x
            ))
            data['reftype'] = data.apply(lambda row: 'outsource' if row['refpath'] == 'google' or
                                                                    row['refpath'] == 'bing' else row['reftype'], axis=1)
            data['reftype'] = data.apply(lambda row: row['reftype'] if pd.notnull(row['reftype']) else 'other', axis=1)
            data['refpath'] = data.apply(lambda row: np.nan if row['reftype']=='other' else row['refpath'], axis=1)

            data['cart-id'] = data['cart-id'].apply(lambda x: np.nan if (x == 0 or x == '') else x)
            data['user-id'] = data['user-id'].apply(lambda x: np.nan if (x == 0 or x == '') else x)
            data['variant-id'] = data['variant-id'].apply(lambda x: np.nan if (x == 0 or x == '') else x)
            data.rename(columns={'pagetype': 'pageType',
                                 'pagepath': 'pagePath',
                                 'reftype': 'referrerType',
                                 'refpath': 'referrer',
                                 'user-id': 'userID',
                                 'cart-id': 'cartID',
                                 'variant-id': 'variantID',
                                 }, inplace=True)


            data['pagePath'] = data['pagePath'].str.slice(0, 150 - 5)
            try:
                data['referrer'] = data['referrer'].str.slice(0, 150 - 5)
            except:
                pass

            data.loc[:, 'date'] = pd.to_datetime(data['date'])
            print(data.shape)
            try:
                data.to_sql(TABLE_NAME, cnxn, method="multi", if_exists='append', index=False, chunksize=10)
                doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
                                        'Successful Insert of {}'.format(ptrn), server_len=len(data.index),
                                        database_len=len(data.index))
                es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
            except Exception as e:
                doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), '{} ERROR: '.format(ptrn)+str(e))
                es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
                print('{} ... {} is Done!'.format(step_time, ptrn))


        #     try:
        #         data.to_sql(TABLE_NAME, cnxn, method="multi", if_exists='append', index=False, chunksize=10)
        #         doc = logger.create_log('Insert', 'Ack', step_time, socket.gethostname(),
        #                                 'Successful Insert', server_len=len(data.index),
        #                                 database_len=len(data.index))
        #         es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
        #     except Exception as e:
        #         doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
        #         es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
        #     print('{} ... {} is Done!'.format(step_time, ptrn))
        # exit()


if __name__ == '__main__':
    main()
