#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################################
### Customized for Python 3.x !! ###
####################################

import urllib
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta

import socket
import time
import sys
import re
import sqlalchemy

from utils import logger
from ga_functions import landing
from utils import es_engine, ga_engine, db_engine
from utils import path_parser
from config.config import elastic_configs


VIEW_ID = 'ga:26751439'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_Landing_Data'
INDX = 'ga_landing'

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


def validation():
    sql_maxdate = 'SELECT MAX ([date]) AS "Max Date" FROM {}.dbo.{};'.format(DB_NAME, TABLE_NAME)
    last_insert = pd.read_sql(sql_maxdate, cnxn).iloc[0][0]

    if last_insert is None:
        ref_date = datetime.datetime.strptime('2019-05-12', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
    return ref_date


def main():
    analytics = ga_engine.initialize_analyticsreporting('web')
    limit_date = datetime.datetime.now().date()
    ref_date = validation()

    ptrns = ['/search/', '/promotion-page/', '/product-list/', '/cart/',
             '/brand/', '/dkp-', '/landing-page/', '/landings/', '/main/', 'homepage']
    types = {'/search/': 'search', '/promotion-page/': 'promotion',
             '/product-list/': 'product-list', '/cart/': 'cart',
             '/brand/': 'brand', '/dkp-': 'product', '/landing-page/': 'landing-page',
             '/landings/': 'landings', '/main/': 'main', 'homepage': 'home-page'}
    for i in range((limit_date - ref_date).days - 1):
        step_time = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
        for ptrn in ptrns:
            data = landing.fetch_data(VIEW_ID, analytics, step_time, ptrn)

            if data.empty:
                continue
            data.columns = ['date', 'landingpage', 'pageview', 'source', 'unique_pageview']
            data['landingpage'] = data['landingpage'].map(lambda x: x.replace('?', '/'))
            data = data[~data['landingpage'].str.contains('/users/register/')]
            data = data[~data['landingpage'].str.contains('/users/login/')]

            #backup
            data['backup'] = data['landingpage']
            # distinguish compare & product
            if ptrn == '/dkp-':
                data['landingpage'] = data['landingpage'].map(lambda x: 'compare' if x.startswith('/compare/dkp-') else
                path_parser.get_dkp(x))
            elif ptrn == 'homepage':
                # get logo data
                list_dfs = [data]
                list_dfs.append(landing.fetch_data(VIEW_ID, analytics, step_time, 'dk-logo'))
                if list_dfs[1].empty:
                    continue
                list_dfs[1].columns = ['date', 'landingpage', 'pageview', 'source', 'unique_pageview']
                list_dfs[1]['landingpage'] = 'dk-logo'
                data = pd.concat(list_dfs)
            else:
                data['landingpage'] = data['landingpage'].map(lambda x: ptrn[1:] + x.split(ptrn,1)[-1])
                special_subcats = lambda x: x.split('/',2)[1] if x.startswith('search/category-') \
                    else ('search' if x.startswith('search/') \
                              else ('cart' if x.startswith('cart/')
                                    else ('landing-page' if x.startswith('landing-page/')
                                          else x.split('/', 2)[1])))
                data['landingpage'] = data['landingpage'].map(special_subcats)

            data['pageType'] = types[ptrn]
            if ptrn in ['/promotion-page/', '/product-list/']:
                data['pageType'] = data.apply(lambda x: 'fresh-'+x['pageType'] if 'fresh=1' in x['backup']
                else x['pageType'], axis=1)

            data.rename(columns={'pageview': 'pageView',
                                 'landingpage': 'landingPage',
                                 'unique_pageview': 'uniquePageView'}, inplace=True)
            ordered_cols = ['date', 'pageType', 'landingPage', 'source', 'pageView', 'uniquePageView']
            data = data[ordered_cols]
            data['landingPage'] = data['landingPage'].str.slice(0, 200 - 5)
            data['source'] = data['source'].str.slice(0, 200 - 5)
            data.loc[:, 'date'] = pd.to_datetime(data['date'])
            data = data.groupby(['date', 'pageType', 'landingPage', 'source']).sum().reset_index()

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



if __name__ == '__main__':
    main()
