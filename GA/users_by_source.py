#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################################
### Customized for Python 3.x !! ###
####################################

import urllib
import numpy as np
import pandas as pd
import datetime
import mysql.connector
from dateutil.relativedelta import relativedelta

import socket
import time
import sys
import re
import sqlalchemy

from config.config import local_mysql
from utils import logger
from ga_functions import users_sources
from utils import es_engine, ga_engine, db_engine
from utils import path_parser
from MySQL import mysql_queries

from sqlalchemy import create_engine
from config.config import elastic_configs

VIEW_ID = 'ga:26751439'

DB_NAME = 'data_insight'
TABLE_NAME = 'users_source_fresh'
INDX = 'ga_importer_users_sources'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

mysql_engine = mysql.connector.connect(host=local_mysql['host'],
                                       user=local_mysql['user'],
                                       passwd=local_mysql['passwd'],
                                       database=local_mysql['database'])

es = es_engine.init_engine(elastic_configs['ES_ADDRESS'])
doc = logger.create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)


# Database Connection
try:
    engine = create_engine('mysql://{}:{}@{}/{}'.format(local_mysql['user'], local_mysql['passwd'],
                                                        local_mysql['host'], local_mysql['database']))
    doc = logger.create_log('DB Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to DB!")
    es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
except Exception as e:
    doc = logger.create_log('DB Connection', 'Nack', hostname=socket.gethostname(), text=str(e))
    es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
    sys.exit()


def validation():
    with engine.connect() as conn, conn.begin():
        data = pd.read_sql_query('select MAX(`date`) as ref FROM {}.{}'.format(DB_NAME, TABLE_NAME), conn)
        last_insert = data.iloc[0][0]
        if last_insert is None:
            ref_date = datetime.datetime.strptime('2019-07-01', '%Y-%m-%d').date()
        else:
            ref_date = last_insert + relativedelta(days=1)
    return ref_date


def main():
    fresh_suply = pd.DataFrame(mysql_queries.get_fresh_supply_cat(0))
    main_cats = pd.DataFrame(mysql_queries.get_main_cats(0))
    main_cats = main_cats.loc[main_cats['code'] == 'food-beverage']
    fresh_suply['code'] = fresh_suply['code'].map(lambda x: 'category-' + x)
    analytics = ga_engine.initialize_analyticsreporting('web')
    limit_date = datetime.datetime.now().date()
    ref_date = validation()
    # ref_date = datetime.datetime.strptime('2019-07-06', '%Y-%m-%d').date()

    ptrns = ['/search/', '/promotion-page/', '/product-list/',
             '/dkp-', '/main/']
    types = {'/search/': 'search', '/promotion-page/': 'promotion',
             '/product-list/': 'product-list', '/cart/': 'cart',
             '/brand/': 'brand', '/dkp-': 'product', '/landing-page/': 'landing-page',
             '/landings/': 'landings', '/main/': 'main', 'homepage': 'home-page'}
    for i in range((limit_date - ref_date).days - 1):
        step_time = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
        for ptrn in ptrns:
            data = users_sources.fetch_data(VIEW_ID, analytics, step_time, ptrn)
            if data.empty:
                continue
            data.columns = ['date', 'landingpage', 'medium', 'newusers', 'source']
            data['landingpage'] = data['landingpage'].map(lambda x: x.replace('?', '/'))
            data = data[~data['landingpage'].str.contains('/users/register/')]
            data = data[~data['landingpage'].str.contains('/users/login/')]

            # #backup
            data['backup'] = data['landingpage']
            # distinguish compare & product
            if ptrn == '/dkp-':
                data['landingpage'] = data['landingpage'].map(lambda x: 'compare' if x.startswith('/compare/dkp-') else
                path_parser.get_dkp(x))
            elif ptrn == 'homepage':
                # get logo data
                list_dfs = [data]
                list_dfs.append(users_sources.fetch_data(VIEW_ID, analytics, step_time, 'dk-logo'))
                if list_dfs[1].empty:
                    continue
                list_dfs[1].columns = ['date', 'landingpage', 'medium', 'newusers', 'source']
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

            data.rename(columns={'newusers': 'new_users',
                                 'pageType': 'page_type',
                                 'landingpage': 'landingPage'}, inplace=True)
            ordered_cols = ['date', 'page_type', 'source', 'medium', 'landingPage', 'new_users']
            data = data[ordered_cols]
            # data['source'].replace('(none)', sqlalchemy.sql.null(), inplace=True)
            # data['medium'].replace('(none)', sqlalchemy.sql.null(), inplace=True)
            data['landingPage'] = data['landingPage'].str.slice(0, 200 - 5)
            data['source'] = data['source'].str.slice(0, 200 - 5)
            data['meidum'] = data['medium'].str.slice(0, 50 - 5)
            data.loc[:, 'date'] = pd.to_datetime(data['date'])
            data = data.groupby(['date', 'page_type', 'landingPage', 'source', 'medium']).sum().reset_index()

            fresh_suply_tmp = fresh_suply.copy()
            if ptrn == '/dkp-':
                data['landingPage'] = pd.to_numeric(data['landingPage'], errors='coerce')
                data = data.dropna(subset=['landingPage'])
                data['landingPage'] = data['landingPage'].astype(int)
                data.rename(columns={'landingPage': 'product_id'}, inplace=True)
                outcome = data.merge(fresh_suply_tmp, how='inner', on = ['product_id'])
                outcome.drop('code', axis=1, inplace=True)
                outcome.rename(columns={'product_id': 'code'}, inplace=True)
                outcome = outcome.drop_duplicates()
                outcome.drop('supply_cat', axis=1, inplace=True)
            elif ptrn == '/search/':
                fresh_suply_tmp.drop('product_id', axis=1, inplace=True)
                fresh_suply_tmp = fresh_suply_tmp.drop_duplicates()
                data.rename(columns={'landingPage': 'code'}, inplace=True)
                outcome = data.merge(fresh_suply_tmp, how='inner', on=['code'])
                outcome.drop('supply_cat', axis=1, inplace=True)
            elif ptrn == '/product-list/' or ptrn == '/promotion-page/':
                data = data[data['page_type'].str.startswith('fresh-')]
                data.rename(columns={'landingPage': 'code'}, inplace=True)
                outcome = data
            elif ptrn == '/main/':
                data.rename(columns={'landingPage': 'code'}, inplace=True)
                outcome = data.merge(main_cats, how='inner', on=['code'])



            try:
                with engine.connect() as conn, conn.begin():
                    outcome.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
            except Exception as e:
                doc = logger.create_log('Insert', 'Nack', step_time, socket.gethostname(), '{} ERROR: '.format(ptrn)+str(e))
                es_engine.log_into_es(es, 'textlogs-{}'.format(INDX), doc)
            # exit()

if __name__ == '__main__':
    main()
