#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import base64
import datetime
import hashlib
import uuid
import requests
import socket
import pyodbc
import pandas as pd
from datetime import date, timezone
from dateutil.relativedelta import relativedelta

from elasticsearch import Elasticsearch
from config.config import elastic_configs

DB_NAME = 'DB_Marketing'

# Elasticsearch Address
es = Elasticsearch(elastic_configs['ES_ADDRESS'],
                   port=9200,
                   timeout=30,
                   max_retries=3,
                   retry_on_timeout=True)

user = '***********'
secret = '***********'


def create_log(mode, status='ACK', batchdate= datetime.datetime(1,1,1).date(), hostname='Unknown', text='Successful', server_len= 0, database_len= 0):
    doc = {
        'status': status,
        'mode': mode,
        'batch_date': batchdate,
        'host': hostname,
        'text': text,
        'server_len': server_len,
        'db_len': database_len,
        'timestamp': datetime.datetime.now(timezone.utc),
    }
    return  doc

# Database Definition
try:
    cnxn = pyodbc.connect('DSN=MySQLServerDatabase;SERVER=172.30.6.33;DATABASE={};UID=BigData@Marketing;PWD=S@T@RRCdrf45'.format(DB_NAME))
    cursor = cnxn.cursor()
except Exception as e:
    doc = create_log('Connection', 'Nack', socket.gethostname(), str(e))
    es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
    sys.exit()


def querying_email_campaigns(start='2017-08-30', stop='2017-08-31', status='3'):
    return 'https://api.emarsys.net/api/v2/email/' \
           '?fromdate={}&todate={}&status={}'.format(start, stop, status)


def querying_response_summary(email_id):
    return 'https://api.emarsys.net/api/v2/email/{}/responsesummary'.format(email_id)


def build_authentication_variables():
    """
    Build the authentication variables Emarsys' authentication system
    asks for.
    :return: nonce, created, password_digest.
    """
    nonce = uuid.uuid4().hex
    created = datetime.datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S+00:00'
        )
    sha1 = hashlib.sha1(
        str.encode(nonce + created + secret)
    ).hexdigest()
    password_digest = bytes.decode(base64.b64encode(str.encode(sha1)))
    return nonce, created, password_digest


def build_headers():
    """
    Build the headers Emarsys' authentication system asks for.
    :return: headers.
    """

    nonce, created, password_digest = build_authentication_variables()

    wsse_header = ','.join(
        (
            'UsernameToken Username="{}"'.format(user),
            'PasswordDigest="{}"'.format(password_digest),
            'Nonce="{}"'.format(nonce),
            'Created="{}"'.format(created),
        )
    )
    http_headers = {
        'X-WSSE': wsse_header,
        'Content-Type': 'application/json'
    }
    return http_headers


def harvest_data(input_frame):

    des_data = input_frame[['id', 'name', 'subject', 'created']]
    des_data.loc[:, 'created'] = pd.to_datetime(des_data['created'])
    des_data.loc[:, 'created'] = des_data['created'].dt.date

    email_id_set = des_data['id']

    df_collector = [0] * len(email_id_set)

    for idx, email_id in enumerate(email_id_set):
        r = requests.get(
            querying_response_summary(email_id),
            headers=build_headers())
        data = r.json()['data']
        data['id'] = email_id
        df_collector[idx] = pd.DataFrame.from_dict(data, orient='index').transpose()

    part2_data = pd.concat(df_collector)

    all_data = pd.merge(des_data, part2_data, on='id')

    all_data['Open Rate'] = pd.to_numeric(all_data['opened'], errors='coerce') \
                            / pd.to_numeric(all_data['sent'], errors='coerce')

    all_data['Click Rate'] = pd.to_numeric(all_data['unique_clicks'], errors='coerce') \
                             / pd.to_numeric(all_data['sent'], errors='coerce')

    all_data['CTR'] = pd.to_numeric(all_data['unique_clicks'], errors='coerce') \
                      / pd.to_numeric(all_data['opened'], errors='coerce')

    btc_time = pd.Series([datetime.datetime.now()]*len(all_data.index))
    all_data['LastModified'] = btc_time.values

    final_data = all_data[['id', 'name', 'subject', 'created', 'sent',
                           'opened', 'unique_clicks', 'unsubscribe',
                           'Open Rate', 'Click Rate', 'CTR', 'LastModified']]
    final_data.columns = ['ID', 'Name', 'Subject', 'Date', 'Sent',
                          'Opened', 'Clicks', 'Unsubscribe',
                          'Open Rate', 'Click Rate', 'CTR', 'LastModified']

    return final_data


sql_maxdate = 'SELECT MAX ([date]) AS "Max Date" FROM {}.dbo.Emarsys_RawData;'.format(DB_NAME)
last_insert = pd.read_sql(sql_maxdate, cnxn).iloc[0][0]

if last_insert is None:
    ref_date = datetime.datetime.strptime('2017-03-21', '%Y-%m-%d').date()
else:
    ref_date = last_insert + relativedelta(days=1)
    sql_lastbatch = "SELECT ID FROM {}.dbo.Emarsys_RawData " \
                    "WHERE [date] = '{}'".format(DB_NAME, last_insert)
    last_batch = cnxn.execute(sql_lastbatch).fetchall()
    last_len_DB = len(last_batch)

    response = requests.get(querying_email_campaigns(start=last_insert,
                                                     stop=(last_insert + relativedelta(days=1)).strftime('%Y-%m-%d'),
                                                     status='3'),
                            headers=build_headers())
    raw_data = pd.DataFrame.from_dict(response.json()['data'], orient='columns')
    last_len_UP = len(raw_data.index)

    if last_len_DB != last_len_UP:
        ids_db = [element for set_ in last_batch for element in set_]
        ids_up = pd.to_numeric(raw_data['id'], errors='coerce')
        data = harvest_data(raw_data)
        data = data.sort_values('Date')
        data = data.where(pd.notnull(data), None)
        candidates = data[pd.to_numeric(data['ID']).isin(list(set(ids_up)-set(ids_db)))]
        try:
            cursor.fast_executemany = True
            sql_insert = '''
            INSERT INTO [{}].[dbo].[Emarsys_RawData]
            ([ID],[Name],[Subject],[Date],[Sent],[Opened],[Clicks],[Unsubscribe],[OpenRate],[ClickRate],[CTR],[LastModified])
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''.format(DB_NAME)
            cursor.executemany(sql_insert, candidates.values.tolist())
            cursor.commit()

            doc = create_log('Late Insert', 'Ack', last_insert, socket.gethostname(), 'Successful Insert')
            es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
        except Exception as e:
            doc = create_log('Corruption', 'Nack', last_insert, socket.gethostname(), 'Last Batch does not match!', last_len_UP, last_len_DB)
            es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
            sys.exit()

limit_date = datetime.datetime.now().date()
CI_date = limit_date - relativedelta(days=45)


for i in range((limit_date - ref_date).days - 1):
     step_time = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
     limit_time = (ref_date + relativedelta(days=+(i+1))).strftime('%Y-%m-%d')

     response = requests.get(
         querying_email_campaigns(start=step_time, stop=limit_time , status='3'),
         headers=build_headers())

     raw_data = pd.DataFrame.from_dict(response.json()['data'], orient='columns')
     data = harvest_data(raw_data)
     data = data.sort_values('Date')
     data = data.where(pd.notnull(data), None)

     # Insert Data
     try:
         cursor.fast_executemany = True
         sql_insert = '''
         INSERT INTO [{}].[dbo].[Emarsys_RawData]
         ([ID],[Name],[Subject],[Date],[Sent],[Opened],[Clicks],[Unsubscribe],[OpenRate],[ClickRate],[CTR],[LastModified])
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''.format(DB_NAME)
         cursor.executemany(sql_insert, data.values.tolist())
         cursor.commit()

         doc = create_log('Insert', 'Ack', step_time, socket.gethostname(), 'Successful Insert')
         es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
     except Exception as e:
         doc = create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
         es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
         sys.exit()

     print('INSERT  {}||{}   -----   {} <><><> {}'.format(step_time, limit_time, len(data.index), datetime.datetime.now()))


for i in range((limit_date - CI_date).days - 1):
     step_time = (CI_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
     limit_time = (CI_date + relativedelta(days=+(i+1))).strftime('%Y-%m-%d')

     response = requests.get(
         querying_email_campaigns(start=step_time, stop=limit_time , status='3'),
         headers=build_headers())

     raw_data = pd.DataFrame.from_dict(response.json()['data'], orient='columns')
     data = harvest_data(raw_data)
     data = data.sort_values('Date')
     data = data.where(pd.notnull(data), None)

     # Update Data
     try:
         cursor.fast_executemany = True
         sql_update = '''
         UPDATE [{}].[dbo].[Emarsys_RawData]
         SET Name=?,Subject=?,Date=?,Sent=?,Opened=?,Clicks=?,Unsubscribe=?,OpenRate=?,ClickRate=?,CTR=?,LastModified=?
         WHERE ID=?
         '''.format(DB_NAME)

         pivot = data[
             ['Name', 'Subject', 'Date', 'Sent',
              'Opened', 'Clicks', 'Unsubscribe',
              'Open Rate', 'Click Rate', 'CTR', 'LastModified', 'ID']
         ]
         cursor.executemany(sql_update, pivot.values.tolist())
         cursor.commit()

         doc = create_log('Update', 'Ack', step_time, socket.gethostname(), 'Successful Update')
         es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
     except Exception as e:
         doc = create_log('Update', 'Nack', step_time, socket.gethostname(), str(e))
         es.index(index="textlogs-emarsys_importer", doc_type='log', body=doc)
         sys.exit()

     print('UPDATE  {}||{}   -----   {} <><><> {}'.format(step_time, limit_time, len(data.index), datetime.datetime.now()))
