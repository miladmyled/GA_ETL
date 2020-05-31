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

from elasticsearch import Elasticsearch
from datetime import date, timezone
import socket
import time
import sys
import os

import jalali

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_RawData_Web'
INDX = 'ga_importer_web'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


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

# Elasticsearch Connection
es = Elasticsearch(['172.16.12.162'],
                   port=9200,
                   timeout=30,
                   max_retries=3,
                   retry_on_timeout=True)

doc = create_log('ES Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)

# Database Connection
try:
    cnxn = pyodbc.connect('DSN=MySQLServerDatabase;SERVER=172.30.6.32;DATABASE={};UID=BigData@Marketing;PWD=S@T@RRCdrf45'.format(DB_NAME))
    cursor = cnxn.cursor()
    doc = create_log('DB Connection', 'Ack', hostname=socket.gethostname(), text="Successful Connect to DB!")
    es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
except Exception as e:
    doc = create_log('DB Connection', 'Nack', hostname=socket.gethostname(), text=str(e))
    es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
    sys.exit()


# GA Properties
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = '{}/GA_Importer/Online1/client_secrets.json'.format(os.getcwd()) # Path to client_secrets.json file.
# CLIENT_SECRETS_PATH = 'client_secrets.json'
VIEW_ID = 'ga:26751439'


def initialize_analyticsreporting():
  """Initializes the analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('{}/GA_Importer/Online1/analyticsreporting.dat'.format(os.getcwd()))
  # storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

  return analytics


def get_report(analytics, mode, start='2017-03-01', end='2017-03-01', page='0'):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  if mode == 'day':
      return analytics.reports().batchGet(
          body={
              'reportRequests': [
                  {
                      'viewId': VIEW_ID,
                      'dateRanges': [{'startDate': start, 'endDate': end}],
                      'dimensions': [{"name": "ga:date"}],
                      'metrics': [{'expression': 'ga:users'},{'expression': 'ga:sessions'}],
                      'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                      'samplingLevel':  'LARGE',
                      'pageToken': page,
                      'pageSize': BATCH_SIZE
                  }]
          }
      ).execute()

  elif mode == '2years':
      return analytics.reports().batchGet(
          body={
              'reportRequests': [
                  {
                      'viewId': VIEW_ID,
                      'dateRanges': [{'startDate': start, 'endDate': end}],
                      'dimensions': [{"name": "ga:currencyCode"}],
                      'metrics': [{'expression': 'ga:users'}],
                      'orderBys': [{"fieldName": "ga:currencyCode", "sortOrder": "ASCENDING"}],
                      'samplingLevel':  'LARGE',
                      'pageToken': page,
                      'pageSize': BATCH_SIZE
                  }]
          }
      ).execute()

  else:#if mode == 'month':
      return analytics.reports().batchGet(
          body={
              'reportRequests': [
                  {
                      'viewId': VIEW_ID,
                      'dateRanges': [{'startDate': start, 'endDate': end}],
                      'dimensions': [{"name": "ga:{}".format(mode)}],
                      'metrics': [{'expression': 'ga:users'}],
                      'orderBys': [{"fieldName": "ga:{}".format(mode), "sortOrder": "ASCENDING"}],
                      'samplingLevel':  'LARGE',
                      'pageToken': page,
                      'pageSize': BATCH_SIZE
                  }]
          }
      ).execute()


def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response"""

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print (header + ': ' + dimension)

      for i, values in enumerate(dateRangeValues):
        print ('Date range (' + str(i) + ')')
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print (metricHeader.get('name') + ': ' + value)


def get_response_len(response):
  """Parses and prints the Analytics Reporting API V4 response"""

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    return len(rows)


def create_df(response):
  list = []
  # get report data
  for report in response.get('reports', []):
    # set column headers
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
          dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
          for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
            if ',' in value or ',' in value:
              dict[metric.get('name')] = float(value)
            else:
              dict[metric.get('name')] = int(value)

        list.append(dict)

    df = pd.DataFrame(list)
    return df


def fetch_data(config, input_date):
    response = get_report(config, 'day', input_date, input_date, '0')
    df = create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        response = get_report(config, 'day', input_date, input_date, str(idx))
        df = create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def fetch_data_monthly(config, input_date, end_date):
    response = get_report(config, 'month', input_date, end_date, '0')
    df = create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        response = get_report(config, 'month', input_date, end_date, str(idx))
        df = create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def fetch_data_jalali(config, input_date, end_date, noyears):
    if noyears == 1:
        response = get_report(config, 'year', input_date, end_date, '0')
    else:
        response = get_report(config, '2years', input_date, end_date, '0')
    df = create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        if noyears == 1:
            response = get_report(config, 'year', input_date, end_date, str(idx))
        else:
            response = get_report(config, '2years', input_date, end_date, str(idx))
        df = create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def validation(config):
    sql_maxdate = 'SELECT MAX ([date]) AS "Max Date" FROM {}.dbo.{};'.format(DB_NAME, TABLE_NAME)
    last_insert = pd.read_sql(sql_maxdate, cnxn).iloc[0][0]

    if last_insert is None:
        ref_date = datetime.datetime.strptime('2017-03-01', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
        sql_lastbatch = "SELECT PK FROM {}.dbo.{}" \
                        " WHERE [date] = '{}'".format(DB_NAME, TABLE_NAME, last_insert)
        last_len_DB = len(cnxn.execute(sql_lastbatch).fetchall())
        last_len_GA = len(fetch_data(config, last_insert.strftime('%Y-%m-%d')))
        if (last_len_GA - last_len_DB) > 0.001 * last_len_GA:
            doc = create_log('DB/GA Consistency', 'Nack', hostname=socket.gethostname(),
                             text='Corrupted Last Insert, truncate the last batch!',
                             server_len= last_len_GA, database_len= last_len_DB)
            es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
            sys.exit()
    return ref_date


def main():

    analytics = initialize_analyticsreporting()
    limit_date = datetime.datetime.now().date()
    ref_date = validation(analytics)

    for i in range((limit_date - ref_date).days - 1):
        step_time = ref_date + relativedelta(days=+i)
        # step_time_str = step_time.strftime('%Y-%m-%d')

        # first_str = step_time.replace(day=1).strftime('%Y-%m-%d')
        year, month = jalali.Gregorian(step_time).persian_tuple()[0:2]
        persian_start = jalali.Persian(year, month, 1).gregorian_datetime()

        df_part1 = fetch_data(analytics, step_time.strftime('%Y-%m-%d'))
        df_part1.columns = ['date', 'sessions', 'dailyUsers']
        df_part2 = fetch_data_monthly(analytics, step_time.replace(day=1).strftime('%Y-%m-%d'), step_time.strftime('%Y-%m-%d'))
        df_part2.columns = ['month', 'monthlyUsers']
        if persian_start.year == step_time.year:
            df_part3 = fetch_data_jalali(analytics, persian_start.strftime('%Y-%m-%d'),
                                         step_time.strftime('%Y-%m-%d'), 1)
            df_part3.columns = ['monthlyUsersJalali', 'year']
            df_part3.drop(['year'], axis=1, inplace=True)
        else:
            df_part3 = fetch_data_jalali(analytics, persian_start.strftime('%Y-%m-%d'),
                                         step_time.strftime('%Y-%m-%d'), 2)
            df_part3.columns = ['irrelevant', 'monthlyUsersJalali']
            df_part3.drop(['irrelevant'], axis=1, inplace=True)


        df_part1['date'] = pd.to_datetime(df_part1['date'])
        total_df = pd.concat([df_part1, df_part2, df_part3], axis=1)
        total_df.drop(['month'], axis=1, inplace=True)

        try:
            cursor.fast_executemany = True
            sql_comm = '''INSERT INTO [{}].[dbo].[{}]
            ([date],[sessions],[dailyUsers],[monthlyUsers],[monthlyUsersJalali]) VALUES (?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
            cursor.executemany(sql_comm,total_df.values.tolist())
            cursor.commit()
            doc = create_log('Insert', 'Ack', step_time, socket.gethostname(),
                             'Successful Insert', server_len= len(total_df.index),
                             database_len= len(total_df.index))
            es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
        except Exception as e:
            doc = create_log('Insert', 'Nack', step_time, socket.gethostname(), str(e))
            es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
            sys.exit()

        time.sleep(2)

if __name__ == '__main__':
  main()