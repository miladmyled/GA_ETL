import os
import sys
import socket
import argparse
import httplib2
import datetime
import pandas as pd
from utils import logger
from apiclient.discovery import build
from oauth2client import client,file,tools
from dateutil.relativedelta import relativedelta

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

DKAPPCREDPATH = os.path.join(ROOT_DIR, '../credentials/APP/')
DKWEBCREDPATH = os.path.join(ROOT_DIR, '../credentials/WEB/')
DSWEBCREDPATH = os.path.join(ROOT_DIR, '../credentials/DSWEB/')
DSAPPCREDPATH = os.path.join(ROOT_DIR, '../credentials/DSAPP/')


# GA Properties
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')


def initialize_analyticsreporting(mode):

    """Initializes the analyticsreporting service object.

    Returns:
      analytics an authorized analyticsreporting service object.
    """
    if mode == 'web':
        CLIENT_SECRETS_PATH = '{}client_secrets.json'.format(DKWEBCREDPATH)
        storage = file.Storage('{}analyticsreporting.dat'.format(DKWEBCREDPATH))
    elif mode == 'app':
        CLIENT_SECRETS_PATH = '{}client_secrets.json'.format(DKAPPCREDPATH)
        storage = file.Storage('{}analyticsreporting.dat'.format(DKAPPCREDPATH))
    elif mode == 'ds-web':
        CLIENT_SECRETS_PATH = '{}client_secrets.json'.format(DSWEBCREDPATH)
        storage = file.Storage('{}analyticsreporting.dat'.format(DSWEBCREDPATH))
    elif mode == 'ds-app':
        CLIENT_SECRETS_PATH = '{}client_secrets.json'.format(DSAPPCREDPATH)
        storage = file.Storage('{}analyticsreporting.dat'.format(DSAPPCREDPATH))
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
    # storage = file.Storage('{}/GA_Importer/mobile/analyticsreporting.dat'.format(os.getcwd()))

    # if mode == 'web':
    #     storage = file.Storage('{}analyticsreporting.dat'.format(WEBCREDPATH))
    # else:
    #     storage = file.Storage('{}analyticsreporting.dat'.format(APPCREDPATH))

    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


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
                print(header + ': ' + dimension)

            for i, values in enumerate(dateRangeValues):
                print('Date range (' + str(i) + ')')
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    print(metricHeader.get('name') + ': ' + value)


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
                    # set int as int, float a float
                    if ',' in value or '.' in value:
                        dict[metric.get('name')] = float(value)
                    else:
                        dict[metric.get('name')] = int(value)

            list.append(dict)

        df = pd.DataFrame(list)
        return df


def validation(ref_len, _database_engine, _database_name, _database_table, es_engine, es_index):
    sql_maxdate = 'SELECT MAX ([date]) AS "Max Date" FROM {}.dbo.{};'.format(_database_name, _database_table)
    last_insert = pd.read_sql(sql_maxdate, _database_engine).iloc[0][0]

    if last_insert is None:
        ref_date = datetime.datetime.strptime('2017-03-01', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
        sql_lastbatch = "SELECT PK FROM {}.dbo.{}" \
                        " WHERE [date] = '{}'".format(_database_name, _database_table, last_insert)
        last_len_DB = len(_database_engine.execute(sql_lastbatch).fetchall())
        last_len_GA = ref_len #len(fetch_data_daily(config, last_insert.strftime('%Y-%m-%d')))
        if (last_len_GA - last_len_DB) > 0.001 * last_len_GA:
            doc = logger.create_log('DB/GA Consistency', 'Nack', hostname=socket.gethostname(),
                             text='Corrupted Last Insert, truncate the last batch!',
                             server_len=last_len_GA, database_len=last_len_DB)
            es_engine.log_into_es(es_engine, 'textlogs-{}'.format(es_index), doc)
            sys.exit()

    return ref_date


def splitDataFrameList(df,target_column,separator):
    ''' df = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row.
    The values in the other columns are duplicated across the newly divided rows.
    '''
    row_accumulator = []

    def splitListToRows(row, separator):
        try:
            split_row = row[target_column].split(separator)
            for s in split_row:
                new_row = row.to_dict()
                new_row[target_column] = s
                row_accumulator.append(new_row)
        except:
            pass

    try:
        df.apply(splitListToRows, axis=1, args = (separator, ))
    except Exception as e:
        print(e)
    new_df = pd.DataFrame(row_accumulator)
    return new_df
