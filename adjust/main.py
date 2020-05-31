import sys
import pytz
import socket
import pyodbc
import warnings
import datetime
from datetime import date, timezone
from dateutil.relativedelta import relativedelta

from elasticsearch import Elasticsearch

import pandas as pd
import numpy as np
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.rinterface import RRuntimeWarning
from config.config import elastic_configs

irantime = pytz.timezone('Iran')

warnings.filterwarnings("ignore", category=RRuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Tokens
DIGIKALA_ANDROID = '***********'
DIGIKALA_IOS = '***********'
DIGISTYLE = '***********'

# Initialize R Engine
r = robjects.r

DB_NAME = 'DB_Marketing'
INDX = 'adjust_importer'


def create_log(mode, app_id, status='ACK', batchdate= datetime.datetime(1,1,1).date(),
               hostname='Unknown', text='Successful', server_len= 0, database_len= 0):
    doc = {
        'status': status,
        'app_id': app_id,
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
es = Elasticsearch(elastic_configs['ES_ADDRESS'],
                   port=9200,
                   timeout=30,
                   max_retries=3,
                   retry_on_timeout=True)


def es_connection_check(app_id):

    try:
        doc = create_log('ES Connection', app_id, status='Ack', hostname=socket.gethostname(), text="Successful Connect to ES!")
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
    except Exception as e:
        doc = create_log('ES Connection', app_id, status='Nack', hostname=socket.gethostname(), text=str(e))
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
        sys.exit()


def db_connection(app_id):
    # Database Connection
    try:
        cnxn = pyodbc.connect('DSN=MySQLServerDatabase;SERVER=172.30.6.33;DATABASE={};UID=BigData@Marketing;PWD=S@T@RRCdrf45'.format(DB_NAME))
        cursor = cnxn.cursor()
        doc = create_log('DB Connection', app_id, status='Ack', hostname=socket.gethostname(), text="Successful Connect to DB!")
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
        return cnxn, cursor
    except Exception as e:
        doc = create_log('DB Connection', app_id, status='Nack', hostname=socket.gethostname(), text=str(e))
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
        sys.exit()


def fetch_data(app_struct, input_date, offset):
    # Run the R engine
    rcode = generate_rcode(app_struct.token, input_date, input_date, offset, app_struct.sandbox)
    r(rcode)

    # Get the result
    table = robjects.r['table']
    return pandas2ri.ri2py_dataframe(table)


def validation(app_struct, cnxn):
    sql_maxdate = 'SELECT MAX ([date]) AS "Max Date" FROM {}.dbo.{};'.format(DB_NAME, app_struct.table)
    last_insert = pd.read_sql(sql_maxdate, cnxn).iloc[0][0]

    if last_insert is None:
        ref_date = datetime.datetime.strptime('2017-03-01', '%Y-%m-%d').date()
    else:
        ref_date = last_insert + relativedelta(days=1)
        sql_lastbatch = "SELECT PK FROM {}.dbo.{}" \
                        " WHERE [date] = '{}'".format(DB_NAME, app_struct.table, last_insert)
        last_len_DB = len(cnxn.execute(sql_lastbatch).fetchall())
        null_date = datetime.datetime.strptime('0001-01-01', '%Y-%m-%d')
        offset = null_date + irantime.utcoffset(ref_date + relativedelta(hours=1))
        last_len_UP = len(fetch_data(app_struct, last_insert.strftime('%Y-%m-%d'), offset.strftime('%H:%M')))
        if (last_len_UP - last_len_DB) > 0.1 * last_len_UP:
            doc = create_log('DB/Adjust Consistency', app_struct.id, status='Nack', hostname=socket.gethostname(),
                             text='Corrupted Last Insert, truncate the last batch!',
                             server_len= last_len_UP, database_len= last_len_DB, batchdate=last_insert)
            es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
            sys.exit()
    return ref_date


class Application(object):
    id = ''
    token = ''
    table = ''
    sandbox = ''


    def __init__(self, id, token, table, sandbox):
        self.id = id
        self.token = token
        self.table = table
        self.sandbox = sandbox


def insert_to_db(cursor, app_struct, dataframe, batch_date):

    try:
        cursor.fast_executemany = True
        sql_comm = '''INSERT INTO [{}].[dbo].[{}]
        ([date],[tracker_token],[network],[campaign],[adgroup],[creative],[region],[os_name],
        [clicks],[installs],[sessions],[revenue_events],[revenue])
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'''.format(DB_NAME, app_struct.table)
        cursor.executemany(sql_comm,dataframe.values.tolist())
        cursor.commit()

        doc = create_log('Insert', app_struct.id, status='Ack', batchdate= batch_date, hostname=socket.gethostname(),
                         text='Successful Insert', server_len=len(dataframe.index),
                         database_len=len(dataframe.index))
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
    except Exception as e:
        doc = create_log('Insert', app_struct.id, status='Nack', batchdate= batch_date, hostname=socket.gethostname(),
                         text=str(e))
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
        sys.exit()


def heavy_process(cursor, app_struct, limit_date, ref_date):
    null_date = datetime.datetime.strptime('0001-01-01', '%Y-%m-%d')
    for i in range((limit_date - ref_date).days - 2):
        step_time = (ref_date + relativedelta(days=+i))
        step_time_str = (ref_date + relativedelta(days=+i)).strftime('%Y-%m-%d')
        offset = null_date + irantime.utcoffset(step_time + relativedelta(hours=1))
        # print('{} --- {}'.format(step_time_str, '+' + offset.strftime('%H:%M')))
        try:
            total_df = fetch_data(app_struct, step_time, offset.strftime('%H:%M'))

            total_df.columns = ['date', 'tracker_token', 'network', 'campaign', 'adgroup',
                                'creative', 'region', 'os_name', 'clicks', 'installs',
                                'sessions', 'revenue_events', 'revenue']
            total_df['date'] = pd.to_datetime(total_df['date'])

            total_df['tracker_token'] = total_df['tracker_token'].str.strip()
            total_df['network'] = total_df['network'].str.strip()
            total_df['campaign'] = total_df['campaign'].str.strip()
            total_df['adgroup'] = total_df['adgroup'].str.strip()
            total_df['creative'] = total_df['creative'].str.strip()
            total_df['region'] = total_df['region'].str.strip()
            total_df['os_name'] = total_df['os_name'].str.strip()

            total_df['tracker_token'] = total_df['tracker_token'].str.slice(0, 20)
            total_df['network'] = total_df['network'].str.slice(0, 100)
            total_df['campaign'] = total_df['campaign'].str.slice(0, 300)
            total_df['adgroup'] = total_df['adgroup'].str.slice(0, 500)
            total_df['creative'] = total_df['creative'].str.slice(0, 500)
            total_df['region'] = total_df['region'].str.slice(0, 20)
            total_df['os_name'] = total_df['os_name'].str.slice(0, 20)

            total_df['clicks'] = total_df['clicks'].fillna(0).astype(np.uint32)
            total_df['installs'] = total_df['installs'].fillna(0).astype(np.uint32)
            total_df['sessions'] = total_df['sessions'].fillna(0).astype(np.uint32)
            total_df['revenue_events'] = total_df['revenue_events'].fillna(0).astype(np.uint64)
            total_df['revenue'] = total_df['revenue'].fillna(0).astype(np.uint64)

            insert_to_db(cursor, app_struct, total_df, step_time_str)

        except Exception as e:
            doc = create_log('Bypassed Insert', app_struct.id, status='Ack', batchdate=step_time_str, hostname=socket.gethostname(),
                             text=str(e))
            es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)


def main():
    try:
        app_name = sys.argv[1]
        if app_name == 'DIGIKALA_ANDROID':
            app = Application(app_name, '1kb4w3cqkaao', 'ADJUST_RawData_Android', 'FALSE')
        elif app_name == 'DIGIKALA_ANDROID_SANDBOX':
            app = Application(app_name, '1kb4w3cqkaao', 'ADJUST_RawData_Android_SBOX', 'TRUE')
        elif app_name == 'DIGIKALA_IOS':
            app = Application(app_name, 'v06yl0ipkqv4', 'ADJUST_RawData_iOS', 'FALSE')
        elif app_name == 'DIGISTYLE':
            app = Application(app_name, 'fcriysi2gu0w', 'ADJUST_RawData_DigiStyle', 'FALSE')
        else:
            doc = create_log('Application Run Mode', 'None', status='Nack', hostname=socket.gethostname(),
                             text='Undefined Run Mode')
            es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
            sys.exit()
        es_connection_check(app.id)
        cnxn, cursor = db_connection(app.id)
        limit_date = datetime.datetime.now().date()
        ref_date = validation(app, cnxn)
        heavy_process(cursor, app, limit_date, ref_date)

    except Exception as e:
        doc = create_log('Application Exception', 'None', status='Nack', hostname=socket.gethostname(), text=str(e))
        es.index(index="textlogs-{}".format(INDX), doc_type='log', body=doc)
        sys.exit()


def generate_rcode(app_token, start_date, stop_date, utc_offset, sandbox,
                   usr_token = 'FAoBm7a2FwW1RsssFDgX'):

    query1 = '''
        table1 = adjust.deliverables(
            app.tokens = '{}',
            start_date = '{}',
            end_date = '{}',
            kpis = c('clicks', 'installs', 'sessions'),
            grouping = c('date', 'network', 'campaign', 'adgroup', 'creative',
                         'region', 'os_names'),
            utc_offset= '{}',
            sandbox = {}
        )
    '''.format(app_token, start_date, stop_date, utc_offset, sandbox)

    query2 = '''
        table2 = adjust.events(
            app.tokens = '{}',
            start_date = '{}',
            end_date = '{}',
            grouping = c('date', 'network', 'campaign', 'adgroup', 'creative',
                         'region', 'os_names'),
            utc_offset = '{}',
            sandbox = {}
        )
    '''.format(app_token, start_date, stop_date, utc_offset, sandbox)


    rcode = '''
    library('adjust');
    library('reshape');

    adjust.setup(user.token='{}')

    {}

    {}

    table = merge(x = table1, y= table2,
              by= c('date', 'tracker_token', 'network', 'campaign',
                    'adgroup', 'creative', 'region', 'os_name'), all=TRUE)

    table = table[,c('date','tracker_token','network','campaign','adgroup',            
                 'creative', 'region','os_name','clicks','installs',
                 'sessions','revenue_events','revenue')]

    '''.format(usr_token, query1, query2)

    return rcode


if __name__ == '__main__':
  main()
