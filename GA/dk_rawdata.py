#!/usr/bin/env python
# -*- coding: utf-8 -*-


####################################
### Customized for Python 3.x !! ###
####################################

import pandas as pd
import datetime
import  time
from dateutil.relativedelta import relativedelta

from ga_functions import rawdata
from utils import  ga_engine, db_engine , db_log_error


# VIEW_ID_DM = '26751439' # HELP: digikala.com view (desktop and mobileweb mixed)
# VIEW_ID_APP = '98444990' # HELP: Digikala App view (android and ios mixed)
VIEW_ID = 'ga:26751439'
View_Name = 'App' if VIEW_ID == 'ga:98444990' else 'Desktop'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_RawData_V2_test'
INDX = 'ga_rawdata_v2'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000



# Database Connection
try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()
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
            db_log_error.log_error('Python_dk_rawdata' , e)


    for i in range(5):
        try:
            step_time = (datetime.datetime.now().date() + relativedelta(days=-i)).strftime('%Y-%m-%d')
            # find_row_sql = "SELECT Date FROM TMP_TestDates WHERE Id = {}".format(i+1)
            # cursor.execute(find_row_sql)
            # for row in cursor.fetchall():
            #     selected_date = row.Date
            # step_time = datetime.datetime.strptime(str(selected_date) , '%Y-%m-%d').date().strftime('%Y-%m-%d')
            try:
                total_df = rawdata.fetch_data(VIEW_ID, analytics, step_time, 'trash')
            except:
                time.sleep(30)
                total_df = rawdata.fetch_data(VIEW_ID, analytics, step_time, 'trash')
            if total_df.empty:
                time.sleep(2)
                print(str(step_time) + " is empty")
                continue
            else:
                total_df['ga:adContent'].replace('(not set)', '', inplace=True)
                total_df['ga:campaign'].replace('(not set)', '', inplace=True)
                total_df['ga:keyword'].replace('(not set)', '', inplace=True)
                total_df['view'] = View_Name
                total_df = total_df.rename(columns={'ga:adContent': 'adContent', 'ga:campaign': 'campaign',
                                                    'ga:date': 'date' , 'view' : 'view', 'ga:deviceCategory': 'deviceCategory', 'ga:operatingSystem' : 'OS' ,
                                                    'ga:transactions': 'goal12Completions', 'ga:keyword': 'keyword',
                                                    'ga:medium': 'medium', 'ga:sessions': 'sessions',
                                                    'ga:source': 'source', 'ga:users': 'users'
                                                    })

                ordered_cols = ['adContent', 'campaign', 'date', 'view', 'deviceCategory',
                                'OS', 'ga:goal12Completions', 'keyword', 'medium', 'sessions', 'source' , 'users']

                total_df['date'] = pd.to_datetime(total_df['date'])
                total_df = total_df[ordered_cols]
                total_df['adContent'] = total_df['adContent'].str.strip()
                total_df['campaign'] = total_df['campaign'].str.strip()
                total_df['deviceCategory'] = total_df['deviceCategory'].str.strip()
                total_df['keyword'] = total_df['keyword'].str.strip()
                total_df['medium'] = total_df['medium'].str.strip()
                total_df['source'] = total_df['source'].str.strip()

                total_df['adContent'] = total_df['adContent'].str.slice(0, 500-10)
                total_df['campaign'] = total_df['campaign'].str.slice(0, 500-10)
                total_df['deviceCategory'] = total_df['deviceCategory'].str.slice(0, 100-10)
                total_df['keyword'] = total_df['keyword'].str.slice(0, 500-10)
                total_df['medium'] = total_df['medium'].str.slice(0, 100-10)
                total_df['source'] = total_df['source'].str.slice(0, 100-10)
        except Exception as e:
            print(e)
            db_log_error.log_error('Python_dk_rawdata' , e)

        try:
            delete_sql = "DELETE FROM [DB_Marketing].[dbo].[GA_RawData_V2_test] WHERE date = '" + str(step_time) + "' and [view] = '" + View_Name + "'"
            cursor.execute(delete_sql)
            cursor.commit()
            cursor.fast_executemany = True
            sql_comm = '''INSERT INTO [{}].[dbo].[{}]
            ([adContent],[campaign],[date],[view],[deviceCategory],[OS],[goal12Completions],[keyword],
            [medium],[sessions],[source],[users]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
            cursor.executemany(sql_comm,total_df.values.tolist())
            cursor.commit()
            print("Done " + str(step_time) + 'at :' + str(datetime.datetime.now()))
        except Exception as e:
            db_log_error.log_error('Python_dk_rawdata' , e)


if __name__ == '__main__':
    main()

