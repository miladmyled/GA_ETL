#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################################
### Customized for Python 3.x !! ###
####################################

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta


import time
import sys


from ga_functions import trends
from utils import db_log_error, ga_engine, db_engine


#Desktop_ID = 'ga:164962980'
#MobileWeb_ID = 'ga:164937152'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_DK_Trends'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


# Database Connection
try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()
except Exception as e:
    print(str(e))


view_id = sys.argv[1]
# desktop = 'ga:164962980'
# mobileWeb = 'ga:164937152'
data_type = 'Web' if view_id == 'ga:164962980' else 'Mobile Web'
today_date = datetime.datetime.now().date()


def main():
    analytics = ga_engine.initialize_analyticsreporting('web')

    for i in range(5):
        try:
            step_time = today_date + relativedelta(days=-i)
            # step_time = datetime.datetime.strptime('2019-02-13' , '%Y-%m-%d').date()
            total_df = trends.fetch_data(view_id, analytics, step_time.strftime('%Y-%m-%d'))
            if total_df.empty:
                time.sleep(2)
                continue
            else:
                total_df.columns = ['date' , 'productListName'  , 'productRevenuePerPurchase' , 'unique_purchase']
                total_df['source'] = data_type
                total_df['date'] = pd.to_datetime(total_df['date'])
                total_df['productListName'] = total_df['productListName'].str.strip()
                total_df['productListName'] = total_df['productListName'].str.slice(0, 200 - 10)
                total_df = total_df[['date', 'source', 'productListName', 'productRevenuePerPurchase', 'unique_purchase']]
        except Exception as e:
            db_log_error.log_error('Python_dk_trends_data', e)


        try:
            delete_sql = "DELETE FROM [DB_Marketing].[dbo].[GA_DK_Trends] WHERE date = '{}' AND source = '{}'".format(str(step_time) , data_type)
            cursor.execute(delete_sql)
            cursor.commit()
            cursor.fast_executemany = True
            sql_comm = '''INSERT INTO [{}].[dbo].[{}]([date],[source],[productListName],[productRevenuePerPurchase],[unique_purchase])
                            VALUES (?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
            cursor.executemany(sql_comm, total_df.values.tolist())
            cursor.commit()
            print('done' + ' ' + str(step_time))
            time.sleep(2)
        except Exception as e:
            db_log_error.log_error('Python_dk_trends_data', e)


if __name__ == '__main__':
    main()
