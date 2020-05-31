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
from ga_functions import daily_users
from utils import es_engine, ga_engine, db_engine
from config.config import elastic_configs

#Web = 'ga:164962980'
#MobileWeb = 'ga:164937152'
#App = 'ga:98444990'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_DailyUsersByPeriod'
# INDX = 'ga_carousel_perf'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


# Database Connection
try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()

except Exception as e:
    print(str(e))


view_id = sys.argv[1]
app_type = 'Web' if view_id == 'ga:164962980' else 'App' if view_id == 'ga:98444990' else 'MobileWeb'
today_date = datetime.datetime.now().date()

def main():
    try:
        analytics = ga_engine.initialize_analyticsreporting('web')
    except Exception as e:
         exec_sp = "EXEC [DB_DBA].[log].[Usp_Insert_ErrorLog] 'daily_users_data' , 'bimarketing' , '{}' , '{}' , NULL , NULL , NULL , 'Python' ,NULL , 'm.firoozi@digikala.com;s.shabanian@digikala.com' , '09384149786' , 0 , 0 ".format(
             datetime.datetime.now().replace(microsecond=0), e)
         cursor.execute(exec_sp)
         cursor.commit()
    for i in range(5):
        try:
            step_time = today_date + relativedelta(days=-i - 2)
            find_row_sql = "SELECT Id FROM [DB_Marketing].[dbo].[GA_DailyUsersByPeriod] WHERE CurrentDate = '{}' AND AppType = '{}'".format(step_time , app_type)
            cursor.execute(find_row_sql)
            for row in cursor.fetchall():
                selected_id = row.Id
            for j in range(4):
                selected_value = 'CM_Users' if j == 0 else 'LM_Users' if j == 1 else 'LY_Users' if j==2 else 'CD_Users'
                start_value = 'CurrentMonthStart' if j == 0 else 'LastMonthStart' if j == 1 else 'LastYearStart' if j==2 else 'CurrentDate'
                end_value = 'CurrentMonthEnd' if j == 0 else 'LastMonthEnd' if j == 1 else 'LastYearEnd' if j==2 else 'CurrentDate'
                start_date_sql = "SELECT {} Value FROM [DB_Marketing].[dbo].[GA_DailyUsersByPeriod] WHERE Id = {}".format(start_value , selected_id)
                cursor.execute(start_date_sql)
                for row in cursor.fetchall():
                    start_date = datetime.datetime.strftime(row.Value , '%Y-%m-%d')
                end_date_sql = "SELECT {} Value FROM [DB_Marketing].[dbo].[GA_DailyUsersByPeriod] WHERE Id = {}".format(end_value , selected_id)
                cursor.execute(end_date_sql)
                for row in cursor.fetchall():
                    end_date = datetime.datetime.strftime(row.Value , '%Y-%m-%d')
                total_df = daily_users.get_report(view_id, analytics, start_date , end_date)
                if total_df.empty:
                    time.sleep(2)
                    continue
                else:
                    for index, row in total_df.iterrows():
                        to_update_value = int(row["ga:users"])
                update_sql = "UPDATE [DB_Marketing].[dbo].[GA_DailyUsersByPeriod] SET {} = {} WHERE Id = {}".format(selected_value , to_update_value , selected_id)
                cursor.execute(update_sql)
                cursor.commit()
                print("Date:{} AppTYpe:{} Selected_Value:{} ==> Updated".format(step_time , app_type , selected_value))
                time.sleep(2)
        except Exception as e:
            exec_sp = "EXEC [DB_DBA].[log].[Usp_Insert_ErrorLog] 'daily_users_data' , 'bimarketing' , '{}' , '{}' , NULL , NULL , NULL , 'Python' ,NULL , 'm.firoozi@digikala.com;s.shabanian@digikala.com' , '09384149786' , 0 , 0 ".format(
                datetime.datetime.now().replace(microsecond=0), e)
            cursor.execute(exec_sp)
            cursor.commit()


if __name__ == '__main__':
    main()
