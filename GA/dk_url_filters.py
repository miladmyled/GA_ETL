#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################################
### Customized for Python 3.x !! ###
####################################
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np


import time



from ga_functions import url_filters
from utils import  ga_engine, db_engine


VIEW_ID = 'ga:26751439'

DB_NAME = 'DB_Marketing'
TABLE_NAME = 'GA_dk_url_filters_Data'

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000

# Database Connection
try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()
except Exception as e:
    print(e)

def main():
    analytics = ga_engine.initialize_analyticsreporting('web')

    ptrns = ['/search/', '/promotion-page/', '/product-list/', '/brand/', '/landing-page/', '/incredible-offers',
             '/seller/']
    types = {'/search/': 'search', '/promotion-page/': 'promotion',
             '/product-list/': 'product-list',
             '/brand/': 'brand', '/landing-page/': 'landing-page',
             '/incredible-offers': 'incredible offer', '/seller/': 'seller'}


    for i in range(3):
        step_time = (datetime.datetime.now().date() + relativedelta(days=-i-1)).strftime('%Y-%m-%d')

        for ptrn in ptrns:
            data = url_filters.fetch_data(VIEW_ID, analytics, step_time, ptrn)

            if data.empty:
                time.sleep(3)
                break
            data.columns = ['date', 'pagepath', 'pageView', 'uniquePageView']
            data['url_filter'] = data['pagepath'].str.split("?").str[1]
            data['source_url'] = data['pagepath'].str.split("?").str[0]

            data = ga_engine.splitDataFrameList(data , 'url_filter' , '&')

            new = data["url_filter"].str.split("=", n=1, expand=True)
            data["filter"] = new[0]
            data["value"] = new[1]

            data['pageType'] = types[ptrn]

            ordered_cols = ['date','pageType','source_url','filter','value','pageView','uniquePageView']

            data = data[ordered_cols]

            data = data[data['filter'] != "q"]

            data = data[data['value'] != ""]

            if types[ptrn] == 'search' :
                data = data[data['source_url'].str.contains("search")]

            data['date'] = pd.to_datetime(data['date'])

            data['source_url'] = data['source_url'].str.slice(0, 50 - 5)

            data['filter'] = data['filter'].str.slice(0, 20 - 5)

            data['value'] = data['value'].str.slice(0, 100 - 5)


            data['id'] = np.arange(data.shape[0])

            data_count =  data['id'].count()

            iterate_count = int(data_count / 20)

            start_count = 0

            delete_sql = "DELETE FROM [DB_Marketing].[dbo].[GA_dk_url_filters_Data] WHERE date = '" + str(
                step_time) + "' and pageType = '" + str(types[ptrn]) + "'"
            cursor.execute(delete_sql)
            cursor.commit()
            if not data.empty:
                while start_count < data_count :

                    df = data[(data['id'] > start_count) & (data['id'] <= start_count + iterate_count)]
                    ordered_cols = ['date', 'pageType', 'source_url', 'filter', 'value', 'pageView', 'uniquePageView']
                    df = df[ordered_cols]
                    start_count = start_count + iterate_count

                    try:
                        cursor.fast_executemany = True
                        sql_comm = '''INSERT INTO [{}].[dbo].[{}]([date],[pageType],[source_url],[filter],[value],[pageView],[uniquePageView])
                                                            VALUES (?,?,?,?,?,?,?)'''.format(DB_NAME, TABLE_NAME)
                        cursor.executemany(sql_comm, df.values.tolist())
                        cursor.commit()
                    except Exception as e:
                        print('{} ... {} is Not Done! because ==> {}'.format(step_time, ptrn , e))
                print('{} ... {} is Done!'.format(step_time, ptrn))



if __name__ == '__main__':
    main()
