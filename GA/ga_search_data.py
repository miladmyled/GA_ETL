#!/usr/bin/env python
# coding: utf-8

import os
from utils import ga_engine
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import datetime
import pyodbc
from sqlalchemy import create_engine, event
import sqlalchemy
from urllib.parse import quote_plus

def main():
    days = 7

    # SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    # KEY_FILE_LOCATION = 'client_secret.json'
    VIEW_ID_DM = '26751439' # HELP: digikala.com view (desktop and mobileweb mixed)
    # credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

    # analytics = build('analyticsreporting', 'v4', credentials=credentials)
    analytics = ga_engine.initialize_analyticsreporting('web')
    days_ago = str(days) + 'daysAgo'


    r_search = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': VIEW_ID_DM,
                'dateRanges': [{'startDate': days_ago, 'endDate': 'today'}],
                'metrics': [
                    {'expression': 'ga:searchExits'},
                    {'expression': 'ga:searchUniques'},
                    
                    {'expression': 'ga:searchRefinements'},
                    {'expression': 'ga:searchResultViews'},
                    
                    {'expression': 'ga:searchSessions'}
                ],
                'dimensions': [{'name': 'ga:date'}],
                
                }]
            }
            ).execute()

    df = pd.DataFrame(columns=['search_date', 'search_exits', 'search_uniques', 'search_refinements', 'search_result_views'])

    for item in r_search['reports'][0]['data']['rows']:
        date_ = datetime.datetime.strptime(item['dimensions'][0], "%Y%m%d").date()
        values = item['metrics'][0]['values']
        # HELP: 1th: searchExits, 2nd: searchUniques, 3rd: searchRefinements, 4th: searchResultViews, 5th: searchSessions
        df = df.append(
            {
                'search_date': date_,
                'search_exits': values[0],
                'search_uniques': values[1],
                'search_refinements': values[2],
                'search_result_views': values[3],
            }
            , ignore_index=True
        )                                            

    engine = sqlalchemy.create_engine('mssql+pyodbc://Shopping@Marketing:FDF7D@46B74$@172.30.6.33:1433/DB_Marketing?driver=ODBC+Driver+17+for+SQL+Server')

    df['search_date'] = pd.to_datetime(df['search_date'])

    days = tuple(df.search_date.dt.strftime('%Y-%m-%d').to_list())

    engine.execute("delete from GA_DK_Search where search_date in " + str(days))

    df.to_sql(name='GA_DK_Search', con=engine, if_exists = 'append', chunksize = None, index=False)

if __name__ == '__main__':
    main()