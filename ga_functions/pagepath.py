import pandas as pd
from utils import ga_engine


# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


# https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#FilterLogicalOperator
def get_report(_id, analytics, mode, start='2017-03-01', end='2017-03-01', page='0'):
    if mode == 'homepage':
        return analytics.reports().batchGet(
            body = {
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"}, {"name": "ga:pagepath"}],
                        'metrics': [{'expression': 'ga:pageviews'},{'expression': 'ga:uniquePageviews'}],
                        'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                        'samplingLevel': 'LARGE',
                        "dimensionFilterClauses": [{
                            "filters": {
                                "dimensionName": "ga:pagepath",
                                "operator": "EXACT",
                                "expressions": ['/']
                            }
                        }],
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()
    elif mode == 'dk-logo':
        return analytics.reports().batchGet(
            body = {
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"}, {"name": "ga:pagepath"}],
                        'metrics': [{'expression': 'ga:pageviews'},{'expression': 'ga:uniquePageviews'}],
                        'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                        'samplingLevel': 'LARGE',
                        "dimensionFilterClauses": [{
                            "filters": {
                                "dimensionName": "ga:pagepath",
                                "operator": "EXACT",
                                "expressions": ['/?ref=nav_logo']
                            }
                        }],
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()
    else:
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"}, {"name": "ga:pagepath"}],
                        'metrics': [{'expression': 'ga:pageviews'},{'expression': 'ga:uniquePageviews'}],
                        'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                        'samplingLevel':  'LARGE',
                        "dimensionFilterClauses": [{
                            "filters": {
                                "dimensionName": "ga:pagepath",
                                "operator": "PARTIAL",
                                "expressions": [mode]
                            }
                        }],
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()


def fetch_data(_id, analytics, input_date, mode):
    response = get_report(_id, analytics, mode, input_date, input_date, '0')
    df = ga_engine.create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        response = get_report(_id, analytics, mode, input_date, input_date, str(idx))
        df = ga_engine.create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)
