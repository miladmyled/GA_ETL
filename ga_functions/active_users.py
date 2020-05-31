import pandas as pd
from utils import ga_engine

# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


def get_report_web(_id, analytics, mode, start='2017-03-01', end='2017-03-01', page='0'):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    if mode == 'day':
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"},{"name": "ga:deviceCategory"}],
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
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:currencyCode"},{"name": "ga:deviceCategory"}],
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
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:{}".format(mode)},{"name": "ga:deviceCategory"}],
                        'metrics': [{'expression': 'ga:users'}],
                        'orderBys': [{"fieldName": "ga:{}".format(mode), "sortOrder": "ASCENDING"}],
                        'samplingLevel':  'LARGE',
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()


def get_report_app(_id, analytics, mode, start='2017-03-01', end='2017-03-01', page='0'):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    if mode == 'day':
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"}],
                        'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:sessions'}],
                        'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                        'samplingLevel': 'LARGE',
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
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:currencyCode"}],
                        'metrics': [{'expression': 'ga:users'}],
                        'orderBys': [{"fieldName": "ga:currencyCode", "sortOrder": "ASCENDING"}],
                        'samplingLevel': 'LARGE',
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()

    else:  # if mode == 'month':
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:{}".format(mode)}],
                        'metrics': [{'expression': 'ga:users'}],
                        'orderBys': [{"fieldName": "ga:{}".format(mode), "sortOrder": "ASCENDING"}],
                        'samplingLevel': 'LARGE',
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()


def fetch_data_daily(_id, analytics, input_date, platform):
    if platform == 'app':
        response = get_report_app(_id, analytics, 'day', input_date, input_date, '0')
    else:
        response = get_report_web(_id, analytics, 'day', input_date, input_date, '0')
    df = ga_engine.create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        if platform == 'app':
            response = get_report_app(_id, analytics, 'day', input_date, input_date, str(idx))
        else:
            response = get_report_web(_id, analytics, 'day', input_date, input_date, str(idx))
        df = ga_engine.create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def fetch_data_monthly(_id, analytics, input_date, end_date, platform):
    if platform == 'app':
        response = get_report_app(_id, analytics, 'month', input_date, end_date, '0')
    else:
        response = get_report_web(_id, analytics, 'month', input_date, end_date, '0')
    df = ga_engine.create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        if platform == 'app':
            response = get_report_app(_id, analytics, 'month', input_date, end_date, str(idx))
        else:
            response = get_report_web(_id, analytics, 'month', input_date, end_date, str(idx))
        df = ga_engine.create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def fetch_data_custom(_id, analytics, input_date, end_date, noyears, platform):
    if noyears == 1:
        if platform == 'app':
            response = get_report_app(_id, analytics, 'year', input_date, end_date, '0')
        else:
            response = get_report_web(_id, analytics, 'year', input_date, end_date, '0')
    else:
        if platform == 'app':
            response = get_report_app(_id, analytics, '2years', input_date, end_date, '0')
        else:
            response = get_report_web(_id, analytics, '2years', input_date, end_date, '0')
    df = ga_engine.create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        if noyears == 1:
            if platform == 'app':
                response = get_report_app(_id, analytics, 'year', input_date, end_date, str(idx))
            else:
                response = get_report_web(_id, analytics, 'year', input_date, end_date, str(idx))
        else:
            if platform == 'app':
                response = get_report_app(_id, analytics, '2years', input_date, end_date, str(idx))
            else:
                response = get_report_web(_id, analytics, '2years', input_date, end_date, str(idx))
        df = ga_engine.create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def fetch_data_custom_wrapper(_id, analytics, start_date, end_date, col_name, platform):
    if start_date.year == end_date.year:
        df = fetch_data_custom(_id, analytics, start_date.strftime('%Y-%m-%d'),
                               end_date.strftime('%Y-%m-%d'), 1, platform)
        if platform == 'app':
            df.columns = [col_name, 'year']
            df.drop(['year'], axis=1, inplace=True)
        else:
            df.columns = ['category', col_name, 'year']
            df.drop(['year'], axis=1, inplace=True)
    else:
        df = fetch_data_custom(_id, analytics, start_date.strftime('%Y-%m-%d'),
                               end_date.strftime('%Y-%m-%d'), 2, platform)
        if platform == 'app':
            df.columns = ['irrelevant', col_name]
            df.drop(['irrelevant'], axis=1, inplace=True)
        else:
            df.columns = ['irrelevant', 'category', col_name]
            df.drop(['irrelevant'], axis=1, inplace=True)
    return df

