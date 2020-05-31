import pandas as pd
from utils import ga_engine


# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


def get_report(_id, analytics, mode, start='2017-03-01', end='2017-03-01', page='0'):
    if mode == 'events':
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"},
                                       {"name": "ga:internalPromotionId"},
                                       {"name": "ga:internalPromotionName"},
                                       {"name": "ga:internalPromotionCreative"},
                                       {"name": "ga:internalPromotionPosition"}],
                        'metrics': [{'expression': 'ga:internalPromotionViews'},
                                    {'expression': 'ga:internalPromotionClicks'},
                                    {'expression': 'ga:internalPromotionCTR'},
                                    {'expression': 'ga:transactions'},
                                    {'expression': 'ga:transactionRevenue'},
                                    {'expression': 'ga:goal18Completions'}],
                        'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                        'samplingLevel':  'LARGE',
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
