import pandas as pd
from utils import ga_engine


# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


def get_report(_id, analytics , ptrn, start='2017-03-01', end='2017-03-01', page='0'):
        return analytics.reports().batchGet(
            body = {
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'dimensions': [{"name": "ga:date"}, {"name": "ga:productListName"}, {"name": "ga:pagepath"}],
                        'metrics': [{'expression': 'ga:productListClicks'},
                                    {'expression': 'ga:productAddsToCart'},
                                    {'expression': 'ga:uniquePurchases'},
                                    {'expression': 'ga:productRevenuePerPurchase'}
                                    ],
                        'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                        'samplingLevel': 'LARGE',
                        "dimensionFilterClauses": [{
                            "filters": {
                                "dimensionName": "ga:productListName",
                                "operator": "BEGINS_WITH",
                                "expressions": [ptrn]
                            }
                        }],
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute()


def fetch_data(_id, analytics, input_date, ptrn):
    response = get_report(_id, analytics, ptrn, input_date, input_date, '0')
    df = ga_engine.create_df(response)
    dataframes = []
    dataframes.append(df)
    idx = 0
    while (len(df.index) == BATCH_SIZE):
        idx += BATCH_SIZE
        response = get_report(_id, analytics , mode, ptrn, input_date, input_date, str(idx))
        df = ga_engine.create_df(response)
        dataframes.append(df)

    return pd.concat(dataframes)


def get_report_pageview(_id, analytics, ptrn, start='2017-03-01', end='2017-03-01', page='0'):
    return analytics.reports().batchGet(
        body = {
            'reportRequests': [
                {
                    'viewId': _id,
                    'dateRanges': [{'startDate': start, 'endDate': end}],
                    'dimensions': [{"name": "ga:date"}, {"name": "ga:productListName"}, {"name": "ga:pagepath"}],
                    'metrics': [{'expression': 'ga:pageviews'}],
                    'orderBys': [{"fieldName": "ga:date", "sortOrder": "ASCENDING"}],
                    'samplingLevel': 'LARGE',
                    "dimensionFilterClauses": [{
                        "filters": {
                            "dimensionName": "ga:productListName",
                            "operator": "BEGINS_WITH",
                            "expressions": [ptrn]
                        }
                    }],
                    'pageToken': page,
                    'pageSize': BATCH_SIZE
                }]
        }
    ).execute()
