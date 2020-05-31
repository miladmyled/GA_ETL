import pandas as pd
from utils import ga_engine


# DO NOT CHANGE IT !!!
BATCH_SIZE = 100000


def get_report(_id, analytics , start='2017-03-01', end='2017-03-01', page='0'):

        df = ga_engine.create_df(analytics.reports().batchGet(
            body = {
                'reportRequests': [
                    {
                        'viewId': _id,
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'metrics': [{'expression': 'ga:users'}],
                        'samplingLevel': 'LARGE',
                        'pageToken': page,
                        'pageSize': BATCH_SIZE
                    }]
            }
        ).execute())
        return df
