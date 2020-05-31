import re
import urllib
import sqlalchemy
import numpy as np
import pandas as pd
from ga_functions import pagepath


def shrink_data(df, field, pattern):
    """
    Return data containing the desired pattern
    :param df: input data_frame
    :param field: input column of the given data_frame
    :param pattern: desired pattern to filter
    :return: shortened data_frame
    """
    return df[df[field].str.contains(pattern)]


def summarize(df, field):
    # eliminate copy warning
    df = df.copy()
    url_decoder = lambda x: urllib.parse.unquote(x, encoding='utf-8')
    domain_purger = lambda x: x.split('/', 3)[-1]
    special_subcats = lambda x: 'search' if x.startswith('search/?') \
        else('cart' if x.startswith('cart/')
             else('landing-page' if x.startswith('landing-page/')
                  else x.split('/', 2)[1]))
    df[field] = df[field].map(url_decoder)
    df[field] = df[field].map(domain_purger)
    df[field] = df[field].map(special_subcats)
    return df


def referrer_handler(df, field):
    df.loc[:, field] = df[field].map(lambda x: urllib.parse.unquote(x))
    df.loc[:, field] = df[field].map(lambda x: x.split('/', 3)[-1].split('/', 2)[1]
    if ('search/category' in x or '/main/' in x) else x.split('/', 3)[-1].split('/', 1)[0])
    df.loc[:, field] = df[field].map(lambda x: 'homepage' if x.startswith('?') else x)
    df.loc[:, field] = df[field].map(lambda x: 'homepage' if x == '' else x)
    return df


def data_cleaner(df, pattern, types):
    df.columns = ['date', 'total_events', 'hits']
    # for index, row in df.iterrows():
    #     try:
    #         a,b,c,d,e = row['total_events'].split(';', 5)
    #         # print('javi')
    #     except Exception as e:
    #         print(str(e))
    #         print(row['total_events'])

    df['pagepath'], df['referrer'], df['userID'], df['variantID'], df['cartID'] = df['total_events'].str.split(';', 5).str

    df = shrink_data(df, 'pagepath', pattern)
    df = summarize(df, 'pagepath')
    df = referrer_handler(df, 'referrer')

    df.loc[:,'date'] = pd.to_datetime(df['date'])
    df['pageview_type'] = types[pattern]
    df = df.drop('total_events', 1)

    df['cartID'].replace([np.nan], [sqlalchemy.sql.null()], inplace=True)
    df['userID'].replace(['null'], [sqlalchemy.sql.null()], inplace=True)
    return df


def get_dkp(url_string):
    res_list = re.compile('dkp-([0-9]*)').findall(url_string)
    try:
        return res_list[0]
    except:
        return ''#sqlalchemy.sql.null()


def column_pattern_retriever(df, col, ptrn, _type):
    # if df.empty:
    #     return df[col]
    df[col] = df[col].map(lambda x: x.replace('?', '/'))
    df = df[~df[col].str.contains('/users/register/')]
    df = df[~df[col].str.contains('/users/login/')]

    # backup
    df['backup'] = df[col]

    # distinguish compare & product
    if ptrn == '/dkp-':
        df[col] = df[col].map(lambda x: 'compare' if x.startswith('/compare/dkp-')
        else get_dkp(x))
    elif ptrn == 'homepage' or ptrn == 'mobile-homepage':
        df[col] = df[col].map(lambda x: '/')
    elif ptrn == 'adro.co/':
        df[col] = df[col].map(lambda x: x.split(ptrn, 1)[-1])
        special_subcats = lambda x: x.split('/', 2)[1] if x.startswith('click/') else np.nan
        df[col] = df[col].map(special_subcats)
    else:
        df[col] = df[col].map(lambda x: ptrn[1:] + x.split(ptrn, 1)[-1])
        special_subcats = lambda x: x.split('/', 2)[1] if x.startswith('search/category-') \
            else ('search' if x.startswith('search/') \
                      else ('cart' if x.startswith('cart/')
                            else ('landing-page' if x.startswith('landing-page/')
                                  else x.split('/', 2)[1])))
        df[col] = df[col].map(special_subcats)
    df[col+'pageType'] = _type
    if ptrn in ['/promotion-page/', '/product-list/']:
        df[col+'pageType'] = df.apply(lambda x: 'fresh-' + x[col+'pageType'] if 'fresh=1' in x['backup']
        else x[col+'pageType'], axis=1)
    return df[[col,col+'pageType']]
