import datetime
from datetime import date, timezone


def create_log(mode, status='ACK', batchdate=datetime.datetime(1, 1, 1).date(), hostname='Unknown', text='Successful',
               server_len=0, database_len=0):
    doc = {
        'status': status,
        'mode': mode,
        'batch_date': batchdate,
        'host': hostname,
        'text': text,
        'server_len': server_len,
        'db_len': database_len,
        'timestamp': datetime.datetime.now(timezone.utc),
    }
    return doc