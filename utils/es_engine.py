from elasticsearch import Elasticsearch


def init_engine(address):
    # Elasticsearch Connection
    es = Elasticsearch(address,
                       timeout=30,
                       max_retries=3,
                       retry_on_timeout=True)
    return es


def log_into_es(engine, _index, _doc):
    engine.index(index=_index, doc_type='log', body=_doc)
