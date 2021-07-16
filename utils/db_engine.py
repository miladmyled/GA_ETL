import pyodbc
import sqlalchemy



def init_engine():
    conn = pyodbc.connect('DSN=DBMarketing')
    return conn



def init_engine_alchemy():
    engine = sqlalchemy.create_engine('mssql+pyodbc://DBMarketing')
    return  engine
