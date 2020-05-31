from utils import db_engine
import datetime


try:
    cnxn = db_engine.init_engine()
    cursor = cnxn.cursor()
except Exception as e:
    print(e)


def log_error(script_name , e):
    exec_sp = "EXEC [DB_DBA].[log].[Usp_Insert_ErrorLog] '{}' , 'bimarketing' , '{}' , '{}' , NULL , NULL , NULL , 'Python' , NULL , 'm.firoozi@digikala.com;s.shabanian@digikala.com' , '09384149786' , 0 , 0 ".format(script_name,
        datetime.datetime.now().replace(microsecond=0), e)
    cursor.execute(exec_sp)
    cursor.commit()



