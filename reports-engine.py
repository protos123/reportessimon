import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import sys
import datetime
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

uid=500199
date=datetime.date.today()
date2=datetime.date.today() - datetime.timedelta(days=1)
tomorrow=datetime.date.today() + datetime.timedelta(days=1)
print tomorrow
print date
print date2
cursor.execute("""select * from pps.transaccion where usuario_id=%(uid)s and fecha_creacion>=%(date)s limit 1""",
               {'uid': uid,'date':date})
print cursor.fetchone()
cambios=pd.DataFrame({'B': [9, 8, 7, 6, 5, 4]})

print cambios

name='output'+str(date)+'.xlsx'
writer = pd.ExcelWriter(name)
cambios.to_excel(writer)
writer.save()
logging.warning('bullshit finished %(uid)s', {'uid':len(cambios)})