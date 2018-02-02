import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import sys
import datetime
import queries as qp
import os
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

accounts = pd.DataFrame([500509],columns=['cuenta_id'])
print accounts

start = datetime.date(2017,01,01)
end = datetime.date(2017,12,31)
days = end-start
pricing = pd.DataFrame()
for x in xrange(0,days.days):
    date = start + datetime.timedelta(days=x)
    cambios = qp.controlcambioscuentas(accounts,date)
    pricing = pricing.append(cambios, ignore_index=True)
    cambios = cambios.iloc[0:0]


filename='Nombre de archivo a guardar.xlsx'
writer = pd.ExcelWriter(filename)
pricing.to_excel(writer)
writer.save()

