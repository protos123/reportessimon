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

accounts=pd.DataFrame([500509],columns=['cuenta_id'])
print accounts

for x in range(1,32):
    today =datetime.date(2017, 12, x)
    cambios = qp.controlcambioscuentas(accounts,today)
    # Guardar Cambios en archivo de Excel
    filename = 'Pricing_Report_' + str(today) + ('.xlsx')
    writer = pd.ExcelWriter(filename)
    cambios.to_excel(writer)
    writer.save()
    logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})

path= os.getcwd()
files =os.listdir(path)
print files

files_xlsx=[f for f in files if f[-4:] == 'xlsx']
print files_xlsx

df=pd.DataFrame()

for f in files_xlsx:
    data=pd.read_excel(f,'Sheet1')
    df = df.append(data)

df=df.reset_index(drop= True)
filename='Pricing_Changes_December_2017.xlsx'
writer = pd.ExcelWriter(filename)
df.to_excel(writer)
writer.save()