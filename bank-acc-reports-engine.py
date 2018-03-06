import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import smtplib
import ssl
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
import queries as qp

try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

date=datetime.date.today()-datetime.timedelta(days=1)
cambioscuentas = pd.DataFrame()
accounts = qp.listacuentasbancos(date)
if len(accounts) != 0:
    cambios = qp.controlcambioscuentasbancos(accounts, date)
    cambioscuentas = cambioscuentas.append(cambios, ignore_index=True)
    cambios = cambios.iloc[0:0]


filename = 'Bank_Accounts_Changes_' + str(date) + ('.xlsx')
writer = pd.ExcelWriter(filename,options={'remove_timezone': True})
cambioscuentas.to_excel(writer)
writer.save()
logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})






