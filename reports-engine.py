import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import sys
import datetime
import queries as qp
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

users = qp.listausuarios()

# Ejecutar script de control de cambios
cambios = qp.controlcambios(users)

# Guardar Cambios en archivo de Excel
today = datetime.date.today()
filename = 'Users_Report_' + str(today) + ('.xlsx')
writer = pd.ExcelWriter(filename)
cambios.to_excel(writer)
writer.save()

logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})