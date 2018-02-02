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

start = datetime.date(2017, 6, 30)
end = datetime.date(2017, 6, 30)
days = end - start
userchanges = pd.DataFrame()

for x in xrange(0,days.days+1):
    date = start + datetime.timedelta(days=x)
    users = qp.listausuarios(date)
    if len(users)!=0:
        cambios = qp.controlcambios(users,date)
        userchanges = userchanges.append(cambios, ignore_index=True)
        cambios = cambios.iloc[0:0]

filename = 'User_Changes_06_30.xlsx'
writer = pd.ExcelWriter(filename)
userchanges.to_excel(writer)
writer.save()


    # for x in range(1,31):
#     today = datetime.date(2017, 4, x)
#     users = qp.listausuarios(today)
#     # Ejecutar script de control de cambios
#     if len(users)!=0:
#         cambios = qp.controlcambios(users,today)
#         # Guardar Cambios en archivo de Excel
#         filename = 'Users_Report_BR_' + str(today) + ('.xlsx')
#         writer = pd.ExcelWriter(filename)
#         cambios.to_excel(writer)
#         writer.save()
#     else:
#         cambios = pd.DataFrame(columns=['fecha_rev', 'rev_id', 'email', 'usuario_id', 'nombre_comercio',
#                                               'nombre_contacto', 'url',
#                                               'domicilio'])
#         cambiosord2=pd.DataFrame(columns= ['fecha_revprev', 'rev_idprev', 'emailprev', 'usuario_idprev', 'nombre_comercioprev',
#                                'nombre_contactoprev', 'urlprev', 'domicilioprev'])
#         filename = 'Users_Report_BR_' + str(today) + ('.xlsx')
#         audcambios = pd.concat([cambios, cambiosord2], axis=1)
#         writer = pd.ExcelWriter(filename)
#         audcambios.to_excel(writer)
#         writer.save()
#     logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})