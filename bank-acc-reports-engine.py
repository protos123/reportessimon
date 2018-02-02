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

for x in range(1,31):
    today = datetime.date(2017, 11, x)
    accounts = qp.listacuentasbancos(today)

    # Ejecutar script de control de cambios
    if len(accounts) != 0:
        cambios = qp.controlcambioscuentasbancos(accounts,today)
        # Guardar Cambios en archivo de Excel
        filename = 'Bank_Accounts_Report_' + str(today) + ('.xlsx')
        writer = pd.ExcelWriter(filename)
        cambios.to_excel(writer)
        writer.save()
    else:
        cambios = pd.DataFrame(columns=['fecha_rev', 'rev_id', 'email', 'cuenta_id', 'numero_cuenta_rec',
                                              'titular_cuenta_rec', 'dni_titular_cuenta', 'dni_tipo_titular_cuenta',
                                              'tipo_cuenta_rec', 'banco_id', 'swift', 'pais_cuenta_rec'])
        cambiosord2=pd.DataFrame(columns = ['fecha_rev_prev', 'rev_id_prev', 'email_prev', 'cuenta_id_prev', 'numero_cuenta_rec_prev',
                               'titular_cuenta_rec_prev', 'dni_titular_cuenta_prev', 'dni_tipo_titular_cuenta_prev',
                               'tipo_cuenta_rec_prev', 'banco_id_prev', 'swift_prev', 'pais_cuenta_rec_prev','cambio'])
        filename = 'Bank_Accounts_Report_' + str(today) + ('.xlsx')
        audcambios = pd.concat([cambios, cambiosord2], axis=1)
        writer = pd.ExcelWriter(filename)
        audcambios.to_excel(writer)
        writer.save()
    logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})