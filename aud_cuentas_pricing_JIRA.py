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


def listacuentas(today):
    tomorrow = today + datetime.timedelta(days=1)
    logging.warning('Searching account id with changes on %(today)s', {'today':str(today)})
    cursor.execute("""SELECT c.cuenta_id FROM audit.cuenta_aud c
    inner join audit.revision_auditoria ra ON c.rev = ra.revision_id 
    INNER JOIN pps.cuenta pc on (c.cuenta_id=pc.cuenta_id)
    WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and (to_timestamp(fecha_revision/1000))<%(tomorrow)s
    and pc.fecha_creacion<%(today)s and c.pais_iso_3166='CO' order by c.cuenta_id""",{'today':today,'tomorrow':tomorrow})
    cuentas = pd.DataFrame(cursor.fetchall())
    if len(cuentas)!=0:
        cuentas.columns = ['cuenta_id']
        cuentas = cuentas.drop_duplicates()
    logging.warning('Search finished: %(cantidad)s have received changes', {'cantidad':len(cuentas)})
    return cuentas

# ----------------------------------------------------------------------------------------------------------------------
# CONTROL DE CAMBIOS CUENTAS:
# CORRE POR EL LISTADO DE CUENTAS TRAYENDO LA CONSULTA DE LOS CAMBIOS A LA FECHA Y ANEXANDO EL ULTIMO CAMBIO
# ANTERIOR A LA FECHA DE EJECUCION DEL REPORTE.
# RETORNA UN DATAFRAME CON EL LISTADO DE TODOS LOS CAMBIOS PARA EL LISTADO DE CUENTAS.
def controlcambioscuentas(cuentas,today):

    cuentas = cuentas['cuenta_id'].tolist()
    temp = []
    tomorrow=today + datetime.timedelta(days=1)
    logging.warning('Starting check for %(today)s', {'today':str(today)})
    audcuentas = pd.DataFrame()
    for index in range(0, len(cuentas)):
        cid = cuentas[index]   # Indexar cuentas Id
        logging.info('Checking account %(cid)s', {'cid':cid})
        # Ejecutar consulta de fecha actual
        cursor.execute("""SELECT to_timestamp(fecha_revision/1000) as "fec_cambio", c.rev,  uw.email, c.cuenta_id, 
        c.nombre, c.perfil_usuario_id, c.grupo_perfil_cobranza_id
        FROM audit.cuenta_aud c
        inner join audit.revision_auditoria ra ON (c.rev = ra.revision_id)
        inner join pps.usuario_web uw on (c.usuario_modificacion_id=uw.usuario_web_id)
        WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and (to_timestamp(fecha_revision/1000))<%(tomorrow)s
        and c.cuenta_id=%(cid)s""", {'cid': cid,'today':today,'tomorrow':tomorrow})
        for i in xrange(cursor.rowcount):
            temp.append(cursor.fetchone())
        # Ejecutar consulta de cambio inmediatamente anterior
        cursor.execute("""SELECT to_timestamp(fecha_revision/1000) as "fec_cambio", c.rev,  uw.email, c.cuenta_id, 
        c.nombre, c.perfil_usuario_id, c.grupo_perfil_cobranza_id
        FROM audit.cuenta_aud c
        inner join audit.revision_auditoria ra ON (c.rev = ra.revision_id)
        inner join pps.usuario_web uw on (c.usuario_modificacion_id=uw.usuario_web_id)
        WHERE (to_timestamp(fecha_revision/1000))<%(today)s and c.cuenta_id=%(cid)s 
        order by "fec_cambio" desc limit 1""", {'cid': cid, 'today':today})
        i = 0
        for i in xrange(cursor.rowcount):
            temp.append(cursor.fetchone())
        cambios = pd.DataFrame(temp,columns = ['fecha_rev', 'rev_id', 'email', 'cuenta_id', 'nombre_cuenta',
                                               'perfil_usuario_id','grupo_perfil_cobranza'])
        cambios['fecha_rev'] = pd.to_datetime(cambios.fecha_rev)
        cambios = cambios.sort_values(by='fecha_rev')
        cambiosord2 = cambios.shift()
        cambiosord2.columns = ['fecha_revprev', 'rev_idprev', 'emailprev', 'cuenta_idprev',
                               'nombre_cuenta_prev', 'perfil_usuario_idprev','grupo_perfil_cobranzaprev']
        df = pd.concat([cambios, cambiosord2], axis=1)
        # En caso de que sea un primer cambio, ejecutar script de identificacion
        if len(df)!=0:
            df.drop(df.index[0], inplace=True)
            df['cambio'] = np.where(df['perfil_usuario_id'] != df['perfil_usuario_idprev'], 'perfilcobranza', None)
            df = df[df.cambio.notnull()]
            audcuentas = audcuentas.append(df, ignore_index=True)
        logging.info('Found %(numbers)s changes',{'numbers':len(df)})

        # Limpiar variables iterativas
        df = df.iloc[0:0]
        cambios=cambios.iloc[0:0]
        cambiosord2=cambiosord2.iloc[0:0]
        temp=[]
        logging.info('Account Id %(cid)s finished',{'cid':cid})
    # Eliminar Columnas innecesarias
    logging.warning('Process Finished. Proceeding to concatenate and save')
    return audcuentas

# ----------------------------------------------------------------------------------------------------------------------


start = datetime.date(2017, 11, 01)
end = datetime.date(2017, 12, 31)
days = end - start
pricingchanges = pd.DataFrame()

for x in xrange(0,days.days+1):
    date = start + datetime.timedelta(days=x)
    accounts = listacuentas(date)
    if len(accounts)!=0:
        cambios = controlcambioscuentas(accounts,date)
        pricingchanges = pricingchanges.append(cambios, ignore_index=True)
        cambios = cambios.iloc[0:0]

filename = 'Pricing_changes_Nov_Dec_2017.xlsx'
writer = pd.ExcelWriter(filename)
pricingchanges.to_excel(writer)
writer.save()
