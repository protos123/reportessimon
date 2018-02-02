#!/usr/bin/env python
import pandas as pd
import psycopg2 as db
import logging
import sys
import datetime
import numpy as np

# Configurar Logger
logging.basicConfig(filename='queries.log', filemode='w', level=logging.DEBUG)

# Intentar Conexion a BD
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

# ----------------------------------------------------------------------------------------------------------------------

# Listado de usuarios que recibieron un cambio el dia actual
def listausuarios(today):
    tomorrow = today + datetime.timedelta(days=1)
    logging.warning('Searching user id with changes on %(today)s', {'today':str(today)})
    cursor.execute("""SELECT u.usuario_id FROM audit.usuario_aud u
    inner join audit.revision_auditoria ra ON u.rev = ra.revision_id 
    inner join pps.usuario pu on (u.usuario_id=pu.usuario_id)
    WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and 
    (to_timestamp(fecha_revision/1000))<%(tomorrow)s and pu.fecha_Creacion<%(today)s
    order by u.usuario_id""",{'today':today,'tomorrow':tomorrow})
    usuarios = pd.DataFrame(cursor.fetchall())
    if len(usuarios) != 0:
        usuarios.columns = ['usuario_id']
        usuarios = usuarios.drop_duplicates()
    logging.warning('Search finished: %(cantidad)s have received changes', {'cantidad':len(usuarios)})
    return usuarios
# ----------------------------------------------------------------------------------------------------------------------

# CONTROL DE CAMBIOS:
# CORRE POR EL LISTADO DE USUARIOS TRAYENDO LA CONSULTA DE LOS CAMBIOS A LA FECHA Y ANEXANDO EL ULTIMO CAMBIO
# ANTERIOR A LA FECHA DE EJECUCION DEL REPORTE.
# RETORNA UN DATAFRAME CON EL LISTADO DE TODOS LOS CAMBIOS PARA EL LISTADO DE USUARIOS.

def controlcambios(usuarios,today):

    usuarios = usuarios['usuario_id'].tolist()
    temp = []
    tomorrow = today + datetime.timedelta(days=1)
    logging.warning('Starting check for %(today)s', {'today':str(today)})
    audcambios = pd.DataFrame()
    for index in range(0, len(usuarios)):
        uid = usuarios[index]   # Indexar usuarios Id
        logging.info('Checking user %(uid)s', {'uid':uid})
        # Ejecutar consulta de fecha actual
        cursor.execute("""SELECT to_timestamp(fecha_revision/1000) as "fec_cambio", u.rev, uw.email, 
        u.usuario_id, u.nombres, u.nombre_contacto, u.url, u.direccion
        FROM audit.usuario_aud u 
        inner join audit.revision_auditoria ra ON (u.rev = ra.revision_id) 
        inner join pps.usuario_web uw on (u.usuario_modificacion_id=uw.usuario_web_id)
        WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and (to_timestamp(fecha_revision/1000))<%(tomorrow)s
        and u.usuario_id=%(uid)s""", {'uid': uid,'today':today,'tomorrow':tomorrow})
        for i in xrange(cursor.rowcount):
            temp.append(cursor.fetchone())
        # Ejecutar consulta de cambio inmediatamente anterior
        cursor.execute("""SELECT to_timestamp(fecha_revision/1000) as "fec_cambio", u.rev,uw.email, u.usuario_id, u.nombres, u.nombre_contacto, u.url, u.direccion
        FROM audit.usuario_aud u
        inner join audit.revision_auditoria ra ON u.rev = ra.revision_id 
        inner join pps.usuario_web uw on (u.usuario_modificacion_id=uw.usuario_web_id)
        WHERE (to_timestamp(fecha_revision/1000))<%(today)s and u.usuario_id=%(uid)s
        order by "fec_cambio" desc limit 1""", {'uid': uid, 'today':today})
        i = 0
        for i in xrange(cursor.rowcount):
            temp.append(cursor.fetchone())
        cambios = pd.DataFrame(temp,columns = ['fecha_rev', 'rev_id', 'email', 'usuario_id', 'nombre_comercio', 'nombre_contacto', 'url',
                           'domicilio'])
        cambios['fecha_rev'] = pd.to_datetime(cambios.fecha_rev)
        cambios = cambios.sort_values(by='fecha_rev')
        cambiosord2 = cambios.shift()
        cambiosord2.columns = ['fecha_revprev', 'rev_idprev', 'emailprev', 'usuario_idprev', 'nombre_comercioprev',
                               'nombre_contactoprev', 'urlprev','domicilioprev']
        df = pd.concat([cambios, cambiosord2], axis=1)
        # En caso de que sea un primer cambio, ejecutar script de identificacion
        if len(df)!=0:
            df.drop(df.index[0], inplace=True)
            df['cambio'] = np.where(df['nombre_contacto'] != df['nombre_contactoprev'], 'nombre_contacto',
                                np.where(df['nombre_comercio'] != df['nombre_comercioprev'], 'nombre_comercio',
                                         np.where(df['url'] != df['urlprev'], 'url',
                                                  np.where(df['domicilio'] != df['domicilioprev'], 'domicilio', None))))
            df = df[df.cambio.notnull()]
            audcambios = audcambios.append(df, ignore_index=True)
        logging.info('Found %(numbers)s changes',{'numbers':len(df)})

        # Limpiar variables iterativas
        df = df.iloc[0:0]
        cambios=cambios.iloc[0:0]
        cambiosord2=cambiosord2.iloc[0:0]
        temp=[]
        logging.info('User Id %(uid)s finished',{'uid':uid})
    # Eliminar Columnas innecesarias
    logging.warning('Process Finished. Proceeding to concatenate and save')
    return audcambios
# ----------------------------------------------------------------------------------------------------------------------

# Listado de cuentas que recibieron un cambio el dia actual
def listacuentas():
    tomorrow = datetime.date.today()
    today = datetime.date.today() - datetime.timedelta(days=1)
    logging.warning('Searching account id with changes on %(today)s', {'today':str(today)})
    cursor.execute("""SELECT c.cuenta_id FROM audit.cuenta_aud c
    inner join audit.revision_auditoria ra ON c.rev = ra.revision_id 
    INNER JOIN pps.cuenta pc on (c.cuenta_id=pc.cuenta_id)
    WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and (to_timestamp(fecha_revision/1000))<%(tomorrow)s
    and pc.fecha_creacion<%(today)s order by c.cuenta_id""",{'today':today,'tomorrow':tomorrow})
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
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------


# Listado de cuentas bancarias que recibieron un cambio el dia actual
def listacuentasbancos(today):
    tomorrow = today + datetime.timedelta(days=1)
    logging.warning('Searching account id with changes on %(today)s', {'today':str(today)})
    cursor.execute("""SELECT db.cuenta_id FROM audit.datos_bancarios_aud db
    inner join audit.revision_auditoria ra ON db.rev = ra.revision_id 
    INNER JOIN pps.cuenta pdb on (db.cuenta_id=pdb.cuenta_id)
    WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and (to_timestamp(fecha_revision/1000))<%(tomorrow)s 
    and pdb.fecha_creacion<%(today)s
    order by db.cuenta_id""",{'today':today,'tomorrow':tomorrow})
    cbancos = pd.DataFrame(cursor.fetchall())
    if len(cbancos)!=0:
        cbancos.columns = ['cuenta_id']
        cbancos = cbancos.drop_duplicates()
    logging.warning('Search finished: %(cantidad)s have received changes', {'cantidad':len(cbancos)})
    #print cbancos
    return cbancos

# ----------------------------------------------------------------------------------------------------------------------
# CONTROL DE CAMBIOS CUENTAS BANCOS:
# CORRE POR EL LISTADO DE CUENTAS TRAYENDO LA CONSULTA DE CAMBIOS EN CUENTAS BANCARIAS A LA FECHA Y ANEXANDO E
# L ULTIMO CAMBIO ANTERIOR A LA FECHA DE EJECUCION DEL REPORTE.
# RETORNA UN DATAFRAME CON EL LISTADO DE TODOS LOS CAMBIOS PARA EL LISTADO DE CUENTAS QUE TUVIERON CAMBIOS EN CUENTA
# BANCARIA.
def controlcambioscuentasbancos(cbancos,today):

    cbancos = cbancos['cuenta_id'].tolist()
    temp = []
    tomorrow=today  + datetime.timedelta(days=1)
    logging.warning('Starting check for %(today)s', {'today':str(today)})
    audcuentas = pd.DataFrame()
    for index in range(0, len(cbancos)):
        cidb = cbancos[index]   # Indexar cuentas Id
        logging.info('Checking account %(cidb)s', {'cidb':cidb})
        # Ejecutar consulta de fecha actual
        cursor.execute("""SELECT to_timestamp(fecha_revision/1000) as "fec_cambio_db", db.rev, uw.email, 
        db.cuenta_id, db.numero_cuenta_recaudo, db.titular_cuenta_recaudo, db.documento_titular_cuenta_recaudo,
        db.titular_cr_tipo_documento, db.tipo_cuenta_recaudo, db.banco_id, db.swift, db.pais_cuenta_recaudo
        FROM audit.datos_bancarios_aud db
        inner join audit.revision_auditoria ra ON (db.rev = ra.revision_id)
        inner join pps.usuario_web uw on (db.usuario_modificacion_id=uw.usuario_web_id)
        WHERE (to_timestamp(fecha_revision/1000))>=%(today)s and (to_timestamp(fecha_revision/1000))<%(tomorrow)s
        and db.cuenta_id=%(cid)s""", {'cid': cidb,'today':today,'tomorrow':tomorrow})
        for i in xrange(cursor.rowcount):
            temp.append(cursor.fetchone())
        # Ejecutar consulta de cambio inmediatamente anterior
        cursor.execute("""SELECT to_timestamp(fecha_revision/1000) as "fec_cambio_db", db.rev, 
        uw.email, db.cuenta_id, db.numero_cuenta_recaudo, db.titular_cuenta_recaudo, db.documento_titular_cuenta_recaudo,
        db.titular_cr_tipo_documento, db.tipo_cuenta_recaudo, db.banco_id, db.swift, db.pais_cuenta_recaudo
        FROM audit.datos_bancarios_aud db
        inner join audit.revision_auditoria ra ON (db.rev = ra.revision_id)
        inner join pps.usuario_web uw on (db.usuario_modificacion_id=uw.usuario_web_id)
        WHERE (to_timestamp(fecha_revision/1000))<%(today)s and db.cuenta_id=%(cidb)s
        order by "fec_cambio_db" desc limit 1""", {'cidb': cidb, 'today':today})
        i = 0
        for i in xrange(cursor.rowcount):
            temp.append(cursor.fetchone())
        cambios = pd.DataFrame(temp,columns = ['fecha_rev', 'rev_id', 'email', 'cuenta_id', 'numero_cuenta_rec',
                                               'titular_cuenta_rec','dni_titular_cuenta','dni_tipo_titular_cuenta',
                                               'tipo_cuenta_rec','banco_id','swift','pais_cuenta_rec'])
        cambios['fecha_rev'] = pd.to_datetime(cambios.fecha_rev)
        cambios = cambios.sort_values(by='fecha_rev')
        cambiosord2 = cambios.shift()
        cambiosord2.columns = ['fecha_rev_prev', 'rev_id_prev', 'email_prev', 'cuenta_id_prev', 'numero_cuenta_rec_prev',
                                'titular_cuenta_rec_prev','dni_titular_cuenta_prev','dni_tipo_titular_cuenta_prev',
                                'tipo_cuenta_rec_prev','banco_id_prev','swift_prev','pais_cuenta_rec_prev']
        df = pd.concat([cambios, cambiosord2], axis=1)
        # En caso de que sea un primer cambio, ejecutar script de identificacion
        if len(df)!=0:
            df.drop(df.index[0], inplace=True)
            df['cambio'] = np.where(df['numero_cuenta_rec'] != df['numero_cuenta_rec_prev'], 'numero_cuenta',
                            np.where(df['titular_cuenta_rec'] != df['titular_cuenta_rec_prev'], 'titular_cuenta',
                            np.where(df['dni_titular_cuenta'] != df['dni_titular_cuenta_prev'], 'dni_titular',
                            np.where(df['dni_tipo_titular_cuenta'] != df['dni_tipo_titular_cuenta_prev'], 'tipo_dni_titular',
                            np.where(df['tipo_cuenta_rec'] != df['tipo_cuenta_rec_prev'], 'tipo_cuenta',
                            np.where(df['banco_id'] != df['banco_id_prev'], 'banco_id',
                            np.where(df['swift'] != df['swift_prev'], 'swift',
                            np.where(df['pais_cuenta_rec'] != df['pais_cuenta_rec_prev'], 'pais_cuenta', None
                                     ))))))))
            df = df[df.cambio.notnull()]
            audcuentas = audcuentas.append(df, ignore_index=True)
        logging.info('Found %(numbers)s changes',{'numbers':len(df)})

        # Limpiar variables iterativas
        df = df.iloc[0:0]
        cambios=cambios.iloc[0:0]
        cambiosord2=cambiosord2.iloc[0:0]
        temp=[]
        logging.info('Account Id %(cid)s finished',{'cid':cidb})
    # Eliminar Columnas innecesarias
    logging.warning('Process Finished. Proceeding to concatenate and save')
    return audcuentas

# ----------------------------------------------------------------------------------------------------------------------

