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


# Listado de usuarios que recibieron un cambio el dia 18.
# ARGS=fecha de ejecucion
# -PENDIENTE AGREGAR FECHA EJECUCION
# Agregar como variable la fecha del dia del dia anterior
def listausuarios():
    today = datetime.date.today()
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    logging.warning('Searching user id with changes on %(today)s', {'today':str(today)})
    cursor.execute("""SELECT u.usuario_id FROM audit.usuario_aud u
    inner join audit.revision_auditoria ra ON u.rev = ra.revision_id 
    inner join pps.usuario pu on (u.usuario_id=pu.usuario_id)
    WHERE (to_timestamp(fecha_revision/1000))>='2017-12-18' and 
    (to_timestamp(fecha_revision/1000))<%(today)s and pu.fecha_Creacion<%(tomorrow)s
    order by u.usuario_id""",{'today':today,'tomorrow':tomorrow})
    usuarios = pd.DataFrame(cursor.fetchall())
    usuarios.columns = ['usuario_id']
    usuarios = usuarios.drop_duplicates()
    logging.warning('Search finished: %(cantidad)s have received changes', {'cantidad':len(usuarios)})
    return usuarios


# -------------------------------------------------------------------------------------------------------------

# CONTROL DE CAMBIOS:
# CORRE POR EL LISTADO DE USUARIOS TRAYENDO LA CONSULTA DE LOS CAMBIOS A LA FECHA Y ANEXANDO EL ULTIMO CAMBIO
# ANTERIOR A LA FECHA DE EJECUCION DEL REPORTE.
# RETORNA UN DATAFRAME CON EL LISTADO DE TODOS LOS CAMBIOS PARA EL LISTADO DE USUARIOS.

def controlcambios(usuarios):

    usuarios = usuarios['usuario_id'].tolist()
    temp = []
    today = datetime.date.today()
    tomorrow=datetime.date.today() + datetime.timedelta(days=1)
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
#-------------------------------------------------------------------------------------------------------

# Taer listado de usuarios
users = listausuarios()

# Ejecutar script de control de cambios
cambios = controlcambios(users)

# Guardar Cambios en archivo de Excel
today = datetime.date.today()
filename = 'Users_Report_' + str(today) + ('.xlsx')
writer = pd.ExcelWriter(filename)
cambios.to_excel(writer)
writer.save()

logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})
