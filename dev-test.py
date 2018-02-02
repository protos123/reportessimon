import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import sys
import datetime
import queries as qp
import os

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

#df=df[['fecha_rev', 'rev_id', 'email', 'usuario_id', 'nombre_comercio','nombre_contacto', 'url','domicilio',
#      'fecha_revprev','rev_idprev', 'emailprev', 'usuario_idprev', 'nombre_comercioprev','nombre_contactoprev',
#      'urlprev', 'domicilioprev','cambio']]
print df

filename='Pricing_Changes_January_2017.xlsx'
writer = pd.ExcelWriter(filename)
df.to_excel(writer)
writer.save()