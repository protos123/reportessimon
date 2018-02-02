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


path= os.getcwd()
files =os.listdir(path)

files_xlsx=['DJRCMerchants.xlsx']
print files_xlsx

df=pd.DataFrame()
today = datetime.date.today()
for f in files_xlsx:
    data=pd.read_excel(f,'Hoja1')
    df = df.append(data)
df.columns=['UserId']
df = df.drop_duplicates()
df['CaseID']=df['UserId'].astype(str) + '_PayULatam_'+ str(today)


users=df['UserId'].tolist()
temp = []
for index in range(0, len(users)):
    user=users[index]
    cursor.execute("""SELECT documento, tipo_documento, telefonos, direccion_ciudad, 
                    concat_ws('', direccion, direccion_linea2, direccion_linea3) as direccion, nombres, pais
                    FROM pps.usuario 
                    WHERE usuario_id=%(user)s limit 10""",{'user':user})
    for i in xrange(cursor.rowcount):
        temp.append(cursor.fetchone())

query = pd.DataFrame(temp, columns=['Document', 'IdType','Telephone','City','Address','RelationshipName','Country'])
query['Country'].replace({'AR':'Argentina','PE':'Peru','CL':'Chile','CO':'Colombia','MX':'Mexico','PA':'Panama','BR':'Brazil'},inplace=True)
query['IdType'].replace({'CI':'National ID', 'CC':'National ID', 'CE':'National ID', 'ID':'National ID', 'DNI':'National ID',
               'DNIE':'National ID', 'DE':'National ID', 'RUN':'National ID', 'CPF':'National ID', 'CURP':'National ID',
               'RE':'National ID','CNPJ':'Others (Entity)', 'CUIL':'Others (Entity)', 'CUIT':'Others (Entity)',
               'EIN':'Others (Entity)', 'NIF':'Others (Entity)', 'NIT':'Others (Entity)', 'RFC':'Others (Entity)',
               'RIF':'Others (Entity)', 'RUC':'Others (Entity)', 'RUT':'Others (Entity)','IFE':'Others (Individual)',
               'IDC':'Others (Individual)','PP':'Passport No.','SSN':'Social Security No.'}, inplace=True)
df=pd.concat([df,query], axis=1)
df['CaseName']=df['RelationshipName']
df['RelationShipID']=df['CaseID']
df['RelationshipType'] = np.where(df.IdType.isin(['National ID','Others(Individual)','Passport No.','Social Security No.']),'Individual', 'Entity')
df['First Name']= np.where(df['RelationshipType']== 'Individual', df['RelationshipName'], None)
df['RowAction']=str('Insert')
df['CaseOwner']=str('Liliana Rios')
df['Requestor']=str('Liliana Andrade Rios')
df['Phone']=str('+57 318 848 0913')
df['Email']=str('liliana.andrade@payulatam.com')
df['Priority']=str('Medium')
df['Segment']=df['Country']
df['Comment']=str('AUTOMATED SCREENING PROCESS. PAYU LATAM')
df['CaseStatus']=str('Submitted')
df['MatchRelationship']=str('FALSE')
df['IsClient']=str('True')
df['Screening']=str('Active')
df['Service-DJRC']=str('yes')
df['Service-DJNews']=str('no')
Addons=pd.DataFrame(columns=['Middle Name', 'Surname', 'Gender', 'DoB', 'AlternativeName', 'Occupation', 'Notes1', 'Notes2',
                    'AssociationType', 'IndustrySector', 'DocumentLinks', 'AddressURL', 'State', 'PostalCode'])
df=pd.concat([df,Addons],axis=1)


df=df[['RowAction','CaseID','CaseName','CaseOwner','Requestor','Phone','Email','Priority','Segment','Comment','CaseStatus','MatchRelationship','RelationshipType',
       'RelationShipID','RelationshipName','First Name','Middle Name','Surname','Gender','DoB','AlternativeName','Occupation','IdType','Document','Notes1','Notes2',
       'AssociationType','IndustrySector','IsClient','Screening','Priority','DocumentLinks','Country','Address','AddressURL','Telephone','City','State','PostalCode',
       'Service-DJRC','Service-DJNews']]
print df

filename = 'DJRC' + str(today) + '.xlsx'
writer = pd.ExcelWriter(filename)
df.to_excel(writer, index=False)
writer.save()
