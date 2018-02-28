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
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

def mail_sender(filename):

    body=  'do not reply to this email'

    msg = MIMEMultipart()
    msg['From'] = "jesus.rincon@payulatam.com"
    msg['To'] = "jesus2142@gmail.com"
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = "Reporte Pruebas a enviar"
    msg.attach(MIMEText(body))

    temp_file = open(os.path.join(filename))
    attachment = MIMEText(temp_file.read())
    attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(attachment)
    temp_file.close()

    smtp_server = smtplib.SMTP("smtp.office365.com", 587)
    smtp_server.ehlo()
    smtp_server.starttls()
    smtp_server.ehlo()

    smtp_server.login("soporte@payulatam.com", "soporte2014")
    smtp_server.sendmail("soporte@payulatam.com", "jesus.rincon@payulatam.com", msg.as_string())
    smtp_server.close()

try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

start = datetime.date(2018, 1, 1)
end = datetime.date(2018, 1, 2)
days = end-start
cambioscuentas = pd.DataFrame()
for x in xrange(0, days.days):
        date = start + datetime.timedelta(days=x)
        accounts = qp.listacuentasbancos(date)
        if len(accounts) != 0:
            cambios = qp.controlcambioscuentasbancos(accounts, date)
            cambioscuentas = cambioscuentas.append(cambios, ignore_index=True)
            cambios = cambios.iloc[0:0]


filename = 'Bank_Accounts_Changes_' + str(start) + '_to_' + str(end) + ('.xlsx')
writer = pd.ExcelWriter(filename,options={'remove_timezone': True})
cambioscuentas.to_excel(writer)
writer.save()
logging.warning('Saved in file %(filename)s. Process completed',{'filename':filename})
mail_sender(filename)





