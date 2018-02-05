import os
import pandas as pd
import datetime as dt

print dt.date.today()
path= os.getcwd()
files =os.listdir(path)
print files

files_xlsx=['DJRCMerchants.xlsx']
print files_xlsx