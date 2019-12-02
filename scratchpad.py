

# KRZ 10.04.2018- changed the get_downtime to handle cases where the endtime was null
# KRZ 18.04.2018 - changed the workcenters to use multiple values, changed get_phase for cases whre more that 1 match exist for given workcentre.
                                    #The problem is SAP order may be launched for example room 140 but DMS executed in 145 then there would be no match
# KRZ 12.09.2019 - added yield, ref INC1868867, TASK2003928
# In[16]:
import os
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import cx_Oracle
import pandas as pd
import numpy as np
import datetime
import time

"""DB connections"""
DB = os.environ['DB']
DBDRIVER = os.environ['DBDRIVER']
SERVER = os.environ['SERVER']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
ACTIVITY_TABLE = os.environ['ACTIVITY_TABLE']
XML_PATH = os.environ['XML_PATH']
XML_PATH_ARCHIVE = os.environ['XML_PATH_ARCHIVE']
PHASES = os.environ['PHASES']
OPERATORS = os.environ['OPERATORS']
PLANT = os.environ['PLANT']
FINALCONFIRMATION = os.environ['FINALCONFIRMATION']

DB_XFP_SID = os.environ['XFP_DB_SID']
DB_XFP_IP = os.environ['XFP_DB_IP']
DB_XFP_PORT = os.environ['XFP_DB_PORT']
USERNAME_XFP = os.environ['XFP_USERNAME']
PASSWORD_XFP = os.environ['XFP_PASSWORD']



# In[34]:
# exec all

connection = pyodbc.connect(Driver="{SQL Server}", Server="TIL-KM-01.takeda.dom\\SQLEXPRESS", Database="DMSDAQPROD", user='evros', password='Ireland1')

sql_lots = """select a.activityid, a.activitytype, a.locidsymp, a.starttime, a.endtime,  RTRIM(LTRIM(a.txtid)) as txtid, a.preactivity,
                a.postactivity,RTRIM(LTRIM(a.itemcode)) as itemcode
                , (select locdescription from {}.dbo.tblLoc where a.locidsymp = locid) as workcentre
                from {}.{} a
                where isextracted = 0 and activitytype in ('1', '2') and postactivity is not null""".format(DB, DB, ACTIVITY_TABLE )


df_lots = pd.read_sql(sql_lots, connection)

connection.close()
print('END OF SCRIPT')


# In[ ]:


