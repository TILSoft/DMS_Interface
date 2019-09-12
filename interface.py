

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

columns=['ActivityID', 'ProcessOrder', 'Plant', 'Phase', 'FinalConfirmation', 'ClearReservation', 'PostingDate'
                                 , 'StartDate', 'StartTime', 'FinishDate', 'FinishTime', 'SetUp', 'Machine'
                                 , 'Labor', 'Operators', 'Stop', 'PO_qty', 'PO_unit']

workcenters={'AVI1':('AVI 1 Visual Inspection',),
							'NOT IN DMS':('BL 432 Blending',),
							'BL2':('BL2 Packaging', 'BL3 Packaging', 'BL4 Packaging'),
							'BL3':('BL3 Packaging', 'BL4 Packaging', 'BL2 Packaging'),
							'BL4':('BL4 Packaging', 'BL3 Packaging', 'BL2 Packaging'),
							'TIL1 437 CF':('CF 437 CF',),
							'COAT 138':('COAT 138 Coating',),
							'TIL1 435 EC':('EC 435 Enteric Coating',),
							'GRAN 128':('GRAN 128 Granulating', 'GRAN 129 Granulating'),
							'GRAN 129':('GRAN 129 Granulating', 'GRAN 128 Granulating'),
							'PRINT 140':('PRIN 140 Printing', 'PRIN 145 Printing'),
							'PRINT 145':('PRIN 145 Printing', 'PRIN 140 Printing'),
							'TIL2A':('RBFG TIL2 Granulation',),
							'BLEND 137':('SBL 137 Blending', 'VBL 130 Blending'),
                            'BLEND 130':('VBL 130 Blending', 'SBL 137 Blending'),
							'TP1':('TP1 Tabletting',),
							'TP2':('TP2 Tabletting',),
							'TP3':('TP3 Tabletting',),
							'TP4':('TP4 Tabletting',),
							'TP5':('TP5 Tabletting',),
							'TP6':('TP6 Tabletting',)}

units = {"kg":"KGM", "g":"GRM", "un":"PCE", "kn":"TS"}

df_xml = pd.DataFrame(columns=columns)

# %%
# xfp query
def get_po_details(po):
    """Runs select and returns dataframe"""
    # https://stackoverflow.com/questions/49288724/read-and-write-clob-data-using-python-and-cx-oracle
    def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
        if defaultType == cx_Oracle.CLOB:
            return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
        elif defaultType == cx_Oracle.BLOB:
            return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)

    query = "select quantiteof as qty, uniteof as unit from elan2406PRD.xfp_ofentete where numof = '{}'".format(po)

    try:
        connection_string = cx_Oracle.makedsn(DB_XFP_IP,  DB_XFP_PORT, DB_XFP_SID)
        connection = cx_Oracle.connect(USERNAME_XFP,
                                        PASSWORD_XFP,
                                        connection_string, encoding="UTF-8", nencoding="UTF-8")
        connection.outputtypehandler = OutputTypeHandler
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
    except cx_Oracle.DatabaseError as e:
        print(e)
        print(query)
        raise
    finally:
        connection.close()
    try:
        return data[0]
    except IndexError as e:
        return ("err", "err")

# In[17]:

def get_processorder(lot, item):
    sql = """select workorderno from {}.dbo.tblsapworkorderinterface
                             where lotnoclean = '{}' and itemcode = '{}'""".format(DB, lot, item)
    df = pd.read_sql(sql, connection)
    if df.size == 0:
        return 0
    else:
        return df.at[0,'workorderno'].strip()


# In[18]:

def get_phase(item, stage, workcentre):
    df = df_phases
    phase = 0
    try:
        locations = workcenters[workcentre]
    except:
        return 0
    for location in locations:
        try:
            phase = df.loc[(df['Material'] == item) & (df['Stage'] == stage) & (df['Description'] == location), 'Phase'].iloc[0]
        except:
            pass
        if phase != 0: #the 1st match is returned
            return phase
    return phase


# In[19]:

def get_precoactivity(activityid):
    pre = int(activityid)
    while True:
        try:
            df = df_activities.loc[pre, ['activitytype', 'preactivity']]
        except:
            pre = None
            break # end of the loop, exit
        if df['activitytype'] == 2: # co found
            break
        if df['activitytype'] == 1: # other lot, means there was no co
            pre = None
            break
        if df['activitytype'] == 3: # idle, keep looping
            pre = df['preactivity']
    return pre


# In[20]:

def get_lot(activityid):
    post = int(activityid)
    error_code = 0
    while True:
        try:
            df = df_activities.loc[post, ['activitytype', 'postactivity']]
        except:
            post = None
            error_code = 2
            break # end of the loop, exit
        if df['activitytype'] == 1: # co found
            break
        if df['activitytype'] == 2: # other co, means there was no lot
            error_code = 1
            post = None
            break
        if df['activitytype'] == 3: # idle, keep looping
            post = df['postactivity']
    if post == None:
        return (0, 0, 0, error_code)
    else:
        lot = df_activities.loc[post, 'txtid']
        item = df_activities.loc[post, 'itemcode']
        format = df_activities.loc[post, 'format']
        return (lot, item, format, error_code)


# In[21]:

def get_operators(format):
    try:
        return df_format.loc[format, 'stdpersonnel']
    except:
        return OPERATORS


# In[22]:

def get_format(activityid):
    try:
        return int(df_activities.loc[activityid, 'format'])
    except:
        return 0


# In[23]:

def get_shifttime(loc, start, end):
    npstart = np.datetime64(start)
    npend = np.datetime64(end)

    sql = """select shiftid, locid, shiftstart, shiftend from {}.dbo.tblshifts
                      where locid = '{}' and shiftend >=  '{}' and shiftstart <=  '{}' order by shiftstart desc""".format(DB, loc, start, end)
    df = pd.read_sql(sql, connection,  index_col='shiftid')

    # change start & end to actual times
    df.loc[df['shiftstart'] <= npstart, 'shiftstart'] = npstart
    df.loc[df['shiftend'] >= npend, 'shiftend'] = npend

    df['duration'] = df['shiftend'] - df['shiftstart']
    duration = -1
    try:
        duration = int(df['duration'].sum().total_seconds() / 60)
    except:
        duration = -1
    return duration


# In[24]:

def get_downtime(id):
    sql = """select downtimeid, downtimestart, downtimeend from {}.dbo.tbldowntime
              where downtimelotid = {}""".format(DB, id)
    df = pd.read_sql(sql, connection,  index_col='downtimeid')
    if df.size == 0:
        return 0
    duration = 0
    try:
        df['duration'] = df['downtimeend'] - df['downtimestart']
        duration = int(df['duration'].sum().total_seconds() / 60)
    except:
        duration = 0
    return duration


# In[33]:

def log_failure(activity, step, error_code):
    message = ""
    if step == 1:
        message = "Incorrect shift time"
    elif step == 2:
        if error_code == 2:
            message = "Change over has no lot assigned"
        elif error_code == 1:
            message = "There is other change over for this lot"
        else:
            message = "Process order number not found"
    elif step == 3:
        message = "SAP phase not found, contact ISIT"

    sql = """UPDATE {}.{}
                 SET status = '{}'
                 WHERE activityid = {}""".format(DB, ACTIVITY_TABLE, message, activity)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except:
        print(sql)
        print('Error when updating db from log_failure function')



# In[26]:

def xml_prep():
    now = datetime.datetime.now()
    global df_xml
    x = []
    error_code = 0
    for row in df_lots.itertuples():
        n = []
        n.append(row.activityid)
        co = False
        if row.activitytype == 2:
            co = True
        if co == True:
            lot, item, format, error_code = get_lot(row.postactivity)
            po = get_processorder(lot, item)
            phase = get_phase(item, 'Setup', row.workcentre)
        else:
            po = get_processorder(row.txtid, row.itemcode)
            phase = get_phase(row.itemcode, 'Operation', row.workcentre)
            format = get_format(row.activityid)
        if po == 0:
            log_failure(row.activityid, 2, error_code)
            continue
        machine = get_shifttime(row.locidsymp, row.starttime.strftime('%Y-%m-%d %H:%M:%S')
                                , row.endtime.strftime('%Y-%m-%d %H:%M:%S'))
        if machine == -1:
            log_failure(row.activityid, 1, error_code)
            continue
        setup = '0'
        operators = get_operators(format)
        if co == True:
            setup = machine * operators
            machine = '0'
        if phase == 0:
            log_failure(row.activityid, 3, error_code)
            continue
        n.append(str(po).rjust(12, '0'))
        n.append(PLANT)
        n.append(phase)
        n.append(FINALCONFIRMATION)
        n.append('')
        n.append(now.strftime('%Y%m%d'))
        n.append(row.starttime.strftime('%Y%m%d'))
        n.append(row.starttime.strftime('%H%M%S'))
        n.append(row.endtime.strftime('%Y%m%d'))
        n.append(row.endtime.strftime('%H%M%S'))
        n.append(str(setup))
        n.append(str(machine))
        if co == True:
            #n.append(str(setup * operators))
            # Labour always 0 for setup, ref email "DMS Order Confirmations - Alignment with Global Finance"
            n.append('0')
        else:
            n.append(str(machine * operators))
        n.append(str(get_operators(format)))
        n.append(str(get_downtime(row.activityid)))
        po_details = get_po_details(po)
        po_qty = po_details[0]
        po_unit = units[po_details[1].lower()]
        n.append(po_qty)
        n.append(po_unit)
        x.append(n)
    dftemp = pd.DataFrame.from_records(x, columns=columns)
    df_xml = df_xml.append(dftemp, ignore_index=True)


# In[27]:

def to_xml(df):
    saved_ids = []
    unit = 'MIN'
    for row in df.itertuples():
        xml = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml.append('<ns0:PhaseConfirmation')
        xml.append('xmlns:ns0="urn:tpie.dms.tp.erp.PhaseConfirmation.ZCONFTT01">')
        xml.append('  <PhsConfHeader>')
        xml.append('    <ProcessOrder>{}</ProcessOrder>'.format(row.ProcessOrder))
        xml.append('    <Plant>{}</Plant>'.format(row.Plant))
        xml.append('    <PhsConfItem>')
        xml.append('      <Phase>{}</Phase>'.format(row.Phase))
        xml.append('      <FinalConfirmation>{}</FinalConfirmation>'.format(row.FinalConfirmation))
        xml.append('      <ClearReservation>X</ClearReservation>')
        xml.append('      <Yield UnitOfMeasure=\"{}\">{}</Yield>'.format(row.PO_unit, row.PO_qty))
        xml.append('      <PostingDate>{}</PostingDate>'.format(row.PostingDate))
        xml.append('      <StartDate>{}</StartDate>'.format(row.StartDate))
        xml.append('      <StartTime>{}</StartTime>'.format(row.StartTime))
        xml.append('      <FinishDate>{}</FinishDate>'.format(row.FinishDate))
        xml.append('      <FinishTime>{}</FinishTime>'.format(row.FinishTime))
        xml.append('      <SetUp UnitOfMeasure="{}">{}</SetUp>'.format(unit, row.SetUp))
        xml.append('      <Machine UnitOfMeasure="{}">{}</Machine>'.format(unit, row.Machine))
        xml.append('      <Labor UnitOfMeasure="{}">{}</Labor>'.format(unit, row.Labor))
        xml.append('      <Operators UnitOfMeasure="NO">{}</Operators>'.format(row.Operators))
        xml.append('      <Stop UnitOfMeasure="{}">{}</Stop>'.format(unit, row.Stop))
        xml.append('    </PhsConfItem>')
        xml.append('  </PhsConfHeader>')
        xml.append('</ns0:PhaseConfirmation>')
        xml = '\n'.join(xml)
        if save_xml_file(xml):
            saved_ids.append(row.ActivityID)
    #print(saved_ids)
    return saved_ids


# In[28]:

def save_xml_file(xml):
    success = True
    time.sleep(0.05)
    now = datetime.datetime.now()
    filename = "TPIE_DMS_PC{}-{}-{}.xml".format(now.strftime('%Y%m%d'), now.strftime('%H%M%S'), now.strftime('%f')[:-3])
    filenamesap = XML_PATH + '\\' + filename
    filenamearch = XML_PATH_ARCHIVE + '\\' + filename
    try:
        with open(filenamesap, 'w') as f:
            f.write(xml)
        with open(filenamearch, 'w') as f:
            f.write(xml)
    except:
        success = False
    return success


# In[29]:

def update_db(ids):
    sql_list = []
    for i in ids:
        sql_list.append("'{:.0f}'".format(i))
    if len(sql_list) > 0:
        sql_list = ', '.join(sql_list)
        sql_list = '(' + sql_list + ')'
        sql = """UPDATE {}.{}
                 SET isextracted = 1, status = 'Successfully extracted'
                 WHERE activityid in {}""".format(DB, ACTIVITY_TABLE, sql_list)
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
        except:
            print(sql)
            print('Error when updating db from update_db function')
    else:
        print('Nothing to update')


# In[34]:
# exec all
df_xml.drop(df_xml.index, inplace=True)
connection = pyodbc.connect(Driver=DBDRIVER, Server=SERVER, Database=DB, user=USERNAME, password=PASSWORD)

sql_lots = """select a.activityid, a.activitytype, a.locidsymp, a.starttime, a.endtime,  RTRIM(LTRIM(a.txtid)) as txtid, a.preactivity,
                a.postactivity,RTRIM(LTRIM(a.itemcode)) as itemcode
                , (select locdescription from {}.dbo.tblLoc where a.locidsymp = locid) as workcentre
                from {}.{} a
                where isextracted = 0 and activitytype in ('1', '2') and postactivity is not null""".format(DB, DB, ACTIVITY_TABLE )

sql_activities = """select activityid, activitytype, preactivity, postactivity, RTRIM(LTRIM(txtid)) as txtid,
                    RTRIM(LTRIM(itemcode)) as itemcode, starttime, format
                  from {}.{}
                  where starttime >  dateadd(month, -12, sysdatetime())
                  order by activityid desc""".format(DB, ACTIVITY_TABLE)

sql_format = """select formatid, stdpersonnel from {}.dbo.tblFormat where stdpersonnel is not null""".format(DB)

df_phases = pd.read_csv(PHASES, dtype={'Material': np.object, 'Phase': np.object})
df_lots = pd.read_sql(sql_lots, connection)
df_activities = pd.read_sql(sql_activities, connection, index_col='activityid', coerce_float=False)
df_activities['postactivity'].fillna(value=0, axis='index', inplace=True)
df_activities = df_activities.astype(dtype= {"postactivity": np.int64})
df_format = pd.read_sql(sql_format, connection, index_col='formatid')
xml_prep()
#update_db(to_xml(df_xml))
to_xml(df_xml)
connection.close()
print('END OF SCRIPT')


# In[ ]:
