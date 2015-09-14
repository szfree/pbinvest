import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'database' : 'data/cnstock.db',
    'folder' : 'csv'
}





#id, name, date, ttmpe, adjclose, pe, pb


def main():
    build_db()
    csvs = get_csv_list()
    for csv in csvs:
        load_csv(csv)


def get_value(strval):
    return strval.strip().replace('"','').replace(',','')


def get_csv_list():
    data = os.walk(setting['folder'])
    for root, dirs, files in data:
        if root.upper() == setting['folder'].upper():
            return files

def build_db():
    if os.path.isfile(setting['database']):
        return

    db =  sqlite3.connect(setting['database'])
    cu = db.cursor()

    sql = 'create table quota (id varchar(20), date varchar(10), ttmpe float, adjclose float, pe float, pb float)'
    cu.execute(sql)
    db.commit()
    cu.close()
    db.close()


def load_csv(csv):
    print('load data from csv file: ' + csv + '...')
    f = open(setting['folder']+'\\'+csv)
    lines = f.readlines()
    f.close()
    
    db =  sqlite3.connect(setting['database'])
    cu = db.cursor()
    
    skipfirstline = True
    processid = ''
    count = 0
    for line in lines:

        if skipfirstline:
            skipfirstline = False
            continue

        items = line.split('","')
        
        sid = get_value(items[0]).replace('SH','SS')
        sdate = get_value(items[2])
        sttmpe = get_value(items[3])
        sadjclose = get_value(items[4])
        spe = get_value(items[5])
        spb = get_value(items[6])

        if (sttmpe.strip() == '') or (sadjclose.strip()=='') or (spe.strip()=='') or (spb.strip()==''):
            continue

        sql = 'insert into quota (id, date, ttmpe, adjclose, pe, pb) values ("'+sid+'","'+sdate+'","'+sttmpe+'","'+sadjclose+'","'+spe+'","'+spb+'")'
        cu.execute(sql)

        if processid != sid:
            processid = sid
            count += 1
            print('Import stock '+str(count)+' : '+sid)
        
    cu.close()
    db.commit()
    db.close()
    


main()
    
    
