import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'database' : 'data/cnstockdata.db',
    'folder' : 'cnpbweekly'
}





#id, name, date, ttmpe, adjclose, pe, pb, pbavg3, pbavg4, pbavg5, pbavg6, pbavghistory


def main():
    build_db()
    csvs = get_csv_list()
    for csv in csvs:
       load_csv(csv)

    CalcAvgPB()

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

    sql = 'create table stockdata (id varchar(20), date varchar(10), ttmpe float, adjclose float, pe float, pb float, pbavghistory float, pbavg3 float, pbavg4 float, pbavg5 float, pbavg6 float)'
    cu.execute(sql)

    sql =' create index idx_iddate on stockdata(id, date)'
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

        sql = 'insert into stockdata (id, date, ttmpe, adjclose, pe, pb) values ("'+sid+'","'+sdate+'","'+sttmpe+'","'+sadjclose+'","'+spe+'","'+spb+'")'
        cu.execute(sql)

        if processid != sid:
            processid = sid
            print('Import stock : '+sid)
        
    cu.close()
    db.commit()
    db.close()

def MoveToYearsAgo(sdate, years):
    return str(string.atoi(sdate[0:4])-years)+sdate[4:]
    
def CalcAvgPB():
    print('calcating average pb for stocks...')

    db =  sqlite3.connect(setting['database'])
    cu = db.cursor()

    #list all the dates
    sql = 'select date from stockdata group by date order by date asc'
    cu.execute(sql)
    dates = cu.fetchall()

    for daterow in dates:
        sdate = daterow[0]
        print('processing:  '+sdate+' ...')

        y3ahead = MoveToYearsAgo(sdate, 3)
        y4ahead = MoveToYearsAgo(sdate, 4)
        y5ahead = MoveToYearsAgo(sdate, 5)
        y6ahead = MoveToYearsAgo(sdate, 6)

        sql = 'select id from stockdata where date="'+sdate+'"'
        cu.execute(sql)
        symbols = cu.fetchall()

        for symbolrow in symbols:
            symbol = symbolrow[0]
            sql = 'select avg(pb) from stockdata where id="'+symbol+'" and date>="'+y3ahead+'" and date<="'+sdate+'"'
            cu.execute(sql)
            row = cu.fetchone()
            pb3 = row[0]

            sql = 'select avg(pb) from stockdata where id="'+symbol+'" and date>="'+y4ahead+'" and date<="'+sdate+'"'
            cu.execute(sql)
            row = cu.fetchone()
            pb4 = row[0]

            sql = 'select avg(pb) from stockdata where id="'+symbol+'" and date>="'+y5ahead+'" and date<="'+sdate+'"'
            cu.execute(sql)
            row = cu.fetchone()
            pb5 = row[0]

            sql = 'select avg(pb) from stockdata where id="'+symbol+'" and date>="'+y6ahead+'" and date<="'+sdate+'"'
            cu.execute(sql)
            row = cu.fetchone()
            pb6 = row[0]

            sql = 'select avg(pb) from stockdata where id="'+symbol+'" and date<="'+sdate+'"'
            cu.execute(sql)
            row = cu.fetchone()
            pbhistory = row[0]

            sql = 'update stockdata set pbavghistory='+str(pbhistory)+', pbavg3='+str(pb3)+', pbavg4='+str(pb4)+', pbavg5='+str(pb5)+', pbavg6='+str(pb6)+' where id="'+symbol+'" and date="'+sdate+'"'
            cu.execute(sql)

    cu.close()
    db.commit()
    db.close()



main()
    
    
