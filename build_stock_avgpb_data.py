import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'historydb' : 'data/cnhistory.db',
    'quotadb' : 'data/stockpb.db',
    'cycle' : 5, # 5 years
    'min_tradedays': 200, # minimum trade days per year
    'start' : '2010-06-01'
}



# build  result tables
def InitDB(cu):

    #tablesql = 'create table quota (id varchar(20), name varchar(20), date varchar(10), open float, close float, adjclose float, pb float, avgpb float, incpotentials float)'
    tablesql = 'create table stockpb(id varchar(20), date varchar(10), avgpb float, pb float, incpotentials float)'
    InitTable('stockpb', tablesql, cu)



def InitTable(tablename, tablesql, cu):
    sql = 'select count(*) from sqlite_master where type="table" and name="'+tablename+'"'
    cu.execute(sql)
    r = cu.fetchone()
    if r[0] > 0: #table exist
        sql = 'drop table '+tablename
        cu.execute(sql)

    cu.execute(tablesql)


# id, name, date, open, close, adjclose, pb, avgpb


def BuildStockPB(cu):
    print('build average pb data for stocks...')

    #build memdb and import data to memory
    sql = 'create table memdb.memquota as select id, pb, date from history.quotation'

    print('import data from historydb to memory...')
    cu.execute(sql)

    sql = 'select date from memdb.memquota where date>="'+setting['start']+'" group by date order by date asc'    
    cu.execute(sql)

    records = cu.fetchall()
    l = len(records)
    iCount = 1
    for r in records:
        sdate = r[0]
        print(str(iCount)+'/'+str(l)+': '+sdate)
        iCount += 1
        
        begin = CalcCycleBeginDay(sdate, setting['cycle'])
        trade_days = str(setting['cycle']*setting['min_tradedays'])
        sql = 'insert into stockpb (id, avgpb, date) '
        sql += 'select id, avg(pb),"'+sdate+'" from memdb.memquota where pb>0 and date>="'+begin+'" and date<="'+sdate+'" group by id having count(pb)>'+trade_days
        cu.execute(sql)
    
    #     rows = mcu.fetchall()
    #     print(str(len(rows))+' rows need be processed.')
    #     for row in rows:
    #         sid = row[0]
    #         sdate = end
    #         #sql = 'select pb from memquota where id="'+sid+'" and date="'+sdate+'"'
    #         #mcu.execute(sql)
    #         #stock = mcu.fetchone()
    #         #if stock == None:
    #         #    continue
    #         #pb = stock[0]
    #         avgpb = row[1]
    #         #inc = (avgpb-pb)/pb
    #         sql = 'insert into stockpb (id, date, avgpb) values ("'+sid+'","'+sdate+'", '+str(avgpb)+')'
    #         cu.execute(sql)

    # mcu.close()
    # memdb.close()


def CalcCycleBeginDay(present, cycleyear):
    year = string.atoi(present[0:4])
    begin = str(year-cycleyear)+present[4:10]
    return begin

    




def main():
    db = sqlite3.connect(setting['quotadb'])
    cu = db.cursor()

    db.execute('attach ":memory:" as memdb')
    db.execute('attach "'+setting['historydb']+'" as history')

    InitDB(cu)
    BuildStockPB(cu)

    db.commit()
    cu.close()
    db.close()




main()
    
    
