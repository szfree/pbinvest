import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'historydb' : 'data/cnhistory.db',
    'quotadb' : 'data/stockpb.db',
    'portfolio-cycle' : 20, #every 20 days to build the portofolio
    'limit' : 20, #stocks in the portfolio
    'start' : '2010-06-01',
    'resultfile' : 'data/portfolio.csv',
    'minimum_inc': 0.5
}



# build  result tables
def InitDB(cu):
    tablesql = 'create table portfolio (id varchar(20), calcday varchar(10), buyday varchar(10), sellday varchar(10), buyprice float, sellprice float, pb float, inctarget real)'
    InitTable('portfolio', tablesql, cu )



def InitTable(tablename, tablesql, cu):
    sql = 'select count(*) from sqlite_master where type="table" and name="'+tablename+'"'
    cu.execute(sql)
    r = cu.fetchone()
    if r[0] > 0: #table exist
        sql = 'drop table '+tablename
        cu.execute(sql)
    sql = tablesql
    cu.execute(sql)


# id, name, date, open, close, adjclose, pb, avgpb


def GetCalcDays(cu):
    calcDayList = []
    sql = 'select date from stockpb where date>="'+setting['start']+'" group by date order by date asc'
    cu.execute(sql)
    rows = cu.fetchall()

    count = 0
    for row in rows:
        if count % setting['portfolio-cycle'] == 0:
            calcDayList.append(row[0])
        count += 1
    return calcDayList

def pbinvest(calcday, cu):
    #sql = 'select stockpb.id, stockpb.date, history.quotation.pb, stockpb.avgpb from stockpb where incpotentials>'+str(setting['minimum_inc'])+' and date="'+calcday+'" order by incpotentials desc limit '+str(setting['limit'])
    sql = 'select stockpb.id, stockpb.date, history.quotation.pb, stockpb.avgpb from stockpb, history.quotation where stockpb.id=history.quotation.id and stockpb.date=history.quotation.date'
    sql += ' and stockpb.date="'+calcday+'" and (stockpb.avgpb/history.quotation.pb-1)>'+str(setting['minimum_inc'])+' order by (stockpb.avgpb/history.quotation.pb-1) desc limit ' +str(setting['limit'])
    cu.execute(sql)
    stocks = cu.fetchall()
    Output(stocks, calcday, cu)

def Output(stocks, calcday, cu):
    print(calcday+': portfolio')
    buyday = GetBuyDay(calcday,cu)
    sellday = GetSellDay(calcday,cu)

    for stock in stocks:  

        buyprice = GetBuyPrice(stock[0], calcday, buyday, cu)
        if buyprice == None:
            continue


        sql = 'select date from history.quotation where date<"'+sellday +'" and volume>0 and id="'+stock[0]+'" order by date desc limit 1'
        cu.execute(sql)
        row = cu.fetchone()
        
        
        s = GetSellPrice(stock[0], row[0], cu)
        if s == None:
            continue


        sql = "insert into portfolio (id, calcday, buyday, buyprice,sellday,sellprice) values ('"+stock[0]+"','"+calcday+"','"+buyday+"',"+str(buyprice)+",'"+s[0]+"',"+str(s[1])+")"
        try:
            cu.execute(sql)
        except:
            print(sql)
            raise
        
        
    return buyday

def GetBuyDay(calcday,cu):
    sql = 'select date from history.quotation where date>"'+calcday+'" and volume>0 group by date order by date asc limit 1'
    cu.execute(sql)
    row = cu.fetchone()
    return row[0]

def GetSellDay(calcday, cu):
    sql = 'select date from history.quotation where date>"'+calcday+'" and volume>0 group by date order by date asc limit '+str(setting['portfolio-cycle']+1)
    cu.execute(sql)
    rows = cu.fetchall()
    l = len(rows)
    return rows[l-1][0]

#return none then skip the stock
def GetBuyPrice(stockid, calcday, buyday, cu):
    sql = 'select date, close, adjclose from history.quotation where id="'+stockid+'" and date>="'+calcday+'" and volume>0 order by date asc limit 2'
    
    cu.execute(sql)
    rows = cu.fetchall()
    
    if(len(rows)<2):
        return None #skip the stock

    if rows[1][0] != buyday : 
        return None  # skip the stock
    
    #hit the increasing top point
    if (rows[1][1]-rows[0][1])/rows[0][1] > 0.098 : 
        return None # skip the stock
    
    return rows[1][2]

# return [], #1 is sellday, item2 is sellprice
def GetSellPrice(stockid, minusOneDay, cu):
    sql = 'select close, date, adjclose from history.quotation where date>="' +minusOneDay+ '" and volume>0 and id="'+stockid+'" order by date asc limit 3'
    cu.execute(sql)
    rows = cu.fetchall()
    
    # handle stop trading stocks
    if len(rows)<3:
        return None # skip the stock
        rsellday = rows[0][1]
        rsellprice = rows[0][2]
        r = []
        r.append(rsellday)
        r.append(rsellprice)
        return r
        
    # check to hit the decreasing limit
    if (rows[1][0] - rows[0][0])/rows[0][0] < -0.098: 
        return GetSellPrice(stockid, rows[1][1], cu)
        
    rsellday = rows[1][1]
    rsellprice = rows[1][2]

    dMinusOneDay = datetime.datetime.strptime(minusOneDay,'%Y-%m-%d')
    dSellDay = datetime.datetime.strptime(rsellday,'%Y-%m-%d')
    distance = (dSellDay - dMinusOneDay).days
    if distance>12:
        return None #skip the stock

    r = []
    r.append(rsellday)
    r.append(rsellprice)
    return r


#save the result to csv file
def SaveResult(cu):
    sql = 'select calcday, avg((sellprice - buyprice)/buyprice) from portfolio group by calcday order by calcday asc'
    cu.execute(sql)
    rows = cu.fetchall()

    ls = os.linesep
    try:  
        fobj = open(setting['resultfile'],  'w')  
    except IOError as err:  
        print('file open error: {0}'.format(err))    


    for row in  rows:
        fobj.write(row[0]+','+str(row[1])+'\n')
        #fobj.write(row[0]+','+str(row[1])+ls)
          
    fobj.close()  
      
    print('Save Portfolio done!') 

    




def main():
    db = sqlite3.connect(setting['quotadb'])
    db.execute('attach "' + setting['historydb'] + '" as history')
    cu = db.cursor()

    InitDB(cu)
    

    print("calc days...")
    days = GetCalcDays(cu)

    l = len(days)
    print(str(l))
    for day in days:
        if day == days[l-1]:
            continue
        pbinvest(day, cu)


    db.commit()

    SaveResult(cu)

    cu.close()
    db.close()




main()
    
    
