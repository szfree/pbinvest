import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'database' : 'data/cnstockdata.db',
    'start' : '2005-01-01',
    'limit' : 30,
    'avgyear' : 5,
    'cycle' : 1,
    'belowavg' : -0.3, # pb must be lower average than 30%
    'resultfile' : 'data/portfolio.csv'
}


def main():
    db = sqlite3.connect(setting['database'])
    cu = db.cursor()

    InitDB(cu)
    print("calc days...")
    days = GetCalcDayList(cu)

    l = len(days)
    for day in days:
        if day == days[l-1]:
            continue
        pbinvest(day, cu)


    db.commit()

    SaveResult(cu)

    cu.close()
    db.close()


# get the list of calc day for each month
def GetCalcDayList(cu):
    sql = 'select date from stockdata where date>="' + setting['start']+'" group by date order by date asc'
    cu.execute(sql)
    rows = cu.fetchall()

    calcDayList = []
    m = ''
    d = ''
    for row in rows:
        items = row[0].split('-')
        if (m != items[1]):
            d = row[0]
            m = items[1]
            calcDayList.append(d)
    
    return calcDayList


def pbinvest(calcday, cu):

    calc_day = calcday
    print('calc day: '+calc_day)
    
    targetpb = 'pbavg'+str(setting['avgyear'])
    condition = '(pb/'+targetpb+'-1)'
    sql = 'select id, adjclose from stockdata where ' + condition + '<'+str(setting['belowavg'])+' and date="'+calcday+'" order by '+condition+' asc limit '+str(setting['limit'])
    cu.execute(sql)
    stocks = cu.fetchall()

    trade_day = Output(stocks, calc_day, cu)

def Output(stocks, calc_day, cu):
    print('stocks: '+str(len(stocks)))
    buyday = calc_day
    print(buyday)
    sellday = CalcSellDay(buyday, cu)
    print(sellday)

    for stock in stocks:  

        buyprice = GetBuyPrice(stock[0], calc_day, buyday, cu)
        if buyprice == None:
            continue
        
        
        s = GetSellPrice(stock[0], sellday, cu)
        if s == None:
            continue

        sql = 'insert into portfolio (id, calcday, buyday, buyprice,sellday,sellprice) values ("'+stock[0]+'","'+calc_day+'","'+buyday+'",'+str(buyprice)+',"'+sellday+'","'+str(s)+'")'
        
        try:
            cu.execute(sql)
        except:
            print(sql)
            raise
        
        
    return buyday


def CalcSellDay(buyday, cu):
    sql = 'select date from stockdata where date>"'+buyday+'" group by date order by date asc limit 8'
    cu.execute(sql)
    rows = cu.fetchall()

    buymonth = buyday[5:7]
    for row in rows:
        day = row[0]
        if buymonth != day[5:7]:
            return day

    raise
    return None


#return none then skip the stock
def GetBuyPrice(stockid, calcday, buyday, cu):
    sql = 'select adjclose from stockdata where id="'+stockid+'" and date="'+buyday+'"'
    cu.execute(sql)
    row = cu.fetchone()
    
    if row == None : 
        return None  # skip the stock
    
    return row[0]

# return sell price, ignore the decreasing limit = -10%
def GetSellPrice(stockid, sellday, cu):
    sql = 'select adjclose from stockdata where date="' +sellday+ '" and id="'+stockid+'" '
    cu.execute(sql)
    row = cu.fetchone()
    if row == None:
        return None

    return row[0]

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







# build illiq result tables
def InitDB(cu):
    sql = 'select count(*) from sqlite_master where type = "table" and name="portfolio"'
    cu.execute(sql)
    r = cu.fetchone()
    if r[0] > 0: #table exists
        sql = 'delete from portfolio'
    else:
        sql = 'create table portfolio (id varchar(20), calcday varchar(10), buyday varchar(10), sellday varchar(10), buyprice real, sellprice real, pb real)'
    cu.execute(sql)




    


main()
    