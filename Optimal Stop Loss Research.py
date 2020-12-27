import pandas as pd
import imp
import os
import numpy as np
from scipy import stats
import scipy as sp
import matplotlib.pyplot as plt

auth = imp.load_source('env', os.environ['PYENV'])
fun = imp.load_source('env', "C:\\Users\\jweiss\\Desktop\\Python Codes\\my_functions.py")
 
sql1 = """with dataset as (
    select
            hd.historicalDataReportID,
			historicalDate as analysisDate,
            tickerid,
			ticker,
            priceChgDay,
            alphaTickerPctDayReturn,
            lag(historicalDate) over (partition by hd.historicalDataReportID, tickerid order by historicalDate) as lagHistoricalDate,
            lead(historicalDate) over (partition by hd.historicalDataReportID, tickerid order by historicalDate) as leadHistoricalDate 
    from
        historicalData hd
	where
		shortLong = -1
		and historicalDataReportId not in (select historicalDataReportID from notIncludedInAggregateData)	
)
,leads as (
    select 
            *,
            row_number() over (partition by historicalDataReportID, tickerid order by analysisDate) as rowNumber 
    from
            dataset
    where
            (leadHistoricalDate is null or dateAdd(day,20,analysisDate) < leadHistoricalDate) 
            or (lagHistoricalDate is null or dateAdd(day,-20,analysisDate) > lagHistoricalDate) 
)
,leadsExtended as (
    select
            *,
            case when rowNumber%2 = 1 then analysisDate end as openDate, 
            case when rowNumber%2 = 1 then lead(analysisDate) over (partition by historicalDataReportID, tickerid order by analysisDate) end as closeDate 
    from
            leads
)
,tmp4 as (
    select
        hd.historicalDataReportID,
		historicalDate as analysisDate,
		le.openDate,
		le.closeDate,
        hd.tickerid,
		hd.ticker,
        (exp(sum(log(1 + hd.priceChgDay)) over (partition by hd.historicalDataReportID, hd.tickerid, le.openDate order by historicalDate rows between unbounded preceding and 0 preceding))) -
		(exp(sum(log(1 + hd.alphaTickerPctDayReturn )) over (partition by hd.historicalDataReportID, hd.tickerid, le.openDate order by historicalDate rows between unbounded preceding and 0 preceding))) as alphaCumulativeReturnTicker
    from
        leadsExtended le
		inner join historicalData hd on hd.historicalDataReportID = le.historicalDataReportID and hd.tickerID = le.tickerID
	where
		historicalDate >= le.openDate 
		and historicalDate <= le.closeDate
		and rowNumber%2 = 1
)
,tmp5 as (
	select 
		historicalDataReportID,
		tickerID,
		ticker,
		openDate,
		min(analysisDate) as minAnalysisDate
	from 
		tmp4
	where 
		alphaCumulativeReturnTicker < """

sql2 = """
	group by 
		historicalDataReportID,
		tickerid,
		ticker,
		openDate
)
,alphaPerformancePostBreach as (
	select
		t5.historicalDataReportID,
		hdr.departmentID,
		hdr.departmentName,
		hdr.fundName,
		th.tickerid,
		t5.ticker,
		minAnalysisDate,
		th.tickerDate,
		max(th.tickerDate) over (partition by hdr.historicalDataReportID, th.tickerid, t5.minAnalysisDate order by t5.minAnalysisDate) as maxDate,
		(exp(sum(log(1 + th.priceChgDay)) over (partition by hdr.historicalDataReportID, th.tickerid, t5.minAnalysisDate order by th.tickerDate rows between unbounded preceding and 0 preceding))) -
		(exp(sum(log(1 + th2.priceChgDay)) over (partition by hdr.historicalDataReportID, th.tickerid, t5.minAnalysisDate order by th.tickerDate rows between unbounded preceding and 0 preceding))) as alphaCumulativeReturnTicker

	from
		tmp5 t5
		inner join tickerHistoryATAnalyticsHistoricalData th on th.tickerID = t5.tickerID
		inner join historicalDataReport hdr on hdr.historicalDataReportId = t5.historicalDataReportId
		inner join tickerHistoryATAnalyticsHistoricalData th2 on th2.tickerId = hdr.alphaTickerID and th2.tickerDate = th.tickerDate

	where
	    minAnalysisDate <= dateAdd(month, -"""

sql3 = """, GETDATE())
		and th.tickerDate <= dateAdd(month, """
        
sql4 = """, t5.minAnalysisDate)
		and th.tickerDate >= t5.minAnalysisDate 
		and th.priceChgDay is not null
		and th2.priceChgDay is not null
    )
	select
		fundName,
		avg(alphaCumulativeReturnTicker) as avgPerformance
	from
		alphaPerformancePostBreach
	where
		tickerDate = maxDate
	group by
		fundName"""
        
sql5 = """, t5.minAnalysisDate)
		and th.tickerDate >= t5.minAnalysisDate 
		and th.priceChgDay is not null
		and th2.priceChgDay is not null
    )
	select distinct
		departmentName,
		fundName,
		percentile_cont(0.5) within group(order by alphaCumulativeReturnTicker) over (partition by fundName, departmentName) as alphaTickerReturnMedian
	from
		alphaPerformancePostBreach
	where
		tickerDate = maxDate"""


#Model Parameters
loss =  [str(round(i,2)) for i in np.arange(-.1,-.6,-.1)]
period = ['3','6','12','24','36']

#SQL Concatenation Average
threeMonthScript = [sql1 + i + sql2 + period[0] + sql3 + period[0] + sql4 for i in loss]
sixMonthScript = [sql1 + i + sql2 + period[1] + sql3 + period[1] + sql4  for i in loss]
oneYearScript = [sql1 + i + sql2 + period[2] + sql3 + period[2] + sql4  for i in loss]
twoYearScript = [sql1 + i + sql2 + period[3] + sql3 + period[3] + sql4  for i in loss]
threeYearScript = [sql1 + i + sql2 + period[4] + sql3 + period[4] + sql4  for i in loss]

#SQL Concatenation Median
threeMonthMedScript = [sql1 + i + sql2 + period[0] + sql3 + period[0] + sql5 for i in loss]
sixMonthMedScript = [sql1 + i + sql2 + period[1] + sql3 + period[1] + sql5  for i in loss]
oneYearMedScript = [sql1 + i + sql2 + period[2] + sql3 + period[2] + sql5 for i in loss]
twoYearMedScript = [sql1 + i + sql2 + period[3] + sql3 + period[3] + sql5  for i in loss]
threeYearMedScript = [sql1 + i + sql2 + period[4] + sql3 + period[4] + sql5  for i in loss]

#Data Generation Average
cnxn = auth.db_cnxn_pyodbc
threeMonthFrame = [fun.getData(i,None,cnxn) for i in threeMonthScript]
sixMonthFrame = [fun.getData(i,None,cnxn) for i in sixMonthScript]
oneYearFrame = [fun.getData(i,None,cnxn) for i in oneYearScript]
twoYearFrame = [fun.getData(i,None,cnxn) for i in twoYearScript]
threeYearFrame = [fun.getData(i,None,cnxn) for i in threeYearScript]

#Data Generation Median
threeMonthMedFrame = [fun.getData(i,None,cnxn) for i in threeMonthMedScript]
sixMonthMedFrame = [fun.getData(i,None,cnxn) for i in sixMonthMedScript]
oneYearMedFrame = [fun.getData(i,None,cnxn) for i in oneYearMedScript]
twoYearMedFrame = [fun.getData(i,None,cnxn) for i in twoYearMedScript]
threeYearMedFrame = [fun.getData(i,None,cnxn) for i in threeYearMedScript]

fun.sendEmail('jweiss@alphatheory.com','Optimal Stop Loss','Data Generation is complete')

#Average Client Alpha Performance Post Loss
threeMonthAvg = [i['avgPerformance'].mean() for i in threeMonthFrame]
sixMonthAvg = [i['avgPerformance'].mean() for i in sixMonthFrame]
oneYearAvg = [i['avgPerformance'].mean() for i in oneYearFrame]
twoYearAvg = [i['avgPerformance'].mean() for i in twoYearFrame]
threeYearAvg = [i['avgPerformance'].mean() for i in threeYearFrame]

#Median Client Alpha Performance Post Loss
threeMonthMed = [i['alphaTickerReturnMedian'].median() for i in threeMonthMedFrame]
sixMonthMed = [i['alphaTickerReturnMedian'].median() for i in sixMonthMedFrame]
oneYearMed = [i['alphaTickerReturnMedian'].median() for i in oneYearMedFrame]
twoYearMed = [i['alphaTickerReturnMedian'].median() for i in twoYearMedFrame]
threeYearMed = [i['alphaTickerReturnMedian'].median() for i in threeYearMedFrame]


#Generate Plots
avgDf = pd.DataFrame({'3 Month': threeMonthAvg,'6 Month': sixMonthAvg, '1 Year': oneYearAvg, '2 Year': twoYearAvg, '3 Year': threeYearAvg}, index = loss)
ax = avgDf[['3 Month','6 Month','1 Year','2 Year','3 Year']].plot.bar(title='Average Alpha Performance Post Stop loss (Shorts)')
ax.set_xlabel('Trigger')
ax.set_ylabel('Average Alpha Performance')
vals = ax.get_yticks()
vals2 = [.1,.2,.3,.4,.5]
ax.set_yticklabels(['{:,.1%}'.format(x) for x in vals])
ax.set_xticklabels(['{:,.1%}'.format(x) for x in vals2])

medDf = pd.DataFrame({'3 Month': threeMonthMed,'6 Month': sixMonthMed, '1 Year': oneYearMed, '2 Year': twoYearMed, '3 Year': threeYearMed}, index = loss)
ax = medDf[['3 Month','6 Month','1 Year','2 Year','3 Year']].plot.bar(title='Median Alpha Performance Post Stop Loss (Shorts)')
ax.set_xlabel('Trigger')
ax.set_ylabel('Median Alpha Performance')
vals = ax.get_yticks()
vals2 = [.1,.2,.3,.4,.5]
ax.set_yticklabels(['{:,.1%}'.format(x) for x in vals])
ax.set_xticklabels(['{:,.1%}'.format(x) for x in vals2])

#Concatenate and Send to Excel
def sendToExcel(dataFrame, fileName):
    writer = pd.ExcelWriter('C:/Users/jweiss/Desktop/' + fileName + '.xlsx', engine='xlsxwriter')
    dataFrame.to_excel(writer, sheet_name=fileName)
    writer.save()

def concatAndSend(dataFrame, period, fileName):
    for i in range(len(loss)):
        dataFrame[i]['Period'] = period[i]
    combine = pd.concat(dataFrame)
    return sendToExcel(combine, fileName)
        
concatAndSend(threeMonthFrame, loss, 'Average 3 Month Performance')
concatAndSend(sixMonthFrame, loss, 'Average 6 Month Performance')
concatAndSend(oneYearFrame, loss, 'Average 1 year Performance')
concatAndSend(twoYearFrame, loss, 'Average 2 year Performance')
concatAndSend(threeYearFrame, loss, 'Average 3 year Performance')

concatAndSend(threeMonthMedFrame, loss, 'Median 3 Month Performance')
concatAndSend(sixMonthMedFrame, loss, 'Median 6 Month Performance')
concatAndSend(oneYearMedFrame, loss, 'Median 1 year Performance')
concatAndSend(twoYearMedFrame, loss, 'Median 2 year Performance')
concatAndSend(threeYearMedFrame, loss, 'Median 3 year Performance')


    