import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pymssql
import os
import time
from dateutil.parser import parse

fundcd = '213505'
bmcd = 'G213AHI18019'

if not os.path.exists('AdjustBM Scenario_' + fundcd):
    os.makedirs('AdjustBM Scenario_' + fundcd)

Tgdurgap = 0.02
Tgytmgap = 0.001

xin = 5  # X-axis Step
nitr = 5
datasrc = 'KFR'
mktnr = '36'
unyongcd = 'B7700'
# unyongcd = 'J7700' # 가상펀드 'J7700'

startdt = '20100101'
enddt = '20180930'

s0 = time.time()

print("조정BM 비중 계산용 데이터 가져오기(오래걸림)......")
s1 = time.time()

conn = pymssql.connect(server='172.32.1.31', user='ufi', password='ufi', charset='EUC-KR')
cursor = conn.cursor()

qryi1d = """
    SELECT CONVERT(CHAR(8),A.CHURI_YMD,112) CHURI_DT, CONVERT(CHAR(8),FST_SEOLJ_YMD,112) FST_SEOLJ_DT, CONVERT(FLOAT,A.BM_DURATION), 
           CONVERT(FLOAT,A.BM_GIJUN_GA/B.BM_GIJUN_GA) BM_GIJUN_GA_RT
    FROM AITASDB.DBO.UKS0MG A 
         LEFT JOIN AITASDB.DBO.UKS0MG B ON A.CHURI_YMD = CONVERT(CHAR(8),DATEADD(DAY,1,B.CHURI_YMD),112)
         LEFT JOIN (SELECT FUND_CD, FST_SEOLJ_YMD FROM AITASDB.DBO.SZM0FD) C ON C.FUND_CD = A.FUND_CD AND C.FUND_CD = B.FUND_CD
    WHERE A.FUND_CD = %s AND A.FUND_CD = B.FUND_CD 
          AND A.UNYONG_CD = %s AND A.UNYONG_CD = B.UNYONG_CD
          AND ABS(A.BM_DURATION - B.BM_DURATION) > = %s
          AND A.CHURI_YMD >= C.FST_SEOLJ_YMD
    ORDER BY A.CHURI_YMD
     """
cursor.execute(qryi1d, (fundcd, unyongcd, Tgdurgap))
RawDatai1d = list(cursor.fetchall())
Datai1d = pd.DataFrame(RawDatai1d,
                       columns=['INDEX_DATE', 'FST_SEOLJ_DATE', 'BM_DURATION', 'BM_GIJUN_GA_RT'])

qryi1y = """
    SELECT CONVERT(CHAR(8),A.CHURI_YMD,112) CHURI_DT, CONVERT(CHAR(8),FST_SEOLJ_YMD,112) FST_SEOLJ_DT, CONVERT(FLOAT,A.BM_DURATION), 
           CONVERT(FLOAT,A.BM_GIJUN_GA/B.BM_GIJUN_GA) BM_GIJUN_GA_RT
    FROM AITASDB.DBO.UKS0MG A 
         LEFT JOIN AITASDB.DBO.UKS0MG B ON A.CHURI_YMD = CONVERT(CHAR(8),DATEADD(DAY,1,B.CHURI_YMD),112)
         LEFT JOIN (SELECT FUND_CD, FST_SEOLJ_YMD FROM AITASDB.DBO.SZM0FD) C ON C.FUND_CD = A.FUND_CD AND C.FUND_CD = B.FUND_CD
    WHERE A.FUND_CD = %s AND A.FUND_CD = B.FUND_CD 
          AND A.UNYONG_CD = %s AND A.UNYONG_CD = B.UNYONG_CD
          AND ABS(A.BM_YTM - B.BM_YTM) > = %s
          AND ABS(A.BM_DURATION - B.BM_DURATION) < %s
          AND A.CHURI_YMD >= C.FST_SEOLJ_YMD
    ORDER BY A.CHURI_YMD
     """
cursor.execute(qryi1y, (fundcd, unyongcd, Tgytmgap, Tgdurgap))
RawDatai1y = list(cursor.fetchall())
Datai1y = pd.DataFrame(RawDatai1y,
                       columns=['INDEX_DATE', 'FST_SEOLJ_DATE', 'BM_DURATION', 'BM_GIJUN_GA_RT'])

Datai1 = pd.concat([Datai1d, Datai1y], ignore_index=True)
Datai1 = Datai1.sort_values(by=['INDEX_DATE'])
Datai1 = Datai1.reset_index(drop=True)

fskjdt = Datai1['FST_SEOLJ_DATE'][0]
startdtm = max(parse(fskjdt).strftime("%Y-%m-%d"), parse(startdt).strftime("%Y-%m-%d"))
startdt = str(startdtm)[:4] + str(startdtm)[5:7] + str(startdtm)[-2:]

e1 = time.time()
gap1 = e1 - s1
print(">>>소요시간:", gap1, "초")

print("조정BM 비중 가져오기......")
s2 = time.time()

XAdjData = pd.read_excel('AdjustBM Weight_' + fundcd + '.xlsx',
                         dtype={'BM_CALC_DATE': object, 'SECTOR_GB': str, 'TENOR_GB': int, 'BM_WEIGHT': float,
                                'Adj_BM_WEIGHT': float})

e2 = time.time()
gap2 = e2 - s2
print(">>>소요시간:", gap2, "초")

print("BM 및 AP기준가 시계열 데이터 가져오기......")
s3 = time.time()

qry1 = """
    SELECT CONVERT(DATE,A.CHURI_YMD) CHURI_YMD, CONVERT(FLOAT, A.BM_GIJUN_GA), CONVERT(FLOAT, A.SUJ_GIJUN_GA), CONVERT(FLOAT,A.BM_DURATION), CONVERT(FLOAT,A.FUND_SUJ_DURATION), CONVERT(FLOAT,A.BM_YTM),
		    CONVERT(FLOAT,A.FUND_YTM), CONVERT(FLOAT,A.BM_SUIK_RT), CONVERT(FLOAT,A.SUIK_RT), CONVERT(FLOAT,B.COMP_BM_INDEX), CONVERT(FLOAT,B.COMP_BM_DUR), CONVERT(FLOAT,B.COMP_BM_YTM),
		    CONVERT(FLOAT,A.SUIK_RT-A.BM_SUIK_RT), CONVERT(FLOAT,POWER((A.SUIK_RT-A.BM_SUIK_RT),2)), CONVERT(FLOAT,TRACK_ERROR3), CONVERT(FLOAT,TRACK_ERROR2)
    FROM AITASDB.DBO.UKS0MG A
	LEFT JOIN (SELECT T1.GIJUN_DT COMP_BM_YMD, SUM(T1.WEIGHT/100* T1.IL_TOT_SUIK_JISU) COMP_BM_INDEX,
				SUM(T1.WEIGHT/100* T1.AVG_DUR) COMP_BM_DUR, SUM(T1.WEIGHT/100* T1.AVG_MANGI_SUIK/100) COMP_BM_YTM
				FROM ufi.dbo.BM_MATRIX_DATA T1 
				WHERE T1.JISU_CD = %s 
						AND T1.SECTOR_CD IN ('111000', '112000', '113000', '114000', '115000', '121000', '122000', '123000', '131000', '132000', '137120', '137210', '137220', '137230', '138000', '411000', '421000',
                           '422000', '427100', '427120', '427200', '427210', '427220', '427230', '427300', '427310', '427320', '427330', '436120', '437210', '437220', '437230', '437310', '437320',
                           '437330', '437410', '437420', '447110', '447120', '447130', '447210', '447220', '447230', '447310', '447320', '447330', '447410', '447420', '447440', '512020', '513020',
                           '514020', '515020', '527100', '527120', '527200', '527210', '527220', '527230', '527300', '527310', '527320', '527330', '527400', '527410', '527420', '527430', '717110',
                           '717120', '717130', '717210', '717220', '717230', '717310', '717320', '717330', '717410', '717420', '717430')
						AND T1.MANGI_CD IN('01','02','03','04','05','06','07','08','09','010','011','012','013','014','015')
						AND T1.PGA_CO_CD = %s
				GROUP BY T1.GIJUN_DT) B ON A.CHURI_YMD = B.COMP_BM_YMD
    WHERE A.FUND_CD = %s AND (A.BM_DURATION/B.COMP_BM_DUR) is not NULL AND A.BM_GIJUN_GA<>0
    AND A.CHURI_YMD >= %s AND A.CHURI_YMD <= %s 
    ORDER BY A.CHURI_YMD
    """
cursor.execute(qry1, (bmcd, datasrc, fundcd, startdt, enddt))
RawData1 = list(cursor.fetchall())
Data1 = pd.DataFrame(RawData1,
                     columns=['INDEX_DATE', 'BM_GIJUN_GA', 'SUJ_GIJUN_GA', 'BM_DURATION', 'FUND_SUJ_DURATION', 'BM_YTM',
                              'FUND_YTM', 'BM_SUIK_RT', 'SUIK_RT', 'COMP_BM_INDEX', 'COMP_BM_DUR', 'COMP_BM_YTM',
                              'EXCESS_RT', 'EXCESS_RT_MLT', 'TRACK_ERROR(C)', 'TRACK_ERROR(B)'])

e3 = time.time()
gap3 = e3 - s3
print(">>>소요시간:", gap3, "초")

print("BM 섹터-만기별 시계열 데이터 가져오기(오래걸림)......")
s4 = time.time()

qry20 = """
    SELECT CONVERT(DATE,T1.GIJUN_DT,112) CHURI_YMD, T1.SECTOR_CD, CONVERT(INT, T1.MANGI_CD),                         
        CASE WHEN T1.SECTOR_CD ='111000' THEN 'S01'
             WHEN T1.SECTOR_CD ='411000' THEN 'S01'
             ELSE 'S02' END  CREDIT_GB,
        CONVERT(FLOAT,SUM(T1.WEIGHT/100)) BM_WEIGHT,
		 CASE WHEN SUM(T1.WEIGHT/100) <> 0 THEN CONVERT(FLOAT,SUM(T1.IL_TOT_SUIK_JISU*T1.WEIGHT/100)/SUM(T1.WEIGHT/100)) ELSE 0 END BM_JISU,
		 CASE WHEN SUM(T1.WEIGHT/100) <> 0 THEN CONVERT(FLOAT,SUM(T1.AVG_DUR*T1.WEIGHT/100)/SUM(T1.WEIGHT/100)) ELSE 0 END BM_DURATION,
		 CASE WHEN SUM(T1.WEIGHT/100) <> 0 THEN CONVERT(FLOAT,SUM(T1.AVG_MANGI_SUIK/100*T1.WEIGHT/100)/SUM(T1.WEIGHT/100)) ELSE 0 END BM_YTM
    FROM ufi.dbo.BM_MATRIX_DATA T1 
    WHERE T1.JISU_CD = %s 
		AND T1.SECTOR_CD IN ('111000', '112000', '113000', '114000', '115000', '121000', '122000', '123000', '131000', '132000', '137120', '137210', '137220', '137230', '138000', '411000', '421000',
                           '422000', '427100', '427120', '427200', '427210', '427220', '427230', '427300', '427310', '427320', '427330', '436120', '437210', '437220', '437230', '437310', '437320',
                           '437330', '437410', '437420', '447110', '447120', '447130', '447210', '447220', '447230', '447310', '447320', '447330', '447410', '447420', '447440', '512020', '513020',
                           '514020', '515020', '527100', '527120', '527200', '527210', '527220', '527230', '527300', '527310', '527320', '527330', '527400', '527410', '527420', '527430', '717110',
                           '717120', '717130', '717210', '717220', '717230', '717310', '717320', '717330', '717410', '717420', '717430')
		AND T1.MANGI_CD IN('01','02','03','04','05','06','07','08','09','010','011','012','013','014','015') 
		AND T1.PGA_CO_CD = %s
		AND T1.GIJUN_DT >= %s AND T1.GIJUN_DT <= %s
    GROUP BY T1.GIJUN_DT, T1.SECTOR_CD, T1.MANGI_CD
    ORDER BY T1.GIJUN_DT, T1.SECTOR_CD, T1.MANGI_CD
    """

cursor.execute(qry20, (bmcd, datasrc, startdt, enddt))
RawData20 = list(cursor.fetchall())
Data20 = pd.DataFrame(RawData20,
                      columns=['INDEX_DATE', 'SECTOR_GB', 'TENOR_GB', 'CREDIT_GB', 'BM_WEIGHT(t)', 'BM_GIJUN_GA(t)',
                               'BM_DURATION(t)', 'BM_YTM(t)'])

qry21 = """
    SELECT CONVERT(DATE,A.INDEX_DT,112), CONVERT(FLOAT,A.INDEX_VAL) BM_GIJUN_GA, CONVERT(FLOAT,B.DURATION) BM_DURATION, CONVERT(FLOAT,(C.BASKET_YTM-D.CD-E.UNDP/B.DURATION*4)/100) BM_YTM 
    FROM dbo.MARKET_DB_DATA A 
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL DURATION FROM dbo.MARKET_DB_DATA A) B 
        ON A.INDEX_DT = B.INDEX_DT AND B.INDEX_CD IN('KRMHKT3MDKRW')
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL BASKET_YTM FROM dbo.MARKET_DB_DATA A) C 
        ON A.INDEX_DT = C.INDEX_DT AND C.INDEX_CD IN('KRMHKT3BYKRW')
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL CD FROM dbo.MARKET_DB_DATA A) D 
        ON A.INDEX_DT = D.INDEX_DT AND D.INDEX_CD IN('KRMHCD91DKRW')
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL UNDP FROM dbo.MARKET_DB_DATA A) E
        ON A.INDEX_DT = E.INDEX_DT AND E.INDEX_CD IN('KRMHKT3UPKRW')
    WHERE A.INDEX_CD IN( 'KRMHKTF3YKRW') AND A.INDEX_DT >= %s AND A.INDEX_DT <= %s
    ORDER BY A.INDEX_DT
    """
cursor.execute(qry21, (startdt, enddt))
RawData21 = list(cursor.fetchall())
Data21 = pd.DataFrame(RawData21,
                      columns=['INDEX_DATE', 'BM_GIJUN_GA(t)', 'BM_DURATION(t)', 'BM_YTM(t)'])
Data21['SECTOR_GB'] = 'KTF03Y'
Data21['TENOR_GB'] = 9
Data21['CREDIT_GB'] = 'S01'
Data21['BM_WEIGHT(t)'] = 0.0

Data21 = Data21[['INDEX_DATE', 'SECTOR_GB', 'TENOR_GB', 'CREDIT_GB', 'BM_WEIGHT(t)', 'BM_GIJUN_GA(t)', 'BM_DURATION(t)',
                 'BM_YTM(t)']]

writer = pd.ExcelWriter(
    os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\Test_KTF03Y.xlsx')
Data21.to_excel(writer, 'Test_KTF03Y')
writer.save()

qry22 = """
    SELECT CONVERT(DATE,A.INDEX_DT,112), CONVERT(FLOAT,A.INDEX_VAL) BM_GIJUN_GA,CONVERT(FLOAT,B.DURATION) BM_DURATION, CONVERT(FLOAT,(C.BASKET_YTM-D.CD-E.UNDP/B.DURATION*4)/100) BM_YTM 
    FROM dbo.MARKET_DB_DATA A 
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL DURATION FROM dbo.MARKET_DB_DATA A) B 
        ON A.INDEX_DT = B.INDEX_DT AND B.INDEX_CD IN('KRMHKTXMDKRW')
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL BASKET_YTM FROM dbo.MARKET_DB_DATA A) C 
        ON A.INDEX_DT = C.INDEX_DT AND C.INDEX_CD IN('KRMHKTXBYKRW')
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL CD FROM dbo.MARKET_DB_DATA A) D 
        ON A.INDEX_DT = D.INDEX_DT AND D.INDEX_CD IN('KRMHCD91DKRW')
        LEFT JOIN ( SELECT A.INDEX_DT, A.INDEX_CD, A.INDEX_VAL UNDP FROM dbo.MARKET_DB_DATA A) E
        ON A.INDEX_DT = E.INDEX_DT AND E.INDEX_CD IN('KRMHKTXUPKRW')
    WHERE A.INDEX_CD IN( 'KRMHKTF10KRW') AND A.INDEX_DT >= %s AND A.INDEX_DT <= %s
    ORDER BY A.INDEX_DT
    """
cursor.execute(qry22, (startdt, enddt))
RawData22 = list(cursor.fetchall())
Data22 = pd.DataFrame(RawData22,
                      columns=['INDEX_DATE', 'BM_GIJUN_GA(t)', 'BM_DURATION(t)', 'BM_YTM(t)'])
Data22['SECTOR_GB'] = 'KTF10Y'
Data22['TENOR_GB'] = 12
Data22['CREDIT_GB'] = 'S01'
Data22['BM_WEIGHT(t)'] = 0.0

Data22 = Data22[['INDEX_DATE', 'SECTOR_GB', 'TENOR_GB', 'CREDIT_GB', 'BM_WEIGHT(t)', 'BM_GIJUN_GA(t)', 'BM_DURATION(t)',
                 'BM_YTM(t)']]

writer = pd.ExcelWriter(
    os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\Test_KTF10Y.xlsx')
Data22.to_excel(writer, 'Test_KTF10Y')
writer.save()

qry23 = """
    SELECT CONVERT(DATE,A.CHURI_YMD,112) INDEX_DT, CONVERT(FLOAT,A.JISU) BM_GIJUN_GA, CONVERT(FLOAT,D.CD/100) BM_YTM
    FROM AITASDB.DBO.UKSFBF A
        LEFT JOIN ( SELECT B.INDEX_DT, B.INDEX_CD, B.INDEX_VAL CD FROM dbo.MARKET_DB_DATA B) D 
        ON CONVERT(CHAR(8),A.CHURI_YMD,112) = D.INDEX_DT AND D.INDEX_CD IN('KRMHCD91DKRW')
    WHERE A.FACTOR_CD = '@V_B0619' AND A.CHURI_YMD >= %s AND A.CHURI_YMD <= %s
    ORDER BY CONVERT(CHAR(8),A.CHURI_YMD,112) 
    """
cursor.execute(qry23, (startdt, enddt))
RawData23 = list(cursor.fetchall())
Data23 = pd.DataFrame(RawData23,
                      columns=['INDEX_DATE', 'BM_GIJUN_GA(t)', 'BM_YTM(t)'])
Data23['SECTOR_GB'] = 'L00001'
Data23['TENOR_GB'] = 1
Data23['CREDIT_GB'] = 'S01'
Data23['BM_WEIGHT(t)'] = 0.0
Data23['BM_DURATION(t)'] = 0.0

Data23 = Data23[['INDEX_DATE', 'SECTOR_GB', 'TENOR_GB', 'CREDIT_GB', 'BM_WEIGHT(t)', 'BM_GIJUN_GA(t)', 'BM_DURATION(t)',
                 'BM_YTM(t)']]

writer = pd.ExcelWriter(
    os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\Test_L00001.xlsx')
Data23.to_excel(writer, 'Test_L00001')
writer.save()

Data2 = pd.concat([Data20, Data21, Data22, Data23], ignore_index=True)
Data2 = Data2.sort_values(by=['INDEX_DATE'])
Data2 = Data2.reset_index(drop=True)

writer = pd.ExcelWriter(
    os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\Test_ALL.xlsx')
Data2.to_excel(writer, 'Test_ALL')
writer.save()

e4 = time.time()
gap4 = e4 - s4
print(">>>소요시간:", gap4, "초")

print("최초시점 BM 및 AP기준가 가져오기(오래걸림)......")
s61 = time.time()

qry3 = """SELECT CONVERT(DATE,T1.CHURI_YMD), T1.BM_GIJUN_GA, T1.SUJ_GIJUN_GA
       FROM AITASDB.DBO.UKS0MG T1
       LEFT JOIN (SELECT A.FUND_CD FUND_CD,
       CASE WHEN A.FST_SEOLJ_YMD > = B.MIN_DATE THEN A.FST_SEOLJ_YMD
       WHEN A.FST_SEOLJ_YMD < B.MIN_DATE THEN B.MIN_DATE END SELECTED_YMD
       FROM AITASDB.DBO.SZM0FD A
       LEFT JOIN (SELECT FUND_CD FUND_CD, MIN(CHURI_YMD) MIN_DATE FROM AITASDB.DBO.UKS0MG GROUP BY FUND_CD) B ON A.FUND_CD=B.FUND_CD) T2
       ON T1.FUND_CD=T2.FUND_CD
       WHERE T1.FUND_CD = %s
       AND T1.UNYONG_CD = %s AND T1.CHURI_YMD = T2.SELECTED_YMD"""

cursor.execute(qry3, (fundcd, unyongcd))
RawData3 = list(cursor.fetchall())
Data3 = pd.DataFrame(RawData3,
                     columns=['INITIAL_DATE', 'I_BM_GIJUN_GA', 'I_SUJ_GIJUN_GA'])
IniBM = float(Data3['I_BM_GIJUN_GA'][0])
IniAP = float(Data3['I_SUJ_GIJUN_GA'][0])

e61 = time.time()
gap61 = e61 - s61
print(">>>소요시간:", gap61, "초")

print("AP Credit비중 시계열 데이터 가져오기(매우 오래걸림)......")
s31 = time.time()

qry41 = """
    SELECT CONVERT(DATE,AA.GIJUN_DT) GIJUN_DT, AA.FUND_CD, AA.UNYONG_CD, 
           II.SECTOR_GB CREDIT_GB, 
           CONVERT(FLOAT,sum(AA.SIGA_PG_AEK/ GG.TOTAL_AEK)) CREDIT_WEIGHT,
           CONVERT(FLOAT,sum(AA.SIGA_PG_AEK*AA.MEIB_SUIK_RT)/SUM(AA.SIGA_PG_AEK)) MEIB_SUIK_RT
    FROM EUMATSDB..TBAC21 AA 
         INNER JOIN EUMATSDB..TBAC15 CC ON AA.GIJUN_DT = CC.GIJUN_DT AND AA.JM_CD = CC.JM_CD 
         INNER JOIN EUMATSDB..TBAC13 DD ON CC.KIS_GROUP_CD = DD.KIS_GROUP_CD
         INNER JOIN (SELECT DD.KIS_GROUP_CD KIS_GR_CD,
                            DD.SMART_GROUP_CD SECTOR_CD,
                            CASE WHEN DD.SMART_GROUP_CD = '111' THEN 'RF'
                                 WHEN DD.SMART_GROUP_CD = '211' THEN 'RF'
                                 ELSE 'CR' END SECTOR_GB
                     FROM EUMATSDB..TBAC13 DD) II ON CC.KIS_GROUP_CD = II.KIS_GR_CD AND DD.KIS_GROUP_CD = II.KIS_GR_CD AND DD.SMART_GROUP_CD = II.SECTOR_CD
         INNER JOIN (SELECT CONVERT(DATE,AA.GIJUN_DT) GIJUN_DT, AA.FUND_CD FUND_CD, AA.UNYONG_CD UNYONG_CD, SUM(AA.SIGA_PG_AEK) TOTAL_AEK
                            FROM EUMATSDB..TBAC21 AA 
                            GROUP BY CONVERT(DATE,AA.GIJUN_DT), AA.FUND_CD, AA.UNYONG_CD) AS GG ON GG.FUND_CD =AA.FUND_CD AND GG.GIJUN_DT=AA.GIJUN_DT AND GG.UNYONG_CD=AA.UNYONG_CD
    WHERE AA.FUND_CD= %s AND AA.AEKM <> '0' AND II.SECTOR_GB = 'CR'
          AND AA.GIJUN_DT >= %s AND AA.GIJUN_DT <= %s
          AND AA.UNYONG_CD = %s
    GROUP BY CONVERT(DATE,AA.GIJUN_DT), AA.FUND_CD, AA.UNYONG_CD, II.SECTOR_GB
    ORDER BY CONVERT(DATE,AA.GIJUN_DT), AA.FUND_CD, AA.UNYONG_CD, II.SECTOR_GB
    """
cursor.execute(qry41, (fundcd, startdt, enddt, unyongcd))
RawData41 = list(cursor.fetchall())
Data41 = pd.DataFrame(RawData41, columns=['INDEX_DATE', 'FUND_CD', 'UNYONG_CD', 'AP_CREDIT_GB', 'AP_CREDIT_WEIGHT',
                                          'AP_CREDIT_MEIB_RT'])
Data41 = Data41.drop(['FUND_CD', 'UNYONG_CD'], 1)

e31 = time.time()
gap31 = e31 - s31
print(">>>소요시간:", gap31, "초")

ResultData = Data1.drop(['COMP_BM_INDEX', 'COMP_BM_DUR', 'COMP_BM_YTM'], 1)
ResultData = pd.merge(ResultData, Data41, on=['INDEX_DATE'])
ResultData = ResultData.drop(['AP_CREDIT_GB'], 1)

tn = ResultData['INDEX_DATE'].size

print("조정BM 계산1......")
s5 = time.time()

if tn != 0:

    RawAdjJisu = []
    # RawAdjJisu0 = []
    RawAdjDur = []
    RawAdjYtm = []
    RawBmCrdWg = []
    RawAdjCrdWg = []
    AmdRatio = 1
    for i in range(0, tn):
        SData1 = Data2.loc[Data2['INDEX_DATE'] == ResultData['INDEX_DATE'][i]]
        Tempdts = XAdjData.loc[XAdjData['BM_CALC_DATE'] <= ResultData['INDEX_DATE'][i]]
        Tempdt = Tempdts['BM_CALC_DATE'][Tempdts['BM_CALC_DATE'].size - 1]
        AdjWgData = XAdjData.loc[XAdjData['BM_CALC_DATE'] == Tempdt]
        SData1Adj = pd.merge(SData1, AdjWgData, on=['SECTOR_GB', 'TENOR_GB', 'CREDIT_GB'])
        AdjJisu0 = float(np.dot(SData1Adj['Adj_BM_WEIGHT'].T, SData1Adj['BM_GIJUN_GA(t)']))
        AdjDur = float(np.dot(SData1Adj['Adj_BM_WEIGHT'].T, SData1Adj['BM_DURATION(t)']))
        AdjYtm = float(np.dot(SData1Adj['Adj_BM_WEIGHT'].T, SData1Adj['BM_YTM(t)']))
        bmcrdwg = SData1Adj.loc[SData1Adj['CREDIT_GB'] != 'S01']['BM_WEIGHT(t)'].sum()
        Adjcrdwg = SData1Adj.loc[SData1Adj['CREDIT_GB'] != 'S01']['Adj_BM_WEIGHT'].sum()
        if Tempdt == ResultData['INDEX_DATE'][i]:
            Tempdtstr = str(Tempdt)[:4] + str(Tempdt)[5:7] + str(Tempdt)[-2:]
            if Tempdtstr != fskjdt or i != 0:
                BmRt = float(Datai1.loc[Datai1['INDEX_DATE'] == Tempdtstr]['BM_GIJUN_GA_RT'].sum())
                AmdRatio = (RawAdjJisu[i - 1] * BmRt) / AdjJisu0
        AdjJisu = AdjJisu0 * AmdRatio

        RawAdjJisu.append(AdjJisu)
        # RawAdjJisu0.append(AdjJisu0)
        RawAdjDur.append(AdjDur)
        RawAdjYtm.append(AdjYtm)
        RawBmCrdWg.append(bmcrdwg)
        RawAdjCrdWg.append(Adjcrdwg)

    ResultData.insert(len(ResultData.columns), "BM_CREDIT_WEIGHT", RawBmCrdWg)
    ResultData.insert(len(ResultData.columns), "Adj_BM_GIJUN_GA", RawAdjJisu)
    # ResultData.insert(len(ResultData.columns), "Adj_BM_GIJUN_GA(Z)", RawAdjJisu0)
    ResultData.insert(len(ResultData.columns), "Adj_BM_DURATION", RawAdjDur)
    ResultData.insert(len(ResultData.columns), "Adj_BM_YTM", RawAdjYtm)
    ResultData.insert(len(ResultData.columns), "Adj_CREDIT_WEIGHT", RawAdjCrdWg)

    Inidata = ResultData.loc[ResultData['INDEX_DATE'] == Data3['INITIAL_DATE'][0]]
    Inidata = Inidata.reset_index(drop=True)

    e5 = time.time()
    gap5 = e5 - s5
    print(">>>소요시간:", gap5, "초")

    print("최초시점 BM 및 펀드기준가 가져오기2......")
    s62 = time.time()

    if Inidata.size == 0:

        Tmpinidate = ResultData['INDEX_DATE'][0][:4] + ResultData['INDEX_DATE'][0][5:7] + ResultData['INDEX_DATE'][0][
                                                                                          -2:]
        qry3 = "SELECT CONVERT(DATE,T1.CHURI_YMD), CONVERT(FLOAT, T1.BM_GIJUN_GA), CONVERT(FLOAT,T1.SUJ_GIJUN_GA) " \
               "FROM AITASDB.DBO.UKS0MG T1 " \
               "WHERE T1.FUND_CD ='" + fundcd + "' " \
                                                "AND T1.CHURI_YMD = '" + Tmpinidate + "'"
        cursor.execute(qry3)
        RawData3 = list(cursor.fetchall())
        Data3 = pd.DataFrame(RawData3,
                             columns=['INITIAL_DATE', 'I_BM_GIJUN_GA', 'I_SUJ_GIJUN_GA'])
        IniBM = float(Data3['I_BM_GIJUN_GA'][0])
        IniAP = float(Data3['I_SUJ_GIJUN_GA'][0])
        IniAdjBM = float(ResultData['Adj_BM_GIJUN_GA'][0])
        # IniAdjBM0 = float(ResultData['Adj_BM_GIJUN_GA(Z)'][0])
    else:
        IniAdjBM = float(Inidata['Adj_BM_GIJUN_GA'][0])
        # IniAdjBM0 = float(Inidata['Adj_BM_GIJUN_GA(Z)'][0])

    e62 = time.time()
    gap62 = e62 - s62
    print(">>>소요시간:", gap62, "초")

    print("조정BM 레벨보정......")
    s7 = time.time()

    ResultData.loc[0, 'EXCESS_RT'] = 0
    ResultData.loc[0, 'EXCESS_RT_MLT'] = 0
    ResultData['SUJ_GIJUN_GA'] = ResultData['SUJ_GIJUN_GA'] * (IniBM / IniAP)
    ResultData['Adj_BM_GIJUN_GA'] = ResultData['Adj_BM_GIJUN_GA'] * (IniBM / IniAdjBM)
    # ResultData['Adj_BM_GIJUN_GA(Z)'] = ResultData['Adj_BM_GIJUN_GA(Z)'] * (IniBM / IniAdjBM0)
    ResultData.insert(len(ResultData.columns), "AP_INDEX_GAP",
                      (ResultData['SUJ_GIJUN_GA'] - ResultData['BM_GIJUN_GA']) / IniBM)
    ResultData.insert(len(ResultData.columns), "Adj_BM_INDEX_GAP",
                      (ResultData['Adj_BM_GIJUN_GA'] - ResultData['BM_GIJUN_GA']) / IniBM)
    ResultData.insert(len(ResultData.columns), "AP_DUR_BET",
                      ResultData['FUND_SUJ_DURATION'] / ResultData['BM_DURATION'])
    ResultData.insert(len(ResultData.columns), "Adj_BM_DUR_BET",
                      ResultData['Adj_BM_DURATION'] / ResultData['BM_DURATION'])
    ResultData.insert(len(ResultData.columns), "AP_YTM_GAP", ResultData['FUND_YTM'] - ResultData['BM_YTM'])
    ResultData.insert(len(ResultData.columns), "Adj_BM_YTM_GAP", ResultData['Adj_BM_YTM'] - ResultData['BM_YTM'])
    ResultData.insert(len(ResultData.columns), "AP_CREDIT_GAP",
                      ResultData['AP_CREDIT_WEIGHT'] / ResultData['BM_CREDIT_WEIGHT'])
    ResultData.insert(len(ResultData.columns), "Adj_BM_CREDIT_GAP",
                      ResultData['Adj_CREDIT_WEIGHT'] / ResultData['BM_CREDIT_WEIGHT'])
    ResultData.insert(len(ResultData.columns), "STANDARD(Z)", 0)
    ResultData.insert(len(ResultData.columns), "STANDARD(O)", 1)
    ResultData.insert(len(ResultData.columns), "Adj_BM_RT", ResultData['Adj_BM_GIJUN_GA'].pct_change())
    ResultData.insert(len(ResultData.columns), "Adj_EXCESS_RT", ResultData['Adj_BM_RT'] - ResultData['BM_SUIK_RT'])
    ResultData.insert(len(ResultData.columns), "Adj_EXCESS_RT_MLT", ResultData['Adj_EXCESS_RT'] ** 2)
    ResultData.insert(len(ResultData.columns), "COUNT", 1)
    ResultData.insert(len(ResultData.columns), "TRACK_ERROR(C)M", ((ResultData['COUNT'].cumsum(axis=0) * ResultData[
        'EXCESS_RT_MLT'].cumsum(axis=0) - ResultData['EXCESS_RT'].cumsum(axis=0) ** 2) / (
                                                                            ResultData['COUNT'].cumsum(
                                                                                axis=0) ** 2)) ** 0.5)
    ResultData.insert(len(ResultData.columns), "Adj_TRACK_ERROR(C)", ((ResultData['COUNT'].cumsum(axis=0) * ResultData[
        'Adj_EXCESS_RT_MLT'].cumsum(axis=0) - ResultData['Adj_EXCESS_RT'].cumsum(axis=0) ** 2) / (
                                                                              ResultData['COUNT'].cumsum(
                                                                                  axis=0) ** 2)) ** 0.5)

    e7 = time.time()
    gap7 = e7 - s7
    print(">>>소요시간:", gap7, "초")

    print("시장금리 시계열 데이터 가져오기......")
    s32 = time.time()

    qry42 = """
            SELECT CONVERT(DATE,T1.CHURI_YMD) YTM_DATE, CONVERT(FLOAT,T1.SUIK_RT/100) RF_YTM, CONVERT(FLOAT,(T2.SUIK_RT - T1.SUIK_RT)/100) CR_SP  
            FROM  AITASDB.DBO.MJSIKY T1 
                  LEFT JOIN (select PGA_CO_GB, CHURI_YMD CHURI_YMD, BUNRYU_CD BUNRYU_CD, GIGAN GIGAN, SUIK_RT SUIK_RT from AITASDB.DBO.MJSIKY) T2 
                             ON T2.BUNRYU_CD = '7010122' AND T2.GIGAN = T1.GIGAN AND T2.CHURI_YMD=T1.CHURI_YMD AND T2.PGA_CO_GB = T1.PGA_CO_GB
            WHERE T1.BUNRYU_CD ='1013000' AND T1.PGA_CO_GB = '2' 
            AND T1.GIGAN = %s AND T1.CHURI_YMD >= %s AND T1.CHURI_YMD <= %s
            ORDER BY CONVERT(DATE,T1.CHURI_YMD)
           """
    cursor.execute(qry42, (mktnr, startdt, enddt))
    RawData42 = list(cursor.fetchall())
    Data42 = pd.DataFrame(RawData42, columns=['INDEX_DATE', 'RF_YTM', 'CR_SP'])

    e32 = time.time()
    gap32 = e32 - s32
    print(">>>소요시간:", gap32, "초")

    ResultData = pd.merge(ResultData, Data42, on=['INDEX_DATE'])

    print("매입수익률 및 채권잔고 시계열 데이터 가져오기(오래걸림)......")
    s33 = time.time()

    qry43 = """
            SELECT CONVERT(DATE,AA.GIJUN_DT) GIJUN_DT,
                   CONVERT(FLOAT,sum(SIGA_PG_AEK*MEIB_SUIK_RT)/SUM(SIGA_PG_AEK)) MEIB_SUIK_RT,
                   CONVERT(FLOAT,SUM(SIGA_PG_AEK)/power(10,8)) TOTAL_SIGA_PG_AEK
             FROM EUMATSDB..TBAC21 AA 
             WHERE UNYONG_CD = %s AND FUND_CD= %s and GIJUN_DT >= %s and GIJUN_DT <= %s AND MEIB_SUIK_RT <> 1 
             GROUP BY CONVERT(DATE,AA.GIJUN_DT)
             ORDER BY CONVERT(DATE,AA.GIJUN_DT)
           """
    cursor.execute(qry43, (unyongcd, fundcd, startdt, enddt))
    RawData43 = list(cursor.fetchall())
    Data43 = pd.DataFrame(RawData43, columns=['INDEX_DATE', 'AP_MEIB_RT', 'AP_BOND_PG_AEK'])

    e33 = time.time()
    gap33 = e33 - s33
    print(">>>소요시간:", gap33, "초")

    ResultData = pd.merge(ResultData, Data43, on=['INDEX_DATE'])
    ResultData = ResultData.reset_index(drop=True)
    tn = ResultData['INDEX_DATE'].size

    ResultData.insert(len(ResultData.columns), "BM_SUIK_RT(B)", ResultData['BM_GIJUN_GA'].pct_change())
    ResultData.insert(len(ResultData.columns), "SUIK_RT(B)", ResultData['SUJ_GIJUN_GA'].pct_change())
    ResultData.insert(len(ResultData.columns), "Adj_BM_RT(B)", ResultData['Adj_BM_GIJUN_GA'].pct_change())
    ResultData.insert(len(ResultData.columns), "EXCESS_RT(B)", ResultData['SUIK_RT(B)'] - ResultData['BM_SUIK_RT(B)'])
    ResultData.insert(len(ResultData.columns), "EXCESS_RT_MLT(B)", ResultData['EXCESS_RT(B)'] ** 2)
    ResultData.insert(len(ResultData.columns), "Adj_EXCESS_RT(B)",
                      ResultData['Adj_BM_RT(B)'] - ResultData['BM_SUIK_RT(B)'])
    ResultData.insert(len(ResultData.columns), "Adj_EXCESS_RT_MLT(B)", ResultData['Adj_EXCESS_RT(B)'] ** 2)
    ResultData.insert(len(ResultData.columns), "COUNT(B)", 1)
    ResultData.insert(len(ResultData.columns), "TRACK_ERROR(B)M", ((ResultData['COUNT(B)'].cumsum(axis=0) * ResultData[
        'EXCESS_RT_MLT(B)'].cumsum(axis=0) - ResultData['EXCESS_RT(B)'].cumsum(axis=0) ** 2) / (
                                                                            ResultData['COUNT(B)'].cumsum(
                                                                                axis=0) ** 2)) ** 0.5)
    ResultData.insert(len(ResultData.columns), "Adj_TRACK_ERROR(B)",
                      ((ResultData['COUNT(B)'].cumsum(axis=0) * ResultData[
                          'Adj_EXCESS_RT_MLT(B)'].cumsum(axis=0) - ResultData['Adj_EXCESS_RT(B)'].cumsum(
                          axis=0) ** 2) / (
                               ResultData['COUNT(B)'].cumsum(
                                   axis=0) ** 2)) ** 0.5)

    print("차트 생성......")
    s8 = time.time()

    xi = [i for i in range(0, tn, int(tn / xin))]
    if xi[len(xi) - 1] + int(tn / xin) != tn:
        xi.pop(len(xi) - 1)
    xi.append(tn - 1)
    xn = [ResultData['INDEX_DATE'][i] for i in xi]

    plt.rcParams["font.family"] = 'Malgun Gothic'
    plt.rcParams["font.size"] = 10
    plt.rcParams["figure.figsize"] = (16, 8)
    plt.clf()

    ax1 = plt.subplot(2, 1, 1)
    plt.title(' 펀드성과 추이 (펀드코드: ' + fundcd + ')', y=1.05)
    ax1.grid(color='k', linestyle=':', linewidth=0.5)
    plt.plot(ResultData.index, ResultData['SUJ_GIJUN_GA'], label='AP 기준가', color='royalblue', alpha=1, lw=1.5)
    plt.plot(ResultData.index, ResultData['BM_GIJUN_GA'], label='BM 기준가', color='darkseagreen', alpha=1,
             lw=1.6)
    plt.plot(ResultData.index, ResultData['Adj_BM_GIJUN_GA'], label='조정BM 기준가', color='chocolate', alpha=1, lw=1.5)
    plt.xticks(xi, xn)
    yval1 = ax1.get_yticks()
    ax1.set_yticklabels(['{:}'.format(j1) for j1 in yval1])
    ax1.legend(loc=0)

    ax2 = plt.subplot(2, 1, 2)
    ax2.grid(color='k', linestyle=':', linewidth=0.5)
    plt.plot(ResultData.index, ResultData['AP_INDEX_GAP'], label='BM 대비 AP 기준가', color='royalblue', alpha=1, lw=1.5)
    plt.plot(ResultData.index, ResultData['Adj_BM_INDEX_GAP'], label='BM 대비 조정BM 기준가', color='chocolate', alpha=1,
             lw=1.5)
    plt.plot(ResultData.index, ResultData['STANDARD(Z)'], label='Par 성과 기준선', color='darkseagreen', alpha=1, lw=1.6)
    plt.xticks(xi, xn)
    yval2 = ax2.get_yticks()
    ax2.set_yticklabels(['{:,.2%}'.format(j2) for j2 in yval2])
    ax2.legend(loc=0)

    plt.savefig(os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\AdjustBM Scenario_' + fundcd + '_01.png')

    xi = [i for i in range(0, tn, int(tn / xin))]
    if xi[len(xi) - 1] + int(tn / xin) != tn:
        xi.pop(len(xi) - 1)
    xi.append(tn - 1)
    xn = [ResultData['INDEX_DATE'][i] for i in xi]

    plt.rcParams["font.family"] = 'Malgun Gothic'
    plt.rcParams["font.size"] = 10
    plt.rcParams["figure.figsize"] = (16, 8)
    plt.clf()

    ax3 = plt.subplot(2, 1, 1)
    plt.title(' 듀레이션 추이', y=1.05)
    ax3.grid(color='k', linestyle=':', linewidth=0.5)
    plt.plot(ResultData.index, ResultData['FUND_SUJ_DURATION'], label='AP 듀레이션', color='royalblue', alpha=1, lw=1.5)
    plt.plot(ResultData.index, ResultData['BM_DURATION'], label='BM 듀레이션', color='darkseagreen', alpha=1,
             lw=1.6)
    plt.plot(ResultData.index, ResultData['Adj_BM_DURATION'], label='조정BM 듀레이션', color='chocolate', alpha=1, lw=1.5)
    plt.xticks(xi, xn)
    yval3 = ax3.get_yticks()
    ax3.set_yticklabels(['{:,.3}'.format(j3) for j3 in yval3])
    ax3.legend(loc=0)

    ax4 = plt.subplot(2, 1, 2)
    ax41 = ax4.twinx()
    plt.setp(ax4, xticks=xi, xticklabels=xn)
    ax4.grid(color='k', linestyle=':', linewidth=0.5)
    lns4A = ax4.plot(ResultData.index, ResultData['AP_DUR_BET'], label='BM 대비 AP 듀레이션(좌)', color='royalblue', alpha=1,
                     lw=1.5)
    lns4B = ax4.plot(ResultData.index, ResultData['Adj_BM_DUR_BET'], label='BM 대비 조정BM 듀레이션(좌)', color='chocolate',
                     alpha=1,
                     lw=1.5)
    lns4C = ax4.plot(ResultData.index, ResultData['STANDARD(O)'], label='중립 듀레이션 기준선(좌)', color='darkseagreen', alpha=1,
                     lw=1.6)
    lns4D = ax41.plot(ResultData.index, ResultData['RF_YTM'], label='국고채 금리(우)', color='silver', alpha=.75, lw=1)
    plt.xticks(xi, xn)
    yval4 = ax4.get_yticks()
    ax4.set_yticklabels(['{:,.1%}'.format(j4) for j4 in yval4])
    yval41 = ax41.get_yticks()
    ax41.set_yticklabels(['{:,.2%}'.format(j41) for j41 in yval41])
    lns = lns4A + lns4B + lns4C + lns4D
    labs = [l.get_label() for l in lns]
    ax4.legend(lns, labs, loc=0)

    plt.savefig(os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\AdjustBM Scenario_' + fundcd + '_02.png')

    xi = [i for i in range(0, tn, int(tn / xin))]
    if xi[len(xi) - 1] + int(tn / xin) != tn:
        xi.pop(len(xi) - 1)
    xi.append(tn - 1)
    xn = [ResultData['INDEX_DATE'][i] for i in xi]

    plt.rcParams["font.family"] = 'Malgun Gothic'
    plt.rcParams["font.size"] = 10
    plt.rcParams["figure.figsize"] = (16, 8)
    plt.clf()

    ax5 = plt.subplot(2, 1, 1)
    plt.title(' YTM 추이 ', y=1.05)
    ax5.grid(color='k', linestyle=':', linewidth=0.5)
    plt.plot(ResultData.index, ResultData['FUND_YTM'], label='AP YTM', color='royalblue', alpha=1, lw=1.5)
    plt.plot(ResultData.index, ResultData['BM_YTM'], label='BM YTM', color='darkseagreen', alpha=1,
             lw=1.6)
    plt.plot(ResultData.index, ResultData['Adj_BM_YTM'], label='조정BM YTM', color='chocolate', alpha=1, lw=1.5)
    plt.xticks(xi, xn)
    yval5 = ax5.get_yticks()
    ax5.set_yticklabels(['{:,.1%}'.format(j5) for j5 in yval5])
    ax5.legend(loc=0)

    ax6 = plt.subplot(2, 1, 2)
    ax6.grid(color='k', linestyle=':', linewidth=0.5)
    plt.plot(ResultData.index, ResultData['AP_YTM_GAP'], label='BM 대비 AP YTM', color='royalblue', alpha=1, lw=1.5)
    plt.plot(ResultData.index, ResultData['Adj_BM_YTM_GAP'], label='BM 대비 조정BM YTM', color='chocolate', alpha=1,
             lw=1.5)
    plt.plot(ResultData.index, ResultData['STANDARD(Z)'], label='YTM Par 기준선', color='darkseagreen', alpha=1, lw=1.6)
    plt.xticks(xi, xn)
    yval6 = ax6.get_yticks()
    ax6.set_yticklabels(['{:,.2%}'.format(j6) for j6 in yval6])
    ax6.legend(loc=0)

    plt.savefig(os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\AdjustBM Scenario_' + fundcd + '_04.png')

    xi = [i for i in range(0, tn, int(tn / xin))]
    if xi[len(xi) - 1] + int(tn / xin) != tn:
        xi.pop(len(xi) - 1)
    xi.append(tn - 1)
    xn = [ResultData['INDEX_DATE'][i] for i in xi]

    plt.rcParams["font.family"] = 'Malgun Gothic'
    plt.rcParams["font.size"] = 10
    plt.rcParams["figure.figsize"] = (16, 8)
    plt.clf()

    ax7 = plt.subplot(2, 1, 1)
    plt.title('크레딧비중 추이', y=1.05)
    ax7.grid(color='k', linestyle=':', linewidth=0.5)
    plt.plot(ResultData.index, ResultData['AP_CREDIT_WEIGHT'], label='AP 크레딧비중', color='royalblue', alpha=1, lw=1.5)
    plt.plot(ResultData.index, ResultData['BM_CREDIT_WEIGHT'], label='BM 크레딧비중', color='darkseagreen', alpha=1,
             lw=1.6)
    plt.plot(ResultData.index, ResultData['Adj_CREDIT_WEIGHT'], label='조정BM 크레딧비중', color='chocolate', alpha=1,
             lw=1.5)
    plt.xticks(xi, xn)
    yval7 = ax7.get_yticks()
    ax7.set_yticklabels(['{:,.1%}'.format(j7) for j7 in yval7])
    ax7.legend(loc=0)

    ax8 = plt.subplot(2, 1, 2)
    ax81 = ax8.twinx()
    plt.setp(ax8, xticks=xi, xticklabels=xn)
    ax8.grid(color='k', linestyle=':', linewidth=0.5)
    lns8A = ax8.plot(ResultData.index, ResultData['AP_CREDIT_GAP'], label='BM 대비 AP 크레딧비중(좌)', color='royalblue',
                     alpha=1, lw=1.5)
    lns8B = ax8.plot(ResultData.index, ResultData['Adj_BM_CREDIT_GAP'], label='BM 대비 조정BM 크레딧비중(좌)', color='chocolate',
                     alpha=1,
                     lw=1.5)
    lns8C = ax8.plot(ResultData.index, ResultData['STANDARD(O)'], label='BM 크레딧 기준선(좌)', color='darkseagreen', alpha=1,
                     lw=1.6)
    lns8D = ax81.plot(ResultData.index, ResultData['CR_SP'], label='크레딧 스프레드(우)', color='silver', alpha=.75, lw=1)
    plt.xticks(xi, xn)
    yval8 = ax8.get_yticks()
    ax8.set_yticklabels(['{:,.1%}'.format(j8) for j8 in yval8])
    yval81 = ax81.get_yticks()
    ax81.set_yticklabels(['{:,.2%}'.format(j81) for j81 in yval81])
    lns = lns8A + lns8B + lns8C + lns8D
    labs = [l.get_label() for l in lns]
    ax8.legend(lns, labs, loc=0)

    plt.savefig(os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\AdjustBM Scenario_' + fundcd + '_03.png')

    xi = [i for i in range(0, tn, int(tn / xin))]
    if xi[len(xi) - 1] + int(tn / xin) != tn:
        xi.pop(len(xi) - 1)
    xi.append(tn - 1)
    xn = [ResultData['INDEX_DATE'][i] for i in xi]

    plt.rcParams["font.family"] = 'Malgun Gothic'
    plt.rcParams["font.size"] = 10
    plt.rcParams["figure.figsize"] = (16, 8)
    plt.clf()

    ax9 = plt.subplot(2, 1, 1)
    plt.title(' AP 매입YTM, 채권NAV 추이', y=1.05)
    ax91 = ax9.twinx()
    plt.setp(ax9, xticks=xi, xticklabels=xn)
    ax9.grid(color='k', linestyle=':', linewidth=0.5)
    lns9A = ax9.plot(ResultData.index, ResultData['AP_MEIB_RT'], label='AP 매입YTM(좌)', color='royalblue',
                     alpha=1, lw=1.5)
    # lns9B = ax9.plot(ResultData.index, ResultData['AP_CREDIT_MEIB_RT'], label='AP 크래딧물 매입YTM(좌)', color='lightblue',
    #                 alpha=1,
    #                 lw=1.5)
    lns9D = ax91.plot(ResultData.index, ResultData['AP_BOND_PG_AEK'], label='채권NAV', color='silver', alpha=1, lw=1.5)
    plt.xticks(xi, xn)
    yval9 = ax9.get_yticks()
    ax9.set_yticklabels(['{:,.2%}'.format(j9) for j9 in yval9])
    yval91 = ax91.get_yticks()
    ax91.set_yticklabels([format(j91, ',') for j91 in yval91])
    # lns = lns9A + lns9B + lns9D
    lns = lns9A + lns9D
    labs = [l.get_label() for l in lns]
    ax9.legend(lns, labs, loc=0)

    plt.savefig(os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\AdjustBM Scenario_' + fundcd + '_05.png')

    e8 = time.time()
    gap8 = e8 - s8
    print(">>>소요시간:", gap8, "초")

    print("엑셀 저장......")
    s9 = time.time()

    writer = pd.ExcelWriter(os.getcwd() + '\\AdjustBM Scenario_' + fundcd + '\\AdjustBM Scenario_' + fundcd + '.xlsx')
    ResultData.to_excel(writer, 'AdjustBM Scenario')
    writer.save()

    e9 = time.time()
    gap9 = e9 - s9
    print(">>>소요시간:", gap9, "초")

cursor.close()
conn.close()

e0 = time.time()
gap0 = e0 - s0
print(">>>총 소요시간:", gap0, "초")
print("FIN.")
