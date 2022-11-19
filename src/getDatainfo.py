from dateutil.parser import *
from dateutil.relativedelta import *
from dateutil.rrule import *
import warnings
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
warnings.simplefilter(action='ignore', category=FutureWarning)  # FutureWaring 제거

opendf = pd.read_csv('openDate.csv', index_col='index')  # 2002-09-13~2022-11-07까지의 개장일 csv파일
opendf['Opendate'] = pd.to_datetime(opendf['Opendate'], format='%Y-%m-%d', errors='raise')  # 원소를 datetime타입으로 변경
datetimeList = []
for date in opendf['Opendate']:
    date = pd.Timestamp(date).strftime('%Y-%m-%d')
    datetimeList.append(datetime.strptime(date, '%Y-%m-%d'))

def getPayInDateInfo(start_date, end_date, month_type):  # 납입일 계산 (월초: 0, 월말: 1)

    rtList = []

    if month_type == '0':
        a = list(rrule(MONTHLY,
                       byweekday=(MO, TU, WE, TH, FR),
                       bysetpos=1,
                       dtstart=parse(start_date),
                       until=parse(end_date)))  # 지정된 기간의 매월 첫 평일

        for day in a:
            while 1: # 개장일 까지
                if day not in datetimeList:  # 개장일이 아니면
                    day = day + timedelta(days=1) # 하루 +
                else: # 개장일인 경우 빠져나감
                    break
            rtList.append(day.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환

    elif month_type == '1':
        a = list(rrule(MONTHLY,
                       byweekday=(MO, TU, WE, TH, FR),
                       bysetpos=-1,
                       dtstart=parse(start_date),
                       until=parse(end_date)))  # 지정된 기간의 매월 마지막 평일

        for day in a:
            while 1:  # 개장일 까지
                if day not in datetimeList:  # 개장일이 아니면
                    day = day + timedelta(days=-1)  # 하루 +
                else:  # 개장일인 경우 빠져나감
                    break
            rtList.append(day.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환

    return rtList  # 납입 예정일 리스트 출력


def getDailyDateInfo(start_date, end_date):
    rtList = []

    for day in opendf['Opendate'][start_date:end_date]:
        rtList.append(pd.Timestamp(day).strftime('%Y-%m-%d'))

    return rtList


def getYearlyDateInfo(start_date, end_date):
    rtList = []

    a = list(rrule(YEARLY,
                   byweekday=(MO, TU, WE, TH, FR),
                   bysetpos=1,
                   dtstart=parse(start_date),
                   until=parse(end_date)))  # 지정된 기간의 매월 첫 평일

    for day in a:
        while 1:
            if day not in datetimeList:  # 개장일에 포함되어 있으면
                day = day + timedelta(days=1)
            else:
                break
        rtList.append(day.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환

    return rtList


def getRebalanceDateInfo(start_date, end_date, month_type, interval):  # 리밸런싱 날짜 계산 (월초 or 월말)
    rtList = []  # 반환할 리스트

    sd = datetime.strptime(start_date, '%Y-%m-%d')  # 시작날짜 저장

    # month_type 0: 월초, 1: 월말
    a = []  # 월초/월말 날짜 저장할 리스트

    if month_type == '0':
        a = list(rrule(MONTHLY,
                       interval=interval,
                       byweekday=(MO, TU, WE, TH, FR),
                       bysetpos=1,
                       dtstart=parse(start_date),
                       until=parse(end_date)))  # 지정된 기간의 매월 첫 평일 (월초)

        for day in a:
            while 1:
                if day not in datetimeList:  # 개장일에 포함되어 있으면
                    day = day + timedelta(days=1)
                else:
                    break

            rtList.append(day.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환

        if sd in datetimeList:  # 시작날짜가 개장일이라면
            rtList.insert(0, sd.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환
        else:  # 시작 날짜가 개장일이 아니면
            while 1:
               if sd not in datetimeList:  # 개장일에 포함되어 있으면
                   sd = sd + timedelta(days=1)
               else:
                   break
            rtList.insert(0, sd.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환



    if month_type == '1':
        a = list(rrule(MONTHLY,
                       interval=interval,
                       byweekday=(MO, TU, WE, TH, FR),
                       bysetpos=-1,
                       dtstart=parse(start_date),
                       until=parse(end_date)))  # 지정된 기간의 매월 첫 평일 (월초)

        if sd in datetimeList:  # 시작날짜가 개장일이라면
            rtList.insert(0, sd.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환
        else:  # 시작 날짜가 개장일이 아니면
            while 1:
               if sd not in datetimeList:  # 개장일에 포함되어 있으면
                   sd = sd + timedelta(days=1)
               else:
                   break
            rtList.insert(0, sd.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환

        for day in a:
            while 1:
                if day not in datetimeList:  # 개장일에 포함되어 있으면
                    day = day + timedelta(days=-1)
                else:
                    break
            rtList.append(day.strftime('%Y-%m-%d'))  # yyyy-mm-dd 형식 변환

    rt = [] #중복 제거
    for d in rtList:
        if d not in rt:
            rt.append(d)

    return rt  # 납입 예정일 리스트 출력


# print(getDailyDateInfo('2022-01-01', '2022-11-07'))
# print(getYearlyDateInfo('2020-01-01', '2022-11-07'))
# print(getPayInDateInfo('2020-01-01', '2022-09-07', '1'))
# print(getRebalanceDateInfo('2017-10-11', '2018-05-01', '1', 3))