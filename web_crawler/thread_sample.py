import threading, time
import requests
import datetime as dt
import NTUH_clinic as nc

#門診查詢參數檔案(.CSV file)
ParamaterFileName = "NTUH_params"

#查詢動作的時間間隔
interval = 300 #sec

def query(sess, classname, directory, url, hosp, dept, ampm, querydate):
    bs = nc.BsObject(url, hosp, dept, ampm, querydate, sess)
    soup = bs.getQueryResult()
    df = bs.convertDataToDataFrame(soup)
    nc.exportDataToCSVfile(df, classname, directory, hosp, dept, ampm, querydate)
    sess.close()

def getAmPmFlag():
    #clinic hour:  Morning clinic 09:00~12:00 , Afternoon clinic 14:00~17:00 , Evening clinic 18:30-20:30
    curr = dt.datetime.now()
    am_start = dt.datetime(curr.year,curr.month,curr.day,9,0)
    pm_start = dt.datetime(curr.year,curr.month,curr.day,14,0)
    evn_start = dt.datetime(curr.year,curr.month,curr.day,18,30)
    clinic_end = dt.datetime(curr.year,curr.month,curr.day,23,0) #查詢程式截止時間

    ampm_flag = 0
    if pm_start > curr >= am_start:
        ampm_flag = 1 # Morning clinic
    elif evn_start > curr >= pm_start:
        ampm_flag = 2 # Afternoon clinic
    elif clinic_end > curr >= evn_start:
        ampm_flag = 3 # Evening clinic
    else:
        pass #print("非門診時段")
    return ampm_flag

def demo():
    AmPmFlag = getAmPmFlag()
    #AmPmFlag = 1 #test code
    if AmPmFlag != 0:
        all_param_set = nc.loadParamaterFile(ParamaterFileName)
        #依門診時段取出該時段的查詢條件
        param_set = all_param_set[ all_param_set['ampm'] == str(AmPmFlag) ]
        param_set = param_set.reset_index() #*the index use in for-loop, subset need reset index.
        query_set_nums = len(param_set)
    else:
        query_set_nums = 0 #非門診時段,設查詢條件筆數為0,即不執行查詢動作

    for num in range(query_set_nums):
        sess = requests.Session()
        #print("param_set",param_set)
        t = threading.Thread(target = query, 
                            args = [sess,
                                    param_set.classname[num],
                                    param_set.directory[num],
                                    param_set.url[num],
                                    param_set.hosp[num],
                                    param_set.dept[num],
                                    param_set.ampm[num],
                                    dt.datetime.now().strftime('%Y/%m/%d')])
        t.start()

while True:
    threading.Thread(target=demo).start()
    time.sleep(interval)
