#!/usr/bin/env python
#
# Version: 0.0.1
#
# Objctive: This program will do the followings:
#	1. NTUH clinic current status query.
#	2. web page data convert to CSV files.
#
# Parameters to change:
#       院區: SelectHospDeptAndAMPM:DropListHosp
#            T0:總院區, T2:北護分院, T3:金山分院, T4:新竹分院, T5:竹東分院, Y0:雲林分院
#
#       科部: SelectHospDeptAndAMPM:DropDownDept
#             GERO:老年醫學部, GENE:基因醫學部, ONC:腫瘤醫學部, EOM:環境暨職業醫學部, PC:麻醉部, RAD:影像醫學部
#             NUTR:營飬室, HEMP:血友病中心, CHA:形體美容中心, KBRC:乳房醫學中心, CPC:臨床心理中心, SLP:睡眠中心, CHDH:兒醫
#             MED:內科部, PED:小兒部, DENT:牙科部, SURG:外科部, DERM:皮膚部, ENT:耳鼻喉部, URO:泌尿部, FM:家庭醫學部,
#             NEUR:神經部, ORTH:骨科部, OPH:眼科部, OBGY:婦產部, PMR:復健部, PSYC:精神部
#             ...etc.各院區科部清單不一樣.
#
#       時段: SelectHospDeptAndAMPM:DropDownAMPM
#             1:上午, 2:下午, 3:夜間
#
#       查詢日期: SelectHospDeptAndAMPM:QueryDateText
#                format: yyyy/mm/dd
#
#
# Input Format:
#	           HTTP request Method POST, and query paramaters that using String type.
#
# Requirements:
#               library: pandas, requests, beautifulsoup4
#
# Note:
#      台大醫院-竹東分院 看診進度查詢
#      http://www.chut.ntuh.gov.tw:20280/dashboard/call/call.php
#      clinic hour:  Morning clinic 09:00~12:00 , Afternoon clinic 14:00~17:00 , Evening clinic 18:30-20:30
#
#      台大醫院-總院 查詢主頁 (可查各分院)
#      https://reg.ntuh.gov.tw/WebAdministration/default.aspx
#      POST Request URL: https://reg.ntuh.gov.tw/WebAdministration/ClinicCurrentLightNoByDeptCode.aspx
#
#
# -*- coding: utf8 -*-

import pandas as pd
import requests
import re
import os
import gc
import datetime as dt
from bs4 import BeautifulSoup

hosp_name = "NTUH"  # 台大醫院
hosp_sub_map = {"T0": "總院區", "T2": "北護分院", "T3": "金山分院",
                "T4": "新竹分院", "T5": "竹東分院", "Y0": "雲林分院"}
am_pm_map = {"1": "上午", "2": "下午", "3": "夜間"}

class BsObject(object):
    def __init__(self, QueryURL, Hosp, Dept, AMPM, QueryDate, sess):
        self.url = QueryURL
        self.hosp = Hosp
        self.dept = Dept
        self.ampm = AMPM
        self.querydate = QueryDate
        self.sess = sess

    def getQueryResult(self):
        res = self.sess.get(self.url)
        __VIEWSTATE = re.search(
            b'id="__VIEWSTATE" value="(.*?)"', res.content).group(1)
        __VIEWSTATEGENERATOR = re.search(
            b'id="__VIEWSTATEGENERATOR" value="(.*?)"', res.content).group(1)

        body = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__EVENTVALIDATION": "",
            "SelectHospDeptAndAMPM:DropListHosp": self.hosp,
            "SelectHospDeptAndAMPM:DropDownDept": self.dept,
            "SelectHospDeptAndAMPM:DropDownAMPM": self.ampm,
            "SelectHospDeptAndAMPM:QueryDateText": self.querydate,
            "SelectHospDeptAndAMPM:QueryButton": "查詢"
        }

        res2 = self.sess.post(self.url, data=body)
        __VIEWSTATE = re.search(
            b'id="__VIEWSTATE" value="(.*?)"', res2.content).group(1)
        __VIEWSTATEGENERATOR = re.search(
            b'id="__VIEWSTATEGENERATOR" value="(.*?)"', res2.content).group(1)

        body = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__EVENTVALIDATION": "",
            "SelectHospDeptAndAMPM:DropListHosp": self.hosp,
            "SelectHospDeptAndAMPM:DropDownDept": self.dept,
            "SelectHospDeptAndAMPM:DropDownAMPM": self.ampm,
            "SelectHospDeptAndAMPM:QueryDateText": self.querydate,
            "SelectHospDeptAndAMPM:QueryButton": "查詢"
        }

        html = self.sess.post(self.url, data=body)
        return BeautifulSoup(html.content, 'html.parser')

    def convertDataToDataFrame(self, soup):
        # hospital_name
        hospital_name = hosp_name
        # hospital_subname
        hospital_subname = hosp_sub_map[self.hosp]
        # clinic_name
        clinic_name = []
        for elm in soup.find_all("span", id=re.compile("ClinicName")):
            clinic_name.append(elm.b.string)
        # clinic_no
        clinic_no = []
        for elm in soup.find_all("span", id=re.compile("ClinicNo")):
            clinic_no.append(elm.b.string)
        # dr_name
        dr_name = []
        for elm in soup.find_all("a", id=re.compile("DRName")):
            dr_name.append(elm.b.string)
        # is_close
        is_close = []
        for elm in soup.find_all("span", id=re.compile("isClose")):
            is_close.append(elm.b.string)
        # light_no_show
        light_no_show = []
        for elm in soup.find_all("span", id=re.compile("LightNoShow")):
            light_no_show.append(elm.b.string)
        # light_no_time
        light_no_time = []
        for elm in soup.find_all("span", id=re.compile("LightNoShow")):
            try:
                if elm['title'] is None:  # The variable
                    light_no_time.append("")
            except KeyError:
                light_no_time.append("--:--")
            else:
                light_no_time.append(elm['title'])
        # ampm
        am_pm = am_pm_map[self.ampm]
        # timestamp
        timestamp = [dt.datetime.now()] * len(clinic_no)
        # remark1
        remark1 = []
        for elm in soup.find_all("span", id=re.compile("Remark1")):
            remark1.append(elm.b.string)
        # remark2
        remark2 = ""
        # remark3
        remark3 = ""

        column_name = ["hospital_name", "hospital_subname", "clinic_name", "clinic_no", "dr_name", "is_close", "light_no_show",
                       "light_no_time", "am_pm", "timestamp", "remark1", "remark2", "remark3"]
        query_table = pd.DataFrame(
            {'hospital_name': hospital_name,
             'hospital_subname': hospital_subname,
             'clinic_name': clinic_name,
             'clinic_no': clinic_no,
             'dr_name': dr_name,
             'is_close': is_close,
             'light_no_show': light_no_show,
             'light_no_time': light_no_time,
             'am_pm': am_pm,
             'timestamp': timestamp,
             'remark1': remark1,
             'remark2': remark2,
             'remark3': remark3
             })
        query_table = query_table[column_name]  # set columns order
        return query_table


def exportDataToCSVfile(df, classname, directory, hosp, dept, ampm, querydate):
    # export DataFrame to CSV file, format: [classname]_[hosp]_[dept]_[ampm]_[querydate]_[export_timestamp].csv
    if not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv('{}/{}_{}_{}_{}_{}_{}.csv'.format(directory, classname, hosp, dept, ampm, querydate.replace("/", ""),
                                                dt.datetime.now().strftime('%Y%m%d%H%M%S')), index=False, encoding="utf-8")


def loadParamaterFile(filename):
    # load paramater file (BsObject request paramaters)
    params = pd.read_csv('{}.csv'.format(filename), dtype=str)
    params = params.dropna(
        subset=['classname', 'directory', 'url', 'hosp', 'dept', 'ampm'])
    return params


def main():
    params = loadParamaterFile("NTUH_params")
    #print("Query paramaters:")
    # print(params)
    # class test paramaters:
    ClassName = params.classname[0]
    Directory = params.directory[0]
    QueryURL = params.url[0]
    Hosp = params.hosp[0]
    Dept = params.dept[0]
    AMPM = params.ampm[0]
    QueryDate = dt.datetime.now().strftime('%Y/%m/%d')  # "2017/10/27"
    sess = requests.Session()

    # class test code:
    #print("BsObject:",QueryURL, Hosp, Dept, AMPM, QueryDate)
    bs = BsObject(QueryURL, Hosp, Dept, AMPM, QueryDate, sess)
    soup = bs.getQueryResult()
    df = bs.convertDataToDataFrame(soup)
    sess.close()
    # print(df)
    exportDataToCSVfile(df, ClassName, Directory, Hosp, Dept, AMPM, QueryDate)

    del params
    del bs
    del soup
    del df
    gc.collect()


if __name__ == '__main__':
    main()
