#!/usr/bin/env python
# -*- coding: utf8 -*-
import os

# 查詢指定路徑內的所有資料匣及檔案(以List回傳)
input_path = "download/"

files = []
for _root, _dirs, _files in os.walk(input_path):
    files = _files

# read CSV file by date_flag
import pandas as pd

date_flag = "20180131"

column_name = ["hospital_name", "hospital_subname", "clinic_name", "clinic_no", "dr_name", "is_close", "light_no_show",
               "light_no_time", "am_pm", "timestamp", "remark1", "remark2", "remark3"]

df = pd.DataFrame(columns=column_name)

for fn in files:
    if "_{}_".format(date_flag) in fn:
        tmp = pd.read_csv(input_path + fn)
        df = df.append(tmp, ignore_index=True)

# write daily summary file with date_flag name
import datetime as dt
output_path = "daily_summmary/"

if not os.path.exists(output_path):
    os.makedirs(output_path)

df.to_csv(output_path + '{}.csv'.format(date_flag),
          index=False, encoding="utf-8")
