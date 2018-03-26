#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/14 15:02
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : option_code_timestamp.py
# @Software: PyCharm

from pymongo import MongoClient
from pymongo import (DESCENDING, ASCENDING)
import pandas as pd

client = MongoClient('127.0.0.1', 27017)
my_db = client.option_data_us_tick
my_col = my_db.HeadTimestamps

option_timestamp = pd.DataFrame(list(my_col.find({},{'_id':0,'update_time':0})))
option_timestamp['headTimestamp'][option_timestamp['headTimestamp']=="2922690551202  16:47:04"]="20171122  16:00:00"
option_timestamp.to_csv('option_code_timestamp.csv')

# my_col = my_db.AAPL
# #searching for the start time of existed data
# option_code = my_col.find({},{'_id': 0, 'option_code':1})
# option_code_list = [item['option_code'] for item in option_code]
# option_code = list(set(option_code_list))
# option_start = []
# for item in option_code:
#     a = my_col.find({'option_code':item}).sort('time',1)
#     time = a.next()['time'].replace(hour=9,minute=30,second=0)
#     option_start.append(time.strftime("%Y%m%d %H:%M:%S"))
#
# option_code_start = pd.DataFrame({'option_code':option_code, 'option_start':option_start})
# option_code_start.to_csv('option_code_start.csv')
print('done')
