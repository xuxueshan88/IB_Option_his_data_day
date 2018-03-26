#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/16 14:25
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : Testtt.py
# @Software: PyCharm

import pandas as pd

a = pd.read_csv(r'F:\Github Repository\IB_Opt_his_data\option_code_map.csv')
b=a['option_code'].values.tolist()

print('done')
