#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/2 14:19
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : ttttt.py
# @Software: PyCharm

from ibapi.enum_implem import Enum

TickTypeEnum = Enum("BID_SIZE",
                "BID",
                "ASK",
                "ASK_SIZE",
                "LAST",
                "LAST_SIZE")

print('Done')