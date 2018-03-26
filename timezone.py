#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/2/27 14:30
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : timezone.py
# @Software: PyCharm

import time
import datetime
import pytz


def stamp2time(timestamp, timezone):
    time = datetime.datetime.fromtimestamp(timestamp, pytz.timezone(timezone))
    return time
