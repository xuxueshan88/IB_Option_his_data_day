#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/16 11:15
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : fileio.py
# @Software: PyCharm

for i in range(10):
    fw = open('data.txt', 'a')
    fw.write(str(i)+'\n')
    fw.close()
print('done')