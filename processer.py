#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/14 12:12
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : processer.py
# @Software: PyCharm

import time
import random
from threading import Thread
from ContractSamples import ContractSamples
from date_list import *
from datetime import timedelta
from stock_HeadTimestamp import *
from stock_code_define import *
import pandas as pd
from pymongo import MongoClient
from pymongo import (DESCENDING, ASCENDING)


class Processer(Thread):
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.stock_code_map = stock_code_map

    def run(self):
        if self.client.started:
            return

        self.client.started = True

        if self.client.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.client.reqGlobalCancel()
        else:
            print("Executing requests")
            # self.historicalDataRequests_req_HeadTimestamp()
            # self.historicalDataRequests_req_Seconds()
            # self.historicalDataRequests_req_Days()
            # self.optionsOperations_req()
            self.option_day_req()
            # self.mktData_req_opt()
            # self.historicalDataRequests_req_opt_Seconds()

            print("Executing requests ... finished")

    def historicalDataRequests_req_HeadTimestamp(self):
        # Requesting historical data
        # ! [reqHeadTimeStamp]
        # for index in self.stock_code_map.keys():
        for index in range(1079,len(stock_code_list)+1):
            if self.client.process_done:
                break
            else:
                stock_code = self.stock_code_map[index]
                print('Start to get', stock_code, str(index))
                self.client.reqHeadTimeStamp(index, ContractSamples.OptionWithLocalSymbol(stock_code), "TRADES", 0, 1)
                time.sleep(10)
                print('Finish query', stock_code)

    def historicalDataRequests_req_Seconds(self):
        flag = 0
        for queryTime in date_list:
            if flag == 1:
                queryTime += timedelta(hours=16)
            else:
                queryTime = datetime.datetime(2017,12,8,10,30)
                flag = 1

            if self.client.process_done:
                break

            while queryTime.hour > 9:
                for index in self.stock_code_map.keys():
                    if self.client.process_done:
                        break
                    else:
                        stock_code = self.stock_code_map[index]
                        print('Start to get', stock_code, str(index), str(queryTime))
                        self.client.reqHistoricalData(index, ContractSamples.USStockAtSmart(stock_code),
                                               queryTime.strftime("%Y%m%d %H:%M:%S"),
                                               "1800 S", "1 secs", "TRADES", 1, 1, False, [])
                        time.sleep(10)
                        print('Finish query', stock_code + str(queryTime))
#                print(xx)
                queryTime -= timedelta(seconds=1800)

        print('request done')

    def historicalDataRequests_req_Days(self):
        queryTime = datetime.datetime(2016, 12, 25)
        while queryTime > datetime.datetime(1980, 1, 1):
            for index in self.stock_code_map.keys():
                if self.client.process_done:
                    break
                stock_code = self.stock_code_map[index]
                headTimestamp = datetime.datetime.strptime(stock_HeadTimestamp[stock_code], '%Y/%m/%d')
                if queryTime > headTimestamp:
                    print('Start to get', stock_code, str(index), str(queryTime))
                    self.client.reqHistoricalData(index, ContractSamples.USStockAtSmart(stock_code),
                                                  queryTime.strftime("%Y%m%d %H:%M:%S"),
                                                  "1 Y", "1 day", "TRADES", 1, 1, False, [])
                    time.sleep(10)
                    print('Finish query', stock_code + str(queryTime))
                    #                print(xx)
            queryTime -= timedelta(days=360)

        print('request done')

    def optionsOperations_req(self):
        # ! [reqsecdefoptparams]
        # self.client.reqSecDefOptParams(1, "IBM", "", "STK", 8314)
        # self.client.reqContractDetails(210, ContractSamples.OptionForQuery())
        # self.client.reqMktData(1002, ContractSamples.USStockAtSmart('AAPL'), "", False, False, []) #OptionForQuery()
        # self.client.reqHistoricalData(1, ContractSamples.OptionForQuery(),
        #                               "20180224 16:00:00", "900 S", "1 secs", "TRADES", 1, 1, False, [])

        # self.client.reqHistoricalTicks(0, ContractSamples.OptionWithLocalSymbol("AAPL  180420C00180000"),
        #                         "20171124 09:30:00", "", 1000, "TRADES", 1, True, [])
        # self.opt_tick_req_single_code(0, "AAPL  180420C00180000", datetime.datetime(2017,11,22,9,30))
        # self.client.reqHeadTimeStamp(1, ContractSamples.OptionWithLocalSymbol("AAPL  190118C00135000"), "TRADES", 0, 1)
        self.client.reqHistoricalData(1, ContractSamples.OptionWithLocalSymbol("AAPL  180420C00180000"),"20180319 23:59:59",
                                      "5 D", "1 min", "TRADES", 1, 1, False, [])

        # self.client.reqMktData(1000, ContractSamples.OptionWithLocalSymbol("AAPL  180420C00180000"), "100,101,104,106,233,236,258", False, False, [])
        # ! [reqsecdefoptparams]

        print('request done!!')

    def historicalDataRequests_req_opt_Seconds(self):
        for index in self.stock_code_map.keys():
            queryTime = datetime.datetime(2018, 1, 16, 16, 00)
            endday = queryTime - timedelta(days=180)

            while queryTime > endday:
                if self.client.process_done:
                    break
                else:
                    stock_code = self.stock_code_map[index]
                    print('Start to get', stock_code, str(index), str(queryTime))
                    self.client.reqHistoricalData(index, ContractSamples.OptionWithLocalSymbol(stock_code),
                                           queryTime.strftime("%Y%m%d %H:%M:%S"),
                                           "1800 S", "1 secs", "TRADES", 1, 1, False, [])
                    time.sleep(11)
                    print('Finish query', stock_code + str(queryTime))

                if queryTime.hour == 10 and queryTime.minute == 0:
                    if queryTime.isoweekday() == 1:
                        queryTime -= timedelta(days=2,hours=18)
                    else:
                        queryTime -= timedelta(hours=18)
                else:
                    queryTime -= timedelta(seconds=1800)

        print('request done')

    def mktData_req_opt(self):
        for index in self.stock_code_map.keys():
            stock_code = self.stock_code_map[index]
            print('Start to get', stock_code, str(index))
            self.client.reqMktData(index, ContractSamples.OptionWithLocalSymbol(stock_code), "100,101,104,106,233,236,258", False, False, [])
            print('Finish query', stock_code)

        print('request done')

    def option_day_req(self):
        client = MongoClient('127.0.0.1', 27017)
        my_db = client.option_data_us_day
        index_continue = -1
        if index_continue == -1:
            for index in self.stock_code_map.keys():
                self.client.next_contract = False
                if self.client.process_done:
                    break
                else:
                    stock_code = self.stock_code_map[index]
                    my_db[stock_code].create_index([('date', ASCENDING), ('option_code', ASCENDING)])
                    self.client.reqContractDetails(index, ContractSamples.OptionForQuery(stock_code))
                    print('Asked for ', str(stock_code))

                    while not self.client.next_contract and not self.client.process_done:
                        time.sleep(5)
                        print('waiting for contract', str(stock_code))
            print(self.client.option_code_map)
        else:
            temp = pd.read_csv('option_code_map.csv')
            self.client.option_code_map = temp['option_code'].values.tolist()

        if index_continue < 0:
            index_continue = 0
        option_code_map = self.client.option_code_map

        queryTime = datetime.datetime(2018, 3, 27, 0, 0)
        for index in range(index_continue, len(option_code_map)):
            if self.client.process_done:
                break
            order_id = index * 100000
            option_code = option_code_map[index]
            time_recorder = 0
            self.client.queryTime = queryTime.strftime("%Y%m%d %H:%M:%S")
            self.client.reqHistoricalData(order_id, ContractSamples.OptionWithLocalSymbol(option_code),
                                          queryTime.strftime("%Y%m%d"), "6 M", "1 day", "TRADES", 1, 1,
                                          False, [])
            time.sleep(random.randint(8, 12))

            while not self.client.process_done:
                if self.client.opt_req_next_code:
                    self.client.opt_req_next_code = False
                    print('next code...............')
                    break
                else:
                    time.sleep(1)
                    time_recorder += 1
                    if time_recorder > 120:  # 2分钟内没有返回内容，则跳过
                        fw = open('data.txt', 'a')
                        fw.write(option_code + ', ' + str(order_id) + ', ' + str(queryTime) + '\n')
                        fw.close()
                        self.client.opt_req_next_code = True
                        # break
                    print(datetime.datetime.now()," ", order_id, ' sleeping.................')