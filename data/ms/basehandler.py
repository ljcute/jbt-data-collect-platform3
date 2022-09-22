#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/12 13:56
# @Site    : 
# @File    : basehandler.py
import datetime
import json
import os
import sys
import time
import traceback
from configparser import ConfigParser
from utils.exceptions_utils import ProxyTimeOutEx

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

import pandas as pd
import requests
from data.dao import data_deal
from utils.proxy_utils import get_proxies
from utils.logs_utils import logger
from selenium import webdriver
from kafka.producer import KafkaProducer
from constants import *

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
kafkaList = cf.get('kafka', 'kafkaList')
topic = cf.get('kafka', 'topic')
use_proxy = cf.get('proxy-switch', 'use_proxy')
out_cycle = cf.get('cycles', 'out_cycle')
in_cycle = cf.get('cycles', 'in_cycle')


# 数据采集基类
class BaseHandler(object):

    biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券"
        , 4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}

    def __init__(self):
        self.data_source = ''
        self.url = ''
        self.excel_file_path = None
        self.biz_type = 0
        self.collect_num = 0
        self.total_num = 0
        self.data_list = []
        self.biz_dt = datetime.date.today()
        self.start_dt = datetime.datetime.now()
        self.end_dt = self.start_dt
        self.mq_msg = None
        self.search_date = ''

    def collect_data(self, biz_type, search_date=None):
        if int(use_proxy) == 0:
            logger.info(f"{self.data_source}{self.biz_type_map.get(biz_type)}，此次数据采集不使用代理！")
        self.biz_type = biz_type
        self.search_date = search_date if search_date is not None else datetime.date.today()
        query_date = self.start_dt if self.search_date is None else self.search_date
        logger.info(f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集任务开始执行{query_date}')
        max_retry = 0
        while max_retry < int(out_cycle):
            if max_retry > 0:
                logger.info(f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集重试第{max_retry}次')
            try:
                self.proxies = self.get_proxies()
                if biz_type == 0:
                    self.trading_amount_collect()
                elif biz_type == 1:
                    self.trading_items_collect()
                elif biz_type == 2:
                    self.guaranty_securities_collect()
                elif biz_type == 3:
                    self.rzrq_underlying_securities_collect()
                elif biz_type == 4:
                    self.rz_underlying_securities_collect()
                elif biz_type == 5:
                    self.rq_underlying_securities_collect()
                elif biz_type == 99:
                    self.guaranty_and_underlying_securities_collect()
                logger.info(f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集任务执行结束，已采集数据(条)：{self.collect_num}')
                self.end_dt = datetime.datetime.now()
                # 处理采集结果
                self.process_result()
                break
            except Exception as e:
                if max_retry == 2:
                    logger.error(f'{self.data_source}{self.biz_type_map.get(biz_type)}采集任务异常，业务请求次数上限：{out_cycle}，已重试次数{max_retry}，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                time.sleep(30)
            max_retry += 1

    # 交易所交易总量
    def trading_amount_collect(self):
        pass

    # 交易所交易明细
    def trading_items_collect(self):
        pass

    # 融资融券可充抵保证金证券
    def guaranty_securities_collect(self):
        pass

    # 融资融券标的证券
    def rzrq_underlying_securities_collect(self):
        pass

    # 融资标的证券
    def rz_underlying_securities_collect(self):
        pass

    # 融券标的证券
    def rq_underlying_securities_collect(self):
        pass

    # 融资融券可充抵保证金证券和融资融券标的证券
    def guaranty_and_underlying_securities_collect(self):
        pass

    def get_proxies(self):
        """
        use_proxy ==1 表示使用代理 ==0 表示不使用代理
        """
        if int(use_proxy) == 1:
            logger.info(f'{self.data_source}---开始获取代理ip')
            proxies = get_proxies()
            logger.info(f'获取代理ip结束,proxies:{proxies}')
            return proxies
        elif int(use_proxy) == 0:
            return None
        else:
            raise Exception("use_proxy参数有误，请检查")

    def get_response(self, url, request_type, headers=None, params=None, data=None,
                     allow_redirects=None):
        response = None
        max_retry = 0
        while max_retry < int(in_cycle):
            if max_retry > 0:
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}数据采集重试第{max_retry}次')
            try:
                if request_type == 0:
                    response = requests.get(url=url, params=params, proxies=self.proxies, headers=headers,
                                            allow_redirects=allow_redirects, timeout=30)
                elif request_type == 1:
                    response = requests.post(url=url, data=data, proxies=self.proxies, headers=headers, timeout=30)
                elif request_type == 2:
                    response = session.post(url=url, data=data, headers=headers, proxies=self.proxies, timeout=30)
                else:
                    logger.error(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，request_type参数有误')
                if response is None or response.status_code != 200:
                    raise Exception(f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self.proxies}，请求url为:{url},请求参数为:{params}')
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}数据采集: 总记录数(条){self.total_num}, 已采集数(条){self.collect_num}')
                return response if response.status_code == 200 else None
            except ProxyTimeOutEx as es:
                self.proxies = self.get_proxies()
            except Exception as e:
                if max_retry == 4:
                    logger.error(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，单次请求次数上限：{out_cycle}，已重试次数{max_retry}，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                    raise Exception(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url}')
                time.sleep(30)
            max_retry += 1

    def get_driver(self):
        max_retry = 0
        if int(use_proxy) == 1:
            while max_retry < int(in_cycle):
                if max_retry > 0:
                    logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}数据采集重试第{max_retry}次')
                try:
                    # proxies = get_proxies()
                    proxy = self.proxies
                    if proxy is not None:
                        proxy = proxy['http']
                    options = webdriver.FirefoxOptions()
                    options.add_argument("--headless")
                    options.add_argument("--proxy-server={}".format(proxy))
                    driver = webdriver.Firefox(options=options)
                    driver.implicitly_wait(10)
                    return driver
                except Exception as e:
                    logger.warn(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                    if max_retry == 4:
                        logger.error(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                        raise Exception(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url}')
                    time.sleep(3)
                max_retry += 1
        elif int(use_proxy) == 0:
            raise Exception(f"{self.data_source}{self.biz_type_map.get(self.biz_type)}此次数据采集不使用代理,获取driver失败")
        else:
            raise Exception(f"{self.data_source}{self.biz_type_map.get(self.biz_type)}use_proxy参数有误，请检查")

    def process_result(self):
        used_time = (self.end_dt - self.start_dt).seconds
        if 0 < self.collect_num == self.total_num > 0:
            data_status = 1
        else:
            data_status = 2
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---开始进行数据入库')
        self.biz_dt = self.search_date
        data_deal.insert_data_collect(self.collect_num, self.data_deal(), self.biz_dt, self.biz_type,
                                      self.data_source, self.start_dt,
                                      self.end_dt, used_time, self.url, data_status, self.excel_file_path)
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---数据入库结束,共{self.collect_num}条')

        self.kafka_mq_producer()

    def data_deal(self):
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---开始进行数据结构处理')
        try:
            if self.data_list:
                return pd.DataFrame(self.data_list).to_json(orient="records", force_ascii=False)
        except Exception as e:
            raise Exception(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}：{e}')


    def kafka_mq_producer(self):
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---开始发送mq消息')
        # biz_dt = json.dumps(self.biz_dt, cls=ComplexEncoder)
        biz_dt = str(self.biz_dt).replace('-', '')
        producer = KafkaProducer(bootstrap_servers=kafkaList,
                                 key_serializer=lambda k: json.dumps(k).encode(),
                                 value_serializer=lambda v: json.dumps(v).encode())
        msg = {
            "user_id": 1,
            "biz_dt": '20220922',
            "data_type": self.biz_type,
            "data_source": self.data_source,
            "message": self.mq_msg
        }
        future = producer.send(topic, value=str(msg), key=msg['user_id'], partition=0)
        record_metadata = future.get(timeout=30)
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---topic partition:{record_metadata.partition}')
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---发送mq消息结束，消息内容:{msg}')
