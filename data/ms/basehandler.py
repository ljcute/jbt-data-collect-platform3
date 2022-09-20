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

from kafka.errors import kafka_errors

from utils.exceptions_utils import ProxyTimeOutEx

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

import pandas as pd
import requests
from constants import get_headers
from data.dao import data_deal
from utils.proxy_utils import get_proxies
from utils.logs_utils import logger
from selenium import webdriver
from kafka.producer import KafkaProducer

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
kafkaList = cf.get('kafka', 'kafkaList')
topic = cf.get('kafka', 'topic')
use_proxy = cf.get('proxy-switch', 'use_proxy')
biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券"
    , 4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}


# 数据采集基类
class BaseHandler(object):

    def __init__(self, data_source, url):
        self.data_source = data_source
        self.url = url

    def collect_data(self, biz_type):
        actual_date = datetime.date.today()
        logger.info(f'{self.data_source}{biz_type_map.get(biz_type)}数据采集任务开始执行{actual_date}')
        max_retry = 0
        while max_retry < 5:
            logger.info(f'{self.data_source}{biz_type_map.get(biz_type)}数据采集重试第{max_retry}次')
            try:
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

                logger.info(f'{self.data_source}{biz_type_map.get(biz_type)}数据采集任务执行结束')
                break
            except ProxyTimeOutEx as es:
                pass
            except Exception as e:
                time.sleep(3)
                logger.error(f'{self.data_source}{biz_type_map.get(biz_type)}采集任务异常，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')

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

    def handle_request(cls, deal_type, request_type, url, headers=None, params=None, data=None, **kwargs):
        """
        网页请求处理
        :param deal_type: <int> 处理方式，0为接口请求方式，1为webdriver方式
        :param request_type: <int> 请求方式，0为get请求，1为post请求
        :param url: <str> 请求网页我url
        :param headers: <str> 请求网页的header头信息
        :param params: <str> 请求网页的参数
        :param data: <str> 请求网页的参数(post方式)
        """
        logger.info("开始获取代理ip......")
        headers = get_headers() if headers is None else headers
        proxies = get_proxies()
        if proxies['http'] is None and proxies['https'] is None:
            logger.error("无可用代理ip，停止采集")
            raise Exception("无可用代理ip，停止采集")
        else:
            logger.info("获取代理ip成功!")

        if deal_type == 0:
            return cls.get_response(url, params, data, proxies, headers, request_type)
        elif deal_type == 1:
            return cls.get_driver()

    @classmethod
    def get_proxies(cls):
        """
        use_proxy ==1 表示使用代理 ==0 表示不使用代理
        """
        if int(use_proxy) == 1:
            logger.info("开始获取代理ip")
            proxies = get_proxies()
            logger.info(f'获取代理ip结束,proxies:{proxies}')
            return proxies
        elif int(use_proxy) == 0:
            logger.info("此次数据采集不使用代理！")
            return None
        else:
            raise Exception("use_proxy参数有误，请检查")

    @classmethod
    def parsing(cls, response, driver):
        logger.info("开始进行数据解析......")
        pass

    @classmethod
    def get_response(cls, data_source, url, proxies, request_type, headers=None, params=None, data=None, allow_redirects=None):
        # logger.info("开始获取网页请求......")
        try:
            response = None
            if request_type == 0:
                response = requests.get(url=url, params=params, proxies=proxies, headers=headers,
                                        allow_redirects=allow_redirects, timeout=10)
            elif request_type == 1:
                response = requests.post(url=url, data=data, proxies=proxies, headers=headers, timeout=10)
            else:
                logger.error("request_type参数有误！")
            return response if response.status_code == 200 else None
        except Exception as e:
            logger.error(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params,data},具体异常信息为:{e}')

    @classmethod
    def get_driver(cls):
        logger.info("开始获取webdriver......")
        if int(use_proxy) == 1:
            proxies = get_proxies()
            logger.info(f'==================获取代理ip成功!,proxies:{proxies}====================')
            proxy = proxies
            if proxy is not None:
                proxy = proxy['http']
            options = webdriver.FirefoxOptions()
            options.add_argument("--headless")
            options.add_argument("--proxy-server={}".format(proxy))
            driver = webdriver.Firefox(options=options)
            driver.implicitly_wait(10)
            logger.info("==================webdriver走代理ip成功!====================")
            return driver
        elif int(use_proxy) == 0:
            raise Exception("此次数据采集不使用代理,获取driver失败")
        else:
            raise Exception("use_proxy参数有误，请检查")

    @classmethod
    def data_deal(cls, data_list, title_list):
        logger.info("开始进行数据处理......")
        try:
            if data_list and title_list:
                data_df = pd.DataFrame(data=data_list, columns=title_list)
                if data_df is not None:
                    df_result = {'columns': title_list, 'data': data_df.values.tolist()}
                    # logger.info(f'df_result:{df_result}')
                    return df_result
        except Exception as e:
            raise Exception(e)

    @classmethod
    def data_insert(cls, record_num, data_info, date, data_type, data_source, start_dt,
                    end_dt, used_time, data_url, data_status, excel_file_path=None):
        logger.info("开始进行数据入库......")
        data_deal.insert_data_collect(record_num, json.dumps(data_info, ensure_ascii=False), date, data_type,
                                      data_source, start_dt,
                                      end_dt, used_time, data_url, data_status, excel_file_path)
        logger.info("数据入库结束......")

    @classmethod
    def kafka_mq_producer(cls, biz_dt, data_type, data_source, message):

        logger.info("开始发送mq消息......")
        producer = KafkaProducer(bootstrap_servers=kafkaList,
                                 key_serializer=lambda k: json.dumps(k).encode(),
                                 value_serializer=lambda v: json.dumps(v).encode())

        msg = {
            "user_id": 960529,
            "biz_dt": biz_dt,
            "data_type": data_type,
            "data_source": data_source,
            "message": message
        }

        future = producer.send(topic, value=str(msg), key=msg['user_id'], partition=0)
        record_metadata = future.get(timeout=30)
        logger.info(f'topic partition:{record_metadata.partition}')
        logger.info("发送mq消息结束......")


if __name__ == '__main__':
    s = BaseHandler()
    # s.kafka_mq_producer('20220725', '3', '华泰证券', 'ht_securities_collect')
    s.kafka_mq_producer('2022-09-19', '4', '上海交易所', 'sh_exchange_mt_underlying_and_guaranty_security')
