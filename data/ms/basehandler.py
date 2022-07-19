#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/12 13:56
# @Site    : 
# @File    : basehandler.py
import json
import os
import sys
from configparser import ConfigParser

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


# 数据采集基类
class BaseHandler(object):

    @classmethod
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
            logger.info("开始获取代理ip......")
            proxies = get_proxies()
            if proxies['http'] is None and proxies['https'] is None:
                logger.error("无可用代理ip，停止采集")
                raise Exception("无可用代理ip，停止采集")
            else:
                logger.info("==================获取代理ip成功!====================")
                return proxies
        elif int(use_proxy) == 0:
            logger.info("此次数据采集不使用代理！")
            return None
        else:
            logger.error("use_proxy参数有误，请检查")
            raise Exception("use_proxy参数有误，请检查")

    @classmethod
    def parsing(cls, response, driver):
        logger.info("开始进行数据解析......")
        pass

    @classmethod
    def get_response(cls, url, proxies, request_type, headers=None, params=None, data=None, allow_redirects=None):
        logger.info("开始获取网页请求......")
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
            logger.error(e)

    @classmethod
    def get_driver(cls):
        logger.info("开始获取webdriver......")
        try:
            if int(use_proxy) == 1:
                proxies = get_proxies()
                if proxies['http'] is None and proxies['https'] is None:
                    logger.error("无可用代理ip，停止采集")
                    raise Exception("无可用代理ip，停止采集")
                else:
                    logger.info("==================获取代理ip成功!====================")
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
                logger.info("此次数据采集不使用代理！")
                raise Exception("此次数据采集不使用代理,获取driver失败")
            else:
                logger.error("use_proxy参数有误，请检查")
                raise Exception("use_proxy参数有误，请检查")

        except Exception as e:
            logger.error(e)

    @classmethod
    def data_deal(cls, data_list, title_list):
        logger.info("开始进行数据处理......")
        try:
            if data_list and title_list:
                data_df = pd.DataFrame(data=data_list, columns=title_list)
                if data_df is not None:
                    df_result = {'columns': title_list, 'data': data_df.values.tolist()}
                    return df_result
            else:
                raise Exception('参数data_list,title_list为空，请检查')
        except Exception as e:
            logger.error(e)

    @classmethod
    def data_insert(cls, record_num, data_info, date, data_type, data_source, start_dt,
                    end_dt, used_time, data_url, excel_file_path=None):
        logger.info("开始进行数据入库......")
        try:
            data_deal.insert_data_collect(record_num, json.dumps(data_info, ensure_ascii=False), date, data_type,
                                          data_source, start_dt,
                                          end_dt, used_time, data_url, excel_file_path)
            logger.info("数据入库结束......")
        except Exception as e:
            logger.error(e)

    @classmethod
    def kafka_mq_producer(cls, biz_dt, data_type, data_source, message):
        logger.info("开始发送mq消息......")
        producer = KafkaProducer(bootstrap_servers=kafkaList, key_serializer=lambda k: json.dumps(k).encode('utf-8'),
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        msg = {'user_id': 960529, 'biz_dt': biz_dt, 'data_type': data_type, 'data_source': data_source,
               'message': message}
        future = producer.send(topic, value=json.dumps(msg), key=msg['user_id'])
        try:
            record_metadata = future.get(timeout=30)
            logger.info(f'topic partition:{record_metadata.partition}')
            logger.info("发送mq消息结束......")
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    s = BaseHandler()
    s.kafka_mq_producer('20220202', '1', '1', '555')
