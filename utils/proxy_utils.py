#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author lbj
# 2021/6/18 16:11
import os
import time
from configparser import ConfigParser

import requests

from utils.exceptions_utils import ProxyTimeOutEx
from utils.logs_utils import logger
from utils.db_utils import cf
import json

app_id = 'jbt-data-collect-platform'  # 本系统id
get_ip_url = cf.get('http-proxy-java-service', 'get-ip')
expire_ip_url = cf.get('http-proxy-java-service', 'expire-ip')
proxy_switch = cf.get('http-proxy-java-service', 'switch')
none_proxy = {'http': None, 'https': None}

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
proxy_retry_time = cf.get('proxy', 'proxy_retry_time')
proxy_sleep_time = cf.get('proxy', 'proxy_sleep_time')


def get_proxies(data_type=1, retry_count=3):
    if retry_count <= 0 or proxy_switch == 'N':
        return none_proxy

    this_method_retry_count = int(proxy_retry_time)
    while this_method_retry_count > 0:
        params = {"appId": app_id, "interfaceId": data_type}
        response = requests.get(url=get_ip_url, params=params, timeout=3)
        if response.status_code != 200:
            raise Exception(f'ip代理服务:{get_ip_url}异常，无法获取代理ip!')
        text = json.loads(response.text)
        if text['code'] == '-1':
            return none_proxy

        data = text['data']
        ip = data['ip']
        port = data['port']
        time.sleep(int(proxy_sleep_time))
        if check_proxy_ip_valid(ip, port):  # 从java服务拿到ip再校验一次是否可用
            return create_proxies(ip, port)
        else:
            this_method_retry_count = this_method_retry_count - 1
            continue
    return none_proxy


def create_proxies(ip, port):
    # 代理服务器
    proxy_post = ip
    proxy_port = port
    proxy_meta = "http://%(host)s:%(port)s" % {
        "host": proxy_post,
        "port": proxy_port,
    }
    proxies = {
        "http": proxy_meta,
        "https": proxy_meta
    }
    logger.info(f'获取到的可用ip为:{proxies}')
    return proxies


def judge_proxy_is_fail(exception, url):
    str_e = str(exception)
    if "Caused by SSLError" in str_e or "Cannot connect to proxy" in str_e:
        logger.info("失效ip:{}".format(url))
        url = url.replace('http://', '').split(':')
        ip = url[0]
        expire_ip(ip)
    return True


def expire_ip(ip):
    requests.post(url=expire_ip_url, params={"ip": ip})


def check_proxy_ip_valid(ip, port):
    proxies = create_proxies(ip, port)
    # response = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=10)     # 国外ip，有些代理商不给访问
    try:
        response = requests.get("http://www.baidu.com", proxies=proxies, timeout=10)
        logger.info(f"IP百度测试结果{response.status_code}")
        return True if int(response.status_code) == 200 else False
    except Exception as es:
        raise ProxyTimeOutEx


if __name__ == "__main__":
    # str_ = "2021-06-18 16:23:40"
    # string转时间
    # dt = datetime.datetime.strptime(str_, '%Y-%m-%d %H:%M:%S')
    # # 获取当前时间
    # n_time = datetime.datetime.now()
    # print(n_time.__gt__(dt))

    # get_proxies()
    # print(check_proxy_ip_valid("117.10.187.192", 32947))
    # proxies222 = get_proxies(1, 3)
    # print(proxies222)
    get_proxies()
