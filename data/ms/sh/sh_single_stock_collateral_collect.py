#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yexy
# @Time    : 2023/6/9 11:05
# @Site    :
# @Software: PyCharm
# 上海交易所-单一股票担保物比例信息 http://www.sse.com.cn/services/tradingservice/margin/info/single/
import sys
import time
import datetime
import os
import re
import json
import pandas as pd
from configparser import ConfigParser
from selenium.webdriver.common.by import By

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, argv_param_invoke, logger


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '上海交易所'
        self.collect_num_check = False
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'ba17301551dcbaf9_gdp_user_key=; gdp_user_id=gioenc-646ab0c4%2C02g8%2C50aa%2C8g21%2C0'
                      'a947892866e; ba17301551dcbaf9_gdp_session_id_af7181ed-2878-4af9-b284-ddf009f854'
                      '02=true; ba17301551dcbaf9_gdp_session_id_7908213d-370e-4374-b95c-37ef58db8a0e=tru'
                      'e; ba17301551dcbaf9_gdp_session_id_e1a6c330-57c0-4ea5-a1a9-a4104e901221=true; ba1730'
                      '1551dcbaf9_gdp_session_id_536de887-d5dd-4de2-a90d-2f852cb62a91=true; ba17301551dcbaf9_gdp_se'
                      'ssion_id=f9e12b8f-ffab-4049-ab3f-fb33c268c64f; ba17301551dcbaf9_gdp_session_id_f9e12b8f-'
                      'ffab-4049-ab3f-fb33c268c64f=true; ba17301551dcbaf9_gdp_sequence_ids={%22globalKey%22:234%2C'
                      '%22VISIT%22:6%2C%22PAGE%22:11%2C%22VIEW_CLICK%22:217%2C%22VIEW_CHANGE%22:3}',
            'Host': 'query.sse.com.cn',
            'Referer': 'http://www.sse.com.cn/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36'
        }

    def single_stock_collateral_collect(self):
        logger.info(f"开始爬取上海交易所-单一股票担保物比例信息:{datetime.datetime.now()}")
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        self.url = 'http://query.sse.com.cn/commonSoaQuery.do'
        params = {
            "jsonCallBack": "jsonpCallback97363131",
            "sqlId": "COMMON_SSE_JYFW_ZZRQ_JYXX_DYGPDBBL",
            "isPagination": True,
            "pageHelp.pageSize": 1000000,
            "pageHelp.pageNo": 1,
            "pageHelp.beginPage": 1,
            "pageHelp.cacheSize": 1,
            "pageHelp.endPage": 1,
            "tradeDate": trade_date.replace('-', ''),
            "_": 1686277413634

        }
        if isinstance(param_dt, str):
            logger.info(f'进入查询上交所历史单一股票担保物比例信息!param_dt:{param_dt, type(param_dt)}')
            self.biz_dt = param_dt
            params['tradeDate'] = param_dt
        response = self.get_response(self.url, 0, self.headers, params)
        temp_text = response.text
        s = temp_text[temp_text.index('(') +1 :]
        s = s[:-1]
        data_dict = json.loads(s)
        result_list = data_dict['result']
        if result_list:
            total_pages = data_dict['pageHelp']['pageCount']
            self.total_num = data_dict['pageHelp']['total']
            self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(result_list)])
            for i in range(2, total_pages + 1):
                logger.info(f'第{i}页')
                params['pageHelp.pageNo'] = i
                params['pageHelp.beginPage'] = i
                params['pageHelp.endPage'] = i
                response = self.get_response(self.url, 0, self.headers, params)
                temp_text = response.text
                s = temp_text[temp_text.index('(') + 1:]
                s = s[:-1]
                data_dict = json.loads(s)
                result_list = data_dict['result']
                temp_df = pd.DataFrame(result_list)
                self.tmp_df = pd.concat([self.tmp_df, temp_df])
        if not self.tmp_df.empty:
            self.tmp_df.rename(columns={'secScale': 'rate', 'secCode': '证券代码', 'secAbbr': '证券简称', 'tradeDate': '日期'}, inplace=True)
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3


    def get_trade_date(self):
        try:
            driver = self.get_driver()
            url = 'http://www.sse.com.cn/services/tradingservice/margin/info/single/'
            driver.get(url)
            time.sleep(3)
            trade_date = driver.find_elements(By.XPATH, '/html/body/div[8]/div/div[2]/div/div[1]/h2/span')[0].text
            pattern = r'\d{4}-\d{2}-\d{2}'  # 匹配日期的正则表达式
            date = re.findall(pattern, trade_date)[0]  # 提取第一个匹配的日期字符串
            return date
        except Exception as e:
            raise Exception(e)


if __name__ == "__main__":
    argv = sys.argv
    param_dt = None
    if len(argv) == 3:
        param_dt = argv[2]
    argv_param_invoke(CollectHandler(), (7,), sys.argv)
    #CollectHandler().collect_data(7)