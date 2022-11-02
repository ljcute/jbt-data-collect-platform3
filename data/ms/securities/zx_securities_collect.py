#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/18 10:09
import os
import sys
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, logger, random, USER_AGENTS


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中信证券'

    def _securities_collect(self):
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '42',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'kong.citics.com',
            'Origin': 'https://pb.citics.com',
            'Referer': 'https://pb.citics.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
        }
        curr_page = 1
        page_size = 29
        while True:
            logger.info(f'当前为第{curr_page}页')
            data = {
                'pageSize': page_size,
                'currPage': curr_page,
                'searchDate': self.search_date.strftime('%Y%m%d')
            }
            response = self.get_response(self.url, 1, headers, None, data)
            text = json.loads(response.text)
            if curr_page == 1 and text['errorCode'] == '100008':
                logger.info(f'该日{self.search_date}为非交易日,无相应数据')
                self.collect_num_check = False
                self.data_status = 3
                return
            elif text['errorCode'] == '0':
                self.total_num = int(text['data']['totalRecord'])
                page_size = int(text['data']['pageSize'])
                curr_page = int(text['data']['page'])
                total_page = int(self.total_num / page_size) + 1
                df = pd.DataFrame(text['data']['data'])
                self.tmp_df = pd.concat([self.tmp_df, df])
                self.collect_num += df.index.size
            else:
                logger.error(f"需要跟进的异常情况")
            curr_page += 1
            if curr_page > total_page:
                self.data_text = self.tmp_df.to_csv(index=False)
                return

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://kong.citics.com/pub/api/v1/website/rzrq/rzrqObjects'
        self._securities_collect()

    def guaranty_securities_collect(self):
        self.url = 'https://kong.citics.com/pub/api/v1/website/rzrq/punching'
        self._securities_collect()


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((2, 3), sys.argv)
