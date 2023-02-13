#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author eagle
# 2023/2/13 12:13
# 北京交易所-融资融券可充抵保证金证券

import os
import sys
import warnings
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '北京交易所'
        self.collect_num_check = False
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'yfx_c_g_u_id_10000042=_ck22060711191911485514723010297; '
                      'VISITED_MENU=%5B%228307%22%2C%229729%22%5D; JSESSIONID=771FCD96DF812328467D7B327B093D35; '
                      'gdp_user_id=gioenc-6e004388%2C3d26%2C59c4%2C838g%2C4063ea3a9528; '
                      'ba17301551dcbaf9_gdp_session_id=4a6d84c6-2cd3-4b35-b7eb-4286992ff745; '
                      'ba17301551dcbaf9_gdp_session_id_4a6d84c6-2cd3-4b35-b7eb-4286992ff745=true; '
                      'yfx_f_l_v_t_10000042=f_t_1654571959111__r_t_1655691628385__v_t_1655692038721__r_c_5',
            'Host': 'query.sse.com.cn',
            'Referer': 'http://www.sse.com.cn/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36'
        }

    def guaranty_securities_collect(self):
        self.url = f"https://www.bse.cn/kcdbzjzqController/export.do?transDate={self.search_date.strftime('%Y-%m-%d')}"
        response = self.get_response(self.url, 0, self.headers)
        warnings.filterwarnings('ignore')
        df = pd.read_excel(response.content, header=0)
        if not df.empty:
            self.total_num = df.index.size
            self.collect_num = self.total_num
            self.data_text = df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2,), sys.argv)
