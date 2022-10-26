#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
import time
import os
import sys
import warnings
import pandas as pd
from urllib import parse
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '兴业证券'
        self.url = "https://www.xyzq.com.cn/xysec/"
        self.target_excel_name = '兴业证券融资融券标的证券及保证金比例明细表'
        self.guaranty_excel_name = '兴业证券融资融券可充抵保证金证券及折算率明细表'

    def rzrq_underlying_securities_collect(self):
        title_url = "https://www.xyzq.com.cn/xysec/biz/11116"
        self._securities_collect(title_url, self.target_excel_name, 'bd')

    def guaranty_securities_collect(self):
        title_url = "https://www.xyzq.com.cn/xysec/biz/11117"
        self._securities_collect(title_url, self.guaranty_excel_name, 'db')

    def _securities_collect(self, title_url, word, biz_type):
        data = {'word': word, "cur": 1}
        encode_data = parse.urlencode(data)
        response = self.get_response(title_url, 0, get_headers(), encode_data)
        soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
        # 此处直接查询'.newsBox'这个元素才能获取正确的url以及后续数据
        label_div_list = soup.select('.newsBox')
        if label_div_list:
            label_div = label_div_list[0]
            my_id = label_div['myid']
            my_path = label_div['mypath']

            label_span_list = soup.select('.newsBox span')
            sc_newest_date = label_span_list[1].text.strip() + label_span_list[0].text.strip().replace('-', "")
            full_download_page_url = self.url + my_path + my_id
            response = self.get_response(full_download_page_url, 0, get_headers(), encode_data)
            soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')

            label_aa_list = soup.select("div .fujian a")
            if label_aa_list:
                # download_url = label_aa_list[0]['href']
                download_url = label_aa_list[0]['myurl']  # 兴业改了html,20210817
                response = self.get_response(download_url, 0, get_headers())
                df = pd.read_excel(response.content, header=0)
                if biz_type == 'db':
                    df_sh = df[['序号', '证券代码', '证券简称', '折算率']]
                    df_sh.dropna(axis=0, how='all', inplace=True)
                    df_sh['exchange'] = 'sh'
                    df_sh['序号'] = df_sh['序号'].apply(lambda x: int(x))
                    df_sh['证券代码'] = df_sh['证券代码'].apply(lambda x: int(x))
                    df_sz = df[['序号.1', '证券代码.1', '证券简称.1', '折算率.1']]
                    df_sz.rename(columns={'序号.1': '序号', '证券代码.1': '证券代码', '证券简称.1': '证券简称', '折算率.1': '折算率'},
                                 inplace=True)
                    df_sz['exchange'] = 'sz'
                    self.tmp_df = pd.concat([df_sh, df_sz])
                else:
                    self.tmp_df = df
                self.tmp_df['biz_dt'] = sc_newest_date
                self.collect_num = self.tmp_df.index.size
                self.total_num = self.collect_num
                self.data_text = self.tmp_df.to_string()
                return

        # # 查所有存在href、且title=word的a标签
        # a_hrefs = soup.find_all('a', href=True, title=word)
        # if len(a_hrefs) > 0:
        #     href_str = a_hrefs[0]['href']
        #     # 从a['href']里面提取相对路径和文件ID
        #     arr = href_str[href_str.index('(')+1:href_str.index(')')].replace("'", "").split(',')
        #     url = self.url + arr[0].strip() + arr[1].strip()
        #     response = self.get_response(url, 0, get_headers())
        #     soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
        #     a_hrefs = soup.find_all('a', href=True, myurl=True)
        #     for a_href in a_hrefs:
        #         if a_href.text.startswith(word):
        #             biz_dt = a_href.text.replace(word, '').replace('-', '').replace('.xlsx', '').replace('.xsl', '').replace('.csv', '')
        #             response = self.get_response(a_href['myurl'], 0, get_headers())
        #             warnings.filterwarnings('ignore')
        #             df = pd.read_excel(response.content, header=0)
        #             if biz_type == 'db':
        #                 df_sh = df[['序号', '证券代码', '证券简称', '折算率']]
        #                 df_sh.dropna(axis=0, how='all', inplace=True)
        #                 df_sh['exchange'] = 'sh'
        #                 df_sh['序号'] = df_sh['序号'].apply(lambda x: int(x))
        #                 df_sh['证券代码'] = df_sh['证券代码'].apply(lambda x: int(x))
        #                 df_sz = df[['序号.1', '证券代码.1', '证券简称.1', '折算率.1']]
        #                 df_sz.rename(columns={'序号.1': '序号', '证券代码.1': '证券代码', '证券简称.1': '证券简称', '折算率.1': '折算率'}, inplace=True)
        #                 df_sz['exchange'] = 'sz'
        #                 self.tmp_df = pd.concat([df_sh, df_sz])
        #             else:
        #                 self.tmp_df = df
        #             self.tmp_df['biz_dt'] = biz_dt
        #             self.collect_num = self.tmp_df.index.size
        #             self.total_num = self.collect_num
        #             self.data_text = self.tmp_df.to_string()
        #             return


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((2, 3), sys.argv)
