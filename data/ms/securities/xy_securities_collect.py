#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
import time
import os
import sys

import numpy as np
import pandas as pd
from selenium.webdriver.common.by import By


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, argv_param_invoke, logger


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '兴业证券'
        self.total_page = 0

    def rzrq_underlying_securities_collect(self):
        self.url = "https://www.xyzq.com.cn/xysec/biz/11291"
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.url = "https://www.xyzq.com.cn/xysec/biz/11292"
        self._securities_collect('db')

    def _securities_collect(self, biz_type):
        try:
            driver = self.get_driver()
            driver.get(self.url)
            driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
            time.sleep(3)
            # 找到券商官网写的总条数
            span_element = driver.find_elements(By.CSS_SELECTOR, "#divpageinfoarea > div > b")
            self.total_page = int(span_element[0].text[2:5])
            self.total_num = int(span_element[0].text[9:13])
            # 当前网页内容(第1页)
            html_content = str(driver.page_source)
            _df1 = pd.read_html(html_content)[0]
            _df1.columns = np.array(_df1).tolist()[0]
            _df1.drop([0], inplace=True)
            _df1.sort_values(by=['证券代码', '证券简称'], ascending=[True, True])
            self.tmp_df = pd.concat([self.tmp_df, _df1])
            self.collect_num = self.tmp_df.index.size
            current_page = 2
            logger.info(f"兴业证券{biz_type}第{current_page}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
            while current_page < self.total_page + 1:
                ele = driver.find_elements(By.CSS_SELECTOR, "#divpageinfoarea > div > span > input")[0]
                ele.clear()
                ele.send_keys(current_page)
                ala = driver.find_elements(By.CSS_SELECTOR, "#divpageinfoarea > div > span > a")[0]
                ala.click()
                html_content = str(driver.page_source)
                _df2 = pd.read_html(html_content)[0]
                _df2.columns = np.array(_df2).tolist()[0]
                _df2.drop([0], inplace=True)
                _df2.sort_values(by=['证券代码', '证券简称'], ascending=[True, True])
                ##############
                # 读取内容与上一次完全相同，则休息一下再重新读取内容
                counter = 0
                while _df1.equals(_df2):
                    logger.info(f'兴业证券{biz_type}采集速度过快，休息一下{counter}')
                    time.sleep(0.01)
                    html_content = str(driver.page_source)
                    _df2 = pd.read_html(html_content)[0]
                    _df2.columns = np.array(_df2).tolist()[0]
                    _df2.drop([0], inplace=True)
                    _df2.sort_values(by=['证券代码', '证券简称'], ascending=[True, True])
                    if counter > 100:
                        driver.quit()
                        msg = f"兴业证券{biz_type}采集速度过快,连续休息100次重试还是存在问题!"
                        logger.info(msg)
                        raise Exception(msg)
                    counter += 1
                _df1 = _df2
                self.tmp_df = pd.concat([self.tmp_df, _df1])
                # 与上一次存在重复，否则需要整个重采
                if self.tmp_df.duplicated(['证券代码', '证券简称']).sum() > 0:
                    dep_line = self.tmp_df[self.tmp_df.duplicated(['证券代码', '证券简称'], keep='last')]  # 查看删除重复的行
                    dep_list = dep_line.values.tolist()
                    msg = f'兴业证券{biz_type}采集的重复数据为：{dep_list},共{len(dep_list)}条'
                    logger.info(msg)
                    raise Exception(msg)
                self.collect_num = self.tmp_df.index.size
                logger.info(f"兴业证券{biz_type}第{current_page + 1}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
                current_page += 1
            self.data_text = self.tmp_df.to_csv(index=False)

        finally:
            driver.quit()




    # def __init__(self):
    #     super().__init__()
    #     self.mq_msg = os.path.basename(__file__).split('.')[0]
    #     self.data_source = '兴业证券'
    #     self.url = "https://www.xyzq.com.cn/xysec/"
    #     self.target_excel_name = '兴业证券融资融券标的证券及保证金比例明细表'
    #     self.guaranty_excel_name = '兴业证券融资融券可充抵保证金证券及折算率明细表'
    #
    # def rzrq_underlying_securities_collect(self):
    #     title_url = "https://www.xyzq.com.cn/xysec/biz/11116"
    #     self._securities_collect(title_url, self.target_excel_name, 'bd')
    #
    # def guaranty_securities_collect(self):
    #     title_url = "https://www.xyzq.com.cn/xysec/biz/11117"
    #     self._securities_collect(title_url, self.guaranty_excel_name, 'db')
    #
    # def _securities_collect(self, title_url, word, biz_type):
    #     data = {'word': word, "cur": 1}
    #     encode_data = parse.urlencode(data)
    #     response = self.get_response(title_url, 0, get_headers(), encode_data)
    #     soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
    #     # # 此处直接查询'.newsBox'这个元素才能获取正确的url以及后续数据
    #     # label_div_list = soup.select('.newsBox')
    #     # if label_div_list:
    #     #     label_div = label_div_list[0]
    #     #     my_id = label_div['myid']
    #     #     my_path = label_div['mypath']
    #     #
    #     #     label_span_list = soup.select('.newsBox span')
    #     #     sc_newest_date = label_span_list[1].text.strip() + label_span_list[0].text.strip().replace('-', "")
    #     #     full_download_page_url = self.url + my_path + my_id
    #     #     response = self.get_response(full_download_page_url, 0, get_headers(), encode_data)
    #     #     soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
    #     #
    #     #     label_aa_list = soup.select("div .fujian a")
    #     #     if label_aa_list:
    #     #         # download_url = label_aa_list[0]['href']
    #     #         download_url = label_aa_list[0]['myurl']  # 兴业改了html,20210817
    #     #         response = self.get_response(download_url, 0, get_headers())
    #     #         df = pd.read_excel(response.content, header=0)
    #     #         if biz_type == 'db':
    #     #             df_sh = df[['序号', '证券代码', '证券简称', '折算率']]
    #     #             df_sh.dropna(axis=0, how='all', inplace=True)
    #     #             df_sh['exchange'] = 'sh'
    #     #             df_sh['序号'] = df_sh['序号'].apply(lambda x: int(x))
    #     #             df_sh['证券代码'] = df_sh['证券代码'].apply(lambda x: int(x))
    #     #             df_sz = df[['序号.1', '证券代码.1', '证券简称.1', '折算率.1']]
    #     #             df_sz.rename(columns={'序号.1': '序号', '证券代码.1': '证券代码', '证券简称.1': '证券简称', '折算率.1': '折算率'},
    #     #                          inplace=True)
    #     #             df_sz['exchange'] = 'sz'
    #     #             self.tmp_df = pd.concat([df_sh, df_sz])
    #     #         else:
    #     #             self.tmp_df = df
    #     #         self.tmp_df['biz_dt'] = sc_newest_date
    #     #         self.collect_num = self.tmp_df.index.size
    #     #         self.total_num = self.collect_num
    #     #         self.data_text = self.tmp_df.to_csv(index=False)
    #     #         return
    #
    #     divs = soup.find_all('div', act='curdayinfo', mytitle=word)
    #     url = None
    #     if len(divs) > 0:
    #         myid = divs[0]['myid']
    #         mypath = divs[0]['mypath']
    #         url = self.url + mypath + myid
    #     else:
    #         # 查所有存在href、且title=word的a标签
    #         a_hrefs = soup.find_all('a', href=True, title=word)
    #         if len(a_hrefs) > 0:
    #             href_str = a_hrefs[0]['href']
    #             # 从a['href']里面提取相对路径和文件ID
    #             arr = href_str[href_str.index('(')+1:href_str.index(')')].replace("'", "").split(',')
    #             url = self.url + arr[0].strip() + arr[1].strip()
    #     if url is not None:
    #         response = self.get_response(url, 0, get_headers())
    #         soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
    #         a_hrefs = soup.find_all('a', href=True, myurl=True)
    #         for a_href in a_hrefs:
    #             if a_href.text.startswith(word):
    #                 biz_dt = a_href.text.replace(word, '').replace('-', '').replace('.xlsx', '').replace('.xsl', '').replace('.csv', '')
    #                 response = self.get_response(a_href['myurl'], 0, get_headers())
    #                 warnings.filterwarnings('ignore')
    #                 df = pd.read_excel(response.content, header=0)
    #                 if biz_type == 'db':
    #                     df_sh = df[['序号', '证券代码', '证券简称', '折算率']]
    #                     df_sh.dropna(axis=0, how='all', inplace=True)
    #                     df_sh['exchange'] = 'sh'
    #                     df_sh['序号'] = df_sh['序号'].apply(lambda x: int(x))
    #                     df_sh['证券代码'] = df_sh['证券代码'].apply(lambda x: int(x))
    #                     df_sz = df[['序号.1', '证券代码.1', '证券简称.1', '折算率.1']]
    #                     df_sz.rename(columns={'序号.1': '序号', '证券代码.1': '证券代码', '证券简称.1': '证券简称', '折算率.1': '折算率'}, inplace=True)
    #                     df_sz['exchange'] = 'sz'
    #                     self.tmp_df = pd.concat([df_sh, df_sz])
    #                 else:
    #                     self.tmp_df = df
    #                 self.tmp_df['biz_dt'] = biz_dt
    #                 self.collect_num = self.tmp_df.index.size
    #                 self.total_num = self.collect_num
    #                 self.data_text = self.tmp_df.to_csv(index=False)
    #                 return


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
