#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description : 配置文件读取
@File        : __init__.py
@Project     : jbt-data-collect-platform
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""

__author__ = 'Eagle (liuzh@igoldenbeta.com)'
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), ".."))

import sh.sh_exchange_mt_underlying_and_guaranty_security as sh245
import sz.sz_exchange_mt_guaranty_security_collect as sz2
import sz.sz_exchange_mt_underlying_security_collect as sz3
import bj.bj_exchange_mt_guaranty_security_collect as bj2
import bj.bj_exchange_mt_underlying_security_collect as bj3
import securities.zx_securities_collect as zx23
import securities.gtja_securities_collect as gtja99
import securities.ht_securities_collect as ht23
import securities.yh_securities_collect as yh99
import securities.zs_securities_collect as zs245
import securities.zxjt_securities_collect as zxjt99
import securities.gx_securities_collect as gs245
import securities.gy_securities_collect as gy245
import securities.xy_securities_collect as xy23
# import securities.gf_securities_collect as gf245
import securities.sw_securities_collect as sw23
import securities.ax_securities_collect as ax23
import securities.zjcf_securities_collect as zjcf23


if __name__ == '__main__':
    pass
    # sh245.CollectHandler().collect_data(2, '2022-10-21')
    # sh245.CollectHandler().collect_data(2)
    # sh245.CollectHandler().collect_data(4, '2022-10-21')
    # sh245.CollectHandler().collect_data(4)
    # sh245.CollectHandler().collect_data(5, '2022-10-21')
    # sh245.CollectHandler().collect_data(5)
    #
    # sz2.CollectHandler().collect_data(2, '2022-10-21')
    # sz2.CollectHandler().collect_data(2)
    # sz3.CollectHandler().collect_data(3, '2022-10-21')
    # sz3.CollectHandler().collect_data(3)
    #
    bj2.CollectHandler().collect_data(2, '2023-02-13')
    # bj2.CollectHandler().collect_data(2)
    bj3.CollectHandler().collect_data(3, '2023-02-13')
    # bj3.CollectHandler().collect_data(3)
    #
    # zx23.CollectHandler().collect_data(2, '2022-10-21')
    # zx23.CollectHandler().collect_data(2)
    # zx23.CollectHandler().collect_data(3, '2022-10-21')
    # zx23.CollectHandler().collect_data(3)

    # gtja99.CollectHandler().collect_data(99, '2022-11-02')
    # gtja99.CollectHandler().collect_data(99, '2022-11-03')
    # gtja99.CollectHandler().collect_data(99, '2022-11-04')
    # gtja99.CollectHandler().collect_data(99, "")
    #
    # #
    # ht23.CollectHandler().collect_data(2, '2022-10-21')
    # ht23.CollectHandler().collect_data(2)
    # ht23.CollectHandler().collect_data(3, '2022-10-21')
    # ht23.CollectHandler().collect_data(3)
    #
    # yh99.CollectHandler().collect_data(99, "")
    #
    # zs245.CollectHandler().collect_data(2, '2022-10-21')
    # zs245.CollectHandler().collect_data(2)
    # zs245.CollectHandler().collect_data(4, '2022-10-21')
    # zs245.CollectHandler().collect_data(4)
    # zs245.CollectHandler().collect_data(5, '2022-10-21')
    # zs245.CollectHandler().collect_data(5)

    # zxjt99.CollectHandler().collect_data(99, "")
    #
    # gs245.CollectHandler().collect_data(2, '2022-10-21')
    # gs245.CollectHandler().collect_data(2)
    # gs245.CollectHandler().collect_data(4, '2022-10-21')
    # gs245.CollectHandler().collect_data(4)
    # gs245.CollectHandler().collect_data(5, '2022-10-21')
    # gs245.CollectHandler().collect_data(5)
    #
    # gy245.CollectHandler().collect_data(2, '2022-10-21')
    # gy245.CollectHandler().collect_data(2)
    # gy245.CollectHandler().collect_data(4, '2022-10-21')
    # gy245.CollectHandler().collect_data(4)
    # gy245.CollectHandler().collect_data(5, '2022-10-21')
    # gy245.CollectHandler().collect_data(5)
    # xy23.CollectHandler().collect_data(2, '2022-10-21')
    # xy23.CollectHandler().collect_data(2)
    # xy23.CollectHandler().collect_data(3, '2022-10-21')
    # xy23.CollectHandler().collect_data(3)

    # gf245.CollectHandler().collect_data(2)
    # gf245.CollectHandler().collect_data(2, '2022-10-21')
    # gf245.CollectHandler().collect_data(4, '2022-10-21')
    # gf245.CollectHandler().collect_data(4)
    # gf245.CollectHandler().collect_data(5, '2022-10-21')
    # gf245.CollectHandler().collect_data(5)

    # sw23.CollectHandler().collect_data(2, '2022-10-21')
    # sw23.CollectHandler().collect_data(2)
    # sw23.CollectHandler().collect_data(3, '2022-10-21')
    # sw23.CollectHandler().collect_data(3)

    # ax23.CollectHandler().collect_data(2, '2022-10-21')
    # ax23.CollectHandler().collect_data(2)
    # ax23.CollectHandler().collect_data(3, '2022-10-21')
    # ax23.CollectHandler().collect_data(3)

    # zjcf23.CollectHandler().collect_data(2, '2022-10-21')
    # zjcf23.CollectHandler().collect_data(2)
    # zjcf23.CollectHandler().collect_data(3, '2022-10-21')
    # zjcf23.CollectHandler().collect_data(3)
