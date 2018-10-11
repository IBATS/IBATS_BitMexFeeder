#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/27 14:58
@File    : mess.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
from bravado.client import CallableOperation
from ibats_common.utils.mess import try_n_times, log_param_when_exception
import logging
from bravado.requests_client import RequestsResponseAdapter

logger = logging.getLogger()


# @log_param_when_exception
@try_n_times(3, sleep_time=1, logger=logger)
def try_call_func(func: CallableOperation, *args, **kwargs) -> (list, RequestsResponseAdapter):
    """
    请求频率限制:对我们的 REST API 的请求频率限于每5分钟300次。 此计数连续补充。 如果你没有登录，你的请求是每5分钟150次。 https://www.bitmex.com/app/restAPI
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    return func(*args, **kwargs).result()


def load_against_pagination(func: CallableOperation, page_no_since=0, count=500) -> pd.DataFrame:
    """
    调用接口函数，自动翻译加载全部数据并返回结果
    :param func:
    :param page_no_since:
    :param count:
    :return:
    """
    page_no = page_no_since
    ret_list = []
    while True:
        logger.debug('%s call %s(start=%s, count=%s)',
                     func.operation.path_name, func.operation.operation_id, page_no, count)
        data_list, rsp = try_call_func(func, start=page_no, count=count)  # <class 'bravado.http_future.HttpFuture'>
        if rsp is None:
            break
        if data_list is None or len(data_list) == 0:
            break
        ret_list.extend(data_list)
        page_no += 1

    if len(ret_list)>0:
        ret_df = pd.DataFrame(ret_list)
    else:
        ret_df = None
    return ret_df


if __name__ == "__main__":
    import bitmex
    from bitmexfeeder.config import config
    api = bitmex.bitmex(test=config.TEST_NET)
    func = api.Instrument.Instrument_get
    # 测试 try_call_func 接口
    data_list, rsp = try_call_func(func, start=0, count=10)
    print(len(data_list))

    # 测试 load_against_pagination 接口
    ret_list = load_against_pagination(func)
    print(len(ret_list))
