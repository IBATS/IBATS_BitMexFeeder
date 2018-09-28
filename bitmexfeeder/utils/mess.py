#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/27 14:58
@File    : mess.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from bravado.client import CallableOperation
from ibats_common.utils.mess import try_n_times
import logging

logger = logging.getLogger()


@try_n_times(3, sleep_time=1, logger=logger)
def try_call_func(api: CallableOperation, *args, **kwargs):
    """
    请求频率限制:对我们的 REST API 的请求频率限于每5分钟300次。 此计数连续补充。 如果你没有登录，你的请求是每5分钟150次。 https://www.bitmex.com/app/restAPI
    :param api:
    :param args:
    :param kwargs:
    :return:
    """
    return api(*args, **kwargs)


def load_against_pagination(func: CallableOperation, page_no_since=0, count=500) -> list:
    page_no = page_no_since
    ret_list = []
    while True:
        rsp = try_call_func(func, start=page_no, count=count)  # <class 'bravado.http_future.HttpFuture'>
        if rsp is None:
            break
        data_list, rsp = rsp.result()  # [{...}], <class 'bravado.requests_client.RequestsResponseAdapter'>
        if len(data_list) == 0:
            break
        ret_list.extend(data_list)
        page_no += 1

    return ret_list


if __name__ == "__main__":
    import bitmex
    from bitmexfeeder.config import config
    api = bitmex.bitmex(test=config.TEST_NET)
    func = api.Instrument.Instrument_get
    ret_data = try_call_func(func)
    print(len(ret_data))
