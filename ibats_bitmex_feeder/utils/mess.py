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
from ibats_common.utils.mess import try_n_times, log_param_when_exception, str_2_datetime, datetime_2_str
import logging
from bravado.requests_client import RequestsResponseAdapter
from bravado.exception import HTTPForbidden

logger = logging.getLogger()


# @log_param_when_exception
@try_n_times(3, sleep_time=1.5, logger=logger, exception_exclusion_set={HTTPForbidden})
def try_call_func(func: CallableOperation, *args, **kwargs) -> (list, RequestsResponseAdapter):
    """
    请求频率限制:对我们的 REST API 的请求频率限于每5分钟300次。 此计数连续补充。 如果你没有登录，你的请求是每5分钟150次。 https://www.bitmex.com/app/restAPI
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    return func(*args, **kwargs).result()


def load_list_against_pagination(func: CallableOperation, start_count=0, count=500, page_no_max=None, **kwargs) -> list:
    """
    调用接口函数，自动翻译加载全部数据并返回结果
    :param func:
    :param start_count:
    :param count:
    :param page_no_max:
    :return:
    """
    start_count_cur = start_count
    ret_list = []
    param_str = ', '.join(
        ['{key}={value}'.format(key=str(key), value=str(value))
         for key, value in kwargs.items()]
    )
    call_count = 0
    while True:
        call_count += 1
        # logger.debug(
        #     '%d) %s call %s(start=%s, count=%s, %s)',
        #     call_count, func.operation.path_name, func.operation.operation_id, start_count_cur, count, param_str)
        try:
            data_list, rsp = try_call_func(func, start=start_count_cur, count=count, **kwargs)
        except:
            # logger.exception(
            #     '%d) %s call %s(start=%s, count=%s, %s) Error',
            #     call_count, func.operation.path_name, func.operation.operation_id, start_count_cur, count, param_str)
            break
        if rsp is None:
            break
        if rsp.status_code != 200:
            logger.error(
                "%d) %s(start=%d, count=%d, %s) error status_code=%d, reason=''",
                call_count, func.operation.path_name, start_count_cur, count, param_str, rsp.status_code, rsp.reason)
        if data_list is None or len(data_list) == 0:
            logger.debug(
                '%d) %s call %s(start=%s, count=%s, %s) 0 returned',
                call_count, func.operation.path_name, func.operation.operation_id, start_count_cur, count, param_str)
            break
        if True:
            data_list_len = len(data_list)
            if 'timestamp' in data_list[0]:
                timestamp_list = [str_2_datetime(item['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                                  for item in data_list]
                timestamp_min, timestamp_max = min(timestamp_list), max(timestamp_list)
                logger.debug(
                    '%d) %s call %s(start=%s, count=%s, %s) %d [%s ~ %s]',
                    call_count, func.operation.path_name, func.operation.operation_id, start_count_cur, count, param_str,
                    data_list_len, datetime_2_str(timestamp_min), datetime_2_str(timestamp_max))
            else:
                logger.debug(
                    '%d) %s call %s(start=%s, count=%s, %s) %d data',
                    call_count, func.operation.path_name, func.operation.operation_id, start_count_cur, count, param_str,
                    data_list_len)
        ret_list.extend(data_list)
        start_count_cur += count
        if page_no_max is not None and page_no_max <= call_count:
            break
    return ret_list


def load_df_against_pagination(func: CallableOperation, page_no_since=0, count=500,
                               page_no_max=None, drop_duplicates=True, **kwargs) -> pd.DataFrame:
    """
    调用接口函数，自动翻译加载全部数据并返回结果
    :param func:
    :param page_no_since:
    :param count:
    :param page_no_max:
    :param drop_duplicates:
    :return:
    """
    ret_list = load_list_against_pagination(func, page_no_since, count, page_no_max, **kwargs)

    if len(ret_list) > 0:
        ret_df = pd.DataFrame(ret_list)
        if drop_duplicates:
            ret_df.drop_duplicates(inplace=True)
    else:
        ret_df = None
    return ret_df


if __name__ == "__main__":
    import bitmex
    from ibats_bitmex_feeder.config import config

    api = bitmex.bitmex(test=config.TEST_NET)
    func = api.Instrument.Instrument_get
    # 测试 try_call_func 接口
    data_list, rsp = try_call_func(func, start=0, count=10)
    print(len(data_list))

    # 测试 load_against_pagination 接口
    ret_list = load_df_against_pagination(func)
    print(len(ret_list))
