#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/10/17 16:19
@File    : handler.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
from ibats_common.common import PeriodType
from prodconpattern import ProducerConsumer
from ibats_common.utils.mess import datetime_2_str, STR_FORMAT_DATETIME2
from datetime import datetime
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
from ibats_common.utils.redis import get_redis, get_channel
import json
from ibats_bitmex_feeder.config import config
from ibats_bitmex_feeder.backend import engine_md
logger = logging.getLogger()


class BaseHandler:

    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger(__name__)


class SimpleHandler(BaseHandler):

    def handle(self, msg: dict):
        logger.debug(msg)


class DBHandler(BaseHandler):

    def __init__(self, period, db_model=None, table_name=None):
        """
        接收数据，并插入到对应数据库
        :param period: 周期
        :param db_model: 模型
        :param table_name: 表名
        """
        self.period = period
        if db_model is not None:
            self.table_name = db_model.__tablename__
            self.md_orm_table = db_model.__table__
        elif table_name is not None:
            self.table_name = table_name
            self.md_orm_table = Table(table_name, MetaData(engine_md), autoload=True)
        else:
            raise ValueError('db_model, table_name 需要至少设置一个参数')
        BaseHandler.__init__(self, 'DB[%s]' % self.table_name)
        self.session_maker = sessionmaker(bind=engine_md)
        self.session = None
        self.logger = logging.getLogger(f'{__name__}->{self.table_name}')
        self.md_orm_table_insert = self.md_orm_table.insert(on_duplicate_key_update=True)

    def handle(self, msg: dict):
        self.save_md(msg)
        self.logger.debug('invoke save_md %s', msg)

    @ProducerConsumer(threshold=2000, interval=60, pass_arg_list=True, is_class_method=True)
    def save_md(self, data_dic_list):
        """
        保存md数据到数据库及文件
        :param data_dic_list:
        :param session:
        :return:
        """
        # 仅调试使用
        # if self.table_name == 'md_min60_bc':
        # self.logger.info('%d data will be save to %s', len(data_dic_list), self.table_name)

        if data_dic_list is None or len(data_dic_list) == 0:
            self.logger.warning("data_dic_list 为空")
            return

        md_count = len(data_dic_list)
        # 保存到数据库
        if self.session is None:
            self.session = self.session_maker()
        try:
            self.session.execute(self.md_orm_table_insert, data_dic_list)
            self.session.commit()
            self.logger.info('%d 条实时行情 -> %s 完成', md_count, self.table_name)
        except:
            self.logger.exception('%d 条实时行情 -> %s 失败', md_count, self.table_name)


class PublishHandler(BaseHandler):

    def __init__(self, period):
        BaseHandler.__init__(self, name=self.__class__.__name__)
        self.period = period
        self.r = get_redis(config.REDIS_INFO_DIC['REDIS_HOST'], config.REDIS_INFO_DIC['REDIS_PORT'])

    def handle(self, msg: dict):
        """
        收到数据后，tick数据直接发送，
        channel：md.market.tick.pair
        channel：md.market.min1.pair 每个分钟时点切换时，发送一次分钟线数据
        例如：
        md.huobi.tick.ethusdt
        md.huobi.1min.ethusdt
        通过 redis-cli 可以 PUBSUB CHANNELS 查阅活跃的频道
        PSUBSCRIBE pattern [pattern ...]  查看频道内容
        SUBSCRIBE channel [channel ...]  查看频道内容
        :param msg:
        :return:
        """
        # TODO: 设定一个定期检查机制，只发送订阅的品种，降低网络负载
        md_str = json.dumps(msg)
        symbol = msg['symbol']
        channel = get_channel(config.MARKET_NAME, self.period, symbol)
        self.r.publish(channel, md_str)
