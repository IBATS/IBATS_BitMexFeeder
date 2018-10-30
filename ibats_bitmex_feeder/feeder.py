#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/27 10:40
@File    : feeder.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from threading import Thread
from ibats_bitmex_feeder.backend.bitmex_ws import BitMexWS, TableNoAuth
import bitmex
from pandas._libs.tslibs.timestamps import Timestamp
from sqlalchemy import func
from ibats_bitmex_feeder.backend.check import check_redis
from ibats_bitmex_feeder.config import config
from ibats_bitmex_feeder.utils.mess import load_df_against_pagination, load_list_against_pagination
from sqlalchemy.types import String, Date, DateTime, Time, Integer, Boolean
from sqlalchemy.dialects.mysql import DOUBLE, TIMESTAMP
from ibats_common.utils.db import bunch_insert_on_duplicate_update, with_db_session
from ibats_bitmex_feeder.backend import engine_md
import time
from ibats_common.utils.mess import try_n_times, date_2_str, datetime_2_str
import logging
from datetime import datetime, timedelta
from ibats_bitmex_feeder.backend.orm import MDMin1, MDMin1Temp, MDDaily, MDDailyTemp, MDHour1, MDHour1Temp, \
    MDMin5, MDMin5Temp, BaseModel
from ibats_bitmex_feeder.backend.other_tables import INSTRUMENT_INFO_TABLE_NAME
from ibats_bitmex_feeder.backend.handler import DBHandler, PublishHandler, SimpleHandler
from ibats_common.common import PeriodType

logger = logging.getLogger()
DTYPE_INSTRUMENT = {
    'symbol': String(20),
    'rootSymbol': String(10),
    'state': String(20),
    'typ': String(10),
    'listing': DateTime,
    'front': DateTime,
    'expiry': DateTime,
    'settle': DateTime,
    'relistInterval': DateTime,
    'inverseLeg': String(20),
    'sellLeg': String(20),
    'buyLeg': String(20),
    'optionStrikePcnt': DOUBLE,
    'optionStrikeRound': DOUBLE,
    'optionStrikePrice': DOUBLE,
    'optionMultiplier': DOUBLE,
    'positionCurrency': String(20),
    'underlying': String(20),
    'quoteCurrency': String(20),
    'underlyingSymbol': String(20),
    'reference': String(20),
    'referenceSymbol': String(20),
    'calcInterval': DateTime,
    'publishInterval': DateTime,
    'publishTime': DateTime,
    'maxOrderQty': DOUBLE,
    'maxPrice': DOUBLE,
    'lotSize': DOUBLE,
    'tickSize': DOUBLE,
    'multiplier': DOUBLE,
    'settlCurrency': String(20),
    'underlyingToPositionMultiplier': DOUBLE,
    'underlyingToSettleMultiplier': DOUBLE,
    'quoteToSettleMultiplier': DOUBLE,
    'isQuanto': Boolean,
    'isInverse': Boolean,
    'initMargin': DOUBLE,
    'maintMargin': DOUBLE,
    'riskLimit': DOUBLE,
    'riskStep': DOUBLE,
    'limit': DOUBLE,
    'capped': Boolean,
    'taxed': Boolean,
    'deleverage': Boolean,
    'makerFee': DOUBLE,
    'takerFee': DOUBLE,
    'settlementFee': DOUBLE,
    'insuranceFee': DOUBLE,
    'fundingBaseSymbol': String(20),
    'fundingQuoteSymbol': String(20),
    'fundingPremiumSymbol': String(20),
    'fundingTimestamp': DateTime,
    'fundingInterval': DateTime,
    'fundingRate': DOUBLE,
    'indicativeFundingRate': DOUBLE,
    'rebalanceTimestamp': DateTime,
    'rebalanceInterval': DateTime,
    'openingTimestamp': DateTime,
    'closingTimestamp': DateTime,
    'sessionInterval': DateTime,
    'prevClosePrice': DOUBLE,
    'limitDownPrice': DOUBLE,
    'limitUpPrice': DOUBLE,
    'bankruptLimitDownPrice': DOUBLE,
    'bankruptLimitUpPrice': DOUBLE,
    'prevTotalVolume': DOUBLE,
    'totalVolume': DOUBLE,
    'volume': DOUBLE,
    'volume24h': DOUBLE,
    'prevTotalTurnover': DOUBLE,
    'totalTurnover': DOUBLE,
    'turnover': DOUBLE,
    'turnover24h': DOUBLE,
    'homeNotional24h': DOUBLE,
    'foreignNotional24h': DOUBLE,
    'prevPrice24h': DOUBLE,
    'vwap': DOUBLE,
    'highPrice': DOUBLE,
    'lowPrice': DOUBLE,
    'lastPrice': DOUBLE,
    'lastPriceProtected': DOUBLE,
    'lastTickDirection': String(20),
    'lastChangePcnt': DOUBLE,
    'bidPrice': DOUBLE,
    'midPrice': DOUBLE,
    'askPrice': DOUBLE,
    'impactBidPrice': DOUBLE,
    'impactMidPrice': DOUBLE,
    'impactAskPrice': DOUBLE,
    'hasLiquidity': Boolean,
    'openInterest': DOUBLE,
    'openValue': DOUBLE,
    'fairMethod': String(20),
    'fairBasisRate': DOUBLE,
    'fairBasis': DOUBLE,
    'fairPrice': DOUBLE,
    'markMethod': String(30),
    'markPrice': DOUBLE,
    'indicativeTaxRate': DOUBLE,
    'indicativeSettlePrice': DOUBLE,
    'optionUnderlyingPrice': DOUBLE,
    'settledPrice': DOUBLE,
    'timestamp': DateTime
}


class MDFeeder(Thread):
    """接受订阅数据想redis发送数据"""

    def __init__(self, do_init_symbols=False, do_fill_history=False):
        super().__init__(name='Feeder', daemon=True)
        self.endpoint = "https://testnet.bitmex.com/api/v1" if config.TEST_NET else "https://www.bitmex.com/api/v1"
        self.ws = BitMexWS(endpoint=self.endpoint, api_key=None, api_secret=None)
        self.api = bitmex.bitmex(test=config.TEST_NET)
        self.api_auth = bitmex.bitmex(test=config.TEST_NET,
                                      api_key=config.EXCHANGE_PUBLIC_KEY, api_secret=config.EXCHANGE_SECRET_KEY)
        self.do_init_symbols = do_init_symbols
        self.do_fill_history = do_fill_history
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_working = False
        # self.heart_beat = HeartBeatHandler()

    def init(self):
        """
        初始化，订阅行情
        默认1分钟、1小时、1日
        :return:
        """

        table_name = INSTRUMENT_INFO_TABLE_NAME
        if self.do_init_symbols or not engine_md.has_table(table_name):
            # 获取有效的交易对信息保存（更新）数据库
            ret_df = load_df_against_pagination(self.api.Instrument.Instrument_get)

            for key, val in DTYPE_INSTRUMENT.items():
                if val in (DateTime, TIMESTAMP):
                    ret_df[key] = ret_df[key].apply(
                        lambda x: x.strftime('%Y-%m-%d %H-%M-%S') if isinstance(x, Timestamp) else None)
            data_count = bunch_insert_on_duplicate_update(
                ret_df, table_name, engine_md, dtype=DTYPE_INSTRUMENT,
                myisam_if_create_table=True, primary_keys=['symbol'])
            self.logger.info('更新 instrument 信息 %d 条', data_count)
            symbol_set = set(list(ret_df[ret_df['state'] == 'Open']['symbol']))
        else:
            sql_str = f"select symbol from {table_name} where state='Open'"
            with with_db_session(engine_md) as session:
                symbol_set = set(list(session.execute(sql_str).fetchall()))

        # symbol_list_len = len(symbol_set)
        # self.logger.info('订阅实时行情 %d 项：%s', symbol_list_len, symbol_set)

        # 订阅 instrument
        self.ws.register_handler(
            'db', handler=DBHandler(period=PeriodType.Min1, db_model=MDMin1), table=TableNoAuth.tradeBin1m)
        self.ws.register_handler(
            'db', handler=DBHandler(period=PeriodType.Min5, db_model=MDMin5), table=TableNoAuth.tradeBin5m)
        self.ws.register_handler(
            'db', handler=DBHandler(period=PeriodType.Hour1, db_model=MDHour1), table=TableNoAuth.tradeBin1h)
        self.ws.register_handler(
            'db', handler=DBHandler(period=PeriodType.Day1, db_model=MDDaily), table=TableNoAuth.tradeBin1d)

        # 数据redis广播
        if config.REDIS_PUBLISHER_HANDLER_ENABLE and check_redis():
            self.ws.register_handler(
                'publisher', PublishHandler(period=PeriodType.Min1), table=TableNoAuth.tradeBin1m)
            self.ws.register_handler(
                'publisher', PublishHandler(period=PeriodType.MDMin5), table=TableNoAuth.tradeBin5m)
            self.ws.register_handler(
                'publisher', PublishHandler(period=PeriodType.MDHour1), table=TableNoAuth.tradeBin1h)
            self.ws.register_handler(
                'publisher', PublishHandler(period=PeriodType.MDDaily), table=TableNoAuth.tradeBin1d)

        # Heart Beat
        # self.hb.register_handler(self.heart_beat)
        # logger.info('注册 %s 处理句柄', self.heart_beat.name)
        #
        # server_datetime = self.get_server_datetime()
        # logger.info("api.服务期时间 %s 与本地时间差： %f 秒",
        #             server_datetime, (datetime.now() - server_datetime).total_seconds())
        # self.check_state()

    def stop(self):
        # 关闭所有 ws
        self.ws.exit()
        self.logger.info('结束订阅')

    def run(self):
        self.is_working = True
        self.logger.info('启动')
        self.ws.connect()
        # 补充历史数据
        if self.do_fill_history:
            self.logger.info('开始补充历史数据')
            self.fill_history()
        while self.is_working:
            time.sleep(5)

    def fill_history(self, periods=['1m', '5m', '1h', '1d']):
        for period in periods:
            if period == '1m':
                model_tot, model_tmp = MDMin1, MDMin1Temp
            elif period == '5m':
                model_tot, model_tmp = MDMin5, MDMin5Temp
            elif period == '1h':
                model_tot, model_tmp = MDHour1, MDHour1Temp
            elif period == '1d':
                model_tot, model_tmp = MDDaily, MDDailyTemp
            else:
                self.logger.warning(f'{period} 不是有效的周期')
                raise ValueError(f'{period} 不是有效的周期')

            if not self.is_working:
                break
            self.fill_history_period(period, model_tot, model_tmp)

    def fill_history_period(self, period, model_tot: BaseModel, model_tmp: BaseModel):
        """
        根据数据库中的支持 symbol 补充历史数据
        api.Trade.Trade_get(symbol='XBTUSD').result()
        :param period:
        :param model_tot:
        :param model_tmp:
        :return:
        """
        if period not in {'1m', '5m', '1h', '1d'}:
            logger.error(f'{period} 不是有效的周期')
            raise ValueError(f'{period} 不是有效的周期')

        # 查找有效的 symbol
        table_name = 'bitmex_instrument'
        sql_str = f"select symbol from {table_name} where state='Open'"
        with with_db_session(engine_md) as session:
            symbol_set = set([row[0] for row in session.execute(sql_str).fetchall()])
            pair_datetime_latest_dic = dict(
                session.query(
                    model_tmp.symbol, func.max(model_tmp.timestamp)
                ).group_by(model_tmp.symbol).all()
            )

        # 循环获取每一个交易对的历史数据
        symbol_set_len = len(symbol_set)
        for num, symbol in enumerate(symbol_set):
            if not self.is_working:
                break

            if symbol in pair_datetime_latest_dic:
                datetime_latest = pair_datetime_latest_dic[symbol]
                datetime_start = datetime_latest + timedelta(minutes=1)

                # 设置 startTime
                if period == '1m':
                    size = int((datetime.now() - datetime_start).total_seconds() / 60)
                elif period == '5m':
                    size = int((datetime.now() - datetime_start).total_seconds() / 300)
                elif period == '1h':
                    size = int((datetime.now() - datetime_start).total_seconds() / 3600)
                else:  # if period == '1d':
                    size = (datetime.now() - datetime_start).days

                # start_time = datetime_2_str(datetime_start, '%Y-%m-%d %H:%M:%S')
                # start_time = datetime_start
            else:
                size = 500
                datetime_start = None

            if size <= 0:
                continue

            ret_list = self.get_kline(symbol, period, start_time=datetime_start)
            if ret_list is None or len(ret_list) == 0:
                continue
            ret_list_len = len(ret_list)
            for data_dic in ret_list:
                data_dic['timestamp'] = datetime_2_str(data_dic['timestamp'])
            # data_count = bunch_insert_on_duplicate_update(ret_df, model_tmp.__tablename__, engine_md)
            self._save_md(ret_list, symbol, model_tot, model_tmp)
            logger.info('%d/%d) %s %s 更新 %d 数据成功', num, symbol_set_len, symbol, period, ret_list_len)

    @try_n_times(5, sleep_time=5, logger=logger)
    def get_kline(self, symbol, period, start_time=None) -> list:
        # ret_df = self.api.Trade.Trade_getBucketed(symbol=symbol, binSize=period, startTime=start_time).result()[0]
        ret_list = load_list_against_pagination(self.api.Trade.Trade_getBucketed,
                                                symbol=symbol, binSize=period, startTime=start_time)
        return ret_list

    def _save_md(self, data_dic_list, symbol, model_tot: MDMin1, model_tmp: MDMin1Temp):
        """
        保存md数据到数据库及文件
        :param data_dic_list:
        :param symbol:
        :param model_tot:
        :param model_tmp:
        :return:
        """

        if data_dic_list is None:
            self.logger.warning("data_dic_list 为空")
            return

        md_count = len(data_dic_list)
        if md_count == 0:
            self.logger.warning("data_dic_list len is 0")
            return
        table_name = model_tot.__tablename__
        # 保存到数据库
        with with_db_session(engine_md) as session:
            try:
                # 插入数据到临时表
                session.execute(model_tmp.__table__.insert(on_duplicate_key_update=True), data_dic_list)
                self.logger.info('%d 条 %s 历史数据 -> %s 完成', md_count, symbol, table_name)
                # 将临时表数据导入总表
                sql_str = f"""insert into `{table_name}` 
                    select * from `{model_tmp.__tablename__}` where symbol=:symbol ON DUPLICATE KEY UPDATE 
                    open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
                    trades=VALUES(trades), volume=VALUES(volume), vwap=VALUES(vwap), lastSize=VALUES(lastSize),
                    turnover=VALUES(turnover), homeNotional=VALUES(homeNotional), 
                    foreignNotional=VALUES(foreignNotional)"""
                session.execute(sql_str, params={"symbol": symbol})
                # 删除临时表数据，仅保留最新的一条记录
                datetime_latest = session.query(
                    func.max(model_tmp.timestamp).label('timestamp')
                ).filter(
                    model_tmp.symbol == symbol
                ).scalar()
                # issue:
                # https://stackoverflow.com/questions/9882358/how-to-delete-rows-from-a-table-using-an-sqlalchemy-query-without-orm
                delete_count = session.query(model_tmp).filter(
                    model_tmp.symbol == symbol,
                    model_tmp.timestamp < datetime_latest
                ).delete()
                self.logger.debug('%d 条 %s 历史数据被清理，最新数据日期 %s', delete_count, symbol, datetime_latest)
                session.commit()
            except:
                self.logger.exception('%d 条 %s 数据-> %s 失败', md_count, symbol, table_name)


def start_feeder(init_symbols=False, do_fill_history=False) -> MDFeeder:
    feeder = MDFeeder(do_init_symbols=init_symbols, do_fill_history=do_fill_history)
    feeder.init()
    feeder.start()
    return feeder
