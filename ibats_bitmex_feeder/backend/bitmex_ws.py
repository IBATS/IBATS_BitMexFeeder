#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/10/17 15:54
@File    : bitmex_ws.py
@contact : mmmaaaggg@163.com
@desc    : 根据官网资料 https://github.com/BitMEX/api-connectors/tree/master/official-ws/python 进一步修改
支持订阅全部合约

# You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
# 下面的订阅主题是无需身份验证
# "announcement",// 网站公告
# "chat",        // Trollbox 聊天室
# "connected",   // 已连接用户/机器人的统计数据
# "funding",     // 掉期产品的资金费率更新 每个资金时段发送（通常是8小时）
# "instrument",  // 产品更新，包括交易量以及报价
# "insurance",   // 每日保险基金的更新
# "liquidation", // 进入委托列表的强平委托
# "orderBookL2", // 完整的 level 2 委托列表
# "orderBook10", //  前10层的委托列表，用传统的完整委托列表推送
# "publicNotifications", // 全系统的告示（用于段时间的消息）
# "quote",       // 最高层的委托列表
# "quoteBin1m",  // 每分钟报价数据
# "quoteBin5m",  // 每5分钟报价数据
# "quoteBin1h",  // 每个小时报价数据
# "quoteBin1d",  // 每天报价数据
# "settlement",  // 结算信息
# "trade",       // 实时交易
# "tradeBin1m",  // 每分钟交易数据
# "tradeBin5m",  // 每5分钟交易数据
# "tradeBin1h",  // 每小时交易数据
# "tradeBin1d",  // 每天交易数据

# 下列主题要求进行身份验证︰
# "affiliate",   // 邀请人状态，已邀请用户及分红比率
# "execution",   // 个别成交，可能是多个成交
# "order",       // 你委托的更新
# "margin",      // 你账户的余额和保证金要求的更新
# "position",    // 你仓位的更新
# "privateNotifications", // 个人的通知，现时并未使用
# "transact"     // 资金提存更新
# "wallet"       // 比特币余额更新及总提款存款

"""
import websocket
import threading
import traceback
from time import sleep
import json
import logging
import urllib
from util.api_key import generate_nonce, generate_signature
from collections import defaultdict


class TableNoAuth:
    announcement = "announcement"  # 网站公告
    chat = "chat"  # Trollbox 聊天室
    connected = "connected"  # 已连接用户/机器人的统计数据
    funding = "funding"  # 掉期产品的资金费率更新 每个资金时段发送（通常是8小时）
    instrument = "instrument"  # 产品更新，包括交易量以及报价
    insurance = "insurance"  # 每日保险基金的更新
    liquidation = "liquidation"  # 进入委托列表的强平委托
    orderBookL2 = "orderBookL2"  # 完整的 level 2 委托列表
    orderBook10 = "orderBook10"  # 前10层的委托列表，用传统的完整委托列表推送
    publicNotifications = "publicNotifications"  # 全系统的告示（用于段时间的消息）
    quote = "quote"  # 最高层的委托列表
    quoteBin1m = "quoteBin1m"  # 每分钟报价数据
    quoteBin5m = "quoteBin5m"  # 每5分钟报价数据
    quoteBin1h = "quoteBin1h"  # 每个小时报价数据
    quoteBin1d = "quoteBin1d"  # 每天报价数据
    settlement = "settlement"  # 结算信息
    trade = "trade"  # 实时交易
    tradeBin1m = "tradeBin1m"  # 每分钟交易数据
    tradeBin5m = "tradeBin5m"  # 每5分钟交易数据
    tradeBin1h = "tradeBin1h"  # 每小时交易数据
    tradeBin1d = "tradeBin1d"  # 每天交易数据


class TableAuth:
    affiliate = "affiliate"  # 邀请人状态，已邀请用户及分红比率
    execution = "execution"  # 个别成交，可能是多个成交
    order = "order"  # 你委托的更新
    margin = "margin"  # 你账户的余额和保证金要求的更新
    position = "position"  # 你仓位的更新
    privateNotifications = "privateNotifications"  # 个人的通知，现时并未使用
    transact = "transact"  # 资金提存更新
    wallet = "wallet"  # 比特币余额更新及总提款存款


DEFAULT_SUBSCRIPTIONS = [
    TableNoAuth.quote, TableNoAuth.tradeBin1d, TableNoAuth.tradeBin5m, TableNoAuth.tradeBin1h, TableNoAuth.tradeBin1d]


# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.
class BitMexWS:
    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self, endpoint, api_key=None, api_secret=None, subscriptions=DEFAULT_SUBSCRIPTIONS):
        """Connect to the websocket and initialize data stores."""
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")
        self.endpoint = endpoint
        self.table_handler = defaultdict(lambda: defaultdict(dict))
        self.ws = None

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.keys = {}
        self.exited = False
        self.subscriptions = subscriptions

    def connect(self):
        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        ws_url = self.__get_url()
        self.logger.info("Connecting to %s" % ws_url)
        self.__connect(ws_url)
        self.logger.info('Connected to WS.')

        # Connected. Wait for partials
        # self.__wait_for_symbol(symbol)
        if self.api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def exit(self):
        """Call this to exit - will close websocket."""
        self.exited = True
        try:
            if self.ws is not None:
                self.ws.close()
        except:
            self.logger.exception('ws.close() Error')

    def register_handler(self, name, handler, table='quoteBin1m', actions={'partial', 'insert'}):
        """向ws messsage 事件中注册事件处理具备"""
        for action in actions:
            self.table_handler[table][action][name] = handler
            logging.info('注册事件句柄 %s -> %s -> %s', table, action, name)

    #
    # End Public Methods
    #

    def __connect(self, ws_url):
        """Connect to the websocket in a thread."""
        self.logger.debug("Starting thread")
        import bitmex_websocket
        self.ws = websocket.WebSocketApp(ws_url,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException("Couldn't connect to WS! Exiting.")

    def __get_auth(self):
        """Return auth headers. Will use API Keys if present in settings."""
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            nonce = generate_nonce()
            return [
                "api-nonce: " + str(nonce),
                "api-signature: " + generate_signature(self.api_secret, 'GET', '/realtime', nonce, ''),
                "api-key:" + self.api_key
            ]
        else:
            self.logger.info("Not authenticating.")
            return []

    def __get_url(self):
        """
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        """
        url_parts = list(urllib.parse.urlparse(self.endpoint))
        url_parts[0] = url_parts[0].replace('http', 'ws')
        url_parts[2] = "/realtime?subscribe={}".format(','.join(self.subscriptions))
        return urllib.parse.urlunparse(url_parts)

    def __wait_for_account(self):
        """On subscribe, this data will come down. Wait for it."""
        # Wait for the keys to show up from the ws
        # while not {'margin', 'position', 'order', 'orderBookL2'} <= set(self.data):
        while not {'orderBookL2'} <= set(self.data):
            sleep(0.1)

    def __wait_for_symbol(self):
        """On subscribe, this data will come down. Wait for it."""
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            sleep(0.1)

    def __send_command(self, command, args=None):
        """Send a raw command."""
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))

    def __on_message(self, message):
        """Handler for parsing WS messages."""
        message = json.loads(message)
        # self.logger.debug(json.dumps(message))

        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None
        try:
            if 'subscribe' in message:
                self.logger.debug("Subscribed to %s.", message['subscribe'])
            elif action:

                if table not in self.data:
                    self.data[table] = []

                data = message['data']
                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                if action == 'partial':
                    self.logger.debug("%s: partial", table)
                    self.data[table] += data
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.
                    self.keys[table] = message['keys']
                elif action == 'insert':
                    self.logger.debug('%s: inserting %s', table, data)
                    self.data[table] += data

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBookL2'] and len(self.data[table]) > BitMexWS.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][int(BitMexWS.MAX_TABLE_LEN / 2):]

                elif action == 'update':
                    self.logger.debug('%s: updating %s', table, data)
                    # Locate the item in the collection and update it.
                    for updateData in data:
                        item = findItemByKeys(self.keys[table], self.data[table], updateData)
                        if not item:
                            return  # No item found to update. Could happen before push
                        item.update(updateData)
                        # Remove cancelled / filled orders
                        if table == 'order' and item['leavesQty'] <= 0:
                            self.data[table].remove(item)
                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, data))
                    # Locate the item in the collection and remove it.
                    for deleteData in data:
                        item = findItemByKeys(self.keys[table], self.data[table], deleteData)
                        self.data[table].remove(item)
                else:
                    raise Exception("Unknown action: %s" % action)

                # 处理事件句柄
                for name, handler in self.table_handler[table][action].items():
                    self.logger.debug('%s->%s->%s', table, action, name)
                    for data_dic in data:
                        handler(data_dic)
        except:
            self.logger.error(traceback.format_exc())

    def __on_error(self, ws, error):
        """Called on fatal websocket errors. We exit on these."""
        if not self.exited:
            self.logger.error("Error : %s" % error)
            raise websocket.WebSocketException(error)

    def __on_open(self, ws):
        """Called when the WS opens."""
        self.logger.debug("Websocket Opened.")

    def __on_close(self, ws):
        """Called on websocket close."""
        self.logger.info('Websocket Closed')


# Utility method for finding an item in the store.
# When an update comes through on the websocket, we need to figure out which item in the array it is
# in order to match that item.
#
# Helpfully, on a data push (or on an HTTP hit to /api/v1/schema), we have a "keys" array. These are the
# fields we can use to uniquely identify an item. Sometimes there is more than one, so we iterate through all
# provided keys.
def findItemByKeys(keys, table, matchData):
    for item in table:
        matched = True
        for key in keys:
            if item[key] != matchData[key]:
                matched = False
                break
        if matched:
            return item
