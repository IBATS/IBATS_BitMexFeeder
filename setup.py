#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/14 16:07
@File    : setup.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding='utf-8') as rm:
    long_description = rm.read()

setup(name='IBATS_BitMex_Feeder',
      version='0.2.11',
      description='连接BitMex交易所，获取实时行情、历史行情，保存到mysql数据库同时redis广播，供 ABAT 交易平台进行策略回测、分析，交易使用',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='MG',
      author_email='mmmaaaggg@163.com',
      url='https://github.com/IBATS/IBATS_BitMexFeeder',
      packages=find_packages(),
      python_requires='>=3.6',
      classifiers=(
          "Programming Language :: Python :: 3 :: Only",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Operating System :: Microsoft :: Windows",
          "Operating System :: Unix",
          "Operating System :: POSIX",
          "License :: OSI Approved :: MIT License",
          "Development Status :: 5 - Production/Stable",
          "Environment :: No Input/Output (Daemon)",
          "Intended Audience :: Developers",
          "Natural Language :: Chinese (Simplified)",
          "Topic :: Software Development",
      ),
      install_requires=[
          'websocket',
          'msgpack>=0.5.6',
          'bitmex',
          'bitmex-ws',
          'IBATS_Common',
          'mysqlclient>=1.3.8',
          'numpy==1.14.4',
          'pandas==0.23.0',
          'prodconpattern==0.1.1',
          'redis==2.10.6',
          'SQLAlchemy==1.2.8',
          'xlrd==1.1.0',
      ])
