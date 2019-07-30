# -*- coding: utf-8 -*-
#
# Copyright 2019 Ricequant, Inc
#
# * Commercial Usage: please contact public@ricequant.com
# * Non-Commercial Usage:
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import pickle

from rqalpha.model.instrument import Instrument


class InstrumentStore(object):
    """
    股票信息模型
     {'order_book_id': '002177.XSHE',
      'industry_code': 'C35',
      'market_tplus': 1,
      'symbol': '御银股份',
      'special_type': 'Normal',
      'exchange': 'XSHE',
      'status': 'Active',
      'type': 'CS',
      'de_listed_date': '0000-00-00',
      'listed_date': '2007-11-01',
      'sector_code_name': '信息技术',
      'abbrev_symbol': 'YYGF',
      'sector_code': 'InformationTechnology',
      'round_lot': 100,
      'trading_hours': '09:31-11:30,13:01-15:00',
      'board_type': 'SMEBoard',
      'industry_name': '专用设备制造业'},
    """
    def __init__(self, f):
        with open(f, 'rb') as store:
            d = pickle.load(store)
        self._instruments = [Instrument(i) for i in d]

    def get_all_instruments(self):
        return self._instruments
