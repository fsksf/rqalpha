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

import bcolz
import numpy as np


class DividendStore(object):
    """
    红利；股息；股利
    ctable((29318,), [('announcement_date', '<u4'), ('closure_date', '<u4'), ('ex_date', '<u4'), ('payable_date', '<u4'), ('cash_before_tax', '<u4'), ('round_lot', '<u4')])
      nbytes: 687.14 KB; cbytes: 417.11 KB; ratio: 1.65
      cparams := cparams(clevel=5, shuffle=1, cname='lz4', quantize=0)
      rootdir := 'original_dividends.bcolz'
    [(19940615, 19940629, 19940630, 19940630, 10000, 10)
     (19950721, 19950801, 19950802, 19950804, 20000, 10)
     (19960828, 19960830, 19960902, 19960906, 3000, 10) ...,
     (20070327, 20070330, 20070402, 20070402, 28000, 10)
     (20181217, 20181220, 20181221, 20181226, 55000, 10)
     (20190614, 20190619, 20190620, 20190625, 25000, 10)]
    """
    def __init__(self, f):
        ct = bcolz.open(f, 'r')
        self._index = ct.attrs['line_map']
        self._table = np.empty((len(ct), ), dtype=np.dtype([
            ('announcement_date', '<u4'), ('book_closure_date', '<u4'),
            ('ex_dividend_date', '<u4'), ('payable_date', '<u4'),
            ('dividend_cash_before_tax', np.float), ('round_lot', '<u4')
        ]))
        self._table['announcement_date'][:] = ct['announcement_date']                   # 公告日期
        self._table['book_closure_date'][:] = ct['closure_date']                        # 登记日期
        self._table['ex_dividend_date'][:] = ct['ex_date']                              # 除权日
        self._table['payable_date'][:] = ct['payable_date']                             # 派息日
        self._table['dividend_cash_before_tax'] = ct['cash_before_tax'][:] / 10000.0    # 税前现金？？？
        self._table['round_lot'][:] = ct['round_lot']                                   # 交易单位

    def get_dividend(self, order_book_id):
        try:
            s, e = self._index[order_book_id]
        except KeyError:
            return None

        return self._table[s:e]
