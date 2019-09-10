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
import six

from rqalpha.utils.i18n import gettext as _


class DayBarStore(object):
    def __init__(self, main, converter):
        """
        获取day bar 的抽象层
        :param main: 数据path
        :param converter: 对应的数据转换模型
        """
        self._table = bcolz.open(main, 'r')
        self._index = self._table.attrs['line_map']
        self._converter = converter

    @staticmethod
    def _remove_(l, v):
        try:
            l.remove(v)
        except ValueError:
            pass

    def get_bars(self, order_book_id, fields=None):
        """
        获取单个symbol的整个历史
        :param order_book_id:
        :param fields:
        :return:
        """
        try:
            s, e = self._index[order_book_id]
        except KeyError:
            six.print_(_(u"No data for {}").format(order_book_id))
            return

        if fields is None:
            # the first is date
            fields = self._table.names[1:]

        if len(fields) == 1:
            return self._converter.convert(fields[0], self._table.cols[fields[0]][s:e])

        # remove datetime if exist in fields
        self._remove_(fields, 'datetime')

        dtype = np.dtype([('datetime', np.uint64)] +
                         [(f, self._converter.field_type(f, self._table.cols[f].dtype))
                          for f in fields])
        result = np.empty(shape=(e - s, ), dtype=dtype)
        for f in fields:
            result[f] = self._converter.convert(f, self._table.cols[f][s:e])
        result['datetime'] = self._table.cols['date'][s:e]
        result['datetime'] *= 1000000

        return result

    def get_date_range(self, order_book_id):
        s, e = self._index[order_book_id]
        return self._table.cols['date'][s], self._table.cols['date'][e - 1]
