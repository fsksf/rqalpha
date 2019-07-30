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
import pandas as pd
import numpy as np

from rqalpha.data import risk_free_helper


class YieldCurveStore(object):
    def __init__(self, f):
        """
        ctable((3644,), [('date', '<u4'), ('S0', '<f8'), ('M1', '<f8'), ('M2', '<f8'), ('M3', '<f8'), ('M6', '<f8'), ('M9', '<f8'), ('Y1', '<f8'), ('Y2', '<f8'), ('Y3', '<f8'), ('Y4', '<f8'), ('Y5', '<f8'), ('Y6', '<f8'), ('Y7', '<f8'), ('Y8', '<f8'), ('Y9', '<f8'), ('Y10', '<f8'), ('Y15', '<f8'), ('Y20', '<f8'), ('Y30', '<f8'), ('Y40', '<f8'), ('Y50', '<f8')])
          nbytes: 612.08 KB; cbytes: 611.23 KB; ratio: 1.00
          cparams := cparams(clevel=5, shuffle=1, cname='lz4', quantize=0)
          rootdir := 'yield_curve.bcolz'
        [ (20050104, 0.027393, 0.027712, 0.028029, 0.028344, 0.029278, 0.030195, 0.031095, 0.034527, 0.037687, 0.040577, 0.043195, 0.045542, 0.047618, 0.049423, 0.050958, 0.052221, 0.05447, 0.049944, 0.020563, nan, nan)
         (20050105, 0.025691, 0.025962, 0.026232, 0.026501, 0.0273, 0.028089, 0.028868, 0.031881, 0.03473, 0.037415, 0.039936, 0.042293, 0.044487, 0.046516, 0.048382, 0.050084, 0.056134, 0.058088, 0.049705, nan, nan)
         (20050106, 0.024908, 0.02521, 0.02551, 0.025809, 0.026699, 0.027577, 0.028443, 0.031788, 0.034943, 0.037908, 0.040683, 0.043268, 0.045663, 0.047867, 0.049882, 0.051707, 0.057979, 0.0595, 0.048289, nan, nan)
         ...,
         (20190722, 0.023523, 0.02143, 0.021483, 0.021948, 0.02311, 0.025364, 0.026386, 0.027843, 0.028908, nan, 0.02999, nan, 0.031724, nan, nan, 0.031484, 0.034886, 0.035242, 0.038375, 0.038911, 0.039277)
         (20190723, 0.021023, 0.021111, 0.021198, 0.022086, 0.023121, 0.025291, 0.026302, 0.027747, 0.028845, nan, 0.029919, nan, 0.031699, nan, nan, 0.031484, 0.034843, 0.035199, 0.038288, 0.038856, 0.039253)
         (20190724, 0.020213, 0.02131, 0.021475, 0.021882, 0.023078, 0.025317, 0.026317, 0.027752, 0.029024, nan, 0.030098, nan, 0.0318, nan, nan, 0.03171, 0.035062, 0.035418, 0.038501, 0.038987, 0.039303)]
        """
        self._table = bcolz.open(f, 'r')
        self._dates = self._table.cols['date'][:]

    def get_yield_curve(self, start_date, end_date, tenor):
        # 截取数据片段
        d1 = start_date.year * 10000 + start_date.month * 100 + start_date.day      # 20190101
        d2 = end_date.year * 10000 + end_date.month * 100 + end_date.day

        s = self._dates.searchsorted(d1)
        e = self._dates.searchsorted(d2, side='right')

        if e == len(self._dates):
            e -= 1
        if self._dates[e] == d2:
            # 包含 end_date
            e += 1

        if e < s:
            return None

        df = pd.DataFrame(self._table[s:e])
        df.index = pd.Index(pd.Timestamp(str(d)) for d in df['date'])
        del df['date']

        df.rename(columns=lambda n: n[1:]+n[0], inplace=True)
        """
               date   open  close   high    low   volume  total_turnover  limit_up  limit_down
        0  20050104  33400  34200  34600  32000   289500          974110     37200       30400
        1  20050105  34000  35400  36000  33600   319402         1122529     37600       30800
        2  20050106  35900  35200  36000  35000   296980         1050549     38900       31900
        3  20050107  35400  36600  37200  34600   462410         1665259     38700       31700
        ...
        """
        if tenor is not None:
            return df[tenor]
        return df

    def get_risk_free_rate(self, start_date, end_date):
        # 将start_date,end_date之间天数转换为 0S， 1M， 2M，1Y, 2Y, 10Y...
        tenor = risk_free_helper.get_tenor_for(start_date, end_date)
        tenor = tenor[-1] + tenor[:-1]          # Y1
        d = start_date.year * 10000 + start_date.month * 100 + start_date.day   # 20190101
        pos = self._dates.searchsorted(d)
        if pos > 0 and (pos == len(self._dates) or self._dates[pos] != d):
            pos -= 1

        col = self._table.cols[tenor]
        while pos >= 0 and np.isnan(col[pos]):
            # data is missing ...
            pos -= 1

        return self._table.cols[tenor][pos]
