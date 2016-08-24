# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 18:44:06 2016

@author: yiyuezhuo
"""

from data import get_local


import numpy as np
import scipy
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd


sh = get_local('sh')
c600875 = get_local('600875') # 东方电气
c600283 = get_local('600283') # 钱江水利
c002674 = get_local('002674') # 兴业科技
c002509 = get_local('002509') # 天广中茂
c300033 = get_local('300033') # 同花顺
c300187 = get_local('300187') # 永清环保

base_rate = 0.025 # 基准/无风险利率，2015前半年的定期存款利率

close_df = pd.DataFrame({'sh'     : sh['close'],
                         'c600875' : c600875['close'],
                         'c600283' : c600283['close']})
                         
percent_df = close_df.resample('1M').agg(lambda s: s.head(1).ix[0]/s.tail().ix[0]) - 1
excess_df = percent_df['2015'] - base_rate