# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 17:27:47 2016

@author: yiyuezhuo
"""

import tushare as ts
import pandas as pd
import os
import time
import csv

#df = ts.get_hist_data('600848') #一次性获取全部数据

def read_csv(name):
    return pd.read_csv( name, 
                        encoding = 'gbk', 
                        dtype={'code' : str},
                        index_col = 0)


def use_cache(func):
    def checked(path, *args, **kwargs):
        if os.path.isfile(path):
            print('skip {}'.format(path))
        else:
            r = func(path, *args, **kwargs)
            print('write -> {}'.format(path))
            return r
    return checked
            
def use_sleep(interval):
    def _use_sleep(func):
        def __use_sleep(*args, **kwargs):
            r = func(*args, **kwargs)
            time.sleep(interval)
            return r
        return __use_sleep
    return _use_sleep

@use_cache
@use_sleep(1)
def code_to_file(path, code):
    df = ts.get_hist_data(code)
    df.to_csv(path)

def code_mapping(code_list, 
                 root = 'hist_data'):
    for code in code_list:
        name = code + '.csv'
        path = os.path.join(root, name)
        code_to_file(path, code)

def share_download(all_file = '2016-8-19-all.csv',
                   root = 'hist_data'):
    df = read_csv(all_file)
    _,code_list = zip(*df['code'].items())
    code_mapping(code_list, root = root)
    
def index_download(root = 'hist_data'):
    code_list = ['sh','sz','hs300','sz50','zxb','cyb']
    # （sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
    code_mapping(code_list, root = root)
    
def get_local(code, root = 'hist_data'):
    path = os.path.join(root,code+'.csv')
    df = read_csv(path)
    df.index = pd.DatetimeIndex(df.index)
    return df
    
def get_locals(code_list, root = 'hist_data'):
    return {code:get_local(code) for code in code_list}
