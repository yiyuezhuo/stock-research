# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 17:59:37 2016

@author: yiyuezhuo
"""

import pandas as pd
import numpy as np

import os
import sqlite3
import zipfile

'''
con = sqlite3.connect('test.db')

z = zipfile.ZipFile(r'E:\迅雷下载\share_data\h12gwg3p.zip','r')
with z.open('TRD_Dalyr.csv') as csvFile:
    df = pd.read_csv(csvFile,sep='\t',encoding='utf-16')
'''
def to_database(root, 
                db_name='share.db',
                table_name = 'price',
                csv_name = 'TRD_Dalyr.csv',
                verbose = True):
    
    con = sqlite3.connect(db_name)
    
    name_list = [name for name in os.listdir(root) if os.path.splitext(name)[1] == '.zip']
    size = len(name_list)
    
    for i,name in enumerate(name_list):
        
        path = os.path.join(root,name)
        z = zipfile.ZipFile(path,'r')
        with z.open(csv_name) as csvFile:
            df = pd.read_csv(csvFile, 
                             sep = '\t', 
                             encoding = 'utf-16')
                             
            if verbose:
                print('[{}/{}]: {} -> Python'.format(i, size, path))
            
            df.to_sql(table_name, 
                      con, 
                      if_exists = 'append',
                      dtype={'Stkcd':'TEXT'})
        
        if verbose:
            print('[{}/{}]: {} -> {} :: {}'.format(i, size , path, db_name, table_name))
            
    con.close()
    
def check(db_name = 'share.db',
          table_name = 'price'):
    con = sqlite3.connect(db_name)
    result = con.execute("PRAGMA table_info(price)").fetchall()
    con.close()
    return result
    
'''
Stkcd [证券代码] - 以上交所、深交所公布的证券代码为准
Trddt [交易日期] - 以YYYY-MM-DD表示
Opnprc [日开盘价] - A股以人民币元计，上海B以美元计，深圳B以港币计
Hiprc [日最高价] - A股以人民币元计，上海B以美元计，深圳B以港币计
Loprc [日最低价] - A股以人民币元计，上海B以美元计，深圳B以港币计
Clsprc [日收盘价] - A股以人民币元计，上海B以美元计，深圳B以港币计
Dnshrtrd [日个股交易股数] - 0=没有交易量
Dnvaltrd [日个股交易金额] - 计量货币：人民币元。A股以人民币元计，上海B以美元计，深圳B以港币计，0=没有交易量
Dsmvosd [日个股流通市值] - 个股的流通股数与收盘价的乘积，A股以人民币元计，上海B股以美元计，深圳B股以港币计
Dsmvtll [日个股总市值] - 个股的发行总股数与收盘价的乘积，A股以人民币元计，上海B股以美元计，深圳B股以港币计
Dretwd [考虑现金红利再投资的日个股回报率] - 上市首日的前收盘价取招股价,字段说明见“回报率计算方法”
Dretnd [不考虑现金红利的日个股回报率] - 上市首日的前收盘价取招股价,字段说明见“回报率计算方法”
Adjprcwd [考虑现金红利再投资的收盘价的可比价格] - A股以人民币元计，上海B以美元计，深圳B以港币计，去除由于时间间隔和股本变动原因引起变化的以上市首日为基准的经过调整后的收盘价。
Adjprcnd [不考虑现金红利的收盘价的可比价格] - A股以人民币元计，上海B以美元计，深圳B以港币计，去除由于时间间隔和股本变动原因引起变化的以上市首日为基准的经过调整后的收盘价。
Markettype [市场类型] - 1=上海A，2=上海B，4=深圳A，8=深圳B,  16=创业板
Capchgdt [最新股本变动日期] - 上市公司股本最近一次发生变化的日期
Trdsta [交易状态] - 1=正常交易，2=ST，3＝*ST，4＝S（2006年10月9日及之后股改未完成），5＝SST，6＝S*ST，7=G（2006年10月9日之前已完成股改），8=GST，9=G*ST，10=U（2006年10月9日之前股改未完成），11=UST，12=U*ST，13=N，14=NST，15=N*ST，16=PT

[(0, 'index', 'INTEGER', 0, None, 0),
 (1, 'Stkcd', 'TEXT', 0, None, 0),
 (2, 'Trddt', 'TEXT', 0, None, 0),
 (3, 'Opnprc', 'REAL', 0, None, 0),
 (4, 'Hiprc', 'REAL', 0, None, 0),
 (5, 'Loprc', 'REAL', 0, None, 0),
 (6, 'Clsprc', 'REAL', 0, None, 0),
 (7, 'Dnshrtrd', 'INTEGER', 0, None, 0),
 (8, 'Dnvaltrd', 'REAL', 0, None, 0),
 (9, 'Dsmvosd', 'REAL', 0, None, 0),
 (10, 'Dsmvtll', 'REAL', 0, None, 0),
 (11, 'Dretwd', 'REAL', 0, None, 0),
 (12, 'Dretnd', 'REAL', 0, None, 0),
 (13, 'Adjprcwd', 'REAL', 0, None, 0),
 (14, 'Adjprcnd', 'REAL', 0, None, 0),
 (15, 'Markettype', 'INTEGER', 0, None, 0),
 (16, 'Capchgdt', 'TEXT', 0, None, 0),
 (17, 'Trdsta', 'INTEGER', 0, None, 0)]
'''