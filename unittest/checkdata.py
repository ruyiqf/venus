#coding:GBK
import sys
import os
import scipy.io as sio
import pandas as pd
import numpy as np
import json
import click
from pandas import Series, DataFrame

#CHECKDATA_DIR='../data/checkdata/'
DEVIATION=0.0005
@click.group()
@click.option('-v', '--verbose', count=True)
@click.pass_context
def cli(ctx, verbose):
    ctx.obj['VERBOSE'] = verbose

@cli.command()
@click.option('-h', '--help')
@click.option('-std', '--standard-file', type=click.Path(exists=True))
@click.option('-f', '--new-file', type=click.Path(exists=True))
@click.option('-an', '--array-name', type=click.STRING)
def main(**kwargs):
    print(kwargs)
    stdfile = kwargs['standard_file']
    newfile = kwargs['new_file']
    arrayname = kwargs['array_name']
    cdc = CheckDataCorrection()
    cdc.compare_two_tdays_file(stdfile, newfile, arrayname)
    
class CheckDataCorrection(object):
    
    def __init__(self):
        pass

    def compare_two_tdays_file(self, stdfile, newfile, arrayname):
        """比较两个文件定制部分是否相同
        :stdfile: 比较的标准文件 
        :newfile: 被比较文件
        :arrayname: 提取的数组名字
        """
        std_array = sio.loadmat(stdfile)[arrayname]
        new_array = sio.loadmat(newfile)[arrayname]
        row = min(np.shape(std_array)[0], np.shape(new_array)[0])
        col = min(np.shape(std_array)[1], np.shape(new_array)[1])
        partial_array = std_array[3139:row, 0:col] - new_array[3139:row, 0:col]
        df = DataFrame(partial_array)
        df = df.fillna(0)
        partial_array = np.array(df)
        position = np.where(abs(partial_array) > DEVIATION)
        if (position[0].size == 0):
            print('Two parital array is correct')
        else:
            print(position)

if __name__ == '__main__':
    main()

