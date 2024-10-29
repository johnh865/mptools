# -*- coding: utf-8 -*-
import numpy as np 
import mptools
import os
from os.path import dirname, join, splitext
from io import StringIO
import time


DIRPATH = dirname(__file__)
FPATH = splitext(__file__)[0]
DATA_DIR = FPATH + '_data'

# %%
def create_dummy_files(num: int):
    # num = 200
    print('Writing dummy files...')
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    for ii in range(num):
    
        x = np.linspace(0, 1, 200)
        y = np.linspace(0, 1, 200)
        xg, yg = np.meshgrid(x, y)
        zg = xg * yg ** 2
        name = f'data-{ii}.dat'
        path = join(DATA_DIR, name)
        np.savetxt(path, zg)
        
    
# create_dummy_files()
        
# %%
        
def read(ii: int):
    name = f'data-{ii}.dat'
    path = join(DATA_DIR, name)
    print('1. reading', ii, flush=True)        
    # print('reading', ii, flush=False)        
    return np.genfromtxt(path)
    
        
def process(data: np.ndarray):
    out = ((data**2) / 0.8) ** 0.654
    print('2. processing', flush=True)        
    # print('processing', flush=False)      
    # time.sleep(0.25)
    
    
    file = StringIO()
    np.savetxt(file, out)
    string = file.getvalue()
    return string

    
def write(ii, string):
    name = f'output-{ii}.dat'
    path = join(DATA_DIR, name)        
    print('3. writing', ii, flush=True)        
    # print('writing', ii, flush=False)        

    # return np.savetxt(path, data)
    with open(path, 'w') as file:
        file.write(string)


if __name__ == '__main__':
    num = 400
    # create_dummy_files(num)
    
    # %%
    
    args = np.arange(num)
    # for ii in args:
    #     print('iteration', ii)
    #     data = read(ii)
    #     data2 = process(data)
    #     write(ii, data2)
    
    mptools.mp_read_write(
        args = args,
        f_in=read, 
        f_proc=process,
        f_out=write,
        )