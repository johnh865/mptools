# -*- coding: utf-8 -*-
import numpy as np 
import mptools
import os
from os.path import dirname, join, splitext
from io import StringIO
import time
from glob import glob
import sys
import tqdm


DIRPATH = dirname(__file__)
FPATH = splitext(__file__)[0]
DATA_DIR = FPATH + '_data'


class Capturing(list):
    """Capture stdout.
    https://stackoverflow.com/questions/16571150/how-to-capture-stdout-output-from-a-python-function-call"""
    
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout
        
        
        
        

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
    return ii, np.genfromtxt(path)
    
        
def process(ii, data: np.ndarray):
    # time.sleep(0.5)
    out = ((data**2) / 0.8) ** 0.654
    print('2. processing', flush=True)        
    # print('processing', flush=False)      
    # time.sleep(0.25)
    
    file = StringIO()
    np.savetxt(file, out)
    string = file.getvalue()
    return ii, string

    
def write(ii, string):
    name = f'output-{ii}.dat'
    path = join(DATA_DIR, name)        
    print('3. writing', ii, flush=True)        
    # print('writing', ii, flush=False)        

    # return np.savetxt(path, data)
    with open(path, 'w') as file:
        file.write(string)


def test_mp_readwrite():
    num = 30
    create_dummy_files(num)
    
    # %%
    
    args = np.arange(num)
    # for ii in args:
    #     print('iteration', ii)
    #     data = read(ii)
    #     data2 = process(data)
    #     write(ii, data2)
    
    mptools.MPReadWrite(
        args = args,
        f_in=read, 
        f_proc=process,
        f_out=write,
        pstar=True, ostar=True,
        ).run()
    
    dat_files = glob('data-*.dat', root_dir=DATA_DIR)
    out_files = glob('output-*.dat', root_dir=DATA_DIR)

    # Count number of input and output files. Make sure correct.    
    print('Checking correct number of files generated')
    assert len(dat_files) == num
    assert len(out_files) == num
    
    print('Check that calculations are correct')
    for fdat1, fout1 in zip(dat_files, out_files):
        
        p_dat = join(DATA_DIR, fdat1)
        p_out = join(DATA_DIR, fout1)
        
        dat1 = np.genfromtxt(p_dat)
        out1 = np.genfromtxt(p_out)
        
        calc = ((dat1**2) / 0.8) ** 0.654
        
        assert np.all(np.isclose(calc, out1))
    
    # Clean up test
    print('Clean up test')
    for file in dat_files + out_files:
        path = join(DATA_DIR, file)
        os.remove(path)
    
    
# def test_mp_readwrite_class():
#     num = 30
#     # create_dummy_files(num)

#     args = np.arange(num)
#     mprw = mptools.MPReadWrite(args=args, 
#                                  f_in=read, 
#                                  f_proc=process, 
#                                  f_out=write,
#                                  processes=2,
#                                  pstar=True, ostar=True)
    
#     mprw.progress_bar()
#     # list(mprw)
    # m
    
    
if __name__ == '__main__':
    test_mp_readwrite()
    # test_mp_readwrite_class()