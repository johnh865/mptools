# -*- coding: utf-8 -*-
from multiprocessing import freeze_support
import mptools
from mptools import tqdm_map, tqdm_starmap, tqdm_starwrite
from io import StringIO



def _testfun2(x, y, z):
    return x + y + z


def _testfun1(x):
    return x*2


def test1():    
    freeze_support()
    import numpy as np

    
    args1 = np.arange(10)
    args2 = list(np.ones((10, 3)))
    
    tqdm_map(_testfun1, args1, nprocs=4)
    tqdm_starmap(_testfun2, args2, nprocs=5)
    # 
    
    
    
    
def _writer(x):
    
    
    string_io = StringIO()
    
    
    import numpy as np
    array = np.linspace(0, x, 5000)[:, None] * np.arange(30)
    array = array.sum()
    test = f'''------------------
    hellloooooo boboboboobo 
    yup  ;laksjgl;kdjg;lsdjgdk;gslkjg;kjl
    {array}
    -------------------------------------------
    '''
    # np.savetxt(string_io, array)
    # string_io.seek(0)
    # s1 = string_io.read()
    return test


def test_mp_write():
    
    x = range(200000)
    fname = 'test1_data/test_mp_write.txt'
    with open(fname, 'w') as file:
        tqdm_starwrite(_writer, x, file=file, nprocs=18, chunksize=1000)
    
    

def test_mp_write2():
    x = range(20)
    fname = '/test1_datatest_mp_write3.txt'
    with open(fname, 'w') as file:    
        mptools.mp_write(_writer, x, file=file, nprocs=8)
        file.write('done')
        
        

    
def test2():
    fname = 'test1_data/test_mp_write2.txt'
    string = 'lskadfhp 8ifye098aey098eyfnpas9d8fuasd9pfnds9f8pudspfndsfd\n'
    with open(fname, 'w') as file:
        for ii in range(100000):
            file.write(string)
    
    
if __name__ == '__main__':
    # test1()
    test_mp_write()
    # test2()
    # test_mp_write2()