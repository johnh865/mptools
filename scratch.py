# -*- coding: utf-8 -*-

import timeit
import numpy as np
# from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Any
from multiprocessing import Pool, freeze_support
import tqdm 
from tqdm.contrib.concurrent import process_map  # or thread_map






def _testfun1(x):
    out = np.ones((1000, 100)) * x**2


def test1():
    nprocs = 10
    x = np.arange(1e5)
    with Pool(processes=nprocs) as p:
        iter1 = p.imap(_testfun1, x)
        for ii in iter1:
            pass
            
        
        
        
def test2():
    nprocs = 10
    x = np.arange(1e5)
    with Pool(processes=nprocs) as p:
        p.map(_testfun1, x)
        # list(p.imap(_testfun1, x))
        
if __name__ == '__main__':
    t1 = timeit.default_timer()
    test1()
    t2 = timeit.default_timer()
    print(t2 - t1)

    t1 = timeit.default_timer()
    test2()
    t2 = timeit.default_timer()
    print(t2 - t1)
