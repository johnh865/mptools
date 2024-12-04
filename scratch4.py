# -*- coding: utf-8 -*-

from multiprocessing import Pool
import time

def f(x):
    print('hello', flush=True)
    return x*x

if __name__ == '__main__':
    with Pool(processes=4) as pool:         # start 4 worker processes
        result = pool.apply_async(f, (10,)) # evaluate "f(10)" asynchronously in a single process
  
    
        print(result.get(timeout=1))        # prints "100" unless your computer is *very* slow
