# -*- coding: utf-8 -*-

from multiprocessing import Pool, Process
import time


def f1():
    
    raise ValueError()
    
    
if __name__ == '__main__':
    p = Process(target=f1)
    p.start()
    p.join()