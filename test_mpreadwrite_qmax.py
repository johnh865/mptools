# -*- coding: utf-8 -*-
import time 
from io import StringIO
import logging

from mptools import MPReadWrite

# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

def f_in(x):
    print('input', x, flush=True)
    return x



def f_proc(x):
    time.sleep(100)
    return x**2



class Output:
    """Test output, dump output to StringIO"""
    def __init__(self):
        
        self.io = StringIO()
        
        # file = open('test1412.txt', 'w')
        # self.io = file
        
    def __call__(self, x):
        string = f'{x}\n'
        print('----')
        print(string)
        self.io.write(string)



args = range(20)
f_out = Output()


if __name__ == '__main__':
    mp = MPReadWrite(args, f_in=f_in, f_proc=f_proc, f_out=f_out, input_qmax=5)
    
    mp.progress_bar()