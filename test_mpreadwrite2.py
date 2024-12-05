# -*- coding: utf-8 -*-
# from mptools import MPReadWrite
from mpreadwrite import MPReadWrite
import mptools
# mptools.disable()
from io import StringIO

import logging
from multiprocessing import Queue

# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)


def process(x, y,):
    z = x + y
    return x, y , z
    
    
class Output:
    """Test output, dump output to StringIO"""
    def __init__(self):
        
        self.io = StringIO()
        
        # file = open('test1412.txt', 'w')
        # self.io = file
        
    def __call__(self, x, y, z):
        string = f'{x},{y},{z}\n'
        print('----')
        print(string)
        self.io.write(string)



# class Output:
#     def __init__(self):
#         pass
    
#     def __call__(self, queue: Queue):
#         logger.debug('f output start')
#         io1 = StringIO()
#         self.io = io1
             
#         while True: 
#             logger.debug('f output get')
    
#             x, y, z = queue.get()
            
#             logger.debug('f output retrieved.')
#             string = f'{x},{y},{z}\n'
#             print('----')
#             print(string)
#             io1.write(string)    
#             yield
            

# def f_output(queue: Queue):
#     logger.debug('f output start')
#     io1 = StringIO()

    
#     while True: 
#         logger.debug('f output get')

#         x, y, z = queue.get()
        
#         logger.debug('f output retrieved.')
#         string = f'{x},{y},{z}\n'
#         print('----')
#         print(string)
#         io1.write(string)    
#         yield
        
        
    
def test():
    args = ((1, 2),
            (3, 4), 
            (5, 6))
    
    f_in = None
    f_proc = process
    f_out = Output()
    
    mp = MPReadWrite(args, f_in, f_proc, f_out,
                     # istar=True,
                     istar=False,
                     pstar=True,
                     ostar=True,)
    mp.progress_bar()
    # breakpoint()
    
    io1 = f_out.io
    io1.seek(0)
    
    # breakpoint()
    
    print('Output:')
    output = io1.readlines()
    print(output)
    
    assert '1,2,3\n' in output
    assert '3,4,7\n' in output
    assert '5,6,11\n' in output
    
    
    
    # assert output == '1,2,3\n3,4,7\n5,6,11\n'
    
    
    
    # print(io1.read())
    # breakpoint()
    
    
    
def f_test2_in(x):
    return x**2

def f_test2_proc(y):
    return y**2

def f_test2_out(z):
    print(z)


    
    
def test2():
    args = (1,2,3,4,5)
    
    
    
    mp = MPReadWrite(args, 
                     f_in=f_test2_in,
                     f_proc=f_test2_proc, 
                     f_out=f_test2_out,  )
    mp.run()
        
    
def f_test_star_in(x):
    return x**2, x**3


def f_test_star_proc(x, y):
    return x + y



def f_test_star_out(z):
    print(z)
    
    



def test_star():
    args = (1,2,3,4,5)
    
    
    
    mp = MPReadWrite(args, 
                     f_in=f_test_star_in,
                     f_proc=f_test_star_proc, 
                     f_out=f_test_star_out,
                     pstar=True)
    mp.progress_bar()
    
class MyError(Exception):
    pass


def f_bad(x):
    print('HEY', flush=True)
    raise MyError('breaks')
    
def test_func_error():
    args = (1,2,3,4,5)
    got_error = False
    
    try:
        mp = MPReadWrite(args, 
                         f_in=f_test_star_in,
                         f_proc=f_bad, 
                         f_out=f_test_star_out,
                         pstar=False)    
        mp.progress_bar()
    except MyError:
        got_error =  True
        
    if not got_error:
        assert False, "MPReadWrite did not correctly catch error."
    
        
def test_func_error_in():
    args = (1,2,3,4,5)
    got_error = False
    
    try:
        mp = MPReadWrite(args, 
                         f_in=f_bad,
                         f_proc=f_bad, 
                         f_out=f_test_star_out,
                         pstar=False)    
        mp.progress_bar()
    except MyError:
        got_error =  True
        
    if not got_error:
        assert False, "MPReadWrite did not correctly catch error."
        
        
    
    
    # 
if __name__ == '__main__':
    test()
    test2()
    test_star()
    test_func_error()
    test_func_error_in()
