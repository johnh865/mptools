"""Multiprocessing tools.

For debug purposes, settings.disable is provided for you to disable multiprocessing. 


"""

# from concurrent.futures import ProcessPoolExecutor
import time

from typing import Callable, Any, TextIO, Protocol, TypeVar, TypeAlias
from abc import abstractmethod

import multiprocessing as mp
from multiprocessing import Pool, freeze_support, Manager, Queue
import tqdm 
from tqdm.contrib.concurrent import process_map  # or thread_map


class _Settings:
    """Disable multiprocessing for debug purposes.
    
    >>> import mptools
    >>> mptools.settings.disable = True
    
    """
    disable: bool = False
    
_Settings.disable = False



def disable():
    """Disable multiprocessing for debug purposes."""
    _Settings.disable = True
    
def enable():
    """Enable multiprocessing.
    Usually re-importing mptools is sufficient to re-enable."""
    _Settings.disable = False
    

def tqdm_map(func: Callable, args: list[Any], nprocs: int):
    """Map function for multiprocessing, including TQDM progress bar."""
    total = len(args)
    
    if _Settings.disable:
        r = list(tqdm.tqdm((func(a) for a in args), total=total))
        return r
    
    with Pool(processes=nprocs) as p:
        r = list(tqdm.tqdm(p.imap(func, args,), total=total))
        return r


class _Func1:
    """Create funtion to facilitate starmap."""
    def __init__(self, func):
        self.func = func
        
    def __call__(self, args):
        return self.func(*args)
    
    
    def call_kwargs(self, kwargs: dict[Any, Any]):
        return self.func(**kwargs)
    
        


def tqdm_starmap(func: Callable, args: list[list[Any]], nprocs: int):
    """Starmap function for multiprocessing, including TQDM progress bar."""
    # def func1(args):
    #     return func(*args)
    func1 = _Func1(func)    
    return tqdm_map(func1, args, nprocs)


def tqdm_dictmap(func: Callable, args: list[dict], nprocs: int):
    """Dict map for multiprocessing, including TQDM progress bar. """
    func1 = _Func1(func).call_kwargs
    return tqdm_map(func1, args, nprocs)


def starwrite(func: Callable, 
              args: list[list[Any]], 
              file: TextIO,
              nprocs: int, 
              chunksize=1
              ):
    """Evluate a function that returns a string. Write that string to file. 
    

    Parameters
    ----------
    func : Callable
        Function to call. Must return a string.
    args : list[Any]
        DESCRIPTION.
    file : TextIO
        File to write to.
    nprocs : int
        Number of processes.
    chunksize : TYPE, optional
        Chunk size for parallelization. The default is 1.

    Returns
    -------
    None.

    """
        
    with Pool(processes=nprocs) as p:
        imap = p.imap(func, args, chunksize=chunksize)
        for string in imap:
            file.write(string)
    return


            
            
def tqdm_starwrite(func: Callable, 
              args: list[list[Any]], 
              file: TextIO,
              nprocs: int, 
              chunksize=1
              ):
    
    total = len(args)
    with Pool(processes=nprocs) as p:
        
        imap = p.imap(func, args, chunksize=chunksize)
        # iter1 = (file.write(string) for string in imap)
        # iter1 = tqdm.tqdm(iter1, total=total)
        imap = tqdm.tqdm(imap, total=total)
        for string in imap:
            file.write(string)
    return



Arg = TypeVar('Arg')
InputQ1 = TypeVar('InputQ1')
OutputQ1 = TypeVar('OutputQ1')




class _ReadWorker:
    def __init__(self, f_in: Callable, qmax=1000):
        self.f_in = f_in
        self.qmax = qmax
        
    def __call__(self, args, queue1: mp.Queue):
        for arg in args:
            out = self.f_in(arg)
            queue1.put(out)
            
            # Don't let too many items pile up in the queue
            while True:
                qsize = queue1.qsize()
                if qsize <= self.qmax:
                    break
                else:
                    time.sleep(0.1)
                
                
class _Worker:
    def __init__(self, f: Callable):
        self.f = f
        
        
    def __call__(self, 
                 arg: Any, 
                 data: Any, 
                 is_complete: bool,
                 queue2: mp.Queue
                 ):
        """
        
        Parameters
        ----------
        arg : int
            Identifying arugment.
        data : Any
            Data to process.
        is_complete : bool
            Set to True to stop work. 
        queue : mp.Queue
            Queue.

        """
        out = self.f(data)
        # print('Worker arg, out')
        # print(arg, flush=True)
        # print(out, flush=True)
        # print('putting is_copmlete', is_complete, flush=True)
        queue2.put((arg, out, is_complete))
        

class _WriteWorker:
    def __init__(self, f: Callable, qmax=1000):
        self.f = f
    
    def __call__(self, 
                 queue1: mp.Queue,
                 queue2: mp.Queue):
                
        while True:
            # if queue1.empty() and queue2.empty():
            #     break

            arg, data, is_complete = queue2.get()
            if is_complete:
                break
            # print('WriteWorker arg, data', flush=True)
            # print(arg, flush=True)
            # print(data, flush=True)
            self.f(arg, data)
                
        
def mp_read_write(args: list, 
                  f_in: Callable, 
                  f_proc: Callable,
                  f_out: Callable,
                  processes: int=10,
                  input_qmax: int = 1000,
                  output_qmax: int = 1000,
                  ):
    """Facilitate Read-in, multi-processing, and writing of mass data.
    
    
    Data reading and data writing are put on their own Queues to maximize 
    I/O throughput. Separate processes are opened for data processing. Provide 
    three functions:
        
        * f_in -- Function that reads input
        * f_proc -- Function that processes input and converts it to output. 
        * f_out -- Function that write outupt. 

    Parameters
    ----------
    args : list[Any]
        Arguments to input into function `f_in` as [arg1, arg2, ... argN].
    f_in : Callable
        Read-in function.
        Has signature:  data = f_in(arg).
        
    f_proc : Callable
        Processing function where multiprocessing is applied.
        Has signature: processed = f_proc(data).
        
    f_out : Callable
        Write-out function.
        Has signature: f_out(arg, processed)
        
    processes : int, optional
        Number of processes. The default is 10.

    input_qmax : int, optional
        Max number of f_in outputs in input queue. The default is 1000.
        
    output_qmax : int, optional
        Max number of f_proc outputs in output queue. The default is 1000.
    

    Returns
    -------
    None.

    """
    if _Settings.disable:
        _mp_read_write_disabled(args, f_in, f_proc, f_out)
        return
    
        
    manager = mp.Manager()
    queue1 = manager.Queue()
    queue2 = manager.Queue()
        
    worker_in = _ReadWorker(f_in, qmax=input_qmax)
    worker = _Worker(f_proc)
    worker_out = _WriteWorker(f_out, qmax=output_qmax)
    
    
    # First process for reading in data
    print("START READER #######################", flush=True)
    p = mp.Process(target=worker_in, args=(args, queue1))
    p.start()
    
    print("START WRITER #######################", flush=True)
    # Process for writing output data. 
    p = mp.Process(target=worker_out, args=(queue1, queue2))
    p.start()    
    
    # Pool of processes for computation
    pool = mp.Pool(processes=processes)
    
    jobs = []
    print("START PROCESSING #######################", flush=True)
    
    is_complete = False
    for arg in args[0 : -1]:
        data = queue1.get()
        job = pool.apply_async(worker, args=(arg, data, is_complete, queue2, ))
        jobs.append(job)
        
    is_complete = True
    data = queue1.get()
    job = pool.apply_async(worker, args=(arg, data, is_complete, queue2, ))
    jobs.append(job)

    p.join()
    return
    
        
def _mp_read_write_disabled(args, f_in, f_proc, f_out, ):
    for arg in args:
        data = f_in(arg)
        processed = f_proc(data)
        f_out(arg, processed)
        
    
        
        
        
    
        



    
    
    
    
    
    


# def mp_write(func: Callable, 
#               args: list[list[Any]], 
#               file: TextIO,
#               nprocs: int,
              
#              ):
#     manager = Manager()
#     q = manager.Queue()
#     pool = Pool(nprocs)
    
#     listener = _Qwriter(file)
#     worker = _Qworker(func)
    
    
#     # Put listener to work first
#     watcher = pool.apply_async(listener, (q,))
    
#     # fire off workers
#     jobs = []
#     num = len(args)
#     for ii in range(num):
#         arg = args[ii]
#         # print(ii, arg)
#         job = pool.apply_async(worker, (arg, q))
#         jobs.append(job)
        
    
#     # collect results from the workers through the pool result queue
#     for job in jobs:
#         out = job.get()
#         # print(out)
        
#     # Now we are done, kill the listener.
#     q.put('__kill__')
#     pool.close()
#     pool.join()
    
    
    


# class _Qworker:
#     def __init__(self, func: Callable):
#         self.func = func
    
#     def __call__(self, arg, queue: Queue):
#         out = self.func(arg)
#         queue.put(out)
#         return out
    
    
# class _Qwriter:
#     def __init__(self, file: TextIO,):
#         self.file = file
        
#     def __call__(self, queue: Queue):
#         print('helllo')
#         while True:
#             m = queue.get()
#             if m == '__kill__':
#                 break
#             self.file.write(str(m))
#             self.file.flush()
            
            
    
        



def _testfun2(x, y, z):
    return x + y + z


def _testfun1(x):
    return x*2


if __name__ == '__main__':
    
    freeze_support()
    import numpy as np

    
    args1 = np.arange(10)
    args2 = list(np.ones((10, 3)))
    
    tqdm_map(_testfun1, args1, nprocs=4)
    tqdm_starmap(_testfun2, args2, nprocs=5)
    # 