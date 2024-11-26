"""Multiprocessing tools.

For debug purposes, settings.disable is provided for you to disable multiprocessing. 


"""

# from concurrent.futures import ProcessPoolExecutor
import time
import logging

from typing import Callable, Any, TextIO, Protocol, TypeVar, TypeAlias
from abc import abstractmethod

import multiprocessing as mp
from multiprocessing import Pool, freeze_support, Manager, Queue
import tqdm 
from tqdm.contrib.concurrent import process_map  # or thread_map

logger = logging.getLogger(__name__)


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
        self.completed_count = 0
    
    def __call__(self, queue2: mp.Queue):
                
        while True:

            arg, data, is_complete = queue2.get()

            # print('WriteWorker arg, data', flush=True)
            # print(arg, flush=True)
            # print(data, flush=True)
            self.f(arg, data)
            self.completed_count += 1
            
            if is_complete:
                break            
                
        
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
        Number of cores for processing. The default is 10.

    input_qmax : int, optional
        Max number of f_in outputs in input queue. The default is 1000.
        
    output_qmax : int, optional
        Max number of f_proc outputs in output queue. The default is 1000.
    

    Returns
    -------
    None.

    """
    
    jobnum = len(args)
    
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
    logger.info("START READER #######################", flush=True)
    p = mp.Process(target=worker_in, args=(args, queue1))
    p.start()
    
    logger.info("START WRITER #######################", flush=True)
    # Process for writing output data. 
    p = mp.Process(target=worker_out, args=(queue2,))
    p.start()    
    
    # Pool of processes for computation
    pool = mp.Pool(processes=processes)
    
    jobs = []
    logger.info("START PROCESSING #######################", flush=True)
    
    is_complete = False
    for arg in args[0 : -1]:
        data = queue1.get()
        job = pool.apply_async(worker, args=(arg, data, is_complete, queue2, ))
        jobs.append(job)
        
    # Run the last job, send 'is_complete' signal.
    arg = args[-1]
    is_complete = True
    data = queue1.get()
    job = pool.apply_async(worker, args=(arg, data, is_complete, queue2, ))
    jobs.append(job)

    # breakpoint()
    # def iterator():
    #     for job in jobs:
    #         job.wait()
    #         yield True
            
    # iterator2 = tqdm.tqdm(iterator(), total=jobnum)
    # for _ in iterator2:
    #     pass
    
        
    p.join()
    return

def _f_in_dummy(arg):
    return arg



class MPReadWrite:
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
        
        If f_in is None, pass through the arguments without modification:
            
            def f_in(arg):
                return arg
            
        
    f_proc : Callable
        Processing function where multiprocessing is applied.
        f_proc must be able to take the output of f_in as an argument. 
        
        Has signature: processed = f_proc(data).
        
    f_out : Callable
        Write-out function.
        f_out must be able to take the output of f_proc as its 2nd argument.
        
        Has signature: f_out(arg, processed)
        
    processes : int, optional
        Number of cores for processing. The default is 10.

    input_qmax : int, optional
        Max number of f_in outputs in input queue. The default is 1000.
        
    output_qmax : int, optional
        Max number of f_proc outputs in output queue. The default is 1000.
    
    starmap : bool, optinoal
        If True, map f_in as:
            data = f_in(*arg)
        Otherwise, map f_in as:
            data = f_in(arg)
            
        Defaults False 
    """    
    
    
    def __init__(
            self, 
            args: list, 
            f_in: Callable, 
            f_proc: Callable,
            f_out: Callable,
            processes: int=10,
            input_qmax: int = 1000,
            output_qmax: int = 1000,
            starmap: bool = False,
            ):
        
        if f_in is None:
            f_in = _f_in_dummy
            
        if starmap:
            f_in2 = lambda x: f_in(*x)
            f_in = f_in2
        
        self.f_in = f_in
        self.f_proc = f_proc
        self.f_out = f_out
        self.processes = processes
        self.input_qmax = input_qmax
        self.output_qmax = output_qmax
        
        self.args = args
        
        self._args_len = len(args)
        # self.start()
        return
    
    
    def progress_bar(self):
        total = self._args_len * 2
        
        r = list(tqdm.tqdm(self, total=total))
        return r
    

    def __iter__(self):
        input_qmax = self.input_qmax
        output_qmax = self.output_qmax
        f_in = self.f_in
        f_proc = self.f_proc
        f_out = self.f_out
        args = self.args
        processes = self.processes
        
        # jobnum = len(args)
        
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
        logger.info("START READER #######################", flush=True)
        p1 = mp.Process(target=worker_in, args=(args, queue1))
        p1.start()
        
        logger.info("START WRITER #######################", flush=True)
        # Process for writing output data. 
        p2 = mp.Process(target=worker_out, args=(queue2,))
        p2.start()    
        
        # Pool of processes for computation
        pool = mp.Pool(processes=processes)
        
        jobs = []
        logger.info("START PROCESSING #######################", flush=True)
        
        is_complete = False
        for arg in args[0 : -1]:
            data = queue1.get()
            job = pool.apply_async(worker, args=(arg, data, is_complete, queue2, ))
            jobs.append(job)
            yield True
            
        # Run the last job, send 'is_complete' signal.
        arg = args[-1]
        is_complete = True
        data = queue1.get()
        job = pool.apply_async(worker, args=(arg, data, is_complete, queue2, ))
        jobs.append(job)
        yield True
        
        
        self.queue1 = queue1
        self.queue2 = queue2
        self.manager = manager
        self.jobs = jobs
        
        self.process_reader = p1
        self.process_writer = p2
        
        for job in jobs:
            while not job.ready():
                time.sleep(.01)
            yield True
            
        p1.join()
        p2.join()
        
        
    # def __iter__(self):
    #     jobs = self.jobs
    #     for job in jobs:
            
            
    #         while not job.ready():
    #             time.sleep(0.01)
            
    #         print('!!!!', flush=True)
    #         yield True
        
    #     self.process_reader.join()
    #     self.process_writer.join()        
        

        
            
        # p.join()
    
        
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