# -*- coding: utf-8 -*-

import time
import logging

from typing import (
    Callable, Any, TextIO, Protocol, TypeVar, TypeAlias, Iterator
    )

from abc import abstractmethod

import multiprocessing as mp
from multiprocessing import Pool, freeze_support, Manager, Queue
from threading import Thread
import tqdm 
from tqdm.contrib.concurrent import process_map  # or thread_map

from mptools.tools import _Settings

logger = logging.getLogger(__name__)




Arg = TypeVar('Arg')
InputQ1 = TypeVar('InputQ1')
OutputQ1 = TypeVar('OutputQ1')


MPArg = TypeVar('MPArg') 
MPData = TypeVar('MPData')

MProcessed = TypeVar('MProcessed')
MPQueue1: 'Queue[MPArg]' = mp.Queue
MPQueue2: 'Queue[MProcessed]' = mp.Queue


MPInputFunc = Callable[MPArg, MPData]
MProcFunc = Callable[MPData, MProcessed]
MPOutputFunc = Iterator[MPQueue1]



class _ReadWorker:
    """multiprocessing input stream."""
    def __init__(self, 
                 f_in: MPInputFunc,
                 qmax: int=1000, 
                 starmap: bool=False):
        self.f_in = f_in
        self.qmax = qmax
        self.starmap = starmap
        
        
    def __call__(self, args: list[MPArg], queue1: mp.Queue):
        logger.debug('Start reading')
        found_error = False
        for job_num, arg in enumerate(args):
            
            try:
                if self.starmap:
                    out = self.f_in(*arg)
                else:
                    out = self.f_in(arg)
                logger.debug('read worker, putting contents in queue1')
            except Exception as exc:
                found_error = True
                out = exc
                
            queue1.put((job_num, out))
            if found_error:
                raise out
            
            # Don't let too many items pile up in the queue
            while True:
                qsize = queue1.qsize()
                if qsize <= self.qmax:
                    break
                else:
                    time.sleep(0.1)
                
                
class _Worker:
    """Multiprocessing workers."""
    def __init__(self, 
                 f: MProcFunc, 
                 starmap: bool = False,
                 qmax: int = -1, 
                 sleep: float = .05,):
        
        self.f = f
        self.starmap = starmap
        self.qmax = qmax
        self.sleep = sleep
        
        if qmax == -1:
            self._enable_qmax = False
        else:
            self._enable_qmax = True
            
        logger.debug('initialized _Worker')
        
        
    def __call__(self, queue1: MPQueue1, queue2: MPQueue2):
        """
        
        Parameters
        ----------
        queue1 : mp.Queue
            Data input Queue.
        queue2 : mp.Queue
            Data output Queue.
        """
        
        found_error = False
        
        # Use try/except to catch the exception. 
        # If error occurs, make sure to stick the exception into queue,
        # Then raise the error to make sure it is raised. 
        # The job needs to be returned from queue2 to raise the error.
        try:
                
            logger.debug('calling worker')
            job_num, data = queue1.get()
            logger.debug('job %s data retrieved from queue1', job_num)
            if self.starmap:
                out = self.f(*data)
            else:
                out = self.f(data)
                
            logger.debug('proc worker, putting result in queue2')
            
            if self._enable_qmax:
                while queue2.qsize() > self.qmax:
                    time.sleep(self.sleep)
                
        except Exception as exc:
            logger.error('Exception caught as:\n %s, %s', repr(type(exc)), exc)     
            found_error = True
            out = exc
            
        logger.debug('putting result in queue')
        queue2.put((job_num, out))
        if found_error:
            raise out
        
 
    
class _WriteWorker:
    """Write data to output stream."""
    def __init__(self, f: MPOutputFunc,  starmap: bool):
        self.f = f
        self.starmap = starmap
    
    
    def __call__(self, data):
        if self.starmap:
            self.f(*data)
        else:
            self.f(data)


def _f_in_dummy(arg):
    return arg
def _f_in_dummy2(*arg):
    return arg




class MPReadWrite:
    """Multiprocessing with separate I/O processes. 
    
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
        f_out must be able to take the output of f_proc as its argument.
        
        Has signature: f_out(processed)
        
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
            f_in: MPInputFunc, 
            f_proc: MProcFunc,
            f_out: MPOutputFunc,
            processes: int=10,
            input_qmax: int = 1000,
            output_qmax: int = 1000,
            istar: bool = False,
            pstar: bool = False,
            ostar: bool = False,
            ):
        
        if f_in is None:
            if istar:
                f_in = _f_in_dummy2
            else:
                f_in = _f_in_dummy
            
        self.f_in = f_in
        self.f_proc = f_proc
        self.f_out = f_out
            
        self.processes = processes
        self.input_qmax = input_qmax
        self.output_qmax = output_qmax
        
        self.args = args
        
        self._args_len = len(args)
        # self.starmap = starmap
        self.istar = istar
        self.pstar = pstar
        self.ostar = ostar
        # self.start()
        return
    
    
    def progress_bar(self):
        total = self._args_len * 1
        
        r = list(tqdm.tqdm(self, total=total))
        return r
    
    def run(self):
        """Run all jobs. Wait to finish."""
        for ii in self:
            pass
    

    def __iter__(self):
        input_qmax = self.input_qmax
        output_qmax = self.output_qmax
        f_in = self.f_in
        f_proc = self.f_proc
        f_out = self.f_out
        args = self.args
        istar = self.istar
        pstar = self.pstar
        ostar = self.ostar
        processes = self.processes
        
        jobnum = len(args)
        
        if _Settings.disable:
            _mp_read_write_disabled(args, f_in, f_proc, f_out,
                                    istar=istar,
                                    pstar=pstar,
                                    ostar=ostar,
                                    )
            return
        
        # Set up manager to handle multiprocessing queues
        manager = mp.Manager()
        
        # Input queue
        queue1 = manager.Queue()
        
        # Processing output queue
        queue2 = manager.Queue()
        
        # Create input, processing, and output workers
        worker_in = _ReadWorker(f_in, qmax=input_qmax, starmap=istar)
        worker = _Worker(f_proc, starmap=pstar, qmax=output_qmax)
        worker_out = _WriteWorker(f_out, starmap=ostar)       
        
   

        self.queue1 = queue1
        self.queue2 = queue2
        self.manager = manager   
    
        # First process for reading in data
        logger.debug("START READER #######################")
        p1 = mp.Process(
            target=worker_in,
            args=(args, queue1)
            )
        p1.start()
        
        
        # logger.debug("START WRITER #######################", )
        # p3 = mp.Process(target=worker_out, args=(queue2, queue3))
        # p3.start()
        
        
        logger.debug("START PROCESSING #######################")
        jobs = []
        with mp.Pool(processes=processes) as pool:
            for _ in range(jobnum):
                logger.debug('Starting process job')
                # data = queue1.get()
                # logger.debug('Running worker with %s', data)
                job = pool.apply_async(worker, args=(queue1, queue2))
                jobs.append(job)
                
                
            for _ in range(jobnum):
                logger.debug('Waiting for queue2 result')

                jobnum, output = queue2.get()
                logger.debug('Queue2 result retrieved from %s', jobnum)
                job = jobs[jobnum]
                job.get()
                worker_out(output)
                yield
      

      
        # Process for writing output data. 

        p1.join()
        pool.join()
        # p3.terminate()
        return
        
        
def _mp_read_write_disabled(args, f_in, f_proc, f_out,
                            istar: bool = False,
                            pstar: bool = False,
                            ostar: bool = False,
                            ):
    f_in0 = f_in
    f_proc0 = f_proc
    f_out0 = f_out
    
    if istar:
        def f_in2(x):
            return f_in(*x)
        f_in0 = f_in2
        
    if pstar:
        def f_proc2(x):
            return f_proc(*x)
        f_proc0 = f_proc2
        
        
    if ostar:
        def f_out2(x):
            return f_out(*x)
        f_out0 = f_out2
        
    for arg in args:
        data = f_in0(arg)
        processed = f_proc0(data)
        f_out0(processed)
        
    
        
        
        
    
        



    
    
    
    
    