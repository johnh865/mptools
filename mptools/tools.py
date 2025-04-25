"""Multiprocessing tools.

For debug purposes, settings.disable is provided for you to disable multiprocessing. 


"""

# from concurrent.futures import ProcessPoolExecutor
import time
import logging

from typing import (
    Callable, Any, TextIO, Protocol, TypeVar, TypeAlias, Iterator
    )

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
    


def tqdm_map(func: Callable, args: list[Any], nprocs: int, chunksize=1):
    """Map function for multiprocessing, including TQDM progress bar."""
    total = len(args)
    
    if _Settings.disable or nprocs==1:
        r = list(tqdm.tqdm((func(a) for a in args), total=total))
        return r
    

    with Pool(processes=nprocs) as p:
        result = list(
            tqdm.tqdm(
                p.imap(func, args, chunksize=chunksize,), total=total
                )
            )
        # r = p.map(func, args)

    return result
    
    


        
        


class _Func1:
    """Create funtion to facilitate starmap."""
    def __init__(self, func):
        self.func = func
        
    def __call__(self, args):
        return self.func(*args)
    
    
    def call_kwargs(self, kwargs: dict[Any, Any]):
        return self.func(**kwargs)
    
        


def tqdm_starmap(func: Callable, 
                 args: list[list[Any]],
                 nprocs: int, 
                 chunksize = 1,
                 ):
    """Starmap function for multiprocessing, including TQDM progress bar."""
    # def func1(args):
    #     return func(*args)
    func1 = _Func1(func)    
    return tqdm_map(func1, args, nprocs, chunksize=chunksize)


def tqdm_dictmap(func: Callable, 
                 args: list[dict], 
                 nprocs: int,
                 chunksize = 1,
                 ):
    """Dict map for multiprocessing, including TQDM progress bar. """
    func1 = _Func1(func).call_kwargs
    return tqdm_map(func1, args, nprocs, chunksize=chunksize)


def starwrite(func: Callable, 
              args: list[list[Any]], 
              file: TextIO,
              nprocs: int, 
              chunksize=1
              ):
    """Evaluate a function that returns a string. Write that string to file. 
    

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
    """Evaluate a function that returns a string. Write that string to file. 
    Display a TQDM progress bar while evaluating pool. 
    

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
    
    total = len(args)
    with Pool(processes=nprocs) as p:
        
        imap = p.imap(func, args, chunksize=chunksize)
        # iter1 = (file.write(string) for string in imap)
        # iter1 = tqdm.tqdm(iter1, total=total)
        imap = tqdm.tqdm(imap, total=total)
        for string in imap:
            file.write(string)
    return




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