# -*- coding: utf-8 -*-

import time
import logging

from typing import Callable, Any, TextIO, Protocol, TypeVar, TypeAlias
from abc import abstractmethod

import multiprocessing as mp
from multiprocessing import Pool, freeze_support, Manager, Queue
import tqdm 
from tqdm.contrib.concurrent import process_map  # or thread_map


def worker(x):
    print('worker', x, flush=True)
    raise ValueError('outch')
    return x**2



if __name__ == '__main__':
    jobs = []
    with  mp.Pool(processes=5) as pool:

        for ii in range(5):
            # print(ii)
            job = pool.apply_async(worker, args=(ii, ))
            jobs.append(job)
            print('result', job.get())
    pool.join()
    pool.terminate()

    