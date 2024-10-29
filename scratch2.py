# -*- coding: utf-8 -*-

import multiprocessing as mp
import time

fn = 'c:/temp/temp.txt'
f = open(fn, 'w')
args = range(8000)
ncpus = 10

def func(x):
    return (str(x) + " ") *10 + '\n'


def worker(arg, q):
    '''stupidly simulates long running process'''
    # start = time.clock()
    res = func(arg)
    q.put(res)
    return res

def listener(q):
    '''listens for messages on the q, writes to file. '''

    # with open(fn, 'w') as f:
    while True:
        m = q.get()
        if m == 'kill':
            # f.write('killed')
            break
        f.write(str(m))
        f.flush()

def main():
    #must use Manager queue here, or will not work
    manager = mp.Manager()
    q = manager.Queue()    
    pool = mp.Pool(ncpus)

    #put listener to work first
    watcher = pool.apply_async(listener, (q,))

    #fire off workers
    jobs = []
    for arg in args:
        job = pool.apply_async(worker, (arg, q))
        pool.map_async(func, iterable)
        jobs.append(job)

    # collect results from the workers through the pool result queue
    for job in jobs: 
        job.get()

    #now we are done, kill the listener
    q.put('kill')
    pool.close()
    pool.join()

if __name__ == "__main__":
   main()
   f.close()