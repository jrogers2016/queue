#!/usr/bin/env python

"""
This is a simple queue system take shell script as an input and execute them
as a queue.

Requirements:
1. Shell script as an input and execute with proper environmental variables.
2. Take ntasks parameter and launch concurrent jobs.
3. Assigns job ID when a job is submitted and report job status when asked.
"""

import atexit
import argparse
import collections
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
import logging
import signal
from subprocess import call
import os
from os.path import dirname, basename
import sys
import asyncio

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.netutil
from tornado import gen
import requests

logger = logging.getLogger("silcsbio.scheduler")

# task status
PENDING = 'PENDING'
RUNNING = 'RUNNING'
FAILED = 'FAILED'
DONE = 'DONE'

class TaskQueue(collections.deque):
    """deque instance for Task queue"""

    def __init__(self):
        self.counter = 0
        collections.deque.__init__(self)

    def append(self, item):
        collections.deque.append(self, item)
        self.counter += 1

    def pop(self):
        self.counter -= 1
        return collections.deque.pop(self)


class BaseTaskHandler(tornado.web.RequestHandler):
    def initialize(self, queue, pool=None, gpuids=None):
        self.queue = queue
        self.pool = pool
        self.gpuids = gpuids


class AddTaskHandler(BaseTaskHandler):
    def get(self, taskfile):
        task = {
            'taskid': self.queue.counter,
            'taskfile': taskfile,
            'timesubmitted': datetime.now().isoformat(),
            'taskstatus': PENDING,
        }
        logger.info("Task added: {}".format(taskfile))

        fut = self.pool.submit(worker, taskfile, self.gpuids)
        task['taskworker'] = fut
        self.queue.append(task)

        response = { 'taskid': task['taskid'],
                     'tasksubmitted': True }
        self.write(response)


class ListTaskHandler(BaseTaskHandler):
    def get(self):
        tasks = []
        for task in self.queue:
            task = {
                'taskid': task['taskid'],
                'taskfile': task['taskfile'],
                'timesubmitted': task['timesubmitted'],
                'taskstatus': task['taskstatus']
            }
            tasks.append(task)
        response = { 'ntasks': len(self.queue),
                     'tasks': tasks }
        self.write(response)


class TaskByIdHandler(BaseTaskHandler):
    def get(self, id):
        response = {
            'ntasks': 0,
            'tasks': []
        }
        for task in list(self.queue):
            if task['taskid'] == int(id):
                task = {
                    'taskid': task['taskid'],
                    'taskfile': task['taskfile'],
                    'timesubmitted': task['timesubmitted'],
                    'taskstatus': task['taskstatus']
                }
                response = {
                    'ntasks': 1,
                    'tasks': [task]
                }
                break
        return self.write(response)

class PidRequestHandler(BaseTaskHandler):
    def get(self):
        return self.write({ 'pid' : os.getpid()})
    
class Worker(tornado.web.Application):
    """
    RESTful scheduler
    """
    def __init__(self, queue, workers=None, gpuids=None):
        self.queue = queue
        self.pool = ProcessPoolExecutor(max_workers=workers)
        if gpuids != None:
            self.gpuids = TaskQueue()
            for gpuid in list(gpuids.split(',')):
                self.gpuids.append(gpuid)
        else:
            self.gpuids = None
        handlers = [
            (r"/add/(.*)", AddTaskHandler, {'queue': queue, 'pool': self.pool, 'gpuids': self.gpuids}),
            (r"/list", ListTaskHandler, {'queue': queue}),
            (r"/task/([0-9]+)", TaskByIdHandler, {'queue': queue}),
            (r"/kill", PidRequestHandler, {'queue': queue}),
        ]
        tornado.web.Application.__init__(self, handlers)

    @gen.coroutine
    def task_runner(self):
        for task in self.queue:
            if task['taskstatus'] == PENDING and task['taskworker'].running():
                task['taskstatus'] = RUNNING
                logger.info("Task ID {} {}".format(task['taskid'], task['taskstatus']))
            if task['taskstatus'] in (RUNNING, PENDING) and task['taskworker'].done():
                task['taskstatus'] = task['taskworker'].result()
                logger.info("Task ID {} {}".format(task['taskid'], task['taskstatus']))


def worker(taskfile, gpuids=None):
    try:
        f = open('{}.out'.format(basename_noext(taskfile)),'wb')
        if gpuids != None:
            gpuid = gpuids.pop()
            call(['cd {}; /bin/bash {}'.format(dirname(taskfile), taskfile)], stdout=f, stderr=f, shell=True, env={'GMX_GPU_ID': gpuid} )
            gpuids.append(gpuid)
        else:
            call(['cd {}; /bin/bash {}'.format(dirname(taskfile), taskfile)], stdout=f, stderr=f, shell=True )
        f.close()
        return DONE
    except:
        logger.error("Taskfile FAILED: {}".format(taskfile))
        return FAILED

def main(guestport, guestaddr, hostport, hostaddr, workers, gpuids):
    queue = TaskQueue()
    workers = workers
    app = Worker(queue, workers, gpuids)
    sockets = tornado.netutil.bind_sockets(guestport, address=guestaddr)
    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)
    for s in sockets:
        logger.info("Guest scheduler address: {}:{}".format(*s.getsockname()))
    queue_loop = tornado.ioloop.IOLoop.instance()
    tornado.ioloop.PeriodicCallback(app.task_runner, 1000).start()

    def shutdown_handler(signum, frame):
        #exit_handler()
        detach_from_pool(hostaddr, hostport, guestaddr, guestport)
        exit_handler()
        for task in asyncio.Task.all_tasks():
            task.cancel()
        sys.exit(0)

    @atexit.register
    def exit_handler():
        logger.info("Guest scheduler instance shutting down")
        stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, shutdown_handler)
    else:
        signal.signal(signal.SIGQUIT, shutdown_handler)

    join_worker_pool(hostaddr, hostport, guestaddr, guestport, workers)

    logger.info("Guest scheduler starting up")
    queue_loop.start()

def join_worker_pool(hostaddr, hostport, guestaddr, guestport, workers):
    base_url = 'http://{}:{}'.format(hostaddr, hostport)
    request_url = '{}/join/{}:{}.{}'.format(base_url, guestaddr, guestport, workers)
    r = requests.get(request_url)

def detach_from_pool(hostaddr, hostport, guestaddr, guestport):
    base_url = 'http://{}:{}'.format(hostaddr,hostport)
    request_url = '{}/detach/{}:{}'.format(base_url, guestaddr, guestport)
    try:
        r = requests.get(request_url)
    except:
        logger.info("Host scheduler already shut down, guest scheduler shutting down")

def stop():
    tornado.ioloop.IOLoop.instance().stop()

def basename_noext(filename):
    return '.'.join(filename.split('.')[:-1])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=1, help="Number of workers (default: Number of processors)")
    parser.add_argument('--gpuids', default=None, help="List of available GPU ID's in comma-separated format e.g. 0,1; must be equal in number to the number of workers (default: None)")
    parser.add_argument('--scheduler', default='127.0.0.1:8082', help='Host scheduler address (default: all localhost IP address)')
    parser.add_argument('--address', default=None, help='Guest scheduler address (default: all localhost IP address)')
    parser.add_argument('--port', default=8083, help='Guest scheduler port (default: 8082)')
    args = parser.parse_args()

    if args.gpuids != None:
        try:
            assert len(args.gpuids.split(',')) == args.workers
        except:
            logger.info(parser.print_help())
            sys.exit(0)
    if ':' in args.scheduler:
        hostaddr, hostport = args.scheduler.split(':')
        guestaddr = args.address
    if not guestaddr:
        guestaddr = '127.0.0.1'

    main(args.port, guestaddr, hostport, hostaddr, args.workers, args.gpuids)
