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
from subprocess import call
from os.path import dirname

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.netutil
from tornado import gen

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


class BaseTaskHandler(tornado.web.RequestHandler):
    def initialize(self, queue, pool=None):
        self.queue = queue
        self.pool = pool


class AddTaskHandler(BaseTaskHandler):
    def get(self, taskfile):
        task = {
            'taskid': self.queue.counter,
            'taskfile': taskfile,
            'timesubmitted': datetime.now().isoformat(),
            'taskstatus': PENDING,
        }
        logger.info("Task added: {}".format(taskfile))

        fut = self.pool.submit(worker, taskfile)
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


class Scheduler(tornado.web.Application):
    """
    RESTful scheduler
    """
    def __init__(self, queue, workers=None):
        self.queue = queue
        self.pool = ProcessPoolExecutor(max_workers=workers)
        handlers = [
            (r"/add/(.*)", AddTaskHandler, {'queue': queue, 'pool': self.pool}),
            (r"/list", ListTaskHandler, {'queue': queue}),
            (r"/task/([0-9]+)", TaskByIdHandler, {'queue': queue}),
        ]
        tornado.web.Application.__init__(self, handlers)

    @gen.coroutine
    def task_runner(self):
        for task in self.queue:
            if task['taskstatus'] == PENDING and task['taskworker'].running():
                task['taskstatus'] = RUNNING
                logger.info("Task ID {} {}".format(task['taskid'], task['taskstatus']))
            if task['taskstatus'] == RUNNING and task['taskworker'].done():
                task['taskstatus'] = task['taskworker'].result()
                logger.info("Task ID {} {}".format(task['taskid'], task['taskstatus']))


def worker(taskfile):
    try:
        call(['cd {}; /bin/bash {}'.format(dirname(taskfile), taskfile)], shell=True )
        return DONE
    except:
        logger.error("Taskfile FAILED: {}".format(taskfile))
        return FAILED

def main(port, address, workers):
    queue = TaskQueue()
    app = Scheduler(queue, workers)
    sockets = tornado.netutil.bind_sockets(port, address=address)
    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)
    logger.info("Scheduler starting up")
    for s in sockets:
        logger.info("Scheduler address: {}:{}".format(*s.getsockname()))
    queue_loop = tornado.ioloop.IOLoop.instance()
    tornado.ioloop.PeriodicCallback(app.task_runner, 2000).start()
    queue_loop.start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=1, help="Number of workers (default: Number of processors)")
    parser.add_argument('--address', default=None, help='Task scheduler address (default: all localhost IP address)')
    parser.add_argument('--port', default=8082, help='Task scheduler port (default: 8082)')
    args = parser.parse_args()

    main(args.port, args.address, args.workers)
