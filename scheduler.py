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
import requests

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.netutil
import tornado.httpclient
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
    def initialize(self, queue, servers=None):
        #These should be replaced with the dictionary of lists, namely servers
        #Add number of workers and running jobs in each server
        #First two entries in each list are number of workers and number of running jobs
        self.queue = queue
        self.servers = servers

        
class AddTaskHandler(BaseTaskHandler):
    def get(self, taskfile):
        task = {
            'taskid': self.queue.counter,
            'taskfile': taskfile,
            'timesubmitted': datetime.now().isoformat(),
            'taskstatus': PENDING,
        }
        logger.info("Task added: {}".format(taskfile))

        
        #Determine which url to send to
        task['server'] = None
        while not task['server']:
            for server,jobs in self.servers.items():
                if jobs[0] > jobs[1]:  #ie it has more processors than running jobs
                    #Send response to server
                    tornado.httpclient.AsyncHTTPClient().fetch(
                        tornado.httpclient.HTTPRequest('http://{}/add/{}:{}'.format(server,taskfile,task['taskid'])))
                    task['server'] = server
                    jobs.append(task)
                    break
        
        self.queue.append(task)

        response = { 'taskid': task['taskid'],
                     'tasksubmitted': True }
        self.write(response)


class ListTaskHandler(BaseTaskHandler):
    def get(self):
        tasks = []
        for server,jobs in self.servers.items():
            processes = jobs[2:]
            for task in processes:
                task = {
                    'taskid': task['taskid'],
                    'taskfile': task['taskfile'],
                    'timesubmitted': task['timesubmitted'],
                    'taskstatus': task['taskstatus']
                }
                tasks.append(task)
            #This should be inside the outer loop, respond and write per server
            response = { 'ntasks': len(tasks),
                         'tasks': tasks,
                         'server': server}
            self.write(response)


class TaskByIdHandler(BaseTaskHandler):
    def get(self, id):
        response = {
            'ntasks': 0,
            'tasks': []
        }
        for server,jobs in self.servers.items():
            processes = jobs[2:]
            for task in processes:
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
            if task['taskid'] == int(id):
                break
        return self.write(response)


class AddGuestHandler(BaseTaskHandler):
    def get(self, addr, port, workers):
        url = '{}:{}'.format(addr, port)
        tasks = [int(workers),0]
        self.servers[url] = tasks
        return self.write({})

class DetachGuestHandler(BaseTaskHandler):
    def get(self, addr, port):
        url = '{}:{}'.format(addr,port)
        if url in self.servers.keys():
            del self.servers[url]

class Scheduler(tornado.web.Application):
    """
    RESTful scheduler
    """
    def __init__(self, queue, workers=None):
        self.queue = queue
        self.servers = {}
        handlers = [
            (r"/add/(.*)", AddTaskHandler, {'queue': queue, 'servers': self.servers}),
            (r"/list", ListTaskHandler, {'queue': queue, 'servers': self.servers}),
            (r"/task/([0-9]+)", TaskByIdHandler, {'queue': queue, 'servers': self.servers}),
            (r"/join/([0-9\.]+):([0-9]+).([0-9]+)", AddGuestHandler, {'queue': queue, 'servers': self.servers}),
            (r"/detach/([0-9\.]+):([0-9]+)", DetachGuestHandler, {'queue': queue, 'servers': self.servers}),
        ]
        tornado.web.Application.__init__(self, handlers)

    @gen.coroutine
    def task_runner(self):
        running = 0
        for server,jobs in self.servers.items():
            processes = jobs[2:]
            for job in processes:
                if job['taskstatus'] in (FAILED, DONE):
                    continue
                num = job['taskid']
                base_url = 'http://{}'.format(server)
                request_url = '{}/task/{}'.format(base_url,num)
                task = requests.get(request_url).json()
                job['taskstatus'] = task['tasks'][0]['taskstatus']
                if job['taskstatus'] == RUNNING:
                    running += 1
            jobs[1] = running
            
def worker(taskfile):
    try:
        call(['cd {}; /bin/bash {} &> {}.out'.format(dirname(taskfile), taskfile, basename_noext(taskfile))], shell=True )
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
    for s in sockets:
        logger.info("Host scheduler address: {}:{}".format(*s.getsockname()))
    queue_loop = tornado.ioloop.IOLoop.instance()
    tornado.ioloop.PeriodicCallback(app.task_runner, 1000).start()

    def shutdown_handler(signum, frame):
        exit_handler()
        if len(list(app.servers.keys())) > 0:
            for server,jobs in app.servers.items():
                req_url = 'http://{}/kill'.format(server)
                pid = requests.get(req_url).json()
                os.kill(pid['pid'],signum)
        for task in asyncio.Task.all_tasks():
            task.cancel()
        sys.exit(0)

    @atexit.register
    def exit_handler():
        logger.info("Host scheduler instance shutting down")
        stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, shutdown_handler)
    else:
        signal.signal(signal.SIGQUIT, shutdown_handler)

    logger.info("Host scheduler starting up")
    queue_loop.start()

def stop():
    tornado.ioloop.IOLoop.instance().stop()

def basename_noext(filename):
    return '.'.join(filename.split('.')[:-1])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=1, help="Number of workers (default: Number of processors)")
    parser.add_argument('--address', default=None, help='Task scheduler address (default: all localhost IP address)')
    parser.add_argument('--port', default=8082, help='Task scheduler port (default: 8082)')
    args = parser.parse_args()

    main(args.port, args.address, args.workers)
