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
        #Maintain queue as a list of all running jobs
        #self.queue = queue
        #self.pool = pool
        self.queue = queue
        self.servers = servers



#class AddServerHandler(BaseTaskHandler) :
#    def get(self,url,num_workers=1):
#        tasks = [num_workers,0]
#        #How do I get the number of workers???
#        self.servers[url] = tasks
#Start periodic callback from server to show it's alive?  

        
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
        #      Send response, detailed below
        #Send response to server
        #name = construct_url_out_of_server here
                    #requests.get('http://{}/add/{}'.format(server,taskfile))
                    tornado.httpclient.AsyncHTTPClient().fetch(
                        tornado.httpclient.HTTPRequest('http://{}/add/{}'.format(server,taskfile)))
                    task['server'] = server
                    jobs.append(task)
                    break
        
        #fut = self.pool.submit(worker, taskfile)
        #task['taskworker'] = fut
        self.queue.append(task)

        response = { 'taskid': task['taskid'],
                     'tasksubmitted': True }
        self.write(response)


class ListTaskHandler(BaseTaskHandler):
    def get(self):
        tasks = []
        #Replace queue with servers and inside loop for server
        #for task in self.queue:
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
        #Replace queue with servers and inside loop for server
        #for task in list(self.queue):
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
        # (r"/workers", NumWorkersHandler, {'workers': workers})
        #tasks = [num_workers,0]
        url = '{}:{}'.format(addr,port)
        #num_workers = tornado.httpclient.AsyncHTTPClient().fetch('{}/workers'.format(url))
        logger.info("About to get num workers from http://{}/workers".format(url))
        request_url = 'http://{}/workers'.format(url)
        #num_workers = requests.get(request_url)
        #num_workers = num_workers.json()
        logger.info("Got num workers")
        tasks = [int(workers),0]
        self.servers[url] = tasks
        #print(addr, port)
        return self.write({})


#        tasks = [num_workers,0]
#        #How do I get the number of workers???
#        self.servers[url] = tasks

    """
def join_worker_pool(hostaddr, hostport, guestaddr, guestport):
    base_url = 'http://{}:{}'.format(hostaddr, hostport)
    request_url = '{}/join/{}:{}'.format(base_url, guestaddr, guestport)
    r = requests.get(request_url)
    result = r.json()
    print(result)
"""

class Scheduler(tornado.web.Application):
    """
    RESTful scheduler
    """
    def __init__(self, queue, workers=None):
        #Replace with servers, add in the new handler
        self.queue = queue
        #self.pool = ProcessPoolExecutor(max_workers=workers)
        self.servers = {}
        handlers = [
            (r"/add/(.*)", AddTaskHandler, {'queue': queue, 'servers': self.servers}),
            (r"/list", ListTaskHandler, {'queue': queue, 'servers': self.servers}),
            (r"/task/([0-9]+)", TaskByIdHandler, {'queue': queue, 'servers': self.servers}),
            #(r"/server", AddServerHandler, {'queue': queue, 'servers': self.servers}),
            (r"/join/([0-9\.]+):([0-9]+).([0-9]+)", AddGuestHandler, {'queue': queue, 'servers': self.servers}),
        ]
        tornado.web.Application.__init__(self, handlers)

    @gen.coroutine
    def task_runner(self):
        #for task in self.queue:
        running = 0
        for server,jobs in self.servers.items():
            processes = jobs[2:]
            #Get the corresponding task from guest here
            for job in processes:
                num = job['taskid']
                base_url = 'http://{}'.format(server)
                request_url = '{}/task/{}'.format(base_url,num)
                task = requests.get(request_url).json()
                #logger.info(task)
                #for task in processes:
                if task['tasks'][0]['taskstatus'] == RUNNING:
                    running += 1
                    logger.info(running)
                #if task['taskstatus'] in (FAILED, DONE):
                    #remove task here    
                    #pass
                #if task['taskstatus'] == PENDING and task['taskworker'].running():
                #    task['taskstatus'] = RUNNING
                #    running += 1
                #    logger.info("Task ID {} {} {}".format(server, task['taskid'], task['taskstatus']))
                #if task['taskstatus'] in (RUNNING, PENDING) and task['taskworker'].done():
                #    task['taskstatus'] = task['taskworker'].result()
                #    logger.info("Task ID {} {} {}".format(server, task['taskid'], task['taskstatus']))
                #if task['taskstatus'] == RUNNING and not task['taskworker'].done():
                #    running += 1
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
        logger.info("Scheduler address: {}:{}".format(*s.getsockname()))
    queue_loop = tornado.ioloop.IOLoop.instance()
    tornado.ioloop.PeriodicCallback(app.task_runner, 1000).start()

    def shutdown_handler(signum, frame):
        exit_handler()
        for task in asyncio.Task.all_tasks():
            task.cancel()
        sys.exit(0)

    @atexit.register
    def exit_handler():
        logger.info("Scheduler instance shutting down")
        stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, shutdown_handler)
    else:
        signal.signal(signal.SIGQUIT, shutdown_handler)

    logger.info("Scheduler starting up")
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
