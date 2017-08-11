#!/usr/bin/env python

"""
This is a simple queue client for adding a task to task queue.

Requirements:
1. submit a task to the queue
2. return the task ID to the user
"""

import argparse
import sys
from os.path import realpath, exists
import requests

def add_task(taskfile, port, addr):
    base_url = 'http://{}:{}'.format(addr, port)
    request_url = '{}/add/{}'.format(base_url, taskfile)
    r = requests.get(request_url)
    if r.status_code >= 400:
        print("Task submission failed with code {}".format(r.status_code))
        return
    result = r.json()
    if result['tasksubmitted']:
        print("Task {} submitted".format(result['taskid']))
    else:
        print("Task submission failed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('taskfile', help="Task filename")
    parser.add_argument('--address', default='localhost', help='Task scheduler address (default: localhost)')
    parser.add_argument('--port', default=8082, help='Task scheduler port (default: 8082)')
    args = parser.parse_args()

    if not exists(args.taskfile):
        parser.print_usage()
        print("Taskfile not found.")
        sys.exit(1)

    add_task(realpath(args.taskfile), args.port, args.address)
