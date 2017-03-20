#!/usr/bin/env python

"""
This is a simple queue client for querying task list.

Requirements:
1. request list of tasks in the queue
2. request detailed task info of a given task ID
"""

import argparse
import requests

def list_task(taskid='', port=8082, addr='localhost'):
    base_url = 'http://{}:{}'.format(addr, port)
    request_url = '{}/list'.format(base_url, taskid)
    if taskid:
        request_url = '{}/task/{}'.format(base_url, taskid)
    r = requests.get(request_url)
    result = r.json()
    for i in range(result['ntasks']):
        task = result['tasks'][i]
        print("{} {} {}".format(task['taskid'], task['taskfile'], task['taskstatus']))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('taskid', nargs='?', help="Task ID")
    parser.add_argument('--address', default='localhost', help='Task scheduler address (default: localhost)')
    parser.add_argument('--port', default=8082, help='Task scheduler port (default: 8082)')
    args = parser.parse_args()

    list_task(args.taskid, args.port, args.address)
