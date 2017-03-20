"""
Test scheduler
"""

import json
import pytest
from scheduler import *

@pytest.fixture
def queue():
    queue = TaskQueue()
    return queue

@pytest.fixture
def app(queue):
    app = Scheduler(queue)
    return app

@pytest.mark.gen_test
def test_add_task(queue, http_client, base_url):
    request_url = "{}/add/1".format(base_url)
    response = yield http_client.fetch(request_url)
    assert response.code == 200
    assert len(queue) == 1

    response = yield http_client.fetch(request_url)
    assert response.code == 200
    assert len(queue) == 2

@pytest.mark.gen_test
def test_list_task(queue, http_client, base_url):
    taskfile = '1'
    request_url = "{}/add/{}".format(base_url, taskfile)
    response = yield http_client.fetch(request_url)

    request_url = "{}/list".format(base_url)
    response = yield http_client.fetch(request_url)
    result = json.loads(response.body.decode('utf-8'))
    tasklist = result['tasks']
    assert response.code == 200
    assert taskfile == tasklist[0]['taskfile']

    # test list growing
    taskfile = '1'
    request_url = "{}/add/{}".format(base_url, taskfile)
    response = yield http_client.fetch(request_url)

    request_url = "{}/list".format(base_url)
    response = yield http_client.fetch(request_url)
    result = json.loads(response.body.decode('utf-8'))
    assert response.code == 200
    assert 2 == result['ntasks']

@pytest.mark.gen_test
def test_list_task_by_id(queue, http_client, base_url):
    taskfile = '1'
    request_url = "{}/add/{}".format(base_url, taskfile)
    response = yield http_client.fetch(request_url)
    result = json.loads(response.body.decode('utf-8'))
    taskid = result['taskid']

    request_url = "{}/list/{}".format(base_url, taskid)
    response = yield http_client.fetch(request_url)
    result = json.loads(response.body.decode('utf-8'))
    tasklist = result['tasks']
    assert response.code == 200
    assert taskfile == tasklist[0]['taskfile']

    # add another task and check if the first task is preserved
    taskfile2 = '2'
    request_url = "{}/add/{}".format(base_url, taskfile2)
    response = yield http_client.fetch(request_url)

    request_url = "{}/list/{}".format(base_url, taskid)
    response = yield http_client.fetch(request_url)
    result = json.loads(response.body.decode('utf-8'))
    tasklist = result['tasks']
    assert response.code == 200
    assert taskfile == tasklist[0]['taskfile']

    # test probing no non-existent task id
    request_url = "{}/list/{}".format(base_url, 999)
    response = yield http_client.fetch(request_url)
    result = json.loads(response.body.decode('utf-8'))
    assert response.code == 200
    assert 0 == result['ntasks']
