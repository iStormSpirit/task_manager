import json
import logging
import multiprocessing
import os
from typing import Callable

from job import Job


def coroutine(f) -> Callable:
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap


def add_list_to_dict(task_dict: dict, task_list: list) -> dict:
    for task in task_list:
        if task.name in task_dict.keys():
            continue
        else:
            task_dict[task.name] = 'running'
    return task_dict


def get_list_jobs(task_dict: dict) -> list:
    actual_task_list = []
    jobs = list(Job.get_instances())
    for key, value in task_dict.items():
        if value == 'running':
            for job in jobs:
                if job.name == key:
                    actual_task_list.append(job)
    return actual_task_list


def write_to_file(write_dict: dict):
    with open('data.txt', 'w') as outfile:
        json.dump(write_dict, outfile)


def read_from_file():
    file_list = os.listdir(".")
    if 'data.txt' not in file_list:
        open("data.txt", "w")
        write_to_file({})
    with open('data.txt') as json_file:
        task_dict = json.load(json_file)
    return task_dict


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self.pool_size = pool_size
        self.task_dict = {}

    @coroutine
    def schedule(self):
        while True:
            task_list = (yield)
            self.task_dict = read_from_file()
            self.task_dict = add_list_to_dict(self.task_dict, task_list)
            write_to_file(self.task_dict)
            actual_task_list = get_list_jobs(self.task_dict)
            processes = []
            for task in actual_task_list:
                p = multiprocessing.Process(target=task.run)
                processes.append(p)
                p.start()
            for process in processes:
                process.join()
            self.task_dict = {}
            write_to_file(self.task_dict)
            logging.info('Все загруженные задачи выполнены!')
