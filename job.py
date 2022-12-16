import json
import logging
import time
import weakref
from collections import defaultdict
from datetime import date, datetime
from logging.config import fileConfig
from typing import Callable

from wrapt_timeout_decorator import timeout

CHECK_PERIOD: int = 1


def dep_done(dep_list: list) -> bool:
    if dep_list:
        with open('data.txt') as json_file:
            task_dict = json.load(json_file)
        for dep in dep_list:
            if task_dict[dep.name] == 'running':
                return False
    return True


class KeepRefs(object):
    __refs__ = defaultdict(list)

    def __init__(self):
        self.__refs__[self.__class__].append(weakref.ref(self))

    @classmethod
    def get_instances(cls):
        for inst_ref in cls.__refs__[cls]:
            inst = inst_ref()
            if inst is not None:
                yield inst


class Job(KeepRefs):
    def __init__(self, start_at: date = "", max_working_time: int = -1, tries: int = 1, dependencies: list = None,
                 name: str = "", args: list = None, func: Callable = None):
        super(Job, self).__init__()
        if args is None:
            args = []
        if dependencies is None:
            dependencies = []
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.name = name
        self.status = 'running'
        self.func = func
        self.args = args

    def run(self):
        fileConfig('logging.ini')
        for tri in range(self.tries):
            while True:
                cur_time = datetime.now()
                if ((not self.start_at or self.start_at < cur_time)
                        and dep_done(self.dependencies)):
                    logging.info(
                        f'Выполняю задачу: [{self.name}]. '
                        f'Попытка {tri + 1} из {self.tries}'
                    )

                    try:
                        if self.max_working_time > 0:
                            f = timeout(self.max_working_time)(self.func)
                        else:
                            f = self.func
                        result_func = f(*self.args)
                    except TimeoutError:
                        logging.info(
                            'Превышено максимальное время работы функции')
                        result_func = 'timeout failure'
                    break

                else:
                    logging.info(
                        f'Ожидание времени старта '
                        f'для задачи [{self.name}].'
                    )
                    time.sleep(CHECK_PERIOD)

            if result_func == 'success':
                self.status = 'done'
                logging.info(f'Задача [{self.name}] выполнена.')

                with open('data.txt') as json_file:
                    task_dict = json.load(json_file)
                task_dict[self.name] = 'done'

                with open('data.txt', 'w') as outfile:
                    json.dump(task_dict, outfile)
                break
