# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import calendar
import logging
import time
import os
import multiprocessing
from airflow.contrib.kubernetes.kubernetes_pod_builder import KubernetesPodBuilder
from airflow.contrib.kubernetes.kubernetes_helper import KubernetesHelper
from queue import Queue
from kubernetes import watch
from airflow import settings
from airflow.contrib.kubernetes.kubernetes_request_factory import SimplePodRequestFactory
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow import configuration
import json
# TODO this is just for proof of concept. remove before merging.




def _prep_command_for_container(command):
    """  
    When creating a kubernetes pod, the yaml expects the command
    in the form of ["cmd","arg","arg","arg"...]
    This function splits the command string into tokens 
    and then matches it to the convention.

    :param command:

    :return:

    """
    return '"' + '","'.join(command.split(' ')[1:]) + '"'


class KubernetesJobWatcher(multiprocessing.Process, object):
    def __init__(self, watch_function, namespace, result_queue, watcher_queue):
        self.logger = logging.getLogger(__name__)
        multiprocessing.Process.__init__(self)
        self.result_queue = result_queue
        self._watch_function = watch_function
        self._watch = watch.Watch()
        self.namespace = namespace
        self.watcher_queue = watcher_queue

    def run(self):
        self.logger.info("Event: and now my watch begins")
        self.logger.info("Event: proof of image change")
        self.logger.info("Event: running {} with {}".format(str(self._watch_function),
                                                     self.namespace))
        for event in self._watch.stream(self._watch_function, self.namespace):
            task= event['object']
            self.logger.info("Event: {} had an event of type {}".format(task.metadata.name,
                                                                        event['type']))
            self.process_status(task.metadata.name, task.status.phase)

    def process_status(self, job_id, status):
        if status == 'Pending':
            self.logger.info("Event: {} Pending".format(job_id))
        elif status == 'Failed':
            self.logger.info("Event: {} Failed".format(job_id))
            self.watcher_queue.put((job_id, State.FAILED))
        elif status == 'Succeeded':
            self.logger.info("Event: {} Succeeded".format(job_id))
            self.watcher_queue.put((job_id, None))
        elif status == 'Running':
            self.logger.info("Event: {} is Running".format(job_id))
        else:
            self.logger.info("Event: Invalid state {} on job {}".format(status, job_id))


class AirflowKubernetesScheduler(object):
    def __init__(self,
                 task_queue,
                 result_queue,
                 running):
        self.logger = logging.getLogger(__name__)
        self.logger.info("creating kubernetes executor")
        self.task_queue = task_queue
        self.namespace = os.environ['k8s_POD_NAMESPACE']
        self.logger.info("k8s: using namespace {}".format(self.namespace))
        self.result_queue = result_queue
        self.current_jobs = {}
        self.running = running
        self._task_counter = 0
        self.watcher_queue = multiprocessing.Queue()
        self.helper = KubernetesHelper()
        w = KubernetesJobWatcher(self.helper.pod_api.list_namespaced_pod, self.namespace,
                                 self.result_queue, self.watcher_queue)
        w.start()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevent info in the current_jobs map so we can track the job's
        status

        :return: 

        """
        self.logger.info('k8s: job is {}'.format(str(next_job)))
        (key, command) = next_job
        self.logger.info("running for command {}".format(command))
        epoch_time = calendar.timegm(time.gmtime())
        command_list = ["/usr/local/airflow/entrypoint.sh"] + command.split()[1:] + \
                       ['-km']
        self._set_host_id(key)
        pod_id = self._create_job_id_from_key(key=key, epoch_time=epoch_time)
        self.current_jobs[pod_id] = key

        image = configuration.get('core','k8s_image')
        print("k8s: launching image {}".format(image))
        pod = KubernetesPodBuilder(
            image= image,
            cmds=command_list,
            kub_req_factory=SimplePodRequestFactory(),
            namespace=self.namespace)
        pod.add_name(pod_id)
        pod.launch()
        self._task_counter += 1

        self.logger.info("k8s: Job created!")

    def delete_job(self, key):
        job_id = self.current_jobs[key]
        self.helper.delete_job(job_id, namespace=self.namespace)

    def sync(self):
        """

        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, it's status is placed in the result queue to 
        be sent back to the scheduler.

        :return:

        """
        while not self.watcher_queue.empty():
            self.end_task()

    def end_task(self):
        job_id, state = self.watcher_queue.get()
        if job_id in self.current_jobs:
            key = self.current_jobs[job_id]
            self.logger.info("finishing job {}".format(key))
            if state:
                self.result_queue.put((key, state))
            self.current_jobs.pop(job_id)
            self.running.pop(key)

    def _create_job_id_from_key(self, key, epoch_time):
        """

        Kubernetes pod names must unique and match specific conventions 
        (i.e. no spaces, period, etc.)
        This function creates a unique name using the epoch time and internal counter

        :param key: 

        :param epoch_time: 

        :return:

        """

        keystr = '-'.join([str(x).replace(' ', '-') for x in key[:2]])
        job_fields = [keystr, str(self._task_counter), str(epoch_time)]
        unformatted_job_id = '-'.join(job_fields)
        job_id = unformatted_job_id.replace('_', '-')
        return job_id

    def _set_host_id(self, key):
        (dag_id, task_id, ex_time) = key
        session = settings.Session()
        item = session.query(TaskInstance) \
            .filter_by(dag_id=dag_id, task_id=task_id, execution_date=ex_time).one()

        host_id = item.hostname
        print("host is {}".format(host_id))


class KubernetesExecutor(BaseExecutor):

    def start(self):
        self.logger.info('k8s: starting kubernetes executor')
        self.task_queue = Queue()
        self._session = settings.Session()
        self.result_queue = Queue()
        self.kub_client = AirflowKubernetesScheduler(self.task_queue,
                                                     self.result_queue,
                                                     running=self.running)

    def sync(self):
        self.kub_client.sync()
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.logger.info("reporting {}".format(results))
            self.change_state(*results)

        # TODO this could be a job_counter based on max jobs a user wants
        if len(self.kub_client.current_jobs) > 3:
            self.logger.info("currently a job is running")
        else:
            self.logger.info("queue ready, running next")
            if not self.task_queue.empty():
                (key, command) = self.task_queue.get()
                self.kub_client.run_next((key, command))

    def terminate(self):
        pass

    def change_state(self, key, state):
        self.logger.info("k8s: setting state of {} to {}".format(key, state))
        if state != State.RUNNING:
            self.kub_client.delete_job(key)
            self.running.pop(key)
        self.event_buffer[key] = state
        (dag_id, task_id, ex_time) = key
        item = self._session.query(TaskInstance).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=ex_time).one()

        if item.state == State.RUNNING or item.state == State.QUEUED:
            item.state = state
            self._session.add(item)
            self._session.commit()

    def end(self):
        self.logger.info('ending kube executor')
        self.task_queue.join()

    def execute_async(self, key, command, queue=None):
        self.logger.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))
