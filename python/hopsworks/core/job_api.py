#
#   Copyright 2022 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from hopsworks import client, job, util
from hopsworks.client.exceptions import RestAPIError
from hopsworks.client.exceptions import JobException
import json


class JobsApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name

    def create_job(self, name: str, config: dict):

        _client = client.get_instance()

        if self.exists(name):
            raise JobException("A job named {} already exists".format(name))

        config = util.validate_job_conf(config, self._project_name)

        path_params = ["project", self._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            ),
            self._project_id,
            self._project_name,
        )

    def get_job(self, name: str):
        """Get the job.
        :param job: metadata object of job to delete
        :type model_instance: Job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            name,
        ]
        query_params = {"expand": ["creator"]}
        return job.Job.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

    def exists(self, name: str):
        try:
            self.get_job(name)
            return True
        except RestAPIError:
            return False

    def get_configuration(self, type: str):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            type.lower(),
            "configuration",
        ]

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def _delete(self, job):
        """Delete the job and all executions.
        :param job: metadata object of job to delete
        :type model_instance: Job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            str(job.name),
        ]
        _client._send_request("DELETE", path_params)

    def _update_job(self, name: str, config: dict):

        _client = client.get_instance()

        config = util.validate_job_conf(config, self._project_name)

        path_params = ["project", self._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            ),
            self._project_id,
            self._project_name,
        )
