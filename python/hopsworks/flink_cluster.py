#
#   Copyright 2023 Hopsworks AB
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

import time
from hopsworks import job, execution
from hopsworks import flink_execution
from hopsworks.engine import execution_engine
from hopsworks.core import execution_api
from hopsworks.core import flink_cluster_api


class FlinkCluster():
    def __init__(
            self,
            job,
            execution,
            project_id=None,
            project_name=None,
    ):
        self._job = job
        self._execution = execution
        self._project_id=project_id
        self._project_name=project_name

        self._execution_engine = execution_engine.ExecutionEngine(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)
        self._flink_cluster_api = flink_cluster_api.FlinkClusterApi(
            project_id, project_name
        )


    def start(self, await_time=120):
        """Start the flink cluster.

        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        flink_cluster.start()
        ```
        # Arguments
            await_time: defaults to 120 seconds.
        # Raises
            `RestAPIError`: If unable to start the flink cluster.
        """

        execution = self._execution_api._start(self._job)
        updated_execution = self._execution_api._get(self._job, execution.id)
        while updated_execution.state == "INITIALIZING":
            updated_execution = self._execution_api._get(self._job, execution.id)
            if updated_execution.state == "RUNNING":
                print("Cluster is running")
                break

            self._execution_engine._log.info(
                "Waiting for cluster to start. Current state: {}.".format(
                    updated_execution.state
                )
            )

            await_time -= 1
            time.sleep(3)

        if execution.state != "RUNNING":
            raise "FlinkCluster {} did not start within the allocated time and exited with state {}".format(
                execution.id, execution.state
            )

        self._execution = updated_execution

    def get_jobs(self):
        """Get jobs from the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this flink cluster
        flink_cluster.get_jobs()
        ```

        # Returns
            `List[Dict]`: The array of dicts with flink job id and and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        return self._flink_cluster_api._get_jobs(self._execution)

    def get_job(self, job_id):
        """Get specific job from the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this execution
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster.get_job( job_id)
        ```

        # Arguments
            job_id: id of the job within this execution
        # Returns
            `Dict`: Dict with flink job id and and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        return self._flink_cluster_api._get_job(self._execution, job_id)

    def stop_job(self, job_id):
        """Stop specific job in the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # stop the job
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster.stop_job(job_id)
        ```

        # Arguments
            job_id: id of the job within this flink cluster.
        # Raises
            `RestAPIError`: If unable to stop the job
        """
        self._flink_cluster_api._stop_job(self._execution, job_id)

    def get_jars(self):
        """Get already uploaded jars from the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jar files from this execution
        flink_cluster.get_jars()
        ```

        # Returns
            `List[Dict]`: The array of dicts with jar metadata.
        # Raises
            `RestAPIError`: If unable to get jars from the flink cluster.
        """
        return self._flink_cluster_api._get_jars(self._execution)

    def upload_jar(self, jar_file):
        """Uploaded jar file to the specific execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # upload jar file jobs from this execution
        jar_file_path = "./flink-example.jar"
        flink_cluster.upload_jar(jar_file_path)
        ```

        # Arguments
            jar_file: path to the jar file.
        # Raises
            `RestAPIError`: If unable to upload jar file
        """

        self._flink_cluster_api._upload_jar(self._execution, jar_file)

    def submit_job(self, jar_id, main_class, job_arguments=None):
        """Submit job using the specific jar file, already uploaded to this execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # upload jar file jobs from this execution
        main_class = "com.example.Main"
        job_arguments = "-arg1 arg1 -arg2 arg2"

        #get jar file metadata (and select the 1st one for demo purposes)
        jar_metadata = execution.get_jars()[0]
        jar_id = jar_metadata["id"]
        flink_cluster.submit_job(jar_id, main_class, job_arguments=job_arguments)
        ```

        # Arguments
            jar_id: id if the jar file
            main_class: path to the main class of the the jar file
            job_arguments: Job arguments (if any), defaults to none.
        # Returns
            `str`:  job id.
        # Raises
            `RestAPIError`: If unable to submit the job.
        """

        return self._flink_cluster_api._submit_job(
            self._execution, jar_id, main_class, job_arguments
        )

    def job_state(self, job_id):
        """Gets state of the job submitted to the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this flink cluster
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster.job_status(job_id)
        ```

        # Arguments
            job_id: id of the job within this flink cluster
        # Returns
            `str`: status of the job. Possible states:  "INITIALIZING", "CREATED", "RUNNING", "FAILING", "FAILED",
            "CANCELLING", "CANCELED",  "FINISHED", "RESTARTING", "SUSPENDED", "RECONCILING".
        # Raises
            `RestAPIError`: If unable to get the job state from the flink cluster.
        """

        return self._flink_cluster_api._job_state(self._execution, job_id)

    def stop(self):
        """Stop this cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        flink_cluster.stop()
        ```

        # Raises
            `RestAPIError`: If unable to stop the flink cluster.
        """
        self._flink_cluster_api._stop_execution(self._execution)

    @property
    def id(self):
        """Id of the job"""
        return self._job._id

    @property
    def name(self):
        """Name of the job"""
        return self._job._name

    @property
    def creation_time(self):
        """Date of creation for the job"""
        return self._job._creation_time

    @property
    def config(self):
        """Configuration for the job"""
        return self._job._config

    @property
    def creator(self):
        """Creator of the job"""
        return self._job._creator