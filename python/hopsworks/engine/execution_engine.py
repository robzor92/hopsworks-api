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

from hopsworks.core import dataset_api, execution_api
import os
import uuid
import time
import logging


class ExecutionEngine:
    def __init__(self, project_id=None):
        self._dataset_api = dataset_api.DatasetApi(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)
        self._log = logging.getLogger(__name__)

    def download_logs(self, execution):
        download_log_dir = os.path.join(os.getcwd(), str(uuid.uuid4()))
        os.mkdir(download_log_dir)

        out_path = None
        if execution.stdout_path is not None and self._dataset_api.exists(
            execution.stdout_path
        ):
            out_path = self._dataset_api.download(
                execution.stdout_path, download_log_dir
            )

        err_path = None
        if execution.stderr_path is not None and self._dataset_api.exists(
            execution.stderr_path
        ):
            err_path = self._dataset_api.download(
                execution.stderr_path, download_log_dir
            )

        return out_path, err_path

    def wait_until_finished(self, job, execution):

        is_yarn_job = (
            job.job_type.lower() == "spark"
            or job.job_type.lower() == "pyspark"
            or job.job_type.lower() == "flink"
        )

        updated_execution = self._execution_api.get(job, execution.id)
        execution_state = None
        while updated_execution.success is None:
            updated_execution = self._execution_api.get(job, execution.id)
            if execution_state != updated_execution.state:
                if is_yarn_job:
                    self._log.info(
                        "Waiting for execution to finish. Current state: {}. Final status: {}".format(
                            updated_execution.state, updated_execution.final_status
                        )
                    )
                else:
                    self._log.info(
                        "Waiting for execution to finish. Current state: {}".format(
                            updated_execution.state
                        )
                    )
            execution_state = updated_execution.state
            time.sleep(3)

        # wait for log files to be aggregated, max 1 minute
        await_time = 20
        log_aggregation_complete = False
        self._log.info("Waiting for log aggregation to finish.")
        while not log_aggregation_complete and await_time >= 0:
            updated_execution = self._execution_api.get(job, execution.id)
            log_aggregation_complete = self._dataset_api.exists(
                updated_execution.stdout_path
            ) and self._dataset_api.exists(updated_execution.stderr_path)
            await_time -= 1
            time.sleep(3)

        if updated_execution is not None:
            execution._final_status = updated_execution.final_status
            execution._state = updated_execution.state
            execution._stdout_path = updated_execution.stdout_path
            execution._stderr_path = updated_execution.stderr_path

        if is_yarn_job and not execution.success:
            self._log.error(
                "Execution failed with status: {}. See the logs for more information.".format(
                    execution.final_status
                )
            )
        elif not is_yarn_job and not execution.success:
            self._log.error(
                "Execution failed with status: {}. See the logs for more information.".format(
                    execution.state
                )
            )
        else:
            self._log.info("Execution finished successfully.")
            return execution
