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

from hopsworks import (
    client,
    git_repo,
    git_op_execution,
    util,
    git_commit,
    git_file_status,
)
from hopsworks.client.exceptions import GitException
from hopsworks.engine import git_engine
from hopsworks.core import git_provider_api
from typing import List, Union
from hopsworks.git_file_status import GitFileStatus

import json


class GitReposApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name
        self._git_engine = git_engine.GitEngine(project_id, project_name)
        self._git_provider_api = git_provider_api.GitProviderApi(project_id, project_name)

    def clone(self, url: str, path: str, provider: str, branch: str = None):

        _client = client.get_instance()

        # Support absolute and relative path to dataset
        path = util.convert_to_abs(path, self._project_name)

        path_params = ["project", self._project_id, "git", "clone"]

        clone_config = {
            "url": url,
            "path": path,
            "provider": provider,
            "branch": branch,
        }
        query_params = {"expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps(clone_config),
                query_params=query_params,
            ), self._project_id, self._project_name
        )
        git_op = self._git_engine.execute_op_blocking(git_op, "CLONE")
        return self.get_repo(git_op.repository.name, git_op.repository.path)

    def get_repos(self):
        """Get the repo.
        :param job: metadata object of job to delete
        :type model_instance: Job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
        ]
        query_params = {"expand": "creator"}
        return git_repo.GitRepo.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id, self._project_name
        )

    def get_providers(self):
        return self._git_provider_api.get_providers()

    def get_provider(self, provider: str):
        return self._git_provider_api.get_provider(provider)

    def set_provider(self, provider: str, username: str, token: str):
        self._git_provider_api.set_provider(provider, username, token)

    def get_repo(self, name: str, path: str = None):
        """Get the repo.
        :param job: metadata object of job to delete
        :type model_instance: Job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
        ]
        query_params = {"expand": "creator"}

        repos = git_repo.GitRepo.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id, self._project_name
        )

        if path is not None:
            path = util.convert_to_abs(path, self._project_name)

        filtered_repos = []
        for repository in repos:
            if repository.name == name:
                if path is None:
                    filtered_repos.append(repository)
                elif repository.path == path:
                    filtered_repos.append(repository)

        if len(filtered_repos) == 1:
            return filtered_repos[0]
        elif len(filtered_repos) > 1:
            raise GitException(
                "Multiple repositories found matching name {}, please specify the repository by setting the path keyword".format(
                    name
                )
            )
        else:
            raise GitException("No git repository found matching name {}".format(name))

    def _create(self, repo_id, branch: str, checkout=False):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {"branchName": branch, "expand": "repository"}

        if checkout:
            query_params["action"] = "CREATE_CHECKOUT"
        else:
            query_params["action"] = "CREATE"

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _delete(self, repo_id, branch: str):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {
            "action": "DELETE",
            "branchName": branch,
            "expand": "repository",
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _checkout(self, repo_id, branch: str = None, commit: str = None, force=False):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {"branchName": branch, "commit": commit, "expand": "repository"}

        if force:
            query_params["action"] = "CHECKOUT_FORCE"
        else:
            query_params["action"] = "CHECKOUT"

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _status(self, repo_id):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "STATUS", "expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps({}),
            ), self._project_id, self._project_name
        )
        git_op = self._git_engine.execute_op_blocking(git_op, query_params["action"])

        status_dict = json.loads(git_op.command_result_message)
        file_status = None
        if type(status_dict["status"]) is list:
            file_status = []
            for status in status_dict["status"]:
                file_status.append(
                    git_file_status.GitFileStatus.from_response_json(status)
                )

        return file_status

    def _commit(self, repo_id, message: str, all=False, files=None):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "COMMIT", "expand": ["repository", "user"]}
        commit_config = {
            "type": "commitCommandConfiguration",
            "message": message,
            "all": all,
            "files": files,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(commit_config),
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _push(self, repo_id, remote: str, branch: str, force=False):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "PUSH", "expand": ["repository", "user"]}
        push_config = {
            "type": "pushCommandConfiguration",
            "remoteName": remote,
            "force": force,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(push_config),
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _pull(self, repo_id, remote: str, branch: str, force=False):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "PULL", "expand": ["repository", "user"]}
        push_config = {
            "type": "pullCommandConfiguration",
            "remoteName": remote,
            "force": force,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(push_config),
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _checkout_files(self, repo_id, files: Union[List[str], List[GitFileStatus]]):

        files = util.convert_git_status_to_files(files)

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "file",
        ]

        query_params = {"expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps({"files": files}),
            ), self._project_id, self._project_name
        )
        _ = self._git_engine.execute_op_blocking(git_op, "CHECKOUT_FILES")

    def _get_commits(self, repo_id, branch: str):

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
            branch,
            "commit",
        ]

        headers = {"content-type": "application/json"}
        return git_commit.GitCommit.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )
