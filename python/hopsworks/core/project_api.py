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

from hopsworks import client, project, constants
import json
from hopsworks.client.exceptions import RestAPIError


class ProjectApi:
    def exists(self, name: str):
        """Check if a given project exists.
        :param name: name of the project
        :type name: str
        :return: True if project exists, otherwise False
        :rtype: bool
        """
        try:
            self.get_project(name)
            return True
        except RestAPIError:
            return False

    def get_project(self, name: str):
        """Get an existing project.
        :param name: name of the project
        :type name: str
        :return: Project object
        :rtype: Project
        """
        _client = client.get_instance()
        path_params = [
            "project",
            "asShared",
            "getProjectInfo",
            name,
        ]
        project_json = _client._send_request("GET", path_params)
        return project.Project.from_response_json(project_json)

    def create_project(self, name: str, description: str = None):
        """Create a new project.
        :param name: name of the project
        :type name: str
        :param description: optional description of the project
        :type description: str
        :return: Project object
        :rtype: Project
        """
        _client = client.get_instance()

        path_params = ["project"]
        query_params = {"projectName": name}
        headers = {"content-type": "application/json"}

        data = {
            "projectName": name,
            "services": constants.SERVICES.LIST,
            "description": description,
        }
        _client._send_request(
            "POST",
            path_params,
            headers=headers,
            query_params=query_params,
            data=json.dumps(data),
        )

        # The return of the project creation is not a ProjectDTO, so get the correct object after creation
        return self.get_project(name)
