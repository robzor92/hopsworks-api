#
#   Copyright 2022 Hopsworks AB
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

import json

from hopsworks import client, library


class LibraryApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name

    def install(self, library_name: str, library_spec: dict):
        """Create Python environment for the project"""
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            client.get_python_version(),
            "libraries",
            library_name,
        ]

        headers = {"content-type": "application/json"}
        library_rest = library.Library.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(library_spec)
            ),
            environment=self,
            project_id=self._project_id,
        )
        return library_rest
