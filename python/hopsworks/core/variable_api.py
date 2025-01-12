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

from hopsworks import client


class VariableApi:
    def __init__(self):
        pass

    def get_variable(self, variable: str):
        """Get the configured value for a variable"""

        _client = client.get_instance()

        path_params = ["variables", variable]
        domain = _client._send_request("GET", path_params)

        return domain["successMessage"]

    def get_version(self, software: str):

        _client = client.get_instance()
        path_params = [
            "variables",
            "versions",
        ]

        resp = _client._send_request("GET", path_params)
        for entry in resp:
            if entry["software"] == software:
                return entry["version"]
        return None
