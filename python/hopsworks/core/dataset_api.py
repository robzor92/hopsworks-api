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

import math
import os
from tqdm.auto import tqdm
import shutil

from hopsworks import client
from hopsworks.client.exceptions import RestAPIError
from hopsworks.client.exceptions import DatasetException


class DatasetApi:
    def __init__(
        self,
        project_id,
    ):
        self._project_id = project_id

    DEFAULT_FLOW_CHUNK_SIZE = 1048576

    def download(self, path: str, local_path: str = None, overwrite: bool = False):
        """Download a file from the Hopsworks filesystem.

        ```python

        dataset_api = project.get_dataset_api()

        dataset_api.download("Resources/myfile.txt")

        ```

        :param path: path in Hopsworks filesystem
        :type path: str
        :param local_path: path where to download the file in the local filesystem
        :type local_path: str
        :param overwrite: overwrite file if exists
        :type overwrite: bool
        :return: absolute path to downloaded file on local filesystem
        :rtype: str
        """

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "dataset",
            "download",
            "with_auth",
            path,
        ]
        query_params = {"type": "DATASET"}

        # Build the path to download the file on the local fs and return to the user, it should be absolute for consistency
        # Download in CWD if local_path not specified
        if local_path is None:
            local_path = os.path.join(os.getcwd(), os.path.basename(path))
        # If local_path specified, ensure it is absolute
        else:
            if os.path.isabs(local_path):
                local_path = os.path.join(local_path, os.path.basename(path))
            else:
                local_path = os.path.join(
                    os.getcwd(), local_path, os.path.basename(path)
                )

        if os.path.exists(local_path):
            if overwrite:
                if os.path.isfile:
                    os.remove(local_path)
                else:
                    shutil.rmtree(local_path)
            else:
                raise IOError(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        file_size = int(self._get(path)["attributes"]["size"])
        with _client._send_request(
            "GET", path_params, query_params=query_params, stream=True
        ) as response:
            with open(local_path, "wb") as f:
                pbar = tqdm(
                    total=file_size,
                    bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                    desc="Downloading",
                )
                for chunk in response.iter_content(
                    chunk_size=self.DEFAULT_FLOW_CHUNK_SIZE
                ):
                    f.write(chunk)
                    pbar.update(len(chunk))

                pbar.close()
        return local_path

    def upload(self, local_path: str, upload_path: str, overwrite: bool = False):
        """Upload a file to the Hopsworks filesystem.

        ```python

        dataset_api = project.get_dataset_api()

        dataset_api.upload("myfile.txt", "Resources")

        ```

        :param local_path: local path to file
        :type local_path: str
        :param upload_path: path to directory where to upload the file
        :type upload_path: str
        :param overwrite: overwrite file if exists
        :type overwrite: bool
        :return: path to uploaded file on Hopsworks filesystem
        :rtype: str
        """

        # local path could be absolute or relative,
        if not os.path.isabs(local_path) and os.path.exists(
            os.path.join(os.getcwd(), local_path)
        ):
            local_path = os.path.join(os.getcwd(), local_path)

        file_size = os.path.getsize(local_path)

        _, file_name = os.path.split(local_path)

        destination_path = upload_path + "/" + file_name

        if self.exists(destination_path):
            if overwrite:
                self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        num_chunks = math.ceil(file_size / self.DEFAULT_FLOW_CHUNK_SIZE)

        base_params = self._get_flow_base_params(file_name, num_chunks, file_size)

        chunk_number = 1
        with open(local_path, "rb") as f:
            pbar = tqdm(
                total=file_size,
                bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                desc="Uploading",
            )
            while True:
                chunk = f.read(self.DEFAULT_FLOW_CHUNK_SIZE)
                if not chunk:
                    break

                query_params = base_params
                query_params["flowCurrentChunkSize"] = len(chunk)
                query_params["flowChunkNumber"] = chunk_number

                self._upload_request(query_params, upload_path, file_name, chunk)
                pbar.update(query_params["flowCurrentChunkSize"])
                chunk_number += 1

            pbar.close()

        return upload_path + "/" + os.path.basename(local_path)

    def _get_flow_base_params(self, file_name, num_chunks, size):
        return {
            "templateId": -1,
            "flowChunkSize": self.DEFAULT_FLOW_CHUNK_SIZE,
            "flowTotalSize": size,
            "flowIdentifier": str(size) + "_" + file_name,
            "flowFilename": file_name,
            "flowRelativePath": file_name,
            "flowTotalChunks": num_chunks,
        }

    def _upload_request(self, params, path, file_name, chunk):

        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", "upload", path]

        # Flow configuration params are sent as form data
        _client._send_request(
            "POST", path_params, data=params, files={"file": (file_name, chunk)}
        )

    def _get(self, path: str):
        """Get dataset.

        :param path: path to check
        :type path: str
        :return: dataset metadata
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def exists(self, path: str):
        """Check if a path exists in datasets.

        ```python

        dataset_api = project.get_dataset_api()

        dataset_api.exists("Resources/myfile.txt")

        ```

        :param path: path to check
        :type path: str
        :return: boolean whether path exists
        :rtype: bool
        """
        try:
            self._get(path)
            return True
        except RestAPIError:
            return False

    def remove(self, path: str):
        """Remove a path in datasets.

        ```python

        dataset_api = project.get_dataset_api()

        dataset_api.remove("Resources/myfile.txt")

        ```

        :param path: path to remove
        :type path: str
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        return _client._send_request("DELETE", path_params)
