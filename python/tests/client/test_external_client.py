#
#   Copyright 2026 Hopsworks AB
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

import contextlib
import os
import stat
import sys
import tempfile

from hopsworks_common.client.external import Client


# Windows doesn't support Unix-style file permissions
IS_WINDOWS = sys.platform == "win32"


class TestExternalClient:
    def _create_client(self, cert_folder, mocker):
        """Create a test client with mocked dependencies."""
        mocker.patch("hopsworks_common.client.external.Client._get_verify", return_value=False)
        mocker.patch("requests.session")

        client = Client.__new__(Client)
        client._host = "test.hopsworks.ai"
        client._port = 443
        client._base_url = "https://test.hopsworks.ai:443"
        client._cert_folder_base = cert_folder
        client._project_name = "test_project"
        client._project_id = "123"
        client._username = "testuser"
        client._connected = True
        client._engine = "python"
        client._cert_key = None
        # User-specific cert folder
        client._cert_folder = os.path.join(
            cert_folder, "test.hopsworks.ai", "test_project", "testuser"
        )

        return client

    def test_certificate_paths_are_in_user_directory(self, mocker):
        """Test that certificate file paths are in user-specific directory."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            # Verify all certificate paths are in user directory
            user_dir = os.path.join(cert_folder, "test.hopsworks.ai", "test_project", "testuser")
            assert client._get_ca_chain_path().startswith(user_dir)
            assert client._get_client_cert_path().startswith(user_dir)
            assert client._get_client_key_path().startswith(user_dir)
            assert client._get_material_passwd_path().startswith(user_dir)

    def test_certificate_paths_have_fixed_names(self, mocker):
        """Test that certificate files have fixed names (no username prefix)."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            # Verify files have fixed names for delta-rs compatibility
            assert client._get_ca_chain_path().endswith("ca_chain.pem")
            assert client._get_client_cert_path().endswith("client_cert.pem")
            assert client._get_client_key_path().endswith("client_key.pem")
            assert client._get_material_passwd_path().endswith("material_passwd")

    def test_certificate_paths_structure(self, mocker):
        """Test that certificate paths follow the expected structure."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            expected_base = os.path.join(
                cert_folder, "test.hopsworks.ai", "test_project", "testuser"
            )

            assert client._get_ca_chain_path() == os.path.join(expected_base, "ca_chain.pem")
            assert client._get_client_cert_path() == os.path.join(expected_base, "client_cert.pem")
            assert client._get_client_key_path() == os.path.join(expected_base, "client_key.pem")
            assert client._get_material_passwd_path() == os.path.join(expected_base, "material_passwd")

    def test_write_pem_file_creates_file_with_secure_permissions(self, mocker):
        """Test that _write_pem_file creates files with 0o600 permissions."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)
            test_file = os.path.join(cert_folder, "test.pem")

            client._write_pem_file("test content", test_file)

            # Verify file exists and has correct permissions
            assert os.path.exists(test_file)
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                file_stat = os.stat(test_file)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    def test_write_pem_file_binary_mode(self, mocker):
        """Test that _write_pem_file works with binary mode."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)
            test_file = os.path.join(cert_folder, "test.bin")

            client._write_pem_file(b"binary content", test_file, mode="wb")

            # Verify file exists, has correct permissions, and correct content
            assert os.path.exists(test_file)
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                file_stat = os.stat(test_file)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600

            with open(test_file, "rb") as f:
                assert f.read() == b"binary content"

    def test_makedirs_with_sticky_bit_creates_directories(self, mocker):
        """Test that _makedirs_with_sticky_bit creates host, project, and user directories."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            client._makedirs_with_sticky_bit()

            host_dir = os.path.join(cert_folder, "test.hopsworks.ai")
            project_dir = os.path.join(host_dir, "test_project")
            user_dir = os.path.join(project_dir, "testuser")

            assert os.path.isdir(host_dir)
            assert os.path.isdir(project_dir)
            assert os.path.isdir(user_dir)

    def test_makedirs_with_sticky_bit_sets_correct_permissions(self, mocker):
        """Test that _makedirs_with_sticky_bit sets correct permissions on directories."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            client._makedirs_with_sticky_bit()

            host_dir = os.path.join(cert_folder, "test.hopsworks.ai")
            project_dir = os.path.join(host_dir, "test_project")
            user_dir = os.path.join(project_dir, "testuser")

            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                host_mode = stat.S_IMODE(os.stat(host_dir).st_mode)
                project_mode = stat.S_IMODE(os.stat(project_dir).st_mode)
                user_mode = stat.S_IMODE(os.stat(user_dir).st_mode)

                # Shared directories have sticky bit (1777)
                assert host_mode == 0o1777, f"Expected 0o1777, got {oct(host_mode)}"
                assert project_mode == 0o1777, f"Expected 0o1777, got {oct(project_mode)}"
                # User directory is private (700)
                assert user_mode == 0o700, f"Expected 0o700, got {oct(user_mode)}"

    def test_makedirs_with_sticky_bit_skips_existing_directories(self, mocker):
        """Test that _makedirs_with_sticky_bit doesn't fail on existing directories."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            # Create directories first
            client._makedirs_with_sticky_bit()

            # Call again - should not raise
            client._makedirs_with_sticky_bit()

    def test_close_deletes_files_and_user_directory(self, mocker):
        """Test that _close deletes certificate files and user directory."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            # Create directory structure
            client._makedirs_with_sticky_bit()
            client._trust_store_path = os.path.join(client._cert_folder, "trustStore.jks")
            client._key_store_path = os.path.join(client._cert_folder, "keyStore.jks")

            # Create test certificate files
            test_files = [
                client._trust_store_path,
                client._key_store_path,
                client._get_material_passwd_path(),
                client._get_ca_chain_path(),
                client._get_client_cert_path(),
                client._get_client_key_path(),
            ]

            for test_file in test_files:
                client._write_pem_file("test", test_file)

            # Verify files exist
            for test_file in test_files:
                assert os.path.exists(test_file), f"File should exist: {test_file}"

            user_dir = client._cert_folder
            project_dir = os.path.dirname(user_dir)

            # Close client
            client._close()

            # Verify files are deleted
            for test_file in test_files:
                assert not os.path.exists(test_file), f"File should be deleted: {test_file}"

            # Verify user directory is removed (was empty after file cleanup)
            assert not os.path.exists(user_dir), "User directory should be removed"

            # Verify shared directories still exist
            assert os.path.isdir(project_dir), "Project directory should still exist"

    def test_close_without_cert_folder_does_not_fail(self, mocker):
        """Test that _close handles None cert_folder gracefully."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)
            client._cert_folder = None

            # Should not raise
            client._close()

            assert client._connected is False

    def test_materialize_certs_creates_user_directory(self, mocker):
        """Test that _materialize_certs creates user-specific directory."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)
            client._cert_folder = None  # Reset to test creation

            # Mock _get_credentials to return test data
            mock_credentials = {
                "kStore": "dGVzdA==",  # base64 "test"
                "tStore": "dGVzdA==",
                "password": "testpass",
                "caChain": "chain",
                "clientCert": "cert",
                "clientKey": "key",
            }
            mocker.patch.object(client, "_get_credentials", return_value=mock_credentials)

            client._materialize_certs()

            # Verify user directory was created
            expected_cert_folder = os.path.join(
                cert_folder, "test.hopsworks.ai", "test_project", "testuser"
            )
            assert client._cert_folder == expected_cert_folder
            assert os.path.isdir(client._cert_folder)

            # Verify files have fixed names (no username prefix)
            assert client._trust_store_path == os.path.join(client._cert_folder, "trustStore.jks")
            assert client._key_store_path == os.path.join(client._cert_folder, "keyStore.jks")
            assert client._cert_key_path == os.path.join(client._cert_folder, "material_passwd")

    def test_write_b64_cert_to_bytes_uses_secure_permissions(self, mocker):
        """Test that _write_b64_cert_to_bytes creates files with 0o600 permissions."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)
            test_file = os.path.join(cert_folder, "test.jks")

            # base64 encoded "test content"
            b64_content = "dGVzdCBjb250ZW50"
            client._write_b64_cert_to_bytes(b64_content, test_file)

            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                # Verify file has correct permissions
                file_stat = os.stat(test_file)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    def test_get_certs_folder_returns_user_specific_path(self, mocker):
        """Test that get_certs_folder returns the user-specific directory path."""
        with tempfile.TemporaryDirectory() as cert_folder:
            client = self._create_client(cert_folder, mocker)

            expected = os.path.join(
                cert_folder, "test.hopsworks.ai", "test_project", "testuser"
            )
            assert client.get_certs_folder() == expected

    def _create_client_with_username(self, cert_folder, username, mocker):
        """Create a test client with a specific username."""
        mocker.patch("hopsworks_common.client.external.Client._get_verify", return_value=False)
        mocker.patch("requests.session")

        client = Client.__new__(Client)
        client._host = "test.hopsworks.ai"
        client._port = 443
        client._base_url = "https://test.hopsworks.ai:443"
        client._cert_folder_base = cert_folder
        client._project_name = "test_project"
        client._project_id = "123"
        client._username = username
        client._connected = True
        client._engine = "python"
        client._cert_key = None
        # User-specific directory
        client._cert_folder = os.path.join(
            cert_folder, "test.hopsworks.ai", "test_project", username
        )
        client._trust_store_path = os.path.join(client._cert_folder, "trustStore.jks")
        client._key_store_path = os.path.join(client._cert_folder, "keyStore.jks")

        return client

    def _create_cert_files_for_client(self, client):
        """Create all certificate files for a client and return the list of paths."""
        cert_files = [
            client._trust_store_path,
            client._key_store_path,
            client._get_material_passwd_path(),
            client._get_ca_chain_path(),
            client._get_client_cert_path(),
            client._get_client_key_path(),
        ]

        for cert_file in cert_files:
            client._write_pem_file("test content", cert_file)

        return cert_files

    def test_close_does_not_delete_other_users_certificates(self, mocker):
        """Test that _close only deletes the current user's certificates, not other users'."""
        with tempfile.TemporaryDirectory() as cert_folder:
            # Create two clients with different usernames (simulating two Hopsworks users)
            user1_client = self._create_client_with_username(cert_folder, "alice", mocker)
            user2_client = self._create_client_with_username(cert_folder, "bob", mocker)

            # Create the directory structure for both users
            user1_client._makedirs_with_sticky_bit()
            user2_client._makedirs_with_sticky_bit()

            # Create certificate files for both users
            user1_files = self._create_cert_files_for_client(user1_client)
            user2_files = self._create_cert_files_for_client(user2_client)

            # Verify all files exist
            for f in user1_files + user2_files:
                assert os.path.exists(f), f"File should exist before cleanup: {f}"

            # Save user1's directory path before close (which sets it to None)
            user1_dir = user1_client._cert_folder

            # User 1 logs out
            user1_client._close()

            # Verify user 1's files are deleted
            for f in user1_files:
                assert not os.path.exists(f), f"User1's file should be deleted: {f}"

            # Verify user 1's directory is removed
            assert not os.path.exists(user1_dir), "User1's directory should be removed"

            # Verify user 2's files still exist
            for f in user2_files:
                assert os.path.exists(f), f"User2's file should NOT be deleted: {f}"

    def test_close_does_not_affect_other_users_in_same_project(self, mocker):
        """Test multi-user scenario where users share the same project directory."""
        with tempfile.TemporaryDirectory() as cert_folder:
            # Create three users in the same project
            alice = self._create_client_with_username(cert_folder, "alice", mocker)
            bob = self._create_client_with_username(cert_folder, "bob", mocker)
            charlie = self._create_client_with_username(cert_folder, "charlie", mocker)

            # Create directories for all users
            alice._makedirs_with_sticky_bit()
            bob._makedirs_with_sticky_bit()
            charlie._makedirs_with_sticky_bit()

            # Create certs for all users
            alice_files = self._create_cert_files_for_client(alice)
            bob_files = self._create_cert_files_for_client(bob)
            charlie_files = self._create_cert_files_for_client(charlie)

            project_dir = os.path.dirname(alice._cert_folder)
            bob_dir = bob._cert_folder  # Save before close sets it to None

            # Bob logs out
            bob._close()

            # Alice's files should still exist
            for f in alice_files:
                assert os.path.exists(f), f"Alice's file should exist: {f}"

            # Bob's files should be deleted
            for f in bob_files:
                assert not os.path.exists(f), f"Bob's file should be deleted: {f}"

            # Bob's directory should be removed
            assert not os.path.exists(bob_dir), "Bob's directory should be removed"

            # Charlie's files should still exist
            for f in charlie_files:
                assert os.path.exists(f), f"Charlie's file should exist: {f}"

            # Shared project directory should still exist
            assert os.path.isdir(project_dir), "Shared project directory should still exist"

    def test_multiple_users_have_separate_directories(self, mocker):
        """Test that multiple users have their own separate directories."""
        with tempfile.TemporaryDirectory() as cert_folder:
            users = ["alice", "bob", "charlie", "diana"]
            clients = []
            all_files = {}

            # Create clients and certificates for all users
            for username in users:
                client = self._create_client_with_username(cert_folder, username, mocker)
                client._makedirs_with_sticky_bit()
                clients.append(client)
                all_files[username] = self._create_cert_files_for_client(client)

            # Verify all users have separate directories
            user_dirs = set()
            for client in clients:
                assert os.path.isdir(client._cert_folder)
                assert client._cert_folder not in user_dirs, "User directories should be unique"
                user_dirs.add(client._cert_folder)

            # Verify each user's directory contains their username
            for client in clients:
                assert client._username in client._cert_folder

            # Verify all users' files exist
            for username, files in all_files.items():
                for f in files:
                    assert os.path.exists(f), f"{username}'s file should exist: {f}"


class TestCertFolderParameter:
    """Tests for the cert_folder parameter in hopsworks.login()."""

    def test_cert_folder_default_uses_tempdir(self, mocker):
        """Test that cert_folder defaults to system temp directory when not specified."""
        import hopsworks

        # Mock to prevent actual connection
        mock_connection = mocker.patch("hopsworks.Connection.connection")

        # Clear any existing env var
        env_backup = os.environ.pop("HOPSWORKS_CERT_FOLDER", None)
        try:
            # Call login with minimal args (will fail but we just check the call)
            with contextlib.suppress(Exception):
                hopsworks.login(host="test.hopsworks.ai", api_key_value="fake_key")

            # Verify Connection was called with cert_folder=tempfile.gettempdir()
            mock_connection.assert_called()
            call_kwargs = mock_connection.call_args[1]
            assert call_kwargs.get("cert_folder") == tempfile.gettempdir()
        finally:
            if env_backup is not None:
                os.environ["HOPSWORKS_CERT_FOLDER"] = env_backup
            hopsworks.logout()

    def test_cert_folder_from_environment_variable(self, mocker):
        """Test that HOPSWORKS_CERT_FOLDER environment variable is used."""
        import hopsworks

        mock_connection = mocker.patch("hopsworks.Connection.connection")

        custom_folder = "/custom/cert/folder"
        os.environ["HOPSWORKS_CERT_FOLDER"] = custom_folder
        try:
            with contextlib.suppress(Exception):
                hopsworks.login(host="test.hopsworks.ai", api_key_value="fake_key")

            mock_connection.assert_called()
            call_kwargs = mock_connection.call_args[1]
            assert call_kwargs.get("cert_folder") == custom_folder
        finally:
            del os.environ["HOPSWORKS_CERT_FOLDER"]
            hopsworks.logout()

    def test_cert_folder_parameter_overrides_environment(self, mocker):
        """Test that cert_folder parameter takes precedence over environment variable."""
        import hopsworks

        mock_connection = mocker.patch("hopsworks.Connection.connection")

        os.environ["HOPSWORKS_CERT_FOLDER"] = "/env/cert/folder"
        param_folder = "/param/cert/folder"
        try:
            with contextlib.suppress(Exception):
                hopsworks.login(
                    host="test.hopsworks.ai",
                    api_key_value="fake_key",
                    cert_folder=param_folder,
                )

            mock_connection.assert_called()
            call_kwargs = mock_connection.call_args[1]
            assert call_kwargs.get("cert_folder") == param_folder
        finally:
            del os.environ["HOPSWORKS_CERT_FOLDER"]
            hopsworks.logout()

    def test_cert_folder_stored_in_connection(self):
        """Test that cert_folder is correctly stored in the Connection object."""
        from hopsworks_common.connection import Connection

        custom_folder = "/my/custom/certs"

        # Create connection object without calling connect()
        conn = Connection.__new__(Connection)
        conn._cert_folder = custom_folder

        # Verify cert_folder property returns the correct value
        assert conn.cert_folder == custom_folder

    def test_cert_folder_default_in_connection(self):
        """Test that Connection has the correct default cert_folder."""
        from hopsworks_common.connection import CERT_FOLDER_DEFAULT

        assert CERT_FOLDER_DEFAULT == "/tmp"
