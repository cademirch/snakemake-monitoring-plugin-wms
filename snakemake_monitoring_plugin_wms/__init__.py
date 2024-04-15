from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional

from snakemake_interface_monitoring_plugins.settings import (
    MonitoringProviderSettingsBase,
)
from snakemake_interface_monitoring_plugins.monitoring_provider import (
    MonitoringProviderBase,
)
import sys
import time
import os
import json


@dataclass
class MonitoringProviderSettings(MonitoringProviderSettingsBase):
    workflow_name: Optional[str] = field(
        default=None,
        metadata={"help": "Name for the workflow", "required": False, type: str},
    )
    host: Optional[str] = field(
        default=None,
        metadata={"help": "Address for server", "required": True, "type": str},
    )


class MonitoringProvider(MonitoringProviderBase):
    def __post_init__(self):
        self.token = os.getenv("WMS_MONITOR_TOKEN")
        self.address = self.settings.host
        self.args = []
        self.args = {item[0]: item[1] for item in list(self.args)}

        self.metadata = {}
        self.service_info()

        # Create or retrieve the existing workflow
        self.create_workflow()

    def service_info(self):
        """Service Info ensures that the server is running. We exit on error
        if this isn't the case, so the function can be called in init.
        """
        import requests

        # We first ensure that the server is running, period
        response = requests.get(
            f"{self.address}/api/service-info", headers=self._headers
        )
        if response.status_code != 200:
            sys.stderr.write(f"Problem with server: {self.address} {os.linesep}")
            sys.exit(-1)

        # And then that it's ready to be interacted with
        if response.json().get("status") != "running":
            sys.stderr.write(
                f"The status of the server {self.address} is not in 'running' mode {os.linesep}"
            )
            sys.exit(-1)

    def create_workflow(self):
        """Creating a workflow means pinging the wms server for a new id, or
        if providing an argument for an existing workflow, ensuring that
        it exists and receiving back the same identifier.
        """
        import requests

        # Send the working directory to the server
        workdir = (
            os.getcwd()
            if not self.metadata.get("directory")
            else os.path.abspath(self.metadata["directory"])
        )

        # Prepare a request that has metadata about the job
        metadata = {
            "command": self.metadata.get("command"),
            "workdir": workdir,
        }

        response = requests.get(
            f"{self.address}/create_workflow",
            headers=self._headers,
            params=self.args,
            data=metadata,
        )

        # Extract the id from the response
        id = response.json()["id"]

        # Check the response, will exit on any error
        self.check_response(response, "/create_workflow")

        # Provide server parameters to the logger
        headers = (
            {"Content-Type": "application/json"}
            if self._headers is None
            else {**self._headers, **{"Content-Type": "application/json"}}
        )

        # Send the workflow name to the server
        response_change_workflow_name = requests.put(
            f"{self.address }/api/workflow/{id}",
            headers=headers,
            data=json.dumps(self.args),
        )
        # Check the response, will exit on any error
        self.check_response(response_change_workflow_name, f"/api/workflow/{id}")

        # Provide server parameters to the logger
        self.server = {"url": self.address, "id": id}

    def check_response(self, response, endpoint="wms monitor request"):
        """A helper function to take a response and check for an expected set of
        error codes, 404 (not found), 401 (requires authorization), 403 (permission
        denied), 500 (server error) and 200 (success).
        """
        status_code = response.status_code
        # Cut out early on success
        if status_code == 200:
            return

        if status_code == 404:
            sys.stderr.write(f"The wms {endpoint} endpoint was not found")
            sys.exit(-1)
        elif status_code == 401:
            sys.stderr.write(
                "Authorization is required with a WMS_MONITOR_TOKEN in the environment"
            )
            sys.exit(-1)
        elif status_code == 500:
            sys.stderr.write(
                f"There was a server error when trying to access {endpoint}"
            )
            sys.exit(-1)
        elif status_code == 403:
            sys.stderr.write("Permission is denied to %s." % endpoint)
            sys.exit(-1)

        # Any other response code is not acceptable
        sys.stderr.write(
            f"The {endpoint} response code {response.status_code} is not recognized."
        )

    @property
    def _headers(self):
        """return authenticated headers if the user has provided a token"""
        headers = None
        if self.token:
            headers = {"Authorization": "Bearer %s" % self.token}
        return headers

    def _parse_message(self, msg):
        """Given a message dictionary, we want to loop through the key, value
        pairs and convert some attributes to strings (e.g., jobs are fine to be
        represented as names) and return a dictionary.
        """
        result = {}
        for key, value in msg.items():
            # For a job, the name is sufficient
            if key == "job":
                result[key] = str(value)

            # For an exception, return the name and a message
            elif key == "exception":
                result[key] = "{}: {}".format(
                    msg["exception"].__class__.__name__,
                    msg["exception"] or "Exception",
                )

            # All other fields are json serializable
            else:
                result[key] = value

        # Return a json dumped string
        return json.dumps(result)

    def log_handler(self, msg):
        """Custom wms server log handler.

        Sends the log to the server.

        Args:
            msg (dict):    the log message dictionary
        """
        import requests

        url = self.server["url"] + "/update_workflow_status"
        server_info = {
            "msg": self._parse_message(msg),
            "timestamp": time.asctime(),
            "id": self.server["id"],
        }
        response = requests.post(url, data=server_info, headers=self._headers)
        self.check_response(response, "/update_workflow_status")
