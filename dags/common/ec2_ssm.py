"""EC2 start/stop and SSM Run Command helpers for the GDAL worker.

Written against plain boto3 rather than the Amazon provider's operators so the DAG
does not depend on a particular ``apache-airflow-providers-amazon`` version being
present on the Airflow image.
"""

from __future__ import annotations

import logging
import time

import boto3
from airflow.exceptions import AirflowException

LOG = logging.getLogger(__name__)

TERMINAL_STATUSES = {"Success", "Cancelled", "Failed", "TimedOut"}


def _ec2():
    return boto3.client("ec2")


def _ssm():
    return boto3.client("ssm")


def instance_state(instance_id: str) -> str:
    reservations = _ec2().describe_instances(InstanceIds=[instance_id])["Reservations"]
    return reservations[0]["Instances"][0]["State"]["Name"]


def start_and_wait(instance_id: str, timeout: int = 300, poll: int = 10) -> str:
    """Start the worker if needed and block until SSM will actually accept commands.

    Waiting for the EC2 ``running`` state is not enough: the SSM agent registers
    roughly 30 seconds later, and a SendCommand issued in that window fails with
    InvalidInstanceId. Both waits are needed.
    """
    ec2 = _ec2()
    state = instance_state(instance_id)
    LOG.info("instance %s is %s", instance_id, state)

    if state in ("stopping", "shutting-down"):
        # Racing a shutdown: let it finish, otherwise StartInstances is rejected.
        LOG.info("waiting for in-flight stop to complete before starting")
        ec2.get_waiter("instance_stopped").wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 10, "MaxAttempts": timeout // 10}
        )
        state = instance_state(instance_id)

    if state != "running":
        LOG.info("starting %s", instance_id)
        ec2.start_instances(InstanceIds=[instance_id])

    ec2.get_waiter("instance_running").wait(
        InstanceIds=[instance_id], WaiterConfig={"Delay": 10, "MaxAttempts": max(timeout // 10, 1)}
    )
    LOG.info("EC2 reports running; waiting for the SSM agent to register")

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = _ssm().describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )["InstanceInformationList"]
        if info and info[0].get("PingStatus") == "Online":
            LOG.info("SSM agent online after start")
            return "running"
        time.sleep(poll)

    raise AirflowException(
        f"{instance_id} did not report Online to SSM within {timeout}s. "
        "Check the instance profile has AmazonSSMManagedInstanceCore and that the "
        "private subnet can reach the SSM endpoints through the NAT gateway."
    )


def stop(instance_id: str) -> str:
    state = instance_state(instance_id)
    if state in ("stopped", "stopping"):
        LOG.info("instance %s already %s", instance_id, state)
        return state
    LOG.info("stopping %s", instance_id)
    _ec2().stop_instances(InstanceIds=[instance_id])
    return "stopping"


def run_shell(
    instance_id: str,
    commands: list[str],
    comment: str = "",
    timeout: int = 3600,
    poll: int = 10,
    log_group: str | None = None,
) -> str:
    """Run a shell script on the instance and return its stdout.

    Raises with the captured stderr on any non-Success outcome so the failure shows
    up in the Airflow task log rather than having to be dug out of SSM.
    """
    ssm = _ssm()
    kwargs = {
        "InstanceIds": [instance_id],
        "DocumentName": "AWS-RunShellScript",
        "Comment": comment[:100],
        "Parameters": {"commands": commands, "executionTimeout": [str(timeout)]},
        "TimeoutSeconds": 600,
    }
    if log_group:
        # Full output goes to CloudWatch; the API response itself is capped at ~24 KB.
        kwargs["CloudWatchOutputConfig"] = {
            "CloudWatchLogGroupName": log_group,
            "CloudWatchOutputEnabled": True,
        }

    command_id = ssm.send_command(**kwargs)["Command"]["CommandId"]
    LOG.info("SSM command %s dispatched to %s", command_id, instance_id)

    deadline = time.monotonic() + timeout
    invocation = None
    while time.monotonic() < deadline:
        time.sleep(poll)
        try:
            invocation = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        except ssm.exceptions.InvocationDoesNotExist:
            # Send and invocation registration are eventually consistent.
            continue

        status = invocation["Status"]
        if status in TERMINAL_STATUSES:
            stdout = invocation.get("StandardOutputContent", "")
            stderr = invocation.get("StandardErrorContent", "")
            if stdout:
                LOG.info("stdout:\n%s", stdout)
            if status == "Success":
                return stdout
            raise AirflowException(
                f"SSM command {command_id} finished as {status} "
                f"(exit {invocation.get('ResponseCode')}).\nstderr:\n{stderr or '(empty)'}"
            )
        LOG.info("command %s is %s", command_id, status)

    raise AirflowException(f"SSM command {command_id} did not finish within {timeout}s")
