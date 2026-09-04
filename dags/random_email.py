"""Draw a random integer, email it, and do nothing else."""

from __future__ import annotations

import logging
from datetime import timedelta
from random import randint

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from common.email_render import STYLE, build_message, dummy_email, recipients

LOG = logging.getLogger(__name__)

DAG_ID = "random_email"
CONFIG_VARIABLE = "ice_config"

DRAW_TASK_ID = "draw_num"
WRITE_ID = "write_txt"
CHECK_TASK_ID = "check_num"
NOTIFY_ID = "notify_email"
GOOD_TASK_ID = "even_num"
BAD_TASK_ID = "odd_num"
DELETE_ID = "delete_txt"
GOOD_BRANCH_ID = f"{NOTIFY_ID}.{GOOD_TASK_ID}"
BAD_BRANCH_ID = f"{NOTIFY_ID}.{BAD_TASK_ID}"


def get_config() -> dict:
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


# ---------------------------------------------------------------------------- tasks


def draw_random_int() -> int:
    """Draw an integer between ``LOW`` and ``HIGH`` inclusive, and log it."""
    value = randint(get_config()["LOW"], get_config()["HIGH"])
    LOG.info("drew %s", value)
    return value


def write_text(**context) -> None:
    """Write the number the previous task drew to the configured path."""
    num = context["ti"].xcom_pull(task_ids=DRAW_TASK_ID)
    if num is None:
        # Same reasoning as the email tasks: clearing this one on its own leaves
        # nothing on XCom, and saying so beats writing the string "None" to the file
        # and failing somewhere further downstream.
        raise AirflowException(f"no value on XCom from {DRAW_TASK_ID} — was it cleared or skipped?")

    path = get_config()["filepath_txt"]

    with open(path, "w") as f:
        f.write(str(num))

    LOG.info("wrote %s to %s", num, path)


def check_value(**context) -> str:
    num = context["ti"].xcom_pull(task_ids=DRAW_TASK_ID)
    return GOOD_BRANCH_ID if num % 2 == 0 else BAD_BRANCH_ID


def build_and_send_good_email(**context) -> None:
    """Send the number the previous task drew, and nothing else."""
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    value = context["ti"].xcom_pull(task_ids=DRAW_TASK_ID)
    if value is None:
        # Reachable by clearing this task on its own without the one upstream. Saying
        # so here beats the TypeError the renderer would raise on int(None).
        raise AirflowException(f"no value on XCom from {DRAW_TASK_ID} — was it cleared or skipped?")

    to = recipients(get_config()["email_to"])

    # Context manager rather than a bare constructor: SmtpHook only opens its
    # connection in __enter__, and smtp_client is None until it has.
    with SmtpHook() as smtp:
        message = dummy_email(value, mail_from=smtp.from_email, to=to)
        smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())

    LOG.info("emailed %s to %s", value, ", ".join(to))


def build_and_send_bad_email(**context) -> None:
    """Say the number the previous task drew is not valid, and nothing else."""
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    value = context["ti"].xcom_pull(task_ids=DRAW_TASK_ID)
    if value is None:
        # Same reasoning as the sibling task: clearing this one on its own leaves
        # nothing on XCom, and saying so beats a TypeError from the renderer.
        raise AirflowException(f"no value on XCom from {DRAW_TASK_ID} — was it cleared or skipped?")

    to = recipients(get_config()["email_to"])

    with SmtpHook() as smtp:
        message = build_message(
            mail_from=smtp.from_email,
            to=to,
            subject=f"[test] {value} is not valid",
            html_content=(
                f"<html><head><style>{STYLE}</style></head><body>"
                f"<h2>{value}</h2>"
                '<div class="sub">This number is not valid.</div>'
                "</body></html>"
            ),
        )
        smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())

    LOG.info("emailed %s as not valid to %s", value, ", ".join(to))


# ------------------------------------------------------------------------------ dag

with DAG(
    dag_id=DAG_ID,
    description="Email a random number, as a smoke test of the SMTP path",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={
        "execution_timeout": timedelta(minutes=1),
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["smoke-test", "email"],
) as dag:
    t_random = PythonOperator(task_id=DRAW_TASK_ID, python_callable=draw_random_int)
    t_write = PythonOperator(task_id=WRITE_ID, python_callable=write_text)
    t_check = BranchPythonOperator(task_id=CHECK_TASK_ID, python_callable=check_value)

    with TaskGroup(group_id=NOTIFY_ID, tooltip="one email per parity") as mail_notify:
        t_good_email = PythonOperator(task_id=GOOD_TASK_ID, python_callable=build_and_send_good_email)
        t_bad_email = PythonOperator(task_id=BAD_TASK_ID, python_callable=build_and_send_bad_email)

    t_delete = BashOperator(
        task_id=DELETE_ID,
        bash_command="rm -f '{{ var.json.ice_config.filepath_txt }}'",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_random >> t_write >> t_check >> mail_notify >> t_delete
