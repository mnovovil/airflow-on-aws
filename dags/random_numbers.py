"""We insert a random number from pipeline, email it, and do nothing else."""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common.email_render import STYLE, build_message, dummy_email, recipients

LOG = logging.getLogger(__name__)

DAG_ID = "random_numbers"
CONFIG_VARIABLE = "ice_config"

NUMS_TASK_ID = "get_nums"
DRAW_TASK_ID = "get_num"
WRITE_ID = "write_txt"
CHECK_TASK_ID = "check_num"
GOOD_TASK_ID = "even_num"
BAD_TASK_ID = "odd_num"
DELETE_ID = "delete_txt"


def get_config() -> dict:
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def txt_path(index: int) -> str:
    """The configured path with the mapped instance's index in its name.

    ``/tmp/trigger.txt`` becomes ``/tmp/trigger_0.txt``, one file per mapped instance:
    every task in the chain runs once per number, and a single path would have them
    writing over each other. The index rather than the number itself, because the same
    number typed twice into the form is two instances and would be one file.
    """
    path = Path(get_config()["filepath_txt"])
    return str(path.with_name(f"{path.stem}_{index}{path.suffix}"))


def get_num(num: int | None = None, **context) -> int | list[dict]:
    """Both ends of the mapping, in one function.

    Called with no number — the ``get_nums`` task — it returns one set of op_kwargs
    per number typed into the trigger form, which is what ``expand`` maps over. The
    numbers are read here rather than off ``params`` at the operator because expand
    takes an upstream task's XCom, not a template.

    Called with one — each mapped instance — it hands that number straight back onto
    XCom under its own map_index.
    """
    if num is None:
        return [{"num": value} for value in context["params"]["numbers"]]

    return num


def write_text(num: int, **context) -> None:
    """Write this instance's number to its own path.

    The number arrives as an op_kwarg rather than off XCom: every task here expands
    over the same ``get_nums`` output, so each instance is handed its own number the
    way the mapped ``get_num`` is. Pulling ``get_num`` by task_id alone would give the
    whole mapped list back instead, which is what the single-task version did.
    """
    path = txt_path(context["ti"].map_index)

    with open(path, "w") as f:
        f.write(str(num))

    LOG.info("wrote %s to %s", num, path)


def check_value(num: int, **context) -> str:
    return GOOD_TASK_ID if num % 2 == 0 else BAD_TASK_ID


def build_and_send_good_email(num: int, **context) -> None:
    """Send this instance's number, and nothing else."""
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    to = recipients(get_config()["email_to"])

    # Context manager rather than a bare constructor: SmtpHook only opens its
    # connection in __enter__, and smtp_client is None until it has.
    with SmtpHook() as smtp:
        message = dummy_email(num, mail_from=smtp.from_email, to=to)
        smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())

    LOG.info("emailed %s to %s", num, ", ".join(to))


def build_and_send_bad_email(num: int, **context) -> None:
    """Say this instance's number is not valid, and nothing else."""
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    to = recipients(get_config()["email_to"])

    with SmtpHook() as smtp:
        message = build_message(
            mail_from=smtp.from_email,
            to=to,
            subject=f"[test] {num} is not valid",
            html_content=(
                f"<html><head><style>{STYLE}</style></head><body>"
                f"<h2>{num}</h2>"
                '<div class="sub">This number is not valid.</div>'
                "</body></html>"
            ),
        )
        smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())

    LOG.info("emailed %s as not valid to %s", num, ", ".join(to))


def delete_text(num: int, **context) -> None:
    """Remove the file this instance's ``write_txt`` wrote.

    Python rather than the ``rm -f`` BashOperator this replaced: the path is now one
    per map_index, and the helper that builds it is the same one the write used, so
    the cleanup cannot drift from what was written. ``num`` is here because every task
    expands over the same op_kwargs; only the index is needed.

    ``missing_ok``: this runs on the ``NONE_FAILED_MIN_ONE_SUCCESS`` rule, so it is
    reached after a write that failed or was skipped, with no file to remove.
    """
    path = txt_path(context["ti"].map_index)
    Path(path).unlink(missing_ok=True)

    LOG.info("deleted %s", path)


# ------------------------------------------------------------------------------ dag

with DAG(
    dag_id=DAG_ID,
    description="Email each number given at trigger time, one mapped task per number",
    schedule=None,
    params={"numbers": [1, 2, 3]},
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
    t_nums = PythonOperator(task_id=NUMS_TASK_ID, python_callable=get_num)
    t_num = PythonOperator.partial(task_id=DRAW_TASK_ID, python_callable=get_num).expand(
        op_kwargs=t_nums.output
    )
    t_write = PythonOperator.partial(task_id=WRITE_ID, python_callable=write_text).expand(
        op_kwargs=t_nums.output
    )
    t_check = BranchPythonOperator.partial(task_id=CHECK_TASK_ID, python_callable=check_value).expand(
        op_kwargs=t_nums.output
    )
    t_good_email = PythonOperator.partial(
        task_id=GOOD_TASK_ID, python_callable=build_and_send_good_email
    ).expand(op_kwargs=t_nums.output)
    t_bad_email = PythonOperator.partial(
        task_id=BAD_TASK_ID, python_callable=build_and_send_bad_email
    ).expand(op_kwargs=t_nums.output)
    t_delete = PythonOperator.partial(
        task_id=DELETE_ID,
        python_callable=delete_text,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    ).expand(op_kwargs=t_nums.output)

    t_nums >> t_num >> t_write >> t_check >> t_good_email >> t_delete
    t_check >> t_bad_email >> t_delete
