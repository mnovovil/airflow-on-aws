"""What the random-email smoke test can get wrong without ever failing a run.

This DAG exists to prove the SMTP path works, so a fault here is doubly expensive: it
is both a broken DAG and a broken instrument for diagnosing the others. Three things
can go wrong quietly.

* the branch. ``check_value`` returns a task_id as a *string*. A typo there is not a
  NameError caught at parse time — it is an AirflowException raised mid-run, and the
  run that surfaces it looks like an SMTP failure in the UI, which is exactly the
  wrong conclusion to draw from this DAG.
* the pairing. Both emails read the same XCom slot as the branch does. Pull from the
  wrong task and the number in the inbox is not the number the branch judged, so an
  odd number arrives announced as valid.
* the guard. Clearing one task without the one upstream leaves nothing on XCom. The
  two email tasks name that; the branch does not (see the xfail below).
"""

from __future__ import annotations

import json
import subprocess
from email import message_from_string
from email.header import decode_header, make_header
from pathlib import Path
from types import SimpleNamespace

import pytest
import random_email as re_dag
from airflow.exceptions import AirflowException

CONFIG = {"email_to": "someone@example.com", "LOW": 1, "HIGH": 100}


class FakeTI:
    """Just enough task_instance for a branch callable and the tasks after it."""

    def __init__(self, values: dict | None = None):
        self.values = dict(values or {})

    def xcom_pull(self, task_ids: str, key: str | None = None):
        return self.values.get(task_ids)


def context(*, xcoms: dict | None = None) -> dict:
    """A task context carrying whatever the draw left behind."""
    return {"ti": FakeTI(xcoms)}


def drawn(value: int) -> dict:
    """A context in which ``draw_num`` returned ``value``."""
    return context(xcoms={re_dag.DRAW_TASK_ID: value})


@pytest.fixture(autouse=True)
def config(monkeypatch, tmp_path):
    """The DAG's configuration, with the trigger path pointed somewhere disposable.

    ``filepath_txt`` is per-test rather than a constant: the write and the cleanup
    are the two tasks that touch a real filesystem, and a shared path would let one
    test's leftovers decide another's result.
    """
    settings = {**CONFIG, "filepath_txt": str(tmp_path / "trigger.txt")}
    monkeypatch.setattr(re_dag, "get_config", lambda: dict(settings))
    return settings


# ------------------------------------------------------------------- draw_random_int


def test_the_draw_stays_inside_the_configured_bounds():
    """Sampled rather than asserted once: randint is inclusive at both ends and an
    off-by-one there would show up on roughly one draw in a hundred, which is a
    fortnight of daily runs before anyone sees it."""
    values = [re_dag.draw_random_int() for _ in range(2000)]
    assert min(values) >= CONFIG["LOW"]
    assert max(values) <= CONFIG["HIGH"]


def test_the_draw_is_an_int_that_xcom_can_carry():
    """XCom serialises the return value. A numpy int or a float would either fail to
    serialise or come back as something ``%`` reads differently on the far side."""
    value = re_dag.draw_random_int()
    assert type(value) is int
    assert json.loads(json.dumps(value)) == value


def test_both_parities_are_reachable():
    """The whole DAG is a branch on parity. A range that could only produce one of
    them would leave half of it dead and the run would still be green."""
    values = {re_dag.draw_random_int() % 2 for _ in range(200)}
    assert values == {0, 1}


# ------------------------------------------------------------------------ check_value


def test_an_even_draw_names_the_even_task():
    assert re_dag.check_value(**drawn(42)) == re_dag.GOOD_BRANCH_ID


def test_an_odd_draw_names_the_odd_task():
    assert re_dag.check_value(**drawn(7)) == re_dag.BAD_BRANCH_ID


@pytest.mark.parametrize("value", [CONFIG["LOW"], CONFIG["HIGH"]])
def test_the_bounds_themselves_are_judged(value):
    """LOW and HIGH are the two draws most likely to be special-cased by accident,
    and they land on opposite branches — 1 is odd, 100 is even."""
    assert re_dag.check_value(**drawn(value)) in {re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID}


def test_the_branch_reads_the_same_number_the_email_will():
    """Both tasks pull from DRAW_TASK_ID. If they ever diverge, an odd number arrives
    announced as valid — the one failure of this DAG that still looks like success."""
    ctx = drawn(8)
    assert re_dag.check_value(**ctx) == re_dag.GOOD_BRANCH_ID
    assert ctx["ti"].xcom_pull(task_ids=re_dag.DRAW_TASK_ID) == 8


@pytest.mark.xfail(
    reason="check_value has no None guard, unlike its two downstream siblings — "
    "clearing check_num alone raises TypeError rather than naming the cause",
    raises=TypeError,
    strict=False,
)
def test_a_cleared_branch_without_its_upstream_says_so():
    """The gap this file is here to record, written as the behaviour that is wanted.

    ``build_and_send_good_email`` and ``build_and_send_bad_email`` both guard the
    identical pull and raise something that names the task. The branch does not, so
    the same operator error arrives as ``unsupported operand type(s) for %``. It is
    also the worst of the three to lose: a branch that dies leaves both downstreams
    unscheduled rather than failing one leaf.

    xfail rather than an assertion on the TypeError, so that adding the guard turns
    this green instead of breaking it.
    """
    with pytest.raises(AirflowException, match=re_dag.DRAW_TASK_ID):
        re_dag.check_value(**context(xcoms={}))


# ------------------------------------------------------------------ writing/deleting


def test_the_file_holds_the_number_that_was_drawn(config):
    re_dag.write_text(**drawn(42))
    assert Path(config["filepath_txt"]).read_text() == "42"


def test_the_file_is_written_where_the_config_says(config, tmp_path):
    """The write and the cleanup agree on a path only because both read the same
    config key. Hard-code either one and the cleanup silently stops matching."""
    re_dag.write_text(**drawn(7))
    assert Path(config["filepath_txt"]).exists()
    assert list(tmp_path.iterdir()) == [Path(config["filepath_txt"])]


def test_a_cleared_write_task_without_its_upstream_says_so(config):
    """The same guard the two email tasks carry. Without it the file is written with
    the string "None" in it, and the run fails further downstream — or worse, does
    not fail at all and leaves that behind for the next one."""
    with pytest.raises(AirflowException, match=re_dag.DRAW_TASK_ID):
        re_dag.write_text(**context(xcoms={}))
    assert not Path(config["filepath_txt"]).exists()


def test_a_second_run_does_not_inherit_the_first_ones_number(config):
    """The file is overwritten, not appended to. Open it in "a" and the number in it
    stops being the number this run drew — which is the same class of fault as the
    branch and the email reading different XCom slots."""
    re_dag.write_text(**drawn(42))
    re_dag.write_text(**drawn(7))
    assert Path(config["filepath_txt"]).read_text() == "7"


# --------------------------------------------------------------------------- emailing


class FakeSmtp:
    """SmtpHook is only a context manager here: it opens in __enter__ and its
    smtp_client is None until it has."""

    sent: list[dict] = []
    opened = 0

    def __enter__(self):
        FakeSmtp.opened += 1
        self.from_email = "ice@example.com"
        self.smtp_client = SimpleNamespace(sendmail=self._sendmail)
        return self

    def __exit__(self, *exc):
        return False

    @staticmethod
    def _sendmail(from_addr, to_addrs, msg):
        FakeSmtp.sent.append({"from": from_addr, "to": to_addrs, "msg": msg})


@pytest.fixture
def smtp(monkeypatch):
    FakeSmtp.sent = []
    FakeSmtp.opened = 0
    monkeypatch.setattr("airflow.providers.smtp.hooks.smtp.SmtpHook", FakeSmtp)
    return FakeSmtp


def rendered(message: dict) -> tuple[str, str]:
    """The subject and body of a sent message, as text.

    The body comes off the wire base64-encoded, because MIMEText with a utf-8 charset
    always is, so it is not searchable in the raw string.
    """
    parsed = message_from_string(message["msg"])
    subject = str(make_header(decode_header(parsed["Subject"])))
    html = next(part for part in parsed.walk() if part.get_content_type() == "text/html")
    return subject, html.get_payload(decode=True).decode()


def test_the_even_email_carries_the_number_that_was_drawn(smtp):
    re_dag.build_and_send_good_email(**drawn(42))
    subject, body = rendered(smtp.sent[0])
    assert subject == "[test] 42"
    assert "<h2>42</h2>" in body


def test_the_odd_email_says_the_number_is_not_valid(smtp):
    re_dag.build_and_send_bad_email(**drawn(7))
    subject, body = rendered(smtp.sent[0])
    assert subject == "[test] 7 is not valid"
    assert "<h2>7</h2>" in body
    assert "not valid" in body


def test_the_two_emails_are_distinguishable_in_an_inbox(smtp):
    """Same sender, same recipients, and a body that is mostly one number. The subject
    is the only thing that says which branch ran, so it has to differ."""
    re_dag.build_and_send_good_email(**drawn(42))
    re_dag.build_and_send_bad_email(**drawn(42))
    assert rendered(smtp.sent[0])[0] != rendered(smtp.sent[1])[0]


@pytest.mark.parametrize("send", ["build_and_send_good_email", "build_and_send_bad_email"])
def test_every_email_goes_to_the_configured_recipients(smtp, send):
    getattr(re_dag, send)(**drawn(42))
    assert smtp.sent[0]["to"] == ["someone@example.com"]
    assert smtp.sent[0]["from"] == "ice@example.com"


@pytest.mark.parametrize("send", ["build_and_send_good_email", "build_and_send_bad_email"])
def test_one_run_is_one_message_and_one_handshake(smtp, send):
    getattr(re_dag, send)(**drawn(42))
    assert len(smtp.sent) == 1
    assert smtp.opened == 1


@pytest.mark.parametrize("send", ["build_and_send_good_email", "build_and_send_bad_email"])
def test_a_cleared_email_task_without_its_upstream_says_so(smtp, send):
    """xcom_pull returns None, and the renderer's error for that names neither the
    task nor the cause."""
    with pytest.raises(AirflowException, match=re_dag.DRAW_TASK_ID):
        getattr(re_dag, send)(**context(xcoms={}))
    assert smtp.sent == []


# ------------------------------------------------------------------------- the wiring


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    from conftest import DAGS_DIR

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False).dags[re_dag.DAG_ID]


def test_the_dag_is_the_six_tasks_the_constants_name(dag):
    """Six tasks, not seven: the notify group is a box drawn round two of them, not
    a task of its own. If a TaskGroup ever shows up here, something has gone wrong."""
    assert set(dag.task_ids) == {
        re_dag.DRAW_TASK_ID,
        re_dag.WRITE_ID,
        re_dag.CHECK_TASK_ID,
        re_dag.GOOD_BRANCH_ID,
        re_dag.BAD_BRANCH_ID,
        re_dag.DELETE_ID,
    }


def test_the_branch_can_only_name_tasks_that_exist(dag):
    """The test this file is most worth having.

    ``check_value`` returns a task_id as a string, so a rename on one side and not the
    other is not caught at parse time. It is an AirflowException mid-run, on a DAG
    whose whole job is to tell you whether email is working.
    """
    returnable = {re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID}
    assert returnable <= set(dag.task_ids)
    assert dag.get_task(re_dag.CHECK_TASK_ID).downstream_task_ids == returnable


def test_every_draw_lands_on_a_branch_the_dag_can_follow():
    """Ties the callable to the graph rather than to the constants: whatever the draw
    produces, check_value names one of the two ids the branch actually hangs off."""
    returnable = {re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID}
    for value in range(CONFIG["LOW"], CONFIG["HIGH"] + 1):
        assert re_dag.check_value(**drawn(value)) in returnable


def test_the_draw_happens_before_everything_that_reads_it(dag):
    """The write and the branch both pull DRAW_TASK_ID's XCom. Hang either one off
    anywhere else and it reads an empty slot."""
    assert dag.get_task(re_dag.DRAW_TASK_ID).downstream_task_ids == {re_dag.WRITE_ID}
    assert dag.get_task(re_dag.WRITE_ID).downstream_task_ids == {re_dag.CHECK_TASK_ID}
    assert dag.get_task(re_dag.CHECK_TASK_ID).upstream_task_ids == {re_dag.WRITE_ID}


@pytest.mark.parametrize("task_id", ["notify_email.even_num", "notify_email.odd_num"])
def test_neither_email_is_downstream_of_the_other(dag, task_id):
    """The two are exclusive. Chain them and the skipped one drags its sibling into
    SKIPPED with it, and the run sends nothing at all while reporting success."""
    assert dag.get_task(task_id).upstream_task_ids == {re_dag.CHECK_TASK_ID}
    assert dag.get_task(task_id).downstream_task_ids == {re_dag.DELETE_ID}


def test_the_cleanup_survives_the_branch_skipping_one_email(dag):
    """The cleanup joins both branches, and exactly one of them is skipped on every
    run. Under the default all_success that skip cascades and the cleanup never runs
    — a green DAG that quietly leaves the file behind every single time.
    """
    from airflow.utils.trigger_rule import TriggerRule

    t_delete = dag.get_task(re_dag.DELETE_ID)
    assert t_delete.upstream_task_ids == {re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID}
    assert t_delete.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


def test_the_branch_operator_is_a_branch(dag):
    """A plain PythonOperator here would run both emails every time and put the task_id
    it returned on XCom as data."""
    from airflow.operators.python import BranchPythonOperator

    assert isinstance(dag.get_task(re_dag.CHECK_TASK_ID), BranchPythonOperator)


# ------------------------------------------------------------------- the notify group

# The group around the two emails is presentation: a collapsible box in the graph view,
# not a task, not a SubDAG, nothing the scheduler runs. The one thing it does change is
# the task_ids — a TaskGroup prefixes everything declared inside it — and this DAG hands
# task_ids around as strings, so that rename reaches the branch. These tests hold the
# box to being only a box, and hold the strings to matching the ids it produced.


def test_the_group_is_a_box_around_exactly_the_two_emails(dag):
    """Widen it and the prefix lands on tasks whose ids are written down elsewhere:
    DRAW_TASK_ID in four xcom_pulls, DELETE_ID in the cleanup assertions."""
    group = dag.task_group_dict[re_dag.NOTIFY_ID]
    assert set(group.children) == {re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID}
    assert dag.get_task(re_dag.DRAW_TASK_ID).task_group.group_id is None
    assert dag.get_task(re_dag.DELETE_ID).task_group.group_id is None


def test_the_branch_constants_are_the_ids_the_group_actually_built(dag):
    """GOOD_BRANCH_ID is assembled by hand from two other constants. This asserts that
    guess against the ids Airflow really assigned, so the day the prefixing changes —
    ``prefix_group_id=False``, a nested group, a rename — the string and the graph are
    compared rather than both being read off the same wrong assumption.
    """
    inside = {task.task_id.rsplit(".", 1)[-1]: task.task_id for task in dag.tasks if task.task_group.group_id}
    assert inside[re_dag.GOOD_TASK_ID] == re_dag.GOOD_BRANCH_ID
    assert inside[re_dag.BAD_TASK_ID] == re_dag.BAD_BRANCH_ID


@pytest.mark.parametrize("value", [42, 7])
def test_the_branch_returns_an_id_skip_all_except_will_accept(dag, value):
    """The failure the group introduced, written as the check Airflow itself performs.

    ``SkipMixin._skip_all_except`` validates the returned id against ``set(dag.task_ids)``
    and raises ``'branch_task_ids' must contain only valid task_ids`` when it misses. A
    branch returning ``even_num`` while the task is called ``notify_email.even_num``
    fails there, mid-run, on the DAG whose job is to tell you whether email works.
    """
    assert re_dag.check_value(**drawn(value)) in set(dag.task_ids)


def test_the_group_moved_the_picture_and_not_the_graph(dag):
    """A TaskGroup is not a task, so ``t_check >> mail_notify >> t_delete`` has to come
    out as the same six edges the two explicit chains made before it — the branch to
    both emails, both emails to the cleanup, and no node in between.
    """
    edges = {task.task_id: task.downstream_task_ids for task in dag.tasks}
    assert edges == {
        re_dag.DRAW_TASK_ID: {re_dag.WRITE_ID},
        re_dag.WRITE_ID: {re_dag.CHECK_TASK_ID},
        re_dag.CHECK_TASK_ID: {re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID},
        re_dag.GOOD_BRANCH_ID: {re_dag.DELETE_ID},
        re_dag.BAD_BRANCH_ID: {re_dag.DELETE_ID},
        re_dag.DELETE_ID: set(),
    }


def test_the_group_did_not_take_the_retry_policy_with_it(dag):
    """default_args reach tasks inside a group the same way they reach the rest. A
    TaskGroup can carry its own, and silently shadowing the DAG's here would leave the
    email tasks — the only ones that touch the network — without their retries.
    """
    for task_id in (re_dag.GOOD_BRANCH_ID, re_dag.BAD_BRANCH_ID):
        assert dag.get_task(task_id).retries == 2


def test_a_smoke_test_does_not_notify_on_failure(dag):
    """Deliberate, and unlike gdal_weather: a failure notice from this DAG would
    travel the exact path that just failed to deliver."""
    assert all(task.on_failure_callback in (None, []) for task in dag.tasks)
    assert dag.default_args.get("on_failure_callback") is None


# --------------------------------------------------------------- the cleanup command

# The cleanup is a BashOperator rather than a callable, so there is no function to call
# and nothing here can be asserted by importing one. What can go wrong lives in the
# string: a path that no longer matches the one the write task used, a placeholder that
# never gets rendered, or a glob that takes more than this run's file with it.


def cleanup_command(dag, settings: dict) -> str:
    """The delete task's bash_command as bash will actually receive it.

    Rendered against a supplied ``var.json`` rather than a real Variable, so the test
    needs no Airflow database — the point is the linkage between the template and the
    config, not Airflow's ability to read its own metadata store.
    """
    task = dag.get_task(re_dag.DELETE_ID)
    return task.render_template(task.bash_command, {"var": {"json": {re_dag.CONFIG_VARIABLE: settings}}})


def test_the_cleanup_removes_the_path_the_config_names(dag, config):
    """``bash_command`` is a templated field. If it stops being one — a rename of the
    operator, a move to a field that is not rendered — the placeholder reaches bash
    verbatim and the run cheerfully removes a file called ``{{ var.json... }}``."""
    assert cleanup_command(dag, config) == f"rm -f '{config['filepath_txt']}'"
    assert "bash_command" in dag.get_task(re_dag.DELETE_ID).template_fields


@pytest.mark.parametrize("name", ["trigger.txt", "a trigger.txt"])
def test_the_cleanup_removes_what_the_write_left_and_nothing_else(dag, tmp_path, name):
    """Run for real, because the failure this replaces was a shell one: the command
    was once ``rm -f /tmp/*.txt``, which took every unrelated file beside it. The
    second case is why the path is quoted — unquoted, a space splits it into two
    arguments and the file the run actually wrote survives.
    """
    path = tmp_path / name
    path.write_text("42")
    bystander = tmp_path / "someone-elses.txt"
    bystander.write_text("not this run's")

    subprocess.run(cleanup_command(dag, {"filepath_txt": str(path)}), shell=True, check=True)

    assert not path.exists()
    assert bystander.exists()


def test_the_cleanup_is_a_no_op_when_there_is_nothing_to_clean(dag, config):
    """Clearing and retrying the cleanup on its own must not fail — the file is
    already gone. ``rm`` without ``-f`` exits 1 there, turning a tidy-up into a red
    run on a DAG whose whole job is to report that email works."""
    subprocess.run(cleanup_command(dag, config), shell=True, check=True)
