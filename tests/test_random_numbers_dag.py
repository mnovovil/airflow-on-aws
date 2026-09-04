"""What the mapped version of the random-email smoke test can get wrong quietly.

This DAG is the random-email one with the draw replaced by dynamic task mapping: the
numbers are typed into the trigger form and ``get_num`` runs at both ends of the map —
once with no argument, listing one set of op_kwargs per number, then once per number as
the task expanded over that list. Three things can go wrong without failing at parse
time.

* the key. ``expand(op_kwargs=...)`` hands each dict to the callable as keyword
  arguments, so the key has to be the callable's own parameter name — the two ends
  are one function, and nothing complains until a mapped instance actually runs.
* the source. The numbers are the run's own input, not configuration — a fallback to
  ``ice_config`` here would mean a trigger form that silently does nothing.
* the two ends. ``num=None`` is what tells the function which end it is being called
  from. A default on that parameter that is not None, or a mapped instance that is
  handed nothing, and the listing branch runs where a number was meant to come back.

The last test in the file records the part that is deliberately not mapped: everything
downstream of ``get_num`` still pulls the whole list off XCom.
"""

from __future__ import annotations

import inspect
import json

import pytest
import random_email as re_dag
import random_numbers as rn_dag
from airflow.models.mappedoperator import MappedOperator
from airflow.operators.python import PythonOperator


def form(numbers: list[int]) -> dict:
    """A task context for a run triggered with ``numbers`` in its config."""
    return {"params": {"numbers": numbers}}


# ------------------------------------------------------------------------ listing end


def test_one_mapped_call_per_number_typed_into_the_form():
    """Order included: the mapped instances are indexed in the order this list is in,
    so a set or a dict here would shuffle which number map_index 0 belongs to."""
    assert rn_dag.get_num(**form([4, 7, 8])) == [{"num": 4}, {"num": 7}, {"num": 8}]


def test_the_key_is_the_parameter_the_mapped_end_is_called_with():
    """``op_kwargs`` are passed by keyword. A key that is not the function's own
    parameter name is a TypeError raised once per mapped instance, mid-run — and with
    both ends in one function, a rename cannot break only one of them."""
    signature = inspect.signature(rn_dag.get_num)
    keys = {key for kwargs in rn_dag.get_num(**form([1, 2])) for key in kwargs}
    assert keys == {"num"} <= set(signature.parameters)
    assert signature.parameters["num"].default is None


def test_a_form_with_no_numbers_is_no_mapped_instances():
    """An empty expand input leaves ``get_num`` skipped rather than failed, which is
    the right shape for a run triggered with an empty list — but only if the callable
    returns an empty list rather than raising or inventing a default."""
    assert rn_dag.get_num(**form([])) == []


def test_the_numbers_come_from_the_form_and_not_from_ice_config(monkeypatch):
    """The whole point of the change: the numbers are the run's input. Reaching for
    the Variable here would give every run the same numbers and make the trigger form
    decorative — and it would do it silently, because the config has values to give."""
    monkeypatch.setattr(rn_dag, "get_config", lambda: pytest.fail("read ice_config"))
    assert rn_dag.get_num(**form([5])) == [{"num": 5}]


def test_the_expand_input_is_something_xcom_can_carry():
    """The list travels to the mapped task through XCom, which serialises it. A value
    that does not round-trip is an error on the upstream task, not on the map."""
    kwargs = rn_dag.get_num(**form([1, 2, 3]))
    assert json.loads(json.dumps(kwargs)) == kwargs


# ------------------------------------------------------------------------- mapped end


def test_a_number_is_handed_straight_on():
    """The mapped end is the identity on purpose — the number reaching XCom unchanged
    is what everything downstream reads. It is also the branch a mapped instance must
    take: return the listing here and every number becomes the whole list."""
    value = rn_dag.get_num(num=42, **form([1, 2, 3]))
    assert value == 42
    assert type(value) is int


def test_zero_is_a_number_and_not_an_absent_one():
    """``if num is None`` rather than ``if not num``: 0 is falsy, and the sloppy test
    would send that one mapped instance off to list the form instead."""
    assert rn_dag.get_num(num=0, **form([0])) == 0


# ------------------------------------------------------------------------- the wiring


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    from conftest import DAGS_DIR

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[rn_dag.DAG_ID]


def test_the_draw_is_mapped_rather_than_a_single_task(dag):
    """A plain PythonOperator here is not a broken map, it is a task called with no
    ``num`` at all — one TypeError per run, on a DAG whose job is to look like the
    email path works."""
    assert isinstance(dag.get_task(rn_dag.DRAW_TASK_ID), MappedOperator)
    assert isinstance(dag.get_task(rn_dag.NUMS_TASK_ID), PythonOperator)


def test_both_tasks_run_the_one_callable(dag):
    """The two ends are one function, so both operators have to point at that one
    object — two callables here is the arrangement this file exists to have replaced.

    Compared to each other rather than to ``rn_dag.get_num``: the DagBag loads the DAG
    file as a module of its own, so its functions are not the ones this file imported.
    """
    listing = dag.get_task(rn_dag.NUMS_TASK_ID).python_callable
    mapped = dag.get_task(rn_dag.DRAW_TASK_ID).partial_kwargs["python_callable"]
    assert listing is mapped
    assert listing.__name__ == rn_dag.get_num.__name__


def test_the_mapping_expands_over_what_the_listing_task_returns(dag):
    """The expand input is an XComArg, which is also what creates the dependency. Map
    over a literal list instead and the task stops seeing the form at all, while the
    DAG still parses and still runs."""
    expand_input = dag.get_task(rn_dag.DRAW_TASK_ID).expand_input
    assert set(expand_input.value) == {"op_kwargs"}
    assert expand_input.value["op_kwargs"].operator.task_id == rn_dag.NUMS_TASK_ID
    assert dag.get_task(rn_dag.DRAW_TASK_ID).upstream_task_ids == {rn_dag.NUMS_TASK_ID}


MAPPED_TASK_IDS = {
    rn_dag.DRAW_TASK_ID,
    rn_dag.WRITE_ID,
    rn_dag.CHECK_TASK_ID,
    rn_dag.GOOD_TASK_ID,
    rn_dag.BAD_TASK_ID,
    rn_dag.DELETE_ID,
}


def test_the_numbers_are_read_before_anything_is_mapped(dag):
    """The listing task is the head of the chain, and nothing can expand until it has
    returned — so it has no upstream, and every mapped task has it as one.

    The fan-out to all six rather than to ``get_num`` alone is the design, not an extra
    edge: each task is handed its own number as an op_kwarg because pulling a mapped
    upstream by task_id gives back the whole list instead (see ``write_text``). The
    ordering that matters is still enforced, by the expand input rather than by depth.
    """
    assert dag.get_task(rn_dag.NUMS_TASK_ID).upstream_task_ids == set()
    assert dag.get_task(rn_dag.NUMS_TASK_ID).downstream_task_ids == MAPPED_TASK_IDS


@pytest.mark.parametrize("task_id", sorted(MAPPED_TASK_IDS))
def test_every_mapped_task_expands_over_the_same_listing(dag, task_id):
    """What the assertion above is really about, task by task.

    One task left reading ``DRAW_TASK_ID`` off XCom instead of expanding would still
    leave the fan-out looking right, and would still parse — it would just hand that
    task the whole list where every sibling gets one number.
    """
    from airflow.models.mappedoperator import MappedOperator

    task = dag.get_task(task_id)
    assert isinstance(task, MappedOperator), f"{task_id} is a single task, not a mapped one"
    assert set(task.expand_input.value) == {"op_kwargs"}
    assert task.expand_input.value["op_kwargs"].operator.task_id == rn_dag.NUMS_TASK_ID


def test_the_dag_only_runs_when_it_is_triggered(dag):
    """A schedule would run it with the params' defaults and email whoever is on
    ``email_to`` on a timer. The numbers are a person's input or nothing."""
    assert dag.schedule_interval is None
    assert dag.catchup is False


def test_the_default_numbers_are_something_the_form_can_map(dag):
    """The params' defaults are what the trigger form is pre-filled with, so they have
    to be a list the listing end can turn into op_kwargs — not a string, not a count."""
    numbers = list(dag.params["numbers"])
    assert rn_dag.get_num(**form(numbers)) == [{"num": number} for number in numbers]


def test_this_dag_does_not_shadow_the_one_it_was_copied_from(dagbag):
    """It is a copy of ``random_email`` down to the task ids. Leave the dag_id copied
    too and Airflow bags one of the two and reports the other as a duplicate import
    error — the file just stops existing as far as the UI is concerned."""
    assert rn_dag.DAG_ID != re_dag.DAG_ID
    assert {rn_dag.DAG_ID, re_dag.DAG_ID} <= set(dagbag.dags)


# ------------------------------------------------------------ what is not mapped yet


class FakeTI:
    """``xcom_pull`` on a mapped upstream returns every mapped value, as a list."""

    def __init__(self, values: dict):
        self.values = dict(values)

    def xcom_pull(self, task_ids: str, key: str | None = None):
        return self.values.get(task_ids)


@pytest.mark.xfail(
    reason="only get_num is mapped — the tasks after it pull the whole list off XCom",
    raises=TypeError,
    strict=True,
)
def test_the_branch_judges_one_number_rather_than_the_whole_mapped_list():
    """The gap this file records, written as the behaviour that is wanted.

    ``check_num`` and the tasks after it are still ordinary single tasks pulling
    ``DRAW_TASK_ID``, which on a mapped task is the list of every mapped value. The
    branch's ``% 2`` gets a list and the run stops there. Mapping the rest of the
    chain — ``.partial().expand()`` on each, pulling with ``map_indexes`` — turns this
    green rather than breaking it.
    """
    context = {"ti": FakeTI({rn_dag.DRAW_TASK_ID: [1, 2, 3]})}
    assert rn_dag.check_value(**context) in {rn_dag.GOOD_TASK_ID, rn_dag.BAD_TASK_ID}
