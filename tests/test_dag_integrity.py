"""Guard against the failure mode Airflow reports worst.

A DAG file that raises on import does not appear in the Airflow UI at all — no
error banner, no broken DAG, just an absence. Catching that here is much cheaper
than noticing it after a deploy.
"""

from __future__ import annotations

import pytest

import gdalinfo_notify as dag_module
from conftest import DAGS_DIR


@pytest.fixture(scope="session")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import failures: {dagbag.import_errors}"


@pytest.mark.parametrize(
    "dag_id",
    [
        "gdalinfo_notify",
        "stac_publish",
        "gdal_weather",
        "random_email",
        "random_numbers",
        "stock",
        "temperature",
    ],
)
def test_dag_is_registered(dagbag, dag_id):
    assert dag_id in dagbag.dags


def test_expected_tasks(dagbag):
    dag = dagbag.dags["gdalinfo_notify"]
    assert set(dag.task_ids) == {
        "parse_event",
        "wait_for_object",
        "start_instance",
        "run_gdalinfo",
        "validate_report",
        "build_and_send_email",
        "move_file",
        "publish_stac",
        "delete_file",
        "stop_instance",
    }


def test_expected_stac_publish_tasks(dagbag):
    assert set(dagbag.dags["stac_publish"].task_ids) == {"backfill_items", "write_collection"}


def test_the_branch_can_only_name_tasks_that_exist(dagbag):
    """validate_report returns a task_id as a *string*.

    A typo there is not a NameError caught at parse time — it is an AirflowException
    raised mid-run, after start_instance has already booted the worker.
    """
    dag = dagbag.dags["gdalinfo_notify"]
    assert {dag_module.EMAIL_TASK, dag_module.DELETE_TASK} <= set(dag.task_ids)
    assert dag.get_task("validate_report").downstream_task_ids == {
        dag_module.EMAIL_TASK,
        dag_module.DELETE_TASK,
    }


def test_the_unusable_raster_is_deleted_instead_of_archived(dagbag):
    """The two branches are exclusive and must not share the archive step.

    delete_file hanging off move_file, or move_file downstream of the branch itself,
    would archive a raster that had just been deleted.
    """
    dag = dagbag.dags["gdalinfo_notify"]
    assert dag.get_task("delete_file").upstream_task_ids == {"validate_report"}
    assert dag.get_task("move_file").upstream_task_ids == {"build_and_send_email"}


def test_the_item_is_published_after_the_raster_has_moved(dagbag):
    """publish_stac reads move_file's return value as the key its data asset points at.

    Ordering is the whole of the contract: run it beside move_file, or anywhere before
    it, and every item in the catalogue references the pre-archive key — which no
    longer exists by the time anyone resolves it.
    """
    dag = dagbag.dags["gdalinfo_notify"]
    assert dag.get_task("publish_stac").upstream_task_ids == {"move_file"}


def test_both_branches_join_at_stop_instance(dagbag):
    """Whichever branch is skipped, all_done still counts it as done and the worker
    is stopped. A branch that dead-ends is a branch that leaves EC2 billing."""
    dag = dagbag.dags["gdalinfo_notify"]
    assert dag.get_task("stop_instance").upstream_task_ids == {"publish_stac", "delete_file"}


def test_the_raster_is_archived_after_the_email_and_before_the_stop(dagbag):
    """The position of move_file in the chain is the design, not an accident.

    After ``build_and_send_email``, so a failed archive cannot withhold a report
    that was already rendered. Before ``stop_instance``, so the move happens while
    the run is still the one holding the worker.

    Stated transitively on the second half: publish_stac sits between the two now, and
    what this test is about is the ordering, not the length of the chain.
    """
    dag = dagbag.dags["gdalinfo_notify"]
    assert dag.get_task("build_and_send_email").downstream_task_ids == {"move_file"}
    assert "stop_instance" in dag.get_task("move_file").get_flat_relative_ids(upstream=False)


def test_the_object_is_waited_for_before_the_worker_is_started(dagbag):
    """Ordering is the whole point of the wait: a key that never arrives has to fail
    before ``start_instance``, otherwise the run boots a worker it will never use and
    bills for it until ``stop_instance`` catches up."""
    dag = dagbag.dags["gdalinfo_notify"]
    assert dag.get_task("parse_event").downstream_task_ids == {"wait_for_object"}
    assert dag.get_task("wait_for_object").downstream_task_ids == {"start_instance"}


def test_single_active_run(dagbag):
    """Concurrency is a correctness constraint here, not a tuning preference.

    Two runs sharing a start/stop worker means one run's stop_instance can kill the
    other's gdalinfo mid-flight.
    """
    assert dagbag.dags["gdalinfo_notify"].max_active_runs == 1


def test_stop_instance_always_runs(dagbag):
    """The instance must be stopped even when the run failed — otherwise a single
    bad raster leaves EC2 billing indefinitely."""
    stop = dagbag.dags["gdalinfo_notify"].get_task("stop_instance")
    assert stop.trigger_rule == "all_done"
    assert stop.retries >= 2
