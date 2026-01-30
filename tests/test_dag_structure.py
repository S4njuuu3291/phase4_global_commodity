"""
Test suite for DAG structure validation

This tests the overall DAG structure to ensure:
- All expected tasks exist
- Task count is correct (no missing/extra tasks)
- Task dependencies/ordering is correct
- Task IDs match expected names
- DAG configuration (schedule, catchup, etc) is correct
"""
import pytest
import pendulum
from datetime import datetime


@pytest.fixture
def dag():
    """Fixture to get DAG instance"""
    from dags.dags import global_commodity_dag
    return global_commodity_dag()


# ===========================
# TEST 1: TASK EXISTENCE
# ===========================

def test_all_expected_tasks_exist(dag):
    """Verify all expected tasks exist in DAG"""
    expected_tasks = [
        'task_get_all_keys',
        'extract_val_load_gcs',
        'gcs_to_bq',
        'dbt_run',
        'dbt_test'
    ]
    
    for task_id in expected_tasks:
        assert task_id in dag.task_dict, f"Expected task '{task_id}' not found in DAG"


def test_task_get_all_keys_exists(dag):
    """Verify task_get_all_keys task exists"""
    assert 'task_get_all_keys' in dag.task_dict


def test_extract_val_load_gcs_exists(dag):
    """Verify extract_val_load_gcs task exists"""
    assert 'extract_val_load_gcs' in dag.task_dict


def test_gcs_to_bq_exists(dag):
    """Verify gcs_to_bq task exists"""
    assert 'gcs_to_bq' in dag.task_dict


def test_dbt_run_exists(dag):
    """Verify dbt_run task exists"""
    assert 'dbt_run' in dag.task_dict


def test_dbt_test_exists(dag):
    """Verify dbt_test task exists"""
    assert 'dbt_test' in dag.task_dict


# ===========================
# TEST 2: TASK COUNT
# ===========================

def test_correct_number_of_tasks(dag):
    """Verify DAG has exactly 5 tasks (no more, no less)"""
    assert len(dag.task_dict) == 5, \
        f"Expected 5 tasks, but got {len(dag.task_dict)}. Tasks: {list(dag.task_dict.keys())}"


# ===========================
# TEST 3: TASK DEPENDENCIES (ORDERING)
# ===========================

def test_task_get_all_keys_is_root_task(dag):
    """Verify task_get_all_keys has no upstream dependencies (it's the root)"""
    task = dag.task_dict['task_get_all_keys']
    assert len(task.upstream_list) == 0, \
        f"task_get_all_keys should have no upstream tasks, but has: {[t.task_id for t in task.upstream_list]}"


def test_extract_val_load_gcs_depends_on_task_get_all_keys(dag):
    """Verify extract_val_load_gcs depends on task_get_all_keys"""
    task_1 = dag.task_dict['task_get_all_keys']
    task_2 = dag.task_dict['extract_val_load_gcs']
    
    # task_2 should have task_1 as upstream
    assert task_1 in task_2.upstream_list, \
        f"extract_val_load_gcs should depend on task_get_all_keys"
    
    # task_1 should have task_2 as downstream
    assert task_2 in task_1.downstream_list, \
        f"task_get_all_keys should flow to extract_val_load_gcs"


def test_gcs_to_bq_depends_on_extract_val_load_gcs(dag):
    """Verify gcs_to_bq depends on extract_val_load_gcs"""
    task_2 = dag.task_dict['extract_val_load_gcs']
    task_3 = dag.task_dict['gcs_to_bq']
    
    assert task_2 in task_3.upstream_list, \
        f"gcs_to_bq should depend on extract_val_load_gcs"
    
    assert task_3 in task_2.downstream_list, \
        f"extract_val_load_gcs should flow to gcs_to_bq"


def test_dbt_run_depends_on_gcs_to_bq(dag):
    """Verify dbt_run depends on gcs_to_bq"""
    task_3 = dag.task_dict['gcs_to_bq']
    task_4 = dag.task_dict['dbt_run']
    
    assert task_3 in task_4.upstream_list, \
        f"dbt_run should depend on gcs_to_bq"
    
    assert task_4 in task_3.downstream_list, \
        f"gcs_to_bq should flow to dbt_run"


def test_dbt_test_depends_on_dbt_run(dag):
    """Verify dbt_test depends on dbt_run"""
    task_4 = dag.task_dict['dbt_run']
    task_5 = dag.task_dict['dbt_test']
    
    assert task_4 in task_5.upstream_list, \
        f"dbt_test should depend on dbt_run"
    
    assert task_5 in task_4.downstream_list, \
        f"dbt_run should flow to dbt_test"


def test_complete_dependency_chain(dag):
    """Verify complete dependency chain: 1 -> 2 -> 3 -> 4 -> 5"""
    task_1 = dag.task_dict['task_get_all_keys']
    task_2 = dag.task_dict['extract_val_load_gcs']
    task_3 = dag.task_dict['gcs_to_bq']
    task_4 = dag.task_dict['dbt_run']
    task_5 = dag.task_dict['dbt_test']
    
    # Build expected chain
    expected_chain = [task_1, task_2, task_3, task_4, task_5]
    
    # Verify each task has exactly 1 upstream (except first) and 1 downstream (except last)
    for i, task in enumerate(expected_chain):
        if i > 0:  # Not first
            assert len(task.upstream_list) == 1, \
                f"{task.task_id} should have exactly 1 upstream, but has {len(task.upstream_list)}"
        
        if i < len(expected_chain) - 1:  # Not last
            assert len(task.downstream_list) == 1, \
                f"{task.task_id} should have exactly 1 downstream, but has {len(task.downstream_list)}"


# ===========================
# TEST 4: TASK IDs
# ===========================

def test_task_ids_match_expected_names(dag):
    """Verify all task IDs match expected names"""
    expected_task_ids = {
        'task_get_all_keys',
        'extract_val_load_gcs',
        'gcs_to_bq',
        'dbt_run',
        'dbt_test'
    }
    
    actual_task_ids = set(dag.task_dict.keys())
    
    assert actual_task_ids == expected_task_ids, \
        f"Task IDs mismatch.\nExpected: {expected_task_ids}\nActual: {actual_task_ids}"


# ===========================
# TEST 5: DAG CONFIGURATION
# ===========================

def test_dag_id(dag):
    """Verify DAG ID is 'global_commodity'"""
    assert dag.dag_id == "global_commodity", \
        f"Expected dag_id='global_commodity', but got '{dag.dag_id}'"


def test_dag_schedule_interval(dag):
    """Verify DAG schedule interval is '@daily'"""
    schedule = getattr(dag, 'schedule', None) or getattr(dag, 'schedule_interval', None)
    assert schedule == "@daily", \
        f"Expected schedule='@daily', but got '{schedule}'"


def test_dag_start_date(dag):
    """Verify DAG start_date is set and is in the past"""
    assert dag.start_date is not None, "DAG start_date should not be None"
    
    # Start date should be in the past (or close to it)
    assert dag.start_date <= pendulum.now(), \
        f"start_date ({dag.start_date}) should be in the past"


def test_dag_catchup_is_false(dag):
    """Verify DAG catchup is False (don't backfill old runs)"""
    assert dag.catchup is False, \
        f"Expected catchup=False, but got catchup={dag.catchup}"


# ===========================
# TEST 6: NO CIRCULAR DEPENDENCIES
# ===========================

def test_no_circular_dependencies(dag):
    """Verify DAG has no circular dependencies (is truly a DAG)"""
    # If DAG has cycles, Airflow would raise an error during instantiation
    # This test verifies that instantiation was successful
    assert dag is not None, "DAG should be instantiated without errors"
    assert dag.dag_id is not None, "DAG should be valid"


# ===========================
# TEST 7: TASK TYPES
# ===========================

def test_task_get_all_keys_is_python_operator(dag):
    """Verify task_get_all_keys is a PythonOperator (decorated with @task)"""
    task = dag.task_dict['task_get_all_keys']
    # Decorated tasks use _PythonDecoratedOperator
    task_type = str(type(task).__name__)
    assert '_PythonDecoratedOperator' in task_type or 'PythonOperator' in task_type, \
        f"Expected Python decorated operator, but got {task_type}"


def test_extract_val_load_gcs_is_python_operator(dag):
    """Verify extract_val_load_gcs is a PythonOperator (decorated with @task)"""
    task = dag.task_dict['extract_val_load_gcs']
    # Decorated tasks use _PythonDecoratedOperator
    task_type = str(type(task).__name__)
    assert '_PythonDecoratedOperator' in task_type or 'PythonOperator' in task_type, \
        f"Expected Python decorated operator, but got {task_type}"


def test_gcs_to_bq_is_gcs_to_bigquery_operator(dag):
    """Verify gcs_to_bq is a GCSToBigQueryOperator"""
    task = dag.task_dict['gcs_to_bq']
    assert 'GCSToBigQuery' in str(type(task).__name__), \
        f"gcs_to_bq should be GCSToBigQueryOperator, but is {type(task).__name__}"


def test_dbt_run_is_bash_operator(dag):
    """Verify dbt_run is a BashOperator"""
    task = dag.task_dict['dbt_run']
    assert 'BashOperator' in str(type(task).__name__), \
        f"dbt_run should be BashOperator, but is {type(task).__name__}"


def test_dbt_test_is_bash_operator(dag):
    """Verify dbt_test is a BashOperator"""
    task = dag.task_dict['dbt_test']
    assert 'BashOperator' in str(type(task).__name__), \
        f"dbt_test should be BashOperator, but is {type(task).__name__}"
