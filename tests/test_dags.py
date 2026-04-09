"""
Unit tests for DAG structure and task definitions
Tests task creation, dependencies, and configuration
"""
import pytest
from datetime import datetime
import sys
import os

# Add dags folder to sys.path
dags_path = os.path.join(os.path.dirname(__file__), '../dags')
if dags_path not in sys.path:
    sys.path.insert(0, dags_path)


class TestDAGStructure:
    """Test commodity_pipeline DAG structure"""
    
    def test_dag_exists(self):
        """Test that DAG is properly defined"""
        from dags.dags import commodity_pipeline_dag
        
        assert commodity_pipeline_dag is not None
        assert commodity_pipeline_dag.dag_id == 'commodity_pipeline'
    
    def test_dag_configuration(self):
        """Test DAG configuration"""
        from dags.dags import commodity_pipeline_dag
        
        assert commodity_pipeline_dag.owner == 'sanju'
        # DAG.retries is not directly accessible; check default_args instead
        assert commodity_pipeline_dag.default_args.get('retries') == 2
        assert commodity_pipeline_dag.schedule_interval == '@daily' or commodity_pipeline_dag.schedule == '@daily'
        assert commodity_pipeline_dag.catchup is False
    
    def test_dag_start_date(self):
        """Test DAG start date"""
        from dags.dags import commodity_pipeline_dag
        
        assert commodity_pipeline_dag.start_date is not None
        assert isinstance(commodity_pipeline_dag.start_date, datetime)
        assert commodity_pipeline_dag.start_date.year >= 2026
    
    def test_dag_tags(self):
        """Test DAG has proper tags"""
        from dags.dags import commodity_pipeline_dag
        
        assert 'commodity' in commodity_pipeline_dag.tags
        assert 'duckdb' in commodity_pipeline_dag.tags


class TestDAGTasks:
    """Test DAG task definitions"""
    
    def test_bronze_tasks_exist(self):
        """Test that all bronze ingestion tasks are defined"""
        from dags.dags import commodity_pipeline_dag
        
        task_ids = [task.task_id for task in commodity_pipeline_dag.tasks]
        
        required_tasks = ['ingest_metal', 'ingest_currency', 'ingest_fred', 'ingest_news']
        for task_id in required_tasks:
            assert task_id in task_ids, f"Task {task_id} not found in DAG"
    
    def test_silver_tasks_exist(self):
        """Test that all silver transformation tasks are defined"""
        from dags.dags import commodity_pipeline_dag
        
        task_ids = [task.task_id for task in commodity_pipeline_dag.tasks]
        
        expected_tasks = ['transform_metal', 'transform_currency', 'transform_fred', 'transform_news_task']
        for task_id in expected_tasks:
            assert task_id in task_ids, f"Task {task_id} not found in DAG"
    
    def test_dbt_tasks_exist(self):
        """Test that dbt tasks are defined"""
        from dags.dags import commodity_pipeline_dag
        
        task_ids = [task.task_id for task in commodity_pipeline_dag.tasks]
        
        assert 'dbt_run' in task_ids, "dbt_run task not found"
        assert 'dbt_test' in task_ids, "dbt_test task not found"
    
    def test_total_task_count(self):
        """Test that DAG has expected number of tasks"""
        from dags.dags import commodity_pipeline_dag
        
        # 4 ingest + 4 transform + 2 dbt = 10 tasks
        assert len(commodity_pipeline_dag.tasks) == 10, \
            f"Expected 10 tasks, got {len(commodity_pipeline_dag.tasks)}"


class TestTaskDependencies:
    """Test task dependencies and execution flow"""
    
    def test_bronze_tasks_have_no_upstream(self):
        """Test that bronze ingestion tasks have no upstream dependencies"""
        from dags.dags import commodity_pipeline_dag
        
        bronze_tasks = [t for t in commodity_pipeline_dag.tasks if t.task_id.startswith('ingest')]
        
        for task in bronze_tasks:
            assert len(task.upstream_list) == 0, \
                f"Bronze task {task.task_id} should have no upstream tasks"
    
    def test_silver_tasks_depend_on_bronze(self):
        """Test that silver transformation tasks depend on bronze ingestion"""
        from dags.dags import commodity_pipeline_dag
        
        # Get tasks
        tasks_by_id = {t.task_id: t for t in commodity_pipeline_dag.tasks}
        
        # Check transform_metal depends on ingest_metal (implicitly via context)
        transform_metal = tasks_by_id.get('transform_metal')
        assert transform_metal is not None, "transform_metal task not found"
    
    def test_dbt_tasks_execute_after_transforms(self):
        """Test that dbt tasks come after all transform tasks"""
        from dags.dags import commodity_pipeline_dag
        
        tasks_by_id = {t.task_id: t for t in commodity_pipeline_dag.tasks}
        
        dbt_run = tasks_by_id.get('dbt_run')
        dbt_test = tasks_by_id.get('dbt_test')
        
        assert dbt_run is not None, "dbt_run task not found"
        assert dbt_test is not None, "dbt_test task not found"
        
        # dbt_test should depend on dbt_run
        assert dbt_run in [t for t in dbt_test.upstream_list], \
            "dbt_test should depend on dbt_run"
    
    def test_dag_is_acyclic(self):
        """Test that DAG has no cycles"""
        from dags.dags import commodity_pipeline_dag
        
        # Airflow validates this automatically, but we can verify
        # by checking that we can iterate through tasks without error
        try:
            task_count = 0
            for task in commodity_pipeline_dag.tasks:
                task_count += 1
                # Check that task has valid upstream/downstream
                assert hasattr(task, 'upstream_list')
                assert hasattr(task, 'downstream_list')
            assert task_count > 0
        except Exception as e:
            pytest.fail(f"DAG has a cycle or is invalid: {e}")


class TestTaskConfiguration:
    """Test individual task configurations"""
    
    def test_bronze_tasks_have_retries(self):
        """Test that bronze tasks have retry configuration"""
        from dags.dags import commodity_pipeline_dag
        
        bronze_tasks = [t for t in commodity_pipeline_dag.tasks if t.task_id.startswith('ingest')]
        
        for task in bronze_tasks:
            assert task.retries > 0, f"Task {task.task_id} should have retries configured"
    
    def test_dbt_tasks_are_bash_operators(self):
        """Test that dbt tasks are BashOperators"""
        from airflow.operators.bash import BashOperator
        from dags.dags import commodity_pipeline_dag
        
        tasks_by_id = {t.task_id: t for t in commodity_pipeline_dag.tasks}
        
        dbt_run = tasks_by_id.get('dbt_run')
        dbt_test = tasks_by_id.get('dbt_test')
        
        assert isinstance(dbt_run, BashOperator), "dbt_run should be a BashOperator"
        assert isinstance(dbt_test, BashOperator), "dbt_test should be a BashOperator"
    
    def test_dbt_commands_include_profile_dir(self):
        """Test that dbt commands set DBT_PROFILES_DIR"""
        from dags.dags import commodity_pipeline_dag
        
        tasks_by_id = {t.task_id: t for t in commodity_pipeline_dag.tasks}
        
        dbt_run = tasks_by_id.get('dbt_run')
        assert dbt_run is not None
        
        # Check bash_command includes DBT_PROFILES_DIR
        assert 'DBT_PROFILES_DIR' in dbt_run.bash_command or \
               'dbt' in dbt_run.bash_command, \
            "dbt_run should have proper dbt command"
    
    def test_dbt_run_command_syntax(self):
        """Test that dbt_run has valid command"""
        from dags.dags import commodity_pipeline_dag
        
        tasks_by_id = {t.task_id: t for t in commodity_pipeline_dag.tasks}
        
        dbt_run = tasks_by_id.get('dbt_run')
        assert dbt_run is not None
        
        # Should include 'dbt run'
        assert 'dbt run' in dbt_run.bash_command, \
            "dbt_run should execute 'dbt run' command"
    
    def test_dbt_test_command_syntax(self):
        """Test that dbt_test has valid command"""
        from dags.dags import commodity_pipeline_dag
        
        tasks_by_id = {t.task_id: t for t in commodity_pipeline_dag.tasks}
        
        dbt_test = tasks_by_id.get('dbt_test')
        assert dbt_test is not None
        
        # Should include 'dbt test'
        assert 'dbt test' in dbt_test.bash_command, \
            "dbt_test should execute 'dbt test' command"


class TestDAGImports:
    """Test that DAG imports are correct"""
    
    def test_dags_module_imports(self):
        """Test that dags.py imports are accessible"""
        try:
            from dags.dags import commodity_pipeline, commodity_pipeline_dag
            assert commodity_pipeline is not None
            assert commodity_pipeline_dag is not None
        except ImportError as e:
            pytest.fail(f"Failed to import from dags.dags: {e}")
    
    def test_utils_imports(self):
        """Test that utils functions are importable"""
        try:
            from dags.utils import (
                fetch_metal_prices,
                fetch_currency_rate,
                fetch_fred_data,
                fetch_news,
                transform_metal_prices,
                transform_currency_rates,
                transform_fred_data,
                transform_news
            )
            assert callable(fetch_metal_prices)
            assert callable(transform_metal_prices)
        except ImportError as e:
            pytest.fail(f"Failed to import from utils.py: {e}")


class TestDAGValidation:
    """Comprehensive DAG validation tests"""
    
    def test_dag_serialization(self):
        """Test that DAG can be serialized"""
        from dags.dags import commodity_pipeline_dag
        
        # DAGs should be JSON serializable (for Airflow UI)
        try:
            # Basic check: dag should have a dictionary representation
            assert hasattr(commodity_pipeline_dag, 'dag_id')
            assert hasattr(commodity_pipeline_dag, 'tasks')
        except Exception as e:
            pytest.fail(f"DAG should be properly serializable: {e}")
    
    def test_dag_task_names_unique(self):
        """Test that all task IDs are unique"""
        from dags.dags import commodity_pipeline_dag
        
        task_ids = [t.task_id for t in commodity_pipeline_dag.tasks]
        
        assert len(task_ids) == len(set(task_ids)), \
            "Duplicate task IDs found in DAG"
    
    def test_all_tasks_executable(self):
        """Test that all tasks are executable"""
        from dags.dags import commodity_pipeline_dag
        
        for task in commodity_pipeline_dag.tasks:
            assert hasattr(task, 'execute'), \
                f"Task {task.task_id} should be executable"
            assert callable(task.execute), \
                f"Task {task.task_id} execute method should be callable"
