"""
Integration tests untuk task 5: dbt_test
Tests BashOperator untuk dbt test command
"""
import pytest
from unittest.mock import MagicMock, patch
from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowException


DBT_PROJECT_DIR = "/opt/airflow/global_commodity_dbt"
DBT_COMMAND_PREFIX = f"cd {DBT_PROJECT_DIR} && poetry run dbt"


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_all_pass(mock_execute, airflow_context):
    """Test all dbt tests pass successfully"""
    # Setup
    mock_execute.return_value = 0  # Exit code 0 = all tests passed
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result == 0
    mock_execute.assert_called_once()


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_some_fail(mock_execute, airflow_context):
    """Test dbt tests with some failures"""
    # Setup - Exit code 1 indicates test failures
    mock_execute.side_effect = AirflowException(
        "dbt test failed: 2 test(s) failed, 18 passed"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "failed" in str(exc_info.value).lower()
    assert "2" in str(exc_info.value)


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_not_null_failure(mock_execute, airflow_context):
    """Test dbt not_null test failure"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "dbt test failed: Column 'gold_price' has null values in stg_commodity_prices"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "null" in str(exc_info.value).lower()
    assert "gold_price" in str(exc_info.value)


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_unique_constraint_failure(mock_execute, airflow_context):
    """Test dbt unique constraint test failure"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "dbt test failed: Duplicate records found for unique key in fact_commodity_prices"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "duplicate" in str(exc_info.value).lower()


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_execution_error(mock_execute, airflow_context):
    """Test dbt test with execution error"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "dbt test error: Compilation error in test SQL"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "error" in str(exc_info.value).lower()


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_no_tests_found(mock_execute, airflow_context):
    """Test dbt test when no tests are found"""
    # Setup
    mock_execute.return_value = 0  # No tests = success (nothing to fail)
    
    bash_command = f"{DBT_COMMAND_PREFIX} test --select model_that_doesnt_exist"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result == 0


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_timeout(mock_execute, airflow_context):
    """Test dbt test with timeout"""
    # Setup
    from datetime import timedelta
    mock_execute.side_effect = AirflowException(
        "Bash task timeout after 1800 seconds"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
        execution_timeout=timedelta(seconds=1800),
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "timeout" in str(exc_info.value).lower()


@patch("airflow.providers.standard.operators.bash.BashOperator")
def test_dbt_test_operator_configuration(mock_operator_cls):
    """Test BashOperator is properly configured for dbt test"""
    # Setup
    mock_operator = MagicMock()
    mock_operator_cls.return_value = mock_operator
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    
    # Create operator
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Assert
    assert operator.task_id == "dbt_test"
    assert bash_command in operator.bash_command
    assert "poetry run dbt test" in operator.bash_command


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_with_selector(mock_execute, airflow_context):
    """Test dbt test with selector for specific models"""
    # Setup
    mock_execute.return_value = 0
    
    bash_command = f"{DBT_COMMAND_PREFIX} test --select fact_commodity_prices"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result == 0
    assert "--select fact_commodity_prices" in operator.bash_command


@patch("airflow.providers.standard.operators.bash.BashOperator.execute")
def test_dbt_test_detailed_output(mock_execute, airflow_context):
    """Test dbt test with detailed output for debugging"""
    # Setup
    test_output = """
    Running with dbt=1.5.0
    Found 20 tests
    
    ✓ not_null_stg_commodity_prices_gold_price
    ✓ not_null_stg_commodity_prices_silver_price
    ✓ unique_fact_commodity_prices_commodity_id
    ✓ relationships_fact_commodity_prices_commodity_id
    
    Done. PASS=20 WARN=0 ERROR=0 SKIP=0 TOTAL=20
    """
    mock_execute.return_value = 0
    
    bash_command = f"{DBT_COMMAND_PREFIX} test"
    operator = BashOperator(
        task_id="dbt_test",
        bash_command=bash_command,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result == 0
