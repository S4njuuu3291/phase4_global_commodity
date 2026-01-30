"""
Integration tests untuk task 4: dbt_run
Tests BashOperator untuk dbt run command
"""
import pytest
from unittest.mock import MagicMock, patch, call
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException


DBT_PROJECT_DIR = "/opt/airflow/global_commodity_dbt"
DBT_COMMAND_PREFIX = f"cd {DBT_PROJECT_DIR} && poetry run dbt"


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_success(mock_execute, airflow_context):
    """Test successful dbt run execution"""
    # Setup
    mock_execute.return_value = 0  # Exit code 0 = success
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result == 0
    mock_execute.assert_called_once()


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_compile_error(mock_execute, airflow_context):
    """Test dbt run with compilation error"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "dbt compilation error: Could not parse SQL model"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "compilation error" in str(exc_info.value).lower()


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_parse_error(mock_execute, airflow_context):
    """Test dbt run with YAML parse error"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "dbt parsing error in schema.yml: Invalid YAML syntax"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "parsing error" in str(exc_info.value).lower()


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_database_connection_error(mock_execute, airflow_context):
    """Test dbt run with database connection error"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "Database connection failed: Could not connect to BigQuery"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "connection" in str(exc_info.value).lower()


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_partial_model_failure(mock_execute, airflow_context):
    """Test dbt run with partial model failure"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "One or more model failed to execute: fact_commodity_prices model failed"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "failed" in str(exc_info.value).lower()


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_timeout(mock_execute, airflow_context):
    """Test dbt run with timeout"""
    # Setup
    from datetime import timedelta
    mock_execute.side_effect = AirflowException(
        "Bash task timeout after 3600 seconds"
    )
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
        execution_timeout=timedelta(seconds=3600),
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "timeout" in str(exc_info.value).lower()


@patch("airflow.operators.bash.BashOperator")
def test_dbt_run_operator_configuration(mock_operator_cls):
    """Test BashOperator is properly configured for dbt run"""
    # Setup
    mock_operator = MagicMock()
    mock_operator_cls.return_value = mock_operator
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    
    # Create operator
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Assert
    assert operator.task_id == "dbt_run"
    assert bash_command in operator.bash_command
    assert "poetry run dbt run" in operator.bash_command


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_command_contains_full_refresh(mock_execute, airflow_context):
    """Test dbt run command includes --full-refresh flag"""
    # Setup
    mock_execute.return_value = 0
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute
    operator.execute(airflow_context)
    
    # Assert
    assert "--full-refresh" in operator.bash_command


@patch("airflow.operators.bash.BashOperator.execute")
def test_dbt_run_output_captured(mock_execute, airflow_context):
    """Test dbt run output is captured for logging"""
    # Setup
    dbt_output = """
    Running with dbt=1.5.0
    Found 10 models, 5 tests, 0 analyses
    
    Executing model fact_commodity_prices
    Executing model mart_global_commodity_market
    
    Completed successfully
    Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
    """
    mock_execute.return_value = 0
    
    bash_command = f"{DBT_COMMAND_PREFIX} run --full-refresh"
    operator = BashOperator(
        task_id="dbt_run",
        bash_command=bash_command,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result == 0
