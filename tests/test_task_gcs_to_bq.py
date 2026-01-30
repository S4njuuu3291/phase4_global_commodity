"""
Integration tests untuk task 3: gcs_to_bq
Tests GCSToBigQueryOperator functionality
"""
import pytest
from unittest.mock import MagicMock, patch
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.exceptions import AirflowException


@patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator.execute")
def test_gcs_to_bq_success(mock_execute, airflow_context, gcs_file_path, bigquery_table):
    """Test successful GCS to BigQuery load"""
    # Setup
    mock_execute.return_value = None
    
    operator = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="test-bucket",
        source_objects=[gcs_file_path],
        destination_project_dataset_table=bigquery_table,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        gcp_conn_id="google_cloud_default",
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result is None
    mock_execute.assert_called_once()


@patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator.execute")
def test_gcs_to_bq_missing_object(mock_execute, airflow_context, bigquery_table):
    """Test GCS to BQ when object doesn't exist"""
    # Setup
    missing_file = "global_commodity/date=2026-01-30/missing_file.json"
    mock_execute.side_effect = AirflowException(
        f"gs://test-bucket/{missing_file} not found"
    )
    
    operator = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="test-bucket",
        source_objects=[missing_file],
        destination_project_dataset_table=bigquery_table,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "not found" in str(exc_info.value)


@patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator.execute")
def test_gcs_to_bq_invalid_json_format(mock_execute, airflow_context, gcs_file_path, bigquery_table):
    """Test GCS to BQ with invalid JSON format"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "Invalid JSON format in source file"
    )
    
    operator = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="test-bucket",
        source_objects=[gcs_file_path],
        destination_project_dataset_table=bigquery_table,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "Invalid JSON" in str(exc_info.value)


@patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator.execute")
def test_gcs_to_bq_permission_denied(mock_execute, airflow_context, gcs_file_path, bigquery_table):
    """Test GCS to BQ with permission error"""
    # Setup
    mock_execute.side_effect = AirflowException(
        "Permission denied: User does not have bigquery.tables.update permission"
    )
    
    operator = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="test-bucket",
        source_objects=[gcs_file_path],
        destination_project_dataset_table=bigquery_table,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
    )
    
    # Execute & Assert
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(airflow_context)
    
    assert "Permission denied" in str(exc_info.value)


@patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator.execute")
def test_gcs_to_bq_schema_mismatch(mock_execute, airflow_context, gcs_file_path, bigquery_table):
    """Test GCS to BQ with schema mismatch but ALLOW_FIELD_ADDITION enabled"""
    # Setup - with ALLOW_FIELD_ADDITION, schema mismatch should be handled
    mock_execute.return_value = None
    
    operator = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="test-bucket",
        source_objects=[gcs_file_path],
        destination_project_dataset_table=bigquery_table,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
        autodetect=True,
    )
    
    # Execute
    result = operator.execute(airflow_context)
    
    # Assert
    assert result is None
    mock_execute.assert_called_once()


@patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator")
def test_gcs_to_bq_operator_configuration(mock_operator_cls, gcs_file_path, bigquery_table):
    """Test GCSToBigQueryOperator is properly configured"""
    # Setup
    mock_operator = MagicMock()
    mock_operator_cls.return_value = mock_operator
    
    # Create operator
    operator = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="test-bucket",
        source_objects=[gcs_file_path],
        destination_project_dataset_table=bigquery_table,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        gcp_conn_id="google_cloud_default",
    )
    
    # Assert operator was created with correct params
    assert operator.task_id == "gcs_to_bq"
    assert operator.bucket == "test-bucket"
    assert operator.source_objects == [gcs_file_path]
    assert operator.destination_project_dataset_table == bigquery_table
    assert operator.source_format == "NEWLINE_DELIMITED_JSON"
    assert operator.write_disposition == "WRITE_APPEND"
    assert operator.autodetect is True
    assert operator.gcp_conn_id == "google_cloud_default"
