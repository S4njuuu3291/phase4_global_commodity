"""
Test suite for task_get_all_keys in global_commodity DAG
"""
from unittest.mock import patch
import pytest


def test_task_get_all_keys_success():
    """Test task_get_all_keys returns all 4 API keys successfully"""
    with patch('dags.dags.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = [
            'mocked_metal_key',
            'mocked_currency_key',
            'mocked_fred_key',
            'mocked_news_key'
        ]

        from dags.dags import global_commodity_dag
        dag = global_commodity_dag()
        task_func = dag.task_dict['task_get_all_keys'].python_callable

        result = task_func()

        assert result == {
            "metal_key": 'mocked_metal_key',
            "currency_key": 'mocked_currency_key',
            "fred_key": 'mocked_fred_key',
            "news_key": 'mocked_news_key',
        }
        assert mock_get_secret.call_count == 4


def test_task_get_all_keys_failure_on_first_secret():
    """Test task_get_all_keys raises exception when first secret fails"""
    with patch('dags.dags.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = Exception("Secret not found")

        from dags.dags import global_commodity_dag
        dag = global_commodity_dag()
        task_func = dag.task_dict['task_get_all_keys'].python_callable

        with pytest.raises(Exception) as e:
            task_func()
        assert "Secret not found" in str(e.value)


def test_task_get_all_keys_failure_on_middle_secret():
    """Test task_get_all_keys raises exception when middle secret fails"""
    with patch('dags.dags.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = [
            'mocked_metal_key',
            Exception("Currency secret not found"),
        ]

        from dags.dags import global_commodity_dag
        dag = global_commodity_dag()
        task_func = dag.task_dict['task_get_all_keys'].python_callable

        with pytest.raises(Exception) as e:
            task_func()
        assert "Currency secret not found" in str(e.value)
        assert mock_get_secret.call_count == 2
