.PHONY: init-superset dbt-shell test

init-superset:
	docker exec -i 05-global-commodity-aws-superset-1 bash init-superset.sh

dbt-shell:
	docker exec -it 05-global-commodity-aws-airflow-worker-1 bash -c "cd commodity_dbt && exec bash"

test:
	bash run_tests.sh all