install:
	pip install -r requirements.txt

fetch_historical:
	python ./scripts/fetch_historical_data.py

fetch_iot:
	python ./scripts/fetch_IoT_data.py

deploy:
	python ./scripts/deploy_models.py $(ARGS)
