install:
	pip install -r requirements.txt

fetch_historical:
	python ./scripts/fetch_historical_data.py

fetch_iot:
	python ./scripts/fetch_IoT_data.py

deploy:
	python ./scripts/deploy_models.py $(ARGS)

sample_historical_parquet:
	python ./scripts/sample_parquet.py ./data/historical_data_06102.parquet ./data/historical_data_06102_head100.parquet --rows 100

train_dmi-2_0:
	python -u -c 'import importlib; from sklearn.metrics import r2_score; mod = importlib.import_module("models.DMI-2_0"); model = mod.train_model(); X_test, y_test = mod.get_test_set(); y_hat = model.predict(X_test); print("feature_names_in_:", list(model.feature_names_in_)); print("prediction shape:", y_hat.shape); [print(f"{column}: r2={r2_score(y_test[column], y_hat[:, i]):.4f}") for i, column in enumerate(y_test.columns)]'
