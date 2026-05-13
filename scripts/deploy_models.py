import sys
import os
import importlib
from pathlib import Path
from datetime import datetime
import re

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score

import cloudpickle

import psycopg2
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from dotenv import load_dotenv

root = Path(__file__).parent.joinpath("../").resolve()
sys.path.append(str(root))

from utils import get_sets

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_BLOB_CONTAINER_NAME: str = os.getenv("AZURE_BLOB_CONTAINER_NAME")
PSQL_SERVER: str = os.getenv("PSQL_SERVER")
PSQL_PORT: str = os.getenv("PSQL_PORT")
PSQL_DATABASE: str = os.getenv("PSQL_DATABASE")
PSQL_USER: str = os.getenv("PSQL_USER")
PSQL_PASSWORD: str = os.getenv("PSQL_PASSWORD")

RUN_DRY: bool = False

MODEL_NAME_PATTERN = re.compile("^(.{1,3})-(\d+)_(\d+)")

blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)
container_client: ContainerClient = blob_service_client.get_container_client(
    container=AZURE_BLOB_CONTAINER_NAME
)
database_client = psycopg2.connect(
    host=PSQL_SERVER,
    port=PSQL_PORT,
    database=PSQL_DATABASE,
    user=PSQL_USER,
    password=PSQL_PASSWORD,
)

EXPECTED_FEATURES = [
    "time",
    "temperature",
    "humidity",
    "wind_direction",
    "wind_speed",
    "precipitation",
    "light",
    "prediction_offset",
]

data = None


def get_data():
    global data
    if data is None:
        data = pd.read_parquet(root.joinpath("data/historical_data_06102.parquet"))
    return data


def deploy_models():
    models_path = Path(root, "models")
    if not models_path.exists():
        raise FileNotFoundError("No directory with name 'models'")

    model_paths = [
        Path(root, f)
        for f in os.listdir(models_path)
        if Path(models_path, f).is_file() and f.endswith(".py") and f != "__init__.py"
    ]

    EXISTING_MODELS = [
        f.removesuffix(".pkl") for f in container_client.list_blob_names()
    ]

    for model_path in model_paths:
        model_name = model_path.stem

        match = MODEL_NAME_PATTERN.match(model_name)
        if match is None:
            print(
                f"[WARNING] Model '{model_name}' does not follow the model naming convention"
            )
            continue

        module = importlib.import_module(f"models.{model_name}")

        if model_name in EXISTING_MODELS:
            print(f"[INFO] Skipping '{model_name}', as it already exists")
            continue

        if not hasattr(module, "train_model"):
            print(f"[WARNING] Module '{model_name}' has no 'train_model' function")
            continue

        print(f"[INFO] Training model '{model_name}'")
        model: Pipeline = module.train_model()

        features = model.feature_names_in_
        missing_features = set().difference(set(features))
        unexpected_features = set(features).difference(set(EXPECTED_FEATURES))
        if len(unexpected_features) > 0 or len(missing_features) > 0:
            msg = f"[WARNING] Model '{model_name}' does not follow input interface"
            if len(missing_features) > 0:
                msg += f". Missing feature(s): '{missing_features}'"
            if len(unexpected_features) > 0:
                msg += f". Unexpected feature(s): '{unexpected_features}'"
            print(msg)
            continue

        data = get_data()
        data = data.dropna()
        data = data.sample(n=10000, random_state=42)
        X, y = get_sets(data, seed=42, do_split=False)

        y_hat = model.predict(X)
        feature_out_length = len(y_hat[0])
        if feature_out_length != 6:
            print(
                f"[WARNING] Model '{model_name}' does not follow input interface. Expected 6 output features, got {feature_out_length}"
            )
            continue

        failed_scoring = False
        for i, y_feature in enumerate(y.columns):
            score = r2_score(y[y_feature], [row[i] for row in y_hat])
            if score <= 0:
                print(
                    f"[WARNING] Model '{model_name}' scored a r2 score of {score} on {y_feature}, which is too low"
                )
                failed_scoring = True
        if failed_scoring:
            continue

        serialized_model = cloudpickle.dumps(model, protocol=5)

        if RUN_DRY:
            continue

        # Upload model
        blob_client: BlobClient = blob_service_client.get_blob_client(
            container=AZURE_BLOB_CONTAINER_NAME, blob=f"{model_name}.pkl"
        )
        blob_client.upload_blob(serialized_model)

        # Update database
        cursor = database_client.cursor()
        name, major_version, minor_version = (match.group(i) for i in range(1, 4))
        cursor.execute(
            'INSERT INTO "Model" (name, major_version, minor_version, trained_at) VALUES (%s, %s, %s, %s);',
            (name, major_version, minor_version, datetime.now().timestamp()),
        )
        database_client.commit()
        cursor.close()


if __name__ == "__main__":
    RUN_DRY = "dry" in sys.argv
    try:
        if not RUN_DRY and "-y" not in sys.argv:
            answer = input(
                "You are about to run a script that might deploy to Azure blob storage and Database. Are you sure? [y/N] "
            )
            if answer == "y":
                deploy_models()
        else:
            deploy_models()
    finally:
        database_client.close()
