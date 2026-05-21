from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import TransformedTargetRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline, FunctionTransformer
from sklearn.preprocessing import StandardScaler

from utils import get_sets_temporal, split_time, add_wind_direction_cyclic


TRAINING_ROW_LIMIT = 1000000
PREDICTION_OFFSET_LIMIT = 168





def train_model() -> Pipeline:
    regressor = MLPRegressor(
        hidden_layer_sizes=(128, 64, 32),
        activation="relu",
        solver="adam",
        alpha=0.0001,
        batch_size=128,
        learning_rate_init=0.0005,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=10,
        max_iter=200,
        random_state=42,
        verbose=True,
    )

    preprocessor = Pipeline(
        [
            (
                "wind_direction_encoder",
                FunctionTransformer(add_wind_direction_cyclic),
            ),
            (
                "time_spliter",
                FunctionTransformer(
                    lambda X: split_time(
                        X,
                        year=False,
                        make_cyclic=True,
                        day_of_year=True,
                    )
                ),
            ),
            ("scaler", StandardScaler()),
        ]
    )
    regressor = Pipeline([("preprocessor", preprocessor), ("model", regressor)])
    model = TransformedTargetRegressor(
        regressor=regressor,
        transformer=StandardScaler(),
    )

    raw_data = pd.read_parquet(
        Path(__file__, "../../data/historical_data_06102.parquet").resolve()
    )
    future_labels = [label for label in raw_data.columns if label.startswith("future_")]
    required_columns = [
        "time",
        "temperature",
        "humidity",
        "wind_direction",
        "wind_speed",
        "precipitation",
        "light",
        "prediction_offset",
        *future_labels,
    ]
    new_data = raw_data.dropna(subset=required_columns)
    new_data = new_data[new_data["prediction_offset"] <= PREDICTION_OFFSET_LIMIT]
    new_data = new_data.iloc[:TRAINING_ROW_LIMIT]
    train, test = get_sets_temporal(new_data, seed=42, include_validate=False)

    global test_set
    test_set = test

    model.fit(*train)
    return model


test_set = None


def get_test_set() -> tuple[pd.DataFrame, pd.DataFrame]:
    return test_set