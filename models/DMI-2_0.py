from pathlib import Path

import pandas as pd
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline, FunctionTransformer
from sklearn.preprocessing import StandardScaler

from utils import get_sets_temporal, split_time


def train_model() -> Pipeline:
    model = MLPRegressor(
        hidden_layer_sizes=(128, 64, 32),
        activation="relu",
        solver="adam",
        alpha=0.0001,
        batch_size=256,
        learning_rate_init=0.001,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=5,
        max_iter=75,
        random_state=42,
        verbose=True,
    )

    dropper_pipeline = Pipeline(
        [
            ("NaN_dropper", FunctionTransformer(lambda X: X.dropna())),
        ]
    )
    preprocessor = Pipeline(
        [
            ("dropper_pipeline", dropper_pipeline),
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
    model = Pipeline([("preprocessor", preprocessor), ("model", model)])

    raw_data = pd.read_parquet(
        Path(__file__, "../../data/historical_data_06102.parquet").resolve()
    )
    new_data = dropper_pipeline.fit_transform(raw_data)
    train, test = get_sets_temporal(new_data, seed=42, include_validate=False)

    global test_set
    test_set = test

    model.fit(*train)
    return model


test_set = None


def get_test_set() -> tuple[pd.DataFrame, pd.DataFrame]:
    return test_set