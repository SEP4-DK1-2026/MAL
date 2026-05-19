from sklearn.pipeline import Pipeline, FunctionTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
import pandas as pd
from pathlib import Path
from utils import split_time, get_sets


def train_model() -> Pipeline:
    model = MultiOutputRegressor(
        HistGradientBoostingRegressor(
            learning_rate=0.05,
            max_depth=8,
            max_features=0.6,
            max_iter=200,
            max_leaf_nodes=None,
            min_samples_leaf=33,
            random_state=42,
        )
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
                    lambda X: split_time(X, year=False, make_cyclic=True)
                ),
            ),
        ]
    )
    model = Pipeline([("preprocessor", preprocessor), ("model", model)])

    raw_data = pd.read_parquet(Path(__file__, "../../data/IoT_data.parquet").resolve())
    new_data = dropper_pipeline.fit_transform(raw_data)
    train, test = get_sets(new_data, seed=42, include_validate=False)

    global test_set
    test_set = test

    model.fit(*train)
    return model


test_set = None


def get_test_set() -> tuple[pd.DataFrame, pd.DataFrame]:
    return test_set
