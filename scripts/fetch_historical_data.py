import json
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
import time

import pandas as pd
import requests

import sys

root = Path(__file__).parent.joinpath("../").resolve()
sys.path.append(str(root))
from utils import add_target

# documentation: https://www.dmi.dk/friedata/dokumentation/meteorological-observation-api
URL = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"


class WeatherStation(StrEnum):
    BYGHOLM = "06102"  # horsens (stopped recording 2026/03/10 for some reason)
    GALTEN = "06039"  # silkeborg/århus


class WeatherFeature(StrEnum):
    TEMPERATURE = "temp_mean_past1h"  # celcius
    HUMIDITY = "humidity_past1h"  # percentage
    WIND_DIRECTION = "wind_dir_past1h"  # degrees
    WIND_SPEED = "wind_speed_past1h"  # m/s
    PRECIPITATION = "precip_past1h"  # kg/m^2 which is the same as mm. amount in the last hour. -0.1 means the value is anywhere below 0.1
    LIGHT = "radia_glob_past1h"  # W/m^2 which converts approximately to lux with a factor of 1:120


def datetime_to_str(time):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def get_observations(
    parameterId: str, start: datetime, end: datetime, stationId: WeatherStation
) -> pd.DataFrame:
    chunk_size = 300000
    observations = pd.DataFrame()
    features = []
    first_request = True

    while first_request or len(features) == chunk_size:
        parameters = {
            "stationId": stationId,
            "parameterId": parameterId,
            "datetime": f"{datetime_to_str(start)}/{datetime_to_str(end)}",
            "limit": chunk_size,
        }

        parameters_string = (
            f"?{'&'.join([f'{key}={value}' for key, value in parameters.items()])}"
        )
        actual_url = f"{URL}{parameters_string}"

        response = requests.get(actual_url)
        data = json.loads(response.text)

        if response.status_code != 200:
            print(data)
            break

        features = data["features"]
        df = pd.DataFrame(
            {
                "observed": (
                    str_to_datetime(feature["properties"]["observed"]).timestamp()
                    for feature in features
                ),
                parameterId: (feature["properties"]["value"] for feature in features),
            }
        )

        observations = pd.concat([observations, df])

        end = str_to_datetime(features[-1]["properties"]["observed"]) - timedelta(
            seconds=1
        )
        first_request = False

        print(f"Fetched {len(features)} points of {parameterId} since {end}")

    return observations


def get_all_observations(
    weather_features: list[WeatherFeature],
    start: datetime,
    end: datetime,
    stationId: WeatherStation,
):
    df = pd.DataFrame()
    for feature in weather_features:
        observations_df = get_observations(feature, start, end, stationId)
        try:
            df = df.merge(observations_df, how="outer")
        except Exception:
            df = observations_df

    df = df.rename(
        {
            "observed": "time",
            WeatherFeature.TEMPERATURE: "temperature",
            WeatherFeature.HUMIDITY: "humidity",
            WeatherFeature.WIND_DIRECTION: "wind_direction",
            WeatherFeature.WIND_SPEED: "wind_speed",
            WeatherFeature.PRECIPITATION: "precipitation",
            WeatherFeature.LIGHT: "light",
        },
        axis="columns",
    )
    df["light"] *= 122

    return df


def save_data(data, name, format):
    path = Path(__file__, f"../../data/{name}.{format}").resolve()
    match format:
        case "parquet":
            data.to_parquet(
                path,
                index=False,
            )
        case "csv":
            with open(path, "w", newline="\n") as f:
                df.to_csv(f, index=False)


if __name__ == "__main__":
    start_time = time.time()

    end = datetime.now()
    start = end - timedelta(days=365 * 8 + 2)
    stationId = WeatherStation.BYGHOLM

    df = get_all_observations(
        [
            WeatherFeature.TEMPERATURE,
            WeatherFeature.HUMIDITY,
            WeatherFeature.WIND_DIRECTION,
            WeatherFeature.WIND_SPEED,
            WeatherFeature.PRECIPITATION,
            WeatherFeature.LIGHT,
        ],
        start,
        end,
        stationId,
    )

    print(f"Adding target to {len(df)} data points")
    df = add_target(
        df,
        [
            "temperature",
            "humidity",
            "wind_direction",
            "wind_speed",
            "precipitation",
            "light",
        ],
        days_range=7,
    )

    print("Saving data to file")
    save_data(df, f"historical_data_{stationId}", "parquet")

    print(
        f"Seconds taken to fetch, prepare, and save {len(df)} data points: {round(time.time() - start_time, 2)}s"
    )
