from datetime import datetime, timedelta
import json
from enum import StrEnum

import pandas as pd
import requests

URL = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"


class WeatherStation(StrEnum):
    BYGHOLM = "06102"  # horsens (stopped recording 2026/03/10 for some reason)
    GALTEN = "06039"  # silkeborg/århus


class WeatherFeature(StrEnum):
    TEMP_DRY = "temp_dry"  # celcius
    HUMIDITY = "humidity"  # percentage
    PRESSURE = "pressure"  # hPa
    WIND_DIRECTION = "wind_dir"  # degrees
    WIND_SPEED = "wind_speed"  # m/s


class Observation:
    def __init__(self, value, time):
        self.value = value
        self.time = time


def datetime_to_str(time):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def get_observations(
    parameterId: str, start: datetime, end: datetime, stationId: WeatherStation
) -> list[Observation]:
    parameters = {
        "stationId": stationId,
        "parameterId": parameterId,
        "datetime": f"{datetime_to_str(start)}/{datetime_to_str(end)}",
        "limit": "300000",
    }

    parameters_string = (
        f"?{'&'.join([f'{key}={value}' for key, value in parameters.items()])}"
    )
    actual_url = f"{URL}{parameters_string}"

    data = requests.get(actual_url)
    data = json.loads(data.text)

    observations = []
    for feature in data["features"]:
        properties = feature["properties"]
        observations.append(
            Observation(properties["value"], str_to_datetime(properties["observed"]))
        )

    return observations


def add_observation_to_df(
    df,
    weather_feature: WeatherFeature,
    start: datetime,
    end: datetime,
    stationId: WeatherStation,
):
    observations = get_observations(weather_feature, start, end, stationId)
    observations_df = pd.DataFrame(
        {
            "observed": [observation.time for observation in observations],
            weather_feature: [observation.value for observation in observations],
        }
    )

    try:
        return df.merge(observations_df, how="left")
    except Exception:
        return observations_df


def get_all_observations(
    features: list[WeatherFeature],
    start: datetime,
    end: datetime,
    stationId: WeatherStation,
):
    df = pd.DataFrame()
    for feature in features:
        df = add_observation_to_df(df, feature, start, end, stationId)
    return df


if __name__ == "__main__":
    end = datetime.now()
    start = end - timedelta(365 * 4 + 1)  # 4 years back
    stationId = WeatherStation.BYGHOLM

    df = get_all_observations(
        [
            WeatherFeature.TEMP_DRY,
            WeatherFeature.HUMIDITY,
            WeatherFeature.PRESSURE,
            WeatherFeature.WIND_DIRECTION,
            WeatherFeature.WIND_SPEED,
        ],
        start,
        end,
        stationId,
    )

    with open(f"observations_{stationId}.csv", "w", newline="\n") as f:
        df.to_csv(f, index=False)
