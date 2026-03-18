import json
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path

import pandas as pd
import requests

# documentation: https://www.dmi.dk/friedata/dokumentation/meteorological-observation-api
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
    PRECIPITATION = "precip_past1h"  # kg/m^2 which is the same as mm. amount in the last hour. -0.1 means the value is anywhere below 0.1


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
                    str_to_datetime(feature["properties"]["observed"])
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
            df = df.merge(observations_df, how="left")
        except Exception:
            df = observations_df

    return df


if __name__ == "__main__":
    end = datetime.now()
    start = datetime(2000, 1, 1, 0, 0, 0)
    stationId = WeatherStation.BYGHOLM

    df = get_all_observations(
        [
            WeatherFeature.TEMP_DRY,
            WeatherFeature.HUMIDITY,
            WeatherFeature.PRESSURE,
            WeatherFeature.WIND_DIRECTION,
            WeatherFeature.WIND_SPEED,
            WeatherFeature.PRECIPITATION,
        ],
        start,
        end,
        stationId,
    )

    with open(
        Path(__file__, f"../../data/observations_{stationId}.csv"), "w", newline="\n"
    ) as f:
        df.to_csv(f, index=False)
