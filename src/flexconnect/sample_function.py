# (C) 2025 GoodData Corporation
from datetime import datetime, timedelta
from typing import Any, Optional

import gooddata_flight_server as gf
import pyarrow
import requests
import structlog
from gooddata_flexconnect import (
    ExecutionContext,
    FlexConnectFunction,
)
from gooddata_sdk import AbsoluteDateFilter, RelativeDateFilter

_LOGGER = structlog.get_logger("sample_flexconnect_function")


HISTORICAL_URL = "https://api.weatherapi.com/v1/history.json"
FORECAST_URL = "https://api.weatherapi.com/v1/forecast.json"


class SampleFlexConnectFunction(FlexConnectFunction):
    """
    A sample FlexConnect function. This serves weather data from weatherapi.com.
    """

    Name = "SampleFlexConnectFunction"
    Schema = pyarrow.schema(
        [
            pyarrow.field("Location", pyarrow.string()),
            pyarrow.field("Date", pyarrow.timestamp("ms")),
            pyarrow.field("Wind", pyarrow.float64()),
            pyarrow.field("TemperatureMin", pyarrow.float64()),
            pyarrow.field("TemperatureMax", pyarrow.float64()),
            pyarrow.field("Rain", pyarrow.float64()),
        ]
    )
    api_key = "0102b58ffeda496395e125251251706"

    def _handle_date(self, context: Optional[ExecutionContext]) -> tuple[str, str]:
        now = datetime.now()
        for date_filter in getattr(context, "filters", []):
            if isinstance(date_filter, AbsoluteDateFilter):
                return date_filter.from_date, date_filter.to_date
            elif isinstance(date_filter, RelativeDateFilter):
                if date_filter.granularity == "DAY":
                    from_date = (
                        now + timedelta(days=date_filter.from_shift - 1)
                    ).date()
                    to_date = (now + timedelta(days=date_filter.to_shift)).date()
                    if date_filter.to_shift == 0:
                        to_date = now.date()
                elif date_filter.granularity == "WEEK":
                    current_week_start = now - timedelta(days=now.weekday())
                    from_date = current_week_start + timedelta(
                        weeks=date_filter.from_shift
                    )
                    to_date = (
                        current_week_start
                        + timedelta(weeks=date_filter.to_shift + 1)
                        - timedelta(seconds=1)
                    )
                    if date_filter.to_shift == 0:
                        to_date = current_week_start + timedelta(
                            days=6, hours=23, minutes=59, seconds=59
                        )
                else:
                    continue
                return from_date.isoformat(), to_date.isoformat()
        from_date = (now - timedelta(days=7)).date().isoformat()
        to_date = (now + timedelta(days=1)).date().isoformat()
        return from_date, to_date

    def get_historical_data(
        self, from_date: str, to_date: str, location: str
    ) -> dict[str, Any]:
        # WeatherAPI /history.json only supports one day per request, so we must loop
        from_dt = datetime.fromisoformat(from_date)
        to_dt = datetime.fromisoformat(to_date)
        output = {
            "Date": [],
            "Wind": [],
            "TemperatureMin": [],
            "TemperatureMax": [],
            "Rain": [],
        }
        for n in range((to_dt - from_dt).days + 1):
            day = from_dt + timedelta(days=n)
            params = {
                "key": self.api_key,
                "q": location,
                "dt": day.date().isoformat(),
            }
            response = requests.get(HISTORICAL_URL, params=params)
            response.raise_for_status()
            data = response.json()
            for d in data.get("forecast", {}).get("forecastday", []):
                date = datetime.fromtimestamp(d["date_epoch"])
                day_data = d.get("day", {})
                output["Date"].append(date)
                output["Wind"].append(day_data.get("maxwind_kph", None))
                output["TemperatureMin"].append(day_data.get("mintemp_c", None))
                output["TemperatureMax"].append(day_data.get("maxtemp_c", None))
                output["Rain"].append(day_data.get("totalprecip_mm", 0))
        return output

    def get_forecast_data(
        self, from_date: str, to_date: str, location: str
    ) -> dict[str, Any]:
        params = {
            "key": self.api_key,
            "q": location,
            "days": 3,  # WeatherAPI free plan allows up to 3 days
        }
        response = requests.get(FORECAST_URL, params=params)
        response.raise_for_status()
        data = response.json()
        output = {
            "Date": [],
            "Wind": [],
            "TemperatureMin": [],
            "TemperatureMax": [],
            "Rain": [],
        }
        for day in data.get("forecast", {}).get("forecastday", []):
            date = datetime.fromtimestamp(day["date_epoch"])
            day_data = day.get("day", {})
            output["Date"].append(date)
            output["Wind"].append(day_data.get("maxwind_kph", None))
            output["TemperatureMin"].append(day_data.get("mintemp_c", None))
            output["TemperatureMax"].append(day_data.get("maxtemp_c", None))
            output["Rain"].append(day_data.get("totalprecip_mm", 0))
        return output

    def call(
        self,
        parameters: dict,
        columns: Optional[tuple[str, ...]],
        headers: dict[str, list[str]],
    ) -> gf.ArrowData:
        _LOGGER.info("function_called", parameters=parameters)

        execution_context = ExecutionContext.from_parameters(parameters)
        if execution_context is None:
            # This can happen for invalid invocations that do not come from GoodData
            raise ValueError("Function did not receive execution context.")

        _LOGGER.info("execution_context", execution_context=execution_context)
        from_date, to_date = self._handle_date(execution_context)
        location = "Berlin"
        historical_data = self.get_historical_data(from_date, to_date, location)
        forecast_data = self.get_forecast_data(from_date, to_date, location)
        output = {
            "Date": historical_data["Date"] + forecast_data["Date"],
            "Wind": historical_data["Wind"] + forecast_data["Wind"],
            "TemperatureMin": historical_data["TemperatureMin"]
            + forecast_data["TemperatureMin"],
            "TemperatureMax": historical_data["TemperatureMax"]
            + forecast_data["TemperatureMax"],
            "Rain": historical_data["Rain"] + forecast_data["Rain"],
        }
        row_count = len(output["Date"])
        output["Location"] = [location] * row_count
        return pyarrow.Table.from_pydict(output)

    @staticmethod
    def on_load(ctx: gf.ServerContext) -> None:
        """
        You can do one-time initialization here. This function will be invoked
        exactly once during startup.

        Most often, you want to perform function-specific initialization that may be
        further driven by external configuration (e.g., env variables or TOML files).

        The server uses Dynaconf to work with configuration. You can access
        all available configuration values via `ctx.settings`.

        :param ctx: server context
        :return: nothing
        """
        # value = ctx.settings.get("your-setting")
        pass

    def cancel(self) -> bool:
        """
        If you have some long-running operations as part of the `call` method,
        you can implement this method so that it does all the necessary work to cancel them.

        For example, if your `call` body performs some query to a database, you might want
        to implement the query cancellation here.

        Implementing the cancellation is optional.
        If not implemented, the FlexConnect server will still pretend the entire call was
        cancelled - it's just that it will wait for the `call` to finish and then throw the results away.

        If you do not plan on implementing this method, you can delete it altogether:
        it will fall back to the default implementation.

        :return: True, if the cancellation was requested successfully, False otherwise.
        """
        return False
