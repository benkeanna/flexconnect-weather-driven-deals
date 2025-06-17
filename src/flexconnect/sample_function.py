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
            pyarrow.field("Date", pyarrow.timestamp("ms")),
            pyarrow.field("Source", pyarrow.string()),
            pyarrow.field("Temperature", pyarrow.float64()),
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

    def extract_location(self, execution_context: ExecutionContext) -> str:
        location = "San Francisco"
        for filter in getattr(execution_context, "filters", []):
            if (
                hasattr(filter, "label_identifier")
                and filter.label_identifier == "customer_city"
                and hasattr(filter, "values")
                and filter.values
            ):
                location = filter.values[0]
                break
        return location

    def get_historical_data(
        self, from_date: str, to_date: str, location: str
    ) -> dict[str, Any]:
        now_date = datetime.now().date()
        to_date_obj = datetime.fromisoformat(to_date)
        if isinstance(to_date_obj, datetime):
            to_date_obj = to_date_obj.date()
        clamped_end_date = min(now_date, to_date_obj).isoformat()
        params = {
            "key": self.api_key,
            "q": location,
            "dt": from_date,
            "end_dt": clamped_end_date,
        }
        response = requests.get(HISTORICAL_URL, params=params)
        response.raise_for_status()
        data = response.json()
        output = {"Date": [], "Source": [], "Temperature": [], "Rain": []}
        for day in data.get("forecast", {}).get("forecastday", []):
            for hour in day.get("hour", []):
                output["Date"].append(datetime.fromtimestamp(hour["time_epoch"]))
                output["Source"].append("Observation")
                output["Temperature"].append(hour["temp_c"])
                output["Rain"].append(hour.get("chance_of_rain", 0))
        return output

    def get_forecast_data(
        self, from_date: str, to_date: str, location: str
    ) -> dict[str, Any]:
        params = {
            "key": self.api_key,
            "q": location,
            "dt": from_date,
            "end_dt": to_date,
        }
        response = requests.get(FORECAST_URL, params=params)
        response.raise_for_status()
        data = response.json()
        output = {"Date": [], "Source": [], "Temperature": [], "Rain": []}
        for day in data.get("forecast", {}).get("forecastday", []):
            for hour in day.get("hour", []):
                output["Date"].append(datetime.fromtimestamp(hour["time_epoch"]))
                output["Source"].append("Forecast")
                output["Temperature"].append(hour["temp_c"])
                output["Rain"].append(hour.get("chance_of_rain", 0))
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
        location = self.extract_location(execution_context)
        historical_data = self.get_historical_data(from_date, to_date, location)
        forecast_data = self.get_forecast_data(from_date, to_date, location)
        output = {
            "Date": historical_data["Date"] + forecast_data["Date"],
            "Source": historical_data["Source"] + forecast_data["Source"],
            "Temperature": historical_data["Temperature"]
            + forecast_data["Temperature"],
            "Rain": historical_data["Rain"] + forecast_data["Rain"],
        }
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
