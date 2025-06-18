# (C) 2024 GoodData Corporation
import io
from typing import Optional

import gooddata_flight_server as gf
import pyarrow as pa
import pyarrow.csv as pv
import structlog
from gooddata_flexconnect import ExecutionContext, FlexConnectFunction

_LOGGER = structlog.get_logger("csv_flex_function")

# Paste your CSV content here as a multiline string
CSV_CONTENT = """Category,Representative_Price,Competitor_Price
Clothing,54.38,54.38
Electronics,54.94,54.9
Groceries,54.69,54.64
Toys,54.13,54.06
Home & Kitchen,19.71,19.86
Sports & Outdoors,51.59,55.87
Books,114.29,118.98
Health & Beauty,153.73,142.13
Automotive,47.62,51.88
Pet Supplies,159.53,159.15
Baby Products,27.49,26.97
Garden & Outdoor,50.04,51.46
Office Supplies,18.43,19.31
Tools & Home Improvement,163.37,168.07
Jewelry,140.79,128.91
Watches,164.64,154.86
Shoes,183.66,169.48
Luggage,190.96,197.05
Musical Instruments,147.54,159.77
Art Supplies,120.06,113.44
Crafts & Sewing,90.8,87.92
Furniture,14.89,15.99
Lighting,193.98,193.29
Smart Home,47.4,47.11
Bedding,117.99,128.77
Bath,54.52,49.3
Cleaning Supplies,74.6,72.83
Party Supplies,152.54,152.55
Collectibles,117.88,125.64
Board Games,64.99,58.89
Fitness Equipment,134.27,132.15
Camping Gear,176.31,176.84
Cycling,80.16,87.07
Hiking,97.79,93.66
Fishing,180.11,194.93
Winter Sports,130.69,140.67
Water Sports,49.05,46.15
Tech Accessories,156.8,162.24
Mobile Phones,60.26,55.46
Laptops,126.12,135.61
PC Accessories,146.98,161.64
Gaming,97.72,89.18
Software,192.58,204.68
Streaming Devices,30.14,30.21
Cameras,74.55,79.26
Drones,17.71,17.5
3D Printers,132.71,123.37
Networking Devices,99.33,97.12
Car Electronics,154.85,139.66
Home Security,37.35,37.78
"""


class CsvFlexFunction(FlexConnectFunction):
    Name = "CsvFlexFunction"

    Schema = pa.schema(
        [
            pa.field("Category", pa.string()),
            pa.field("Representative_Price", pa.float64()),
            pa.field("Competitor_Price", pa.float64()),
        ]
    )

    def call(
        self,
        parameters: dict,
        columns: Optional[tuple[str, ...]],
        headers: dict[str, list[str]],
    ) -> gf.ArrowData:
        _LOGGER.info("function_called", parameters=parameters, columns=columns)

        execution_context = ExecutionContext.from_parameters(parameters)
        if execution_context is None:
            _LOGGER.error("Function did not receive execution context.")
            raise ValueError("Function did not receive execution context.")
        _LOGGER.info("execution_context", execution_context=execution_context)

        try:
            # Read the CSV from the in-memory string as bytes
            table = pv.read_csv(io.BytesIO(CSV_CONTENT.encode("utf-8")))
            if columns:
                table = table.select(columns)
            return table
        except Exception as e:
            _LOGGER.error("CsvFlexFunction error in call", error=str(e))
            raise

    @staticmethod
    def on_load(ctx):
        _LOGGER.info("on_load called for CsvFlexFunction")
        pass

    def cancel(self) -> bool:
        _LOGGER.info("cancel called for CsvFlexFunction")
        return False
