# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import re
from pathlib import Path

from adbc_drivers_validation import model


class RedisQuirks(model.DriverQuirks):
    name = "redis"
    driver = "adbc_driver_redis"
    driver_name = "ADBC Driver for Redis"
    vendor_name = "Redis"
    vendor_version = re.compile(r"8\.4\.\d+")
    short_version = "8.4"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        # There is a single catalog ("redis"); schemas are key namespaces.
        connection_set_current_catalog=False,
        connection_set_current_schema=True,
        # Autocommit only
        connection_transactions=False,
        get_objects=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=True,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=False,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=False,
        supported_xdbc_fields=["xdbc_type_name", "xdbc_nullable", "xdbc_is_nullable"],
        current_catalog="redis",
        current_schema="public",
        secondary_schema="secondary",
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("REDIS_URI"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries/redis",)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        message = str(error)
        if "does not exist" not in message:
            return False
        return table_name is None or table_name in message

    def split_statement(self, statement: str) -> list[str]:
        # The driver executes ';'-separated scripts itself.
        return [statement]


@functools.cache
def get_quirks(test_config: str) -> RedisQuirks:
    if test_config == "redis":
        return RedisQuirks()
    raise ValueError(f"unsupported test config: {test_config}")
