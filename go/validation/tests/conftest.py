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

import os
import sys
from pathlib import Path

import adbc_driver_manager.dbapi
import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from . import redis


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default="redis")


def _driver_path() -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    return str(
        Path(__file__).parent.parent.parent / f"build/libadbc_driver_redis.{ext}"
    )


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    return redis.get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    return _driver_path()


@pytest.fixture(scope="session", autouse=True)
def secondary_schema(pytestconfig) -> None:
    """Create the secondary schema (key namespace) used by the suite."""
    quirks = redis.get_quirks(pytestconfig.getoption("vendor_version"))
    uri = quirks.setup.database["uri"]
    uri = os.environ.get(uri.env) if isinstance(uri, adbc_drivers_validation.model.FromEnv) else uri
    if uri is None:
        return
    with adbc_driver_manager.dbapi.connect(
        driver=_driver_path(), db_kwargs={"uri": uri}, autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                f"CREATE SCHEMA IF NOT EXISTS {quirks.features.secondary_schema}"
            )
            cursor.adbc_statement.execute_update()
