# Copyright 2025 TAKKT Industrial & Packaging GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

from typing import Annotated

from outpost import Outpost, current_app
from outpost.check import check
from outpost.datasource import Datasource, DatasourceFactory, FromFactory
from outpost.environment import Environment
from outpost.result import CheckResult, ok


class DummyDatasource(Datasource):
    scheduling_strategies = ()


class MockBoto3Client(Datasource):
    def __init__(self, service_name: str, region_name: str):
        self.service_name = service_name
        self.region_name = region_name

    def __repr__(self) -> str:
        return f"<MockBoto3Client service_name={self.service_name!r} region_name={self.region_name!r}>"


class Boto3(DatasourceFactory):
    def new(self, service: str) -> Datasource:
        return MockBoto3Client(service, "eu-central-1")


@check(
    name="dummy",
    service_labels={"foo": "bar"},
    environments=[Environment("test")],
    cache_for=None,
)
def dummy_check_function(
    dummy: DummyDatasource,
    annotated: Annotated[MockBoto3Client, FromFactory(Boto3, "ecs")],
) -> CheckResult:
    print("This is a running check.")
    print(f"Current app: {current_app}")
    print(f"Dummy: {dummy}")
    print(f"Annotated: {annotated}")
    return ok("This is a check result.")


app = Outpost(
    checks=[dummy_check_function],
    execution_environment=Environment("test"),
)

app.register_datasource(DummyDatasource)
app.register_datasource_factory(Boto3)


def main() -> None:
    app.run_checks_once()


if __name__ == "__main__":
    main()
