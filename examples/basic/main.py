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

from outpost import Outpost, current_app
from outpost.check import Check
from outpost.datasource import Datasource
from outpost.environment import Environment
from outpost.result import ok


class DummyDatasource(Datasource):
    argument_name = "dummy"


def dummy_check_function(dummy: DummyDatasource):
    print("This is a running check.")
    print(f"Current app: {current_app}")
    print(f"Dummy: {dummy}")
    return ok("This is a check result.")


def main():
    DummyDatasource.instance = DummyDatasource()
    dummy_check = Check(
        service_name="dummy",
        service_labels={"foo": "bar"},
        check_function=dummy_check_function,
        datasources=[DummyDatasource],
        environments=[Environment("test")],
    )
    app = Outpost(
        checks=[dummy_check],
    )

    app.run_checks_once()


if __name__ == "__main__":
    main()
