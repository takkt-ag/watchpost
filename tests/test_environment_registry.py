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


from watchpost.environment import Environment, EnvironmentRegistry


def test_empty_registry_behavior():
    reg = EnvironmentRegistry()

    assert len(reg) == 0
    assert list(iter(reg)) == []
    assert "prod" not in reg
    assert reg.get("prod") is None

    default_env = Environment("default")
    assert reg.get("prod", default=default_env) is default_env


def test_add_and_access_environment():
    reg = EnvironmentRegistry()
    env = Environment("prod", hostname="host1", role="primary")

    reg.add(env)

    assert len(reg) == 1
    assert "prod" in reg
    assert reg["prod"] is env

    items = list(iter(reg))
    assert items == [env]


def test_add_overwrites_same_name():
    reg = EnvironmentRegistry()
    env1 = Environment("x", a=1)
    env2 = Environment("x", a=2)

    reg.add(env1)
    reg.add(env2)

    assert len(reg) == 1
    # The later add should overwrite the earlier one
    assert reg["x"] is env2
    assert reg["x"].metadata["a"] == 2


def test_new_creates_and_adds_environment_with_metadata():
    reg = EnvironmentRegistry()

    created = reg.new("dev", foo="bar", num=42)

    assert isinstance(created, Environment)
    assert created.name == "dev"
    assert created.metadata == {"foo": "bar", "num": 42}

    # new() must have added it to the registry
    assert "dev" in reg
    assert reg["dev"] is created


def test_new_allows_hostname_and_sets_strategy():
    reg = EnvironmentRegistry()

    env = reg.new("with-hostname", hostname="some-host")

    # Ensure the Environment received the hostname and strategy was set accordingly
    assert env.name == "with-hostname"
    # Strategy equality is identity-based; check properties instead
    from watchpost.hostname import TemplateStrategy

    assert isinstance(env.hostname_strategy, TemplateStrategy)
    assert env.hostname_strategy.template == "some-host"


def test_iteration_yields_environments_and_length_matches():
    reg = EnvironmentRegistry()
    names = ["dev", "stage", "prod"]
    for n in names:
        reg.add(Environment(n))

    iterated = list(reg)
    assert len(iterated) == len(names)
    assert {e.name for e in iterated} == set(names)
