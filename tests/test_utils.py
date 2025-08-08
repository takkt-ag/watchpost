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

import inspect

from outpost.utils import InvocationInformation, get_invocation_information


def return_invocation_information():
    return get_invocation_information()


def test_get_invocation_information():
    current_frame = inspect.currentframe()
    assert current_frame
    expected_line_number = current_frame.f_lineno
    invocation_information = return_invocation_information()
    assert isinstance(invocation_information, InvocationInformation)
    assert invocation_information.relative_path == "tests/test_utils.py"
    assert invocation_information.line_number == expected_line_number + 1
