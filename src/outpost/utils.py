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
from dataclasses import dataclass
from pathlib import Path

import outpost


@dataclass
class InvocationInformation:
    relative_path: str
    line_number: int


def get_invocation_information() -> InvocationInformation | None:
    # Get the frame of the function that called the function that called this
    current_frame = inspect.currentframe()
    if not current_frame or not current_frame.f_back or not current_frame.f_back.f_back:
        return None
    relevant_frame = current_frame.f_back.f_back

    relevant_module = inspect.getmodule(relevant_frame)
    if not relevant_module:
        return None

    if not outpost.__file__ or not relevant_module.__file__:
        return None
    root_directory = Path(outpost.__file__).parent.parent
    relevant_module_path = Path(relevant_module.__file__)

    return InvocationInformation(
        relative_path=str(relevant_module_path.relative_to(root_directory)),
        line_number=relevant_frame.f_lineno,
    )
