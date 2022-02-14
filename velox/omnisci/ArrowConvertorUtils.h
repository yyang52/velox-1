/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cfloat>
#include <cstdint>
#include <string>
#include "velox/vector/arrow/Abi.h"

namespace facebook::velox::cider {
int8_t* convertToCider(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    int num_rows);

void convertToArrow(
    ArrowArray& arrowArray,
    ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    std::string col_type,
    int num_rows);
} // namespace facebook::velox::cider
