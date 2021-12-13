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

#include <cstring>
#include <iostream>
#include <vector>

#include "velox/omnisci/DataConvertor.h"

namespace facebook::velox::omnisci {
class RawDataConvertor : public DataConvertor {
 public:
  RawDataConvertor() {}

  CiderResultSet convertToCider(RowVectorPtr input, int num_rows) override;

  RowVectorPtr convertToRowVector(
      int8_t** col_buffer,
      std::vector<std::string> col_names,
      std::vector<std::string> col_types,
      int num_rows,
      memory::MemoryPool* pool) override;
};
} // namespace facebook::velox::omnisci
