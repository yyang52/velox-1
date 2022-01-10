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
#include <cstring>
#include <iostream>
#include <vector>

#include "velox/common/base/VeloxException.h"
#include "velox/common/memory/Memory.h"
#include "velox/omnisci/CiderNullValues.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;
namespace facebook::velox::cider {
enum CONVERT_TYPE { ARROW, DIRECT };

struct CiderResultSet {
  CiderResultSet(int8_t** col_buffer, int num_rows)
      : colBuffer(col_buffer), numRows(num_rows) {}

  int8_t** colBuffer;
  int numRows;
};

class DataConvertor {
 public:
  DataConvertor() {}

  static std::shared_ptr<DataConvertor> create(CONVERT_TYPE type);

  virtual CiderResultSet convertToCider(
      RowVectorPtr input,
      int num_rows,
      std::chrono::microseconds* timer) = 0;

  virtual RowVectorPtr convertToRowVector(
      int8_t** col_buffer,
      std::vector<std::string> col_names,
      std::vector<std::string> col_types,
      int num_rows,
      memory::MemoryPool* pool) = 0;
};
} // namespace facebook::velox::cider
