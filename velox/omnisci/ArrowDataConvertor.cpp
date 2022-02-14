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

#include "velox/omnisci/ArrowDataConvertor.h"
#include "velox/omnisci/ArrowConvertorUtils.h"
namespace facebook::velox::cider {
void toCiderWithArrow(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  int8_t** col_buffer = *col_buffer_ptr;
  // velox to arrow
  ArrowArray arrowArray;
  exportToArrow(child, arrowArray);
  ArrowSchema arrowSchema;
  exportToArrow(child->type(), arrowSchema);
  // arrow to cider
  int8_t* column = convertToCider(arrowSchema, arrowArray, num_rows);
  col_buffer[idx] = column;
  arrowArray.release(&arrowArray);
  arrowSchema.release(&arrowSchema);
}

CiderResultSet ArrowDataConvertor::convertToCider(
    RowVectorPtr input,
    int num_rows,
    std::chrono::microseconds* timer) {
  RowVector* row = input.get();
  auto* rowVector = row->as<RowVector>();
  auto size = rowVector->childrenSize();
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*) * size);
  auto col_buffer_ptr = &col_buffer;
  for (auto idx = 0; idx < size; idx++) {
    VectorPtr& child = rowVector->childAt(idx);
    switch (child->encoding()) {
      case VectorEncoding::Simple::FLAT:
        toCiderWithArrow(child, idx, col_buffer_ptr, num_rows);
        break;
      case VectorEncoding::Simple::LAZY: {
        // For LazyVector, we will load it here and use as TypeVector to use.
        auto tic = std::chrono::system_clock::now();
        auto vec = (std::dynamic_pointer_cast<LazyVector>(child))
                       ->loadedVectorShared();
        auto toc = std::chrono::system_clock::now();
        if (timer) {
          *timer +=
              std::chrono::duration_cast<std::chrono::microseconds>(toc - tic);
        }
        toCiderWithArrow(vec, idx, col_buffer_ptr, num_rows);
        break;
      }
      default:
        VELOX_NYI(" {} conversion is not supported yet", child->encoding());
    }
  }
  return CiderResultSet(col_buffer, num_rows);
};

VectorPtr toVeloxVectorWithArrow(
    ArrowArray& arrowArray,
    ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    std::string col_type,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  convertToArrow(arrowArray, arrowSchema, data_buffer, col_type, num_rows);
  auto result = importFromArrow(arrowSchema, arrowArray, pool);
  return result;
}

RowVectorPtr ArrowDataConvertor::convertToRowVector(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    std::vector<int32_t> dimens,
    int num_rows,
    memory::MemoryPool* pool) {
  std::shared_ptr<const RowType> rowType;
  std::vector<VectorPtr> columns;
  std::vector<TypePtr> types;
  int num_cols = col_types.size();
  types.reserve(num_cols);
  columns.reserve(num_cols);
  for (int i = 0; i < num_cols; i++) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    columns.push_back(toVeloxVectorWithArrow(
        arrowArray,
        arrowSchema,
        col_buffer[i],
        col_types[i],
        num_rows,
        pool,
        dimens[i]));
    types.push_back(importFromArrow(arrowSchema));
  }
  rowType = std::make_shared<RowType>(move(col_names), move(types));
  return std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), num_rows, columns);
};
} // namespace facebook::velox::cider
