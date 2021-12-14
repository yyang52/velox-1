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
#include "velox/omnisci/RawDataConvertor.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::omnisci {
template <TypeKind kind>
void toCiderImpl(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  using T = typename TypeTraits<kind>::NativeType;
  int8_t** col_buffer = *col_buffer_ptr;
  auto childVal = child->asFlatVector<T>();
  auto* rawValues = childVal->mutableRawValues();
  T* column = (T*)std::malloc(sizeof(T) * num_rows);
  if (child->mayHaveNulls()) {
    auto nulls = child->rawNulls();
    for (auto pos = 0; pos < num_rows; pos++) {
      if (bits::isBitNull(nulls, pos)) {
        rawValues[pos] = std::numeric_limits<T>::min();
      }
    }
  }
  memcpy(column, rawValues, sizeof(T) * num_rows);
  col_buffer[idx] = reinterpret_cast<int8_t*>(column);
}

void toCiderResult(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      toCiderImpl, child->typeKind(), child, idx, col_buffer_ptr, num_rows);
}

CiderResultSet RawDataConvertor::convertToCider(
    RowVectorPtr input,
    int num_rows) {
  RowVector* row = input.get();
  auto* rowVector = row->as<RowVector>();
  auto size = rowVector->childrenSize();
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*) * size);
  auto col_buffer_ptr = &col_buffer;
  for (auto idx = 0; idx < size; idx++) {
    VectorPtr& child = rowVector->childAt(idx);
    switch (child->encoding()) {
      case VectorEncoding::Simple::FLAT:
        toCiderResult(child, idx, col_buffer_ptr, num_rows);
        break;
      default:
        VELOX_NYI(" {} conversion is not supported yet", child->encoding());
    }
  }
  return CiderResultSet(col_buffer, num_rows);
};

TypePtr getVeloxType(std::string typeName) {
  if (typeName == "BOOLEAN")
    return BOOLEAN();
  if (typeName == "TINYINT")
    return TINYINT();
  if (typeName == "SMALLINT")
    return SMALLINT();
  if (typeName == "INTEGER")
    return INTEGER();
  if (typeName == "BIGINT")
    return BIGINT();
  if (typeName == "FLOAT")
    return REAL();
  if (typeName == "DOUBLE")
    return DOUBLE();
  else {
    VELOX_NYI(" {} conversion is not supported yet", typeName);
  }
}

template <TypeKind kind>
VectorPtr toVeloxImpl(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<kind>::NativeType;
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<T>>();
  auto rawValues = flatResult->mutableRawValues();
  T* dataBuffer = reinterpret_cast<T*>(data_buffer);
  memcpy(rawValues, dataBuffer, num_rows * sizeof(T));
  for (auto pos = 0; pos < num_rows; pos++) {
    if (dataBuffer[pos] == std::numeric_limits<T>::min()) {
      result->setNull(pos, true);
    }
  }
  return result;
}

VectorPtr toVeloxVector(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      toVeloxImpl, vType->kind(), vType, data_buffer, num_rows, pool);
}

RowVectorPtr RawDataConvertor::convertToRowVector(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    int num_rows,
    memory::MemoryPool* pool) {
  std::shared_ptr<const RowType> rowType;
  std::vector<VectorPtr> columns;

  // get row type from cider result
  std::vector<TypePtr> types;
  int num_cols = col_types.size();
  types.reserve(num_cols);
  for (int i = 0; i < num_cols; i++) {
    types.push_back(getVeloxType(col_types[i]));
  }

  // convert col buffer to vector ptr
  columns.reserve(num_cols);
  for (int i = 0; i < num_cols; i++) {
    columns.push_back(toVeloxVector(types[i], col_buffer[i], num_rows, pool));
  }
  rowType = std::make_shared<RowType>(move(col_names), move(types));
  return std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), num_rows, columns);
};
} // namespace facebook::velox::omnisci