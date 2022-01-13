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

namespace facebook::velox::cider {

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
  // T* column = (T*)std::malloc(sizeof(T) * num_rows);
  if (child->mayHaveNulls()) {
    auto nulls = child->rawNulls();
    for (auto pos = 0; pos < num_rows; pos++) {
      if (bits::isBitNull(nulls, pos)) {
        if (std::is_integral<T>::value) {
          rawValues[pos] = inline_int_null_value<T>();
        } else if (std::is_same<T, float>::value) {
          rawValues[pos] = FLT_MIN;
        } else if (std::is_same<T, double>::value) {
          rawValues[pos] = DBL_MIN;
        } else {
          VELOX_NYI("Conversion is not supported yet");
        }
      }
    }
  }
  // memcpy(column, rawValues, sizeof(T) * num_rows);
  col_buffer[idx] = reinterpret_cast<int8_t*>(rawValues);
}

template <>
void toCiderImpl<TypeKind::BOOLEAN>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  int8_t** col_buffer = *col_buffer_ptr;
  auto childVal = child->asFlatVector<bool>();
  uint64_t* rawValues = childVal->mutableRawValues<uint64_t>();
  int8_t* column = (int8_t*)std::malloc(sizeof(int8_t) * num_rows);
  auto nulls = child->rawNulls();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, pos)) {
      column[pos] = inline_int_null_value<int8_t>();
    } else {
      column[pos] = bits::isBitSet(rawValues, pos) ? true : false;
    }
  }
  col_buffer[idx] = column;
}

template <>
void toCiderImpl<TypeKind::VARCHAR>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  int8_t** col_buffer = *col_buffer_ptr;
  size_t string_length = 0;
  auto childVal = child->asFlatVector<StringView>();
  auto* rawValues = childVal->mutableRawValues();
  auto nulls = child->rawNulls();
  for (auto i = 0; i < num_rows; i++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, i)) {
      continue;
    }
    string_length += rawValues[i].size();
  }
  int8_t* column = (int8_t*)std::malloc(sizeof(int8_t) * string_length);
  int8_t* p = column;
  size_t current_offset = 0;
  for (auto i = 0; i < num_rows; i++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, i)) {
      continue;
    }
    memcpy(
        (void*)(column + current_offset),
        rawValues[i].data(),
        rawValues[i].size());
    current_offset += rawValues[i].size();
  }
  col_buffer[idx] = column;
}

template <>
void toCiderImpl<TypeKind::VARBINARY>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet");
}

static constexpr int64_t kNanoSecsPerSec = 1000000000;
static constexpr int64_t kMicroSecsPerSec = 1000000;
static constexpr int64_t kMilliSecsPerSec = 1000;

template <>
void toCiderImpl<TypeKind::TIMESTAMP>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  int8_t** col_buffer = *col_buffer_ptr;
  auto childVal = child->asFlatVector<Timestamp>();
  auto* rawValues = childVal->mutableRawValues();
  int64_t* column = (int64_t*)std::malloc(sizeof(int64_t) * num_rows);
  auto nulls = child->rawNulls();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, pos)) {
      column[pos] = std::numeric_limits<int64_t>::min();
    } else {
      // convert to nanoseconds for now to avoid precision loss
      column[pos] = rawValues[pos].getSeconds() * kNanoSecsPerSec +
          rawValues[pos].getNanos();
    }
  }
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
        toCiderResult(child, idx, col_buffer_ptr, num_rows);
        break;
      case VectorEncoding::Simple::LAZY: {
        // For LazyVector, we will load it here and use as TypeVector to use.
        auto tic = std::chrono::system_clock::now();
        auto vec = (std::dynamic_pointer_cast<LazyVector>(child))->loadedVectorShared();
        auto toc = std::chrono::system_clock::now();
        if(timer) {
          *timer +=  std::chrono::duration_cast<std::chrono::microseconds>(toc - tic);
        }
        toCiderResult(vec, idx, col_buffer_ptr, num_rows);
        break;
      }
      default:
        VELOX_NYI(" {} conversion is not supported yet", child->encoding());
    }
  }
  return CiderResultSet(col_buffer, num_rows);
};

TypePtr getVeloxType(std::string typeName) {
  if (typeName == "BOOL")
    return BOOLEAN();
  if (typeName == "TINYINT")
    return TINYINT();
  if (typeName == "SMALLINT")
    return SMALLINT();
  if (typeName == "INT")
    return INTEGER();
  if (typeName == "BIGINT" || typeName == "DECIMAL")
    return BIGINT();
  if (typeName == "FLOAT")
    return REAL();
  if (typeName == "DOUBLE")
    return DOUBLE();
  if (typeName == "VARCHAR") {
    return VARCHAR();
  }
  if (typeName == "TIMESTAMP")
    return TIMESTAMP();
  else
    VELOX_NYI(" {} conversion is not supported yet", typeName);
}

template <TypeKind kind>
VectorPtr toVeloxImpl(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  using T = typename TypeTraits<kind>::NativeType;
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<T>>();
  auto rawValues = flatResult->mutableRawValues();
  T* srcValues = reinterpret_cast<T*>(data_buffer);
  memcpy(rawValues, srcValues, num_rows * sizeof(T));
  for (auto pos = 0; pos < num_rows; pos++) {
    if (std::is_integral<T>::value) {
      if (srcValues[pos] == inline_int_null_value<T>()) {
        result->setNull(pos, true);
      }
    } else if (std::is_same<T, float>::value) {
      if (srcValues[pos] == FLT_MIN) {
        result->setNull(pos, true);
      }
    } else if (std::is_same<T, double>::value) {
      if (srcValues[pos] == DBL_MIN) {
        result->setNull(pos, true);
      }
    } else {
      VELOX_NYI("Conversion is not supported yet");
    }
  }
  return result;
}

template <>
VectorPtr toVeloxImpl<TypeKind::BOOLEAN>(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<bool>>();
  auto rawValues = flatResult->mutableRawValues<uint64_t>();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (data_buffer[pos] == inline_int_null_value<int8_t>()) {
      result->setNull(pos, true);
    } else {
      bits::setBit(rawValues, pos, static_cast<bool>(data_buffer[pos]));
    }
  }
  return result;
}

template <>
VectorPtr toVeloxImpl<TypeKind::VARCHAR>(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
VectorPtr toVeloxImpl<TypeKind::VARBINARY>(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
VectorPtr toVeloxImpl<TypeKind::TIMESTAMP>(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t dimen) {
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<Timestamp>>();
  int64_t* srcValues = reinterpret_cast<int64_t*>(data_buffer);
  auto scaleSecond = 1;
  auto scaleNano = 1;
  switch (dimen) {
    case 0:
      scaleSecond = 1;
      scaleNano = 1;
      break;
    case 3:
      scaleSecond = kMilliSecsPerSec;
      scaleNano = kMicroSecsPerSec;
      break;
    case 6:
      scaleSecond = kMicroSecsPerSec;
      scaleNano = kMilliSecsPerSec;
      break;
    case 9:
      scaleSecond = kNanoSecsPerSec;
      scaleNano = 1;
      break;
    default:
      VELOX_UNREACHABLE("Unknown dimension");
  }
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos] == std::numeric_limits<int64_t>::min()) {
      result->setNull(pos, true);
    } else {
      auto timeValue = srcValues[pos];
      flatResult->set(
          pos,
          Timestamp(
              timeValue / scaleSecond, (timeValue % scaleSecond) * scaleNano));
    }
  }
  return result;
}

VectorPtr toVeloxVector(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t dimen) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      toVeloxImpl, vType->kind(), vType, data_buffer, num_rows, pool, dimen);
}

RowVectorPtr RawDataConvertor::convertToRowVector(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    std::vector<int32_t> dimens,
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
    columns.push_back(
        toVeloxVector(types[i], col_buffer[i], num_rows, pool, dimens[i]));
  }
  rowType = std::make_shared<RowType>(move(col_names), move(types));
  return std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), num_rows, columns);
};
} // namespace facebook::velox::cider
