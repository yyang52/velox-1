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
namespace facebook::velox::cider {
template <TypeKind kind>
void toCiderWithArrowImpl(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  using T = typename TypeTraits<kind>::NativeType;
  int8_t** col_buffer = *col_buffer_ptr;
  // velox to arrow
  ArrowArray arrowArray;
  exportToArrow(child, arrowArray);
  // arrow to cider
  const uint64_t* nulls =
      reinterpret_cast<const uint64_t*>(arrowArray.buffers[0]);
  const T* values = reinterpret_cast<const T*>(arrowArray.buffers[1]);
  // cannot directly change arrow buffers as const array, need memcpy
  T* column = (T*)std::malloc(sizeof(T) * num_rows);
  memcpy(column, values, sizeof(T) * num_rows);
  // null_count MAY be -1 if not yet computed.
  if (arrowArray.null_count != 0) {
    for (auto pos = 0; pos < num_rows; pos++) {
      if (bits::isBitNull(nulls, pos)) {
        if (std::is_integral<T>::value) {
          column[pos] = inline_int_null_value<T>();
        } else if (std::is_same<T, float>::value) {
          column[pos] = FLT_MIN;
        } else if (std::is_same<T, double>::value) {
          column[pos] = DBL_MIN;
        } else {
          VELOX_NYI("Conversion is not supported yet");
        }
      }
    }
  }
  col_buffer[idx] = reinterpret_cast<int8_t*>(column);
  arrowArray.release(&arrowArray);
}

template <>
void toCiderWithArrowImpl<TypeKind::BOOLEAN>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  VELOX_NYI("Conversion is not supported yet");
}

template <>
void toCiderWithArrowImpl<TypeKind::VARCHAR>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
void toCiderWithArrowImpl<TypeKind::VARBINARY>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
void toCiderWithArrowImpl<TypeKind::TIMESTAMP>(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet");
}

void toCiderWithArrow(
    VectorPtr& child,
    int idx,
    int8_t*** col_buffer_ptr,
    int num_rows) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      toCiderWithArrowImpl,
      child->typeKind(),
      child,
      idx,
      col_buffer_ptr,
      num_rows);
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

const char* getArrowFormat(std::string typeName) {
  if (typeName == "BOOL")
    return "b";
  if (typeName == "TINYINT")
    return "c";
  if (typeName == "SMALLINT")
    return "s";
  if (typeName == "INT")
    return "i";
  if (typeName == "BIGINT" || typeName == "DECIMAL")
    return "l";
  if (typeName == "FLOAT")
    return "f";
  if (typeName == "DOUBLE")
    return "g";
  if (typeName == "VARCHAR") {
    return "u";
  }
  if (typeName == "TIMESTAMP")
    // map to ttn for now
    return "ttn";
  else
    VELOX_NYI(" {} conversion is not supported yet", typeName);
}

ArrowSchema makeArrowSchema(const char* format) {
  return ArrowSchema{
      .format = format,
      .name = nullptr,
      .metadata = nullptr,
      .flags = 0,
      .n_children = 0,
      .children = nullptr,
      .dictionary = nullptr,
      .release = nullptr,
      .private_data = nullptr,
  };
}

ArrowArray
makeArrowArray(int64_t length, int64_t nullCount, const void** buffers) {
  return ArrowArray{
      .length = length,
      .null_count = nullCount,
      .offset = 0,
      .n_buffers = 2,
      .n_children = 0,
      .buffers = buffers,
      .children = nullptr,
      .dictionary = nullptr,
      .release = nullptr,
      .private_data = nullptr,
  };
}

template <TypeKind kind>
VectorPtr toVeloxWithArrowImpl(
    const ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  using T = typename TypeTraits<kind>::NativeType;
  int64_t nullCount = 0;
  BufferPtr nulls = AlignedBuffer::allocate<uint64_t>(num_rows, pool);
  auto rawNulls = nulls->asMutable<uint64_t>();
  T* srcValues = reinterpret_cast<T*>(data_buffer);
  for (auto pos = 0; pos < num_rows; pos++) {
    if (std::is_integral<T>::value) {
      if (srcValues[pos] == inline_int_null_value<T>()) {
        bits::setNull(rawNulls, pos);
        nullCount++;
      }
    } else if (std::is_same<T, float>::value) {
      if (srcValues[pos] == FLT_MIN) {
        bits::setNull(rawNulls, pos);
        nullCount++;
      }
    } else if (std::is_same<T, double>::value) {
      if (srcValues[pos] == DBL_MIN) {
        bits::setNull(rawNulls, pos);
        nullCount++;
      }
    } else {
      VELOX_NYI("Conversion is not supported yet");
    }
  }

  const void* buffers[2];
  buffers[0] = (nullCount == 0) ? nullptr : (const void*)rawNulls;
  buffers[1] = (num_rows == 0) ? nullptr : (const void*)srcValues;
  ArrowArray arrowArray = makeArrowArray(num_rows, nullCount, buffers);
  auto result = importFromArrow(arrowSchema, arrowArray, pool);
  return result;
}

template <>
VectorPtr toVeloxWithArrowImpl<TypeKind::BOOLEAN>(
    const ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
VectorPtr toVeloxWithArrowImpl<TypeKind::VARCHAR>(
    const ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
VectorPtr toVeloxWithArrowImpl<TypeKind::VARBINARY>(
    const ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
VectorPtr toVeloxWithArrowImpl<TypeKind::TIMESTAMP>(
    const ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

VectorPtr toVeloxVectorWithArrow(
    const TypePtr& vType,
    const ArrowSchema& arrowSchema,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool,
    int32_t dimen) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      toVeloxWithArrowImpl,
      vType->kind(),
      arrowSchema,
      data_buffer,
      num_rows,
      pool,
      dimen);
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
    auto arrowSchema = makeArrowSchema(getArrowFormat(col_types[i]));
    types.push_back(importFromArrow(arrowSchema));
    columns.push_back(toVeloxVectorWithArrow(
        types[i], arrowSchema, col_buffer[i], num_rows, pool, dimens[i]));
  }
  rowType = std::make_shared<RowType>(move(col_names), move(types));
  return std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), num_rows, columns);
};
} // namespace facebook::velox::cider
