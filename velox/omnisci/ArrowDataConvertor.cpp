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

RowVectorPtr ArrowDataConvertor::convertToRowVector(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    std::vector<int32_t> dimens,
    int num_rows,
    memory::MemoryPool* pool) {
  // TODO
  VELOX_NYI("Arrow conversion not yet supported.");
};
} // namespace facebook::velox::cider
