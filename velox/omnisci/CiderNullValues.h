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
#include <limits>

#include "velox/vector/ComplexVector.h"

#define NULL_FLOAT FLT_MIN
#define NULL_DOUBLE DBL_MIN
#define CONSTEXPR constexpr

using namespace facebook::velox;
template <class T>
constexpr inline int64_t inline_int_null_value() {
  return std::is_signed<T>::value ? std::numeric_limits<T>::min()
                                  : std::numeric_limits<T>::max();
}

template <typename T>
constexpr inline T inline_fp_null_value() {
  return T{};
}

template <>
constexpr inline float inline_fp_null_value<float>() {
  return NULL_FLOAT;
}

template <>
constexpr inline double inline_fp_null_value<double>() {
  return NULL_DOUBLE;
}

inline int64_t inline_int_null_val(const TypePtr& vType) {
  switch (vType->kind()) {
    case TypeKind::BOOLEAN:
      return inline_int_null_value<int8_t>();
    case TypeKind::TINYINT:
      return inline_int_null_value<int8_t>();
    case TypeKind::SMALLINT:
      return inline_int_null_value<int16_t>();
    case TypeKind::INTEGER:
      return inline_int_null_value<int32_t>();
    case TypeKind::BIGINT:
      return inline_int_null_value<int64_t>();
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", vType->kind());
  }
}

inline double inline_fp_null_val(const TypePtr& vType) {
  switch (vType->kind()) {
    case TypeKind::REAL:
      return NULL_FLOAT;
    case TypeKind::DOUBLE:
      return NULL_DOUBLE;
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", vType->kind());
  }
}
