// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This contains all the template implementations for functions defined in
// bit-packing.h. This should be included by files that want to instantiate
// those templates directly. Including this file is not generally necessary -
// instead the templates should be instantiated in bit-packing.cc so that
// compile times stay manageable.

#pragma once

#include "bit-packing.h"
#include "extend_16u.h"
#include "extend_32u.h"
#include "extend_8u.h"
#include "unpack_16u.h"
#include "unpack_32u.h"
#include "unpack_8u.h"
#include "unpack_def.h"

#include <immintrin.h>
#include <limits.h>
#include <algorithm>
#include <type_traits>

#include <boost/preprocessor/repetition/repeat_from_to.hpp>

#define ALWAYS_INLINE __attribute__((always_inline))

#if defined(__GNUC__)
#ifndef UNLIKELY
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#endif
#ifndef LIKELY
#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#endif
#endif

namespace impala {
/// Specialized round up and down functions for frequently used factors,
/// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64).
/// Returns the rounded up number of bytes that fit the number of bits.
constexpr static inline uint32_t RoundUpNumBytes(uint32_t bits) {
  return (bits + 7) >> 3;
}

constexpr static inline uint32_t RoundUpNumi32(uint32_t bits) {
  return (bits + 31) >> 5;
}

constexpr static inline bool IsPowerOf2(int64_t value) {
  return (value & (value - 1)) == 0;
}

constexpr static inline int64_t RoundUp(int64_t value, int64_t factor) {
  return (value + (factor - 1)) / factor * factor;
}

constexpr static inline int64_t Ceil(int64_t value, int64_t divisor) {
  return value / divisor + (value % divisor != 0);
}

inline int64_t BitPacking::NumValuesToUnpack(
    int bit_width,
    int64_t in_bytes,
    int64_t num_values) {
  // Check if we have enough input bytes to decode 'num_values'.
  if (bit_width == 0 || RoundUpNumBytes(num_values * bit_width) <= in_bytes) {
    // Limited by output space.
    return num_values;
  } else {
    // Limited by the number of input bytes. Compute the number of values that
    // can be unpacked from the input.
    return (in_bytes * CHAR_BIT) / bit_width;
  }
}

template <typename T>
constexpr bool IsSupportedUnpackingType() {
  return std::is_same<T, uint8_t>::value || std::is_same<T, uint16_t>::value ||
      std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value;
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues(
    int bit_width,
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case i:                                       \
    return UnpackValues<OutType, i>(in, in_bytes, num_values, out);

  switch (bit_width) {
    // Expand cases from 0 to 64.
    BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default:
      DCHECK(false);
      return std::make_pair(nullptr, -1);
  }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read =
      NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in_pos = Unpack32Values<OutType, BIT_WIDTH>(in_pos, in_bytes, out_pos);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }
  // Then unpack the final partial batch.
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, BIT_WIDTH>(
        in_pos, in_bytes, remainder_values, out_pos);
  }
  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD(
    int bit_width,
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  switch (bit_width) {
    case 0:
      return BitPacking::UnpackValuesSIMD_0<OutType>(
          in, in_bytes, num_values, out);
    case 1:
      return BitPacking::UnpackValuesSIMD_1<OutType>(
          in, in_bytes, num_values, out);
    case 2:
      return BitPacking::UnpackValuesSIMD_2<OutType>(
          in, in_bytes, num_values, out);
    case 3:
      return BitPacking::UnpackValuesSIMD_3<OutType>(
          in, in_bytes, num_values, out);
    case 4:
      return BitPacking::UnpackValuesSIMD_4<OutType>(
          in, in_bytes, num_values, out);
    case 5:
      return BitPacking::UnpackValuesSIMD_5<OutType>(
          in, in_bytes, num_values, out);
    case 6:
      return BitPacking::UnpackValuesSIMD_6<OutType>(
          in, in_bytes, num_values, out);
    case 7:
      return BitPacking::UnpackValuesSIMD_7<OutType>(
          in, in_bytes, num_values, out);
    case 8:
      return BitPacking::UnpackValuesSIMD_8<OutType>(
          in, in_bytes, num_values, out);
    case 9:
      return BitPacking::UnpackValuesSIMD_9<OutType>(
          in, in_bytes, num_values, out);
    case 10:
      return BitPacking::UnpackValuesSIMD_10<OutType>(
          in, in_bytes, num_values, out);
    case 11:
      return BitPacking::UnpackValuesSIMD_11<OutType>(
          in, in_bytes, num_values, out);
    case 12:
      return BitPacking::UnpackValuesSIMD_12<OutType>(
          in, in_bytes, num_values, out);
    case 13:
      return BitPacking::UnpackValuesSIMD_13<OutType>(
          in, in_bytes, num_values, out);
    case 14:
      return BitPacking::UnpackValuesSIMD_14<OutType>(
          in, in_bytes, num_values, out);
    case 15:
      return BitPacking::UnpackValuesSIMD_15<OutType>(
          in, in_bytes, num_values, out);
    case 16:
      return BitPacking::UnpackValuesSIMD_16<OutType>(
          in, in_bytes, num_values, out);
    case 17:
      return BitPacking::UnpackValuesSIMD_17<OutType>(
          in, in_bytes, num_values, out);
    case 18:
      return BitPacking::UnpackValuesSIMD_18<OutType>(
          in, in_bytes, num_values, out);
    case 19:
      return BitPacking::UnpackValuesSIMD_19<OutType>(
          in, in_bytes, num_values, out);
    case 20:
      return BitPacking::UnpackValuesSIMD_20<OutType>(
          in, in_bytes, num_values, out);
    case 21:
      return BitPacking::UnpackValuesSIMD_21<OutType>(
          in, in_bytes, num_values, out);
    case 22:
      return BitPacking::UnpackValuesSIMD_22<OutType>(
          in, in_bytes, num_values, out);
    case 23:
      return BitPacking::UnpackValuesSIMD_23<OutType>(
          in, in_bytes, num_values, out);
    case 24:
      return BitPacking::UnpackValuesSIMD_24<OutType>(
          in, in_bytes, num_values, out);
    case 25:
      return BitPacking::UnpackValuesSIMD_25<OutType>(
          in, in_bytes, num_values, out);
    case 26:
      return BitPacking::UnpackValuesSIMD_26<OutType>(
          in, in_bytes, num_values, out);
    case 27:
      return BitPacking::UnpackValuesSIMD_27<OutType>(
          in, in_bytes, num_values, out);
    case 28:
      return BitPacking::UnpackValuesSIMD_28<OutType>(
          in, in_bytes, num_values, out);
    case 29:
      return BitPacking::UnpackValuesSIMD_29<OutType>(
          in, in_bytes, num_values, out);
    case 30:
      return BitPacking::UnpackValuesSIMD_30<OutType>(
          in, in_bytes, num_values, out);
    case 31:
      return BitPacking::UnpackValuesSIMD_31<OutType>(
          in, in_bytes, num_values, out);
    case 32:
      return BitPacking::UnpackValuesSIMD_32<OutType>(
          in, in_bytes, num_values, out);
    default:
      DCHECK(false);
      return std::make_pair(nullptr, -1);
  }
}

// template <typename OutType>
// std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL(
//     int bit_width,
//     const uint8_t* __restrict__ in,
//     int64_t in_bytes,
//     int64_t num_values,
//     OutType* __restrict__ out) {
//   static_assert(
//       IsSupportedUnpackingType<OutType>(),
//       "Only unsigned integers are supported.");

//   const int64_t values_to_read =
//       NumValuesToUnpack(bit_width, in_bytes, num_values);
//   const uint8_t* in_pos = in;
//   uint8_t* out_pos = (uint8_t*)out;

//   in_pos = BitUnpackKernel(bit_width, in, values_to_read, out);

//   return std::make_pair(in_pos, values_to_read);
// }

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_0(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_set1_epi8(0x00);
    _mm512_storeu_epi8(out_pos, tmp);
    in_pos += 2;
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 0>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_1(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 64;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[i], _mm512_set1_epi8(0x01));

    __m128i tmp1 = _mm512_extracti32x4_epi32(tmp, 0);
    __m512i result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 1);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 2);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 3);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    if (remainder_values >= 32) {
      in_pos = Unpack32Values<OutType, 1>(in_pos, in_bytes, out_pos);
      remainder_values -= 32;
      out_pos += 32;
      in_bytes -= (32 * 1) / CHAR_BIT;
    }
    in_pos = UnpackUpTo31Values<OutType, 1>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_2(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(2, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint32_t* in32_pos = reinterpret_cast<const uint32_t*>(in);
  OutType* out_pos = out;
  __m512i am = _mm512_set_epi32(
      30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
  __m512i mask = _mm512_set1_epi32(0x00000003);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi32(in32_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 2) / CHAR_BIT;
    in_pos += 4;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 2>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_2(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(2, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;
  __m512i am =
_mm512_set_epi16(62,60,58,56,54,52,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,0);
  __m512i mask = _mm512_set1_epi32(0x00000003);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi64(in64_pos[i]);
    __m512i bm = _mm512_multishift_epi64_epi8(am, data);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_castsi512_si256(bm));
    _mm512_storeu_epi8(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm256_loadu_epi64(((uint64_t
*)&bm)+4)); _mm512_storeu_epi8(out_pos+16,_mm512_and_epi32(out2, mask)); out_pos
+= BATCH_SIZE; in_bytes -= (BATCH_SIZE * 2) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    if(remainder_values >= 32)
    {
      in_pos = Unpack32Values<OutType, 1>(in_pos, in_bytes, out_pos);
      remainder_values -= 32;
      out_pos += 32;
      in_bytes -= (32 * 1) / CHAR_BIT;
    }
    in_pos = UnpackUpTo31Values<OutType, 1>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}*/

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_3(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(3, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;
  const uint64_t* in64_pos;
  __m512i am = _mm512_set_epi32(
      45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  __m512i mask = _mm512_set1_epi32(0x00000007);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in64_pos = reinterpret_cast<const uint64_t*>(in_pos);
    __m512i data = _mm512_set1_epi64(in64_pos[0]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_pos += 6;
    in_bytes -= (BATCH_SIZE * 3) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 3>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_4(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(4, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi32(
      60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0);
  __m512i mask = _mm512_set1_epi32(0x0000000f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi64(in64_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 4) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 4>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_5(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(5, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  //__m512i am =
  //_mm512_set_epi8(37,36,37,36,37,36,36,33,33,32,33,32,32,29,32,29,28,25,28,25,28,25,25,24,24,21,24,21,21,20,21,20,17,16,17,16,17,16,16,13,13,12,13,12,12,9,12,9,8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0);
  __m512i am = _mm512_set_epi8(
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0,
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0,
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0,
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    //__m256i data = _mm256_loadu_epi8(in_pos);
    // data = _mm256_maskz_expand_epi8(0x1f1f1f1f,data);
    __m256i data = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 20;
    in_bytes -= (BATCH_SIZE * 5) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 5>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_6(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(6, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x0000003f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 24;
    in_bytes -= (BATCH_SIZE * 6) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 6>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_5(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m256i am =
_mm256_set_epi8(4,3,4,3,4,3,3,2,2,1,2,1,1,0,1,0,4,3,4,3,4,3,3,2,2,1,2,1,1,0,1,0);
  __m512i dm = _mm512_set_epi32(43,6,33,4,39,2,37,0,43,6,33,4,39,2,37,0);
  __m512i mask = _mm512_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x001f001f,in_pos);
    //__m256i data = _mm256_loadu_epi8(in_pos);
    //data = _mm256_maskz_expand_epi8(0x001f001f,data);
    __m256i bm = _mm256_shuffle_epi8(data, am);
    __m512i cm = _mm512_cvtepu16_epi32(bm);
    cm = _mm512_multishift_epi64_epi8(dm, cm);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 10;
    //in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}*/

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_5(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 8;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m128i am = _mm_set_epi8(4,3,4,3,4,3,3,2,2,1,2,1,1,0,1,0);
  __m256i dm = _mm256_set_epi32(43,6,33,4,39,2,37,0);
  __m256i mask = _mm256_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m128i bm = _mm_shuffle_epi8(data, am);
    __m256i cm = _mm256_cvtepu16_epi32(bm);
    cm = _mm256_multishift_epi64_epi8(dm, cm);
    cm = _mm256_maskz_and_epi32(0xff,cm, mask);
    _mm256_storeu_epi8(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 5;
    in_bytes -= (BATCH_SIZE * 5) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 5>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}*/

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_6(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 8;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m128i am = _mm_set_epi8(5,4,5,4,4,3,4,3,2,1,2,1,1,0,1,0);
  __m256i dm = _mm256_set_epi32(42,4,38,0,42,4,38,0);
  __m256i mask = _mm256_set1_epi32(0x0000003f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m128i bm = _mm_shuffle_epi8(data, am);
    __m256i cm = _mm256_cvtepu16_epi32(bm);
    cm = _mm256_multishift_epi64_epi8(dm, cm);
    cm = _mm256_maskz_and_epi32(0xff,cm, mask);
    _mm256_storeu_epi8(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 6;
    in_bytes -= (BATCH_SIZE * 6) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 6>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}*/

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_7(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(7, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0,
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0,
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0,
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0,
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0,
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0,
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x0000007f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 28;
    in_bytes -= (BATCH_SIZE * 7) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 7>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_8(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(8, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m512i data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_epi8(out_pos, data1);
    data = _mm_loadu_epi8(in_pos + 16);
    data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_epi8(out_pos + 16, data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 8) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 8>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_9(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(9, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0,
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0,
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0,
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x000001ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x01ff01ff01ff01ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 36;
    in_bytes -= (BATCH_SIZE * 9) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 9>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_10(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(10, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x000003ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x03ff03ff03ff03ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 40;
    in_bytes -= (BATCH_SIZE * 10) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 10>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_11(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(11, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0,
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0,
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0,
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0,
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0,
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0,
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x000007ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x07ff07ff07ff07ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 44;
    in_bytes -= (BATCH_SIZE * 11) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 11>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_12(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(12, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x00000fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x0fff0fff0fff0fff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 48;
    in_bytes -= (BATCH_SIZE * 12) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 12>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_13(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(13, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i am2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i cm1 = _mm512_set_epi8(
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0,
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0,
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0,
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0);
  __m512i cm2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x00001fff);
  __m512i mask1 = _mm512_set_epi64(
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(
      0x0000000000000000,
      0x1f00000000000000,
      0x0000000000000000,
      0x1f00000000000000,
      0x0000000000000000,
      0x1f00000000000000,
      0x0000000000000000,
      0x1f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x1fff1fff1fff1fff, in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 52;
    in_bytes -= (BATCH_SIZE * 13) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 13>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_14(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(14, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x00003fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x3fff3fff3fff3fff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 56;
    in_bytes -= (BATCH_SIZE * 14) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 14>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_15(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(15, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i am2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i cm1 = _mm512_set_epi8(
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0,
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0,
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0,
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0);
  __m512i cm2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x00007fff);
  __m512i mask1 = _mm512_set_epi64(
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(
      0x0000000000000000,
      0x7f00000000000000,
      0x0000000000000000,
      0x7f00000000000000,
      0x0000000000000000,
      0x7f00000000000000,
      0x0000000000000000,
      0x7f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x7fff7fff7fff7fff, in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 60;
    in_bytes -= (BATCH_SIZE * 15) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 15>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_16(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(16, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_loadu_epi8(in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    _mm512_storeu_epi8(out_pos, data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 16) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 16>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_17(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_18(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_19(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_20(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_21(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_22(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_23(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_24(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_25(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_26(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_27(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_28(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_29(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_30(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_31(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesSIMD_32(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_0(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_set1_epi8(0x00);
    _mm512_storeu_epi8(out_pos, tmp);
    in_pos += 2;
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 0>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_1(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  uint64_t time1, time2, time3;
  static int cnt = 0;
  unsigned int aux = 0;

  // time1 = __rdtscp(&aux);

  constexpr int BATCH_SIZE = 64;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[i], _mm512_set1_epi8(0x01));

    __m128i tmp1 = _mm512_extracti32x4_epi32(tmp, 0);
    __m512i result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 1);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 2);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 3);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_epi8(out_pos, result);
    out_pos += 16;

    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    if (remainder_values >= 32) {
      in_pos = Unpack32Values<OutType, 1>(in_pos, in_bytes, out_pos);
      remainder_values -= 32;
      out_pos += 32;
      in_bytes -= (32 * 1) / CHAR_BIT;
    }
    in_pos = UnpackUpTo31Values<OutType, 1>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  // time2 = __rdtscp(&aux) - time1;
  // if(cnt++<10) printf( "out_num is %d, unpack1_32 latency = %lu \n",
  // batches_to_read*64, time2);

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_2(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(2, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint32_t* in32_pos = reinterpret_cast<const uint32_t*>(in);
  OutType* out_pos = out;
  __m512i am = _mm512_set_epi32(
      30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
  __m512i mask = _mm512_set1_epi32(0x00000003);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi32(in32_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 2) / CHAR_BIT;
    in_pos += 4;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 2>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_2(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(2, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;
  __m512i am =
_mm512_set_epi16(62,60,58,56,54,52,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,0);
  __m512i mask = _mm512_set1_epi32(0x00000003);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi64(in64_pos[i]);
    __m512i bm = _mm512_multishift_epi64_epi8(am, data);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_castsi512_si256(bm));
    _mm512_storeu_epi8(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm256_loadu_epi64(((uint64_t
*)&bm)+4)); _mm512_storeu_epi8(out_pos+16,_mm512_and_epi32(out2, mask)); out_pos
+= BATCH_SIZE; in_bytes -= (BATCH_SIZE * 2) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    if(remainder_values >= 32)
    {
      in_pos = Unpack32Values<OutType, 1>(in_pos, in_bytes, out_pos);
      remainder_values -= 32;
      out_pos += 32;
      in_bytes -= (32 * 1) / CHAR_BIT;
    }
    in_pos = UnpackUpTo31Values<OutType, 1>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}*/

OWN_ALIGNED_64_ARRAY(const uint32_t p_permutex_masks_3u[32]) = {
    0x0,      0x10020,  0x210021, 0x220002, 0x30003,  0x40023,  0x240024,
    0x250005, 0x60006,  0x70026,  0x270027, 0x280008, 0x90009,  0xA0029,
    0x2A002A, 0x2B000B, 0x200000, 0x10020,  0x20021,  0x220002, 0x230003,
    0x40023,  0x50024,  0x250005, 0x260006, 0x70026,  0x80027,  0x280008,
    0x290009, 0xA0029,  0xB002A,  0x2B000B};

// ------------------------------------ 3u
// -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_3u_0[32]) = {
    0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u,
    0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u};
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_3u_1[32]) = {
    5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u,
    5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u};

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_3(
    const uint8_t* __restrict__ src_ptr,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ dst_ptr) {
  uint32_t start_bit = 0;
  uint64_t bit_mask0 = 0x0007000007000007LLU;
  uint64_t bit_mask1 = 0x0700000700000700LLU;
  uint64_t bit_mask2 = 0x0000070000070000LLU;

  if (num_values >= 64u) {
    __mmask64 read_mask = 0xFFFFFF;
    __m512i parse_mask = _mm512_set1_epi8(0x07);
    __m512i permutex_idx_ptr[2];
    permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_3u);
    permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_3u + 16u);

    __m512i shift_mask_ptr[2];
    shift_mask_ptr[0] = _mm512_load_si512(shift_table_3u_0);
    shift_mask_ptr[1] = _mm512_load_si512(shift_table_3u_1);

    while (num_values >= 64u) {
      __m512i srcmm0, srcmm1, zmm[2];

      srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
      srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

      // permuting so in zmm[0] will be elements with even indexes and in zmm[1]
      // - with odd ones
      zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
      zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

      // shifting elements so they start from the start of the word
      zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
      zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

      // gathering even and odd elements together
      zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
      zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

      __m512i tmp_rlt =
          _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(zmm[0], 0));
      _mm512_storeu_si512(dst_ptr, tmp_rlt);
      dst_ptr += 16u;
      tmp_rlt = _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(zmm[0], 1));
      _mm512_storeu_si512(dst_ptr, tmp_rlt);
      dst_ptr += 16u;
      tmp_rlt = _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(zmm[0], 2));
      _mm512_storeu_si512(dst_ptr, tmp_rlt);
      dst_ptr += 16u;
      tmp_rlt = _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(zmm[0], 3));
      _mm512_storeu_si512(dst_ptr, tmp_rlt);
      dst_ptr += 16u;

      src_ptr += 8u * 3u;
      num_values -= 64u;
    }
  }
  if (num_values > 32u) {
    bit_byte_pool64_t bit_byte_pool64;
    uint64_t* tmp_src64 = (uint64_t*)src_ptr;
    uint64_t src64 = *tmp_src64;
    src_ptr = src_ptr + 8u;
    bit_byte_pool32_t bit_byte_pool32;
    uint32_t* tmp_src32 = (uint32_t*)src_ptr;
    uint32_t src32 = (*tmp_src32);
    src_ptr = src_ptr + 4u;

    bit_byte_pool64.bit_buf = src64 & bit_mask0;
    dst_ptr[0] = bit_byte_pool64.byte_buf[0];
    dst_ptr[8] = bit_byte_pool64.byte_buf[3];
    dst_ptr[16] = bit_byte_pool64.byte_buf[6];
    bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
    dst_ptr[3] = bit_byte_pool64.byte_buf[1];
    dst_ptr[11] = bit_byte_pool64.byte_buf[4];
    dst_ptr[19] = bit_byte_pool64.byte_buf[7];
    bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
    dst_ptr[6] = bit_byte_pool64.byte_buf[2];
    dst_ptr[14] = bit_byte_pool64.byte_buf[5];
    bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask0;
    dst_ptr[1] = bit_byte_pool64.byte_buf[0];
    dst_ptr[9] = bit_byte_pool64.byte_buf[3];
    dst_ptr[17] = bit_byte_pool64.byte_buf[6];
    bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask1;
    dst_ptr[4] = bit_byte_pool64.byte_buf[1];
    dst_ptr[12] = bit_byte_pool64.byte_buf[4];
    dst_ptr[20] = bit_byte_pool64.byte_buf[7];
    bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask2;
    dst_ptr[7] = bit_byte_pool64.byte_buf[2];
    dst_ptr[15] = bit_byte_pool64.byte_buf[5];
    bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask0;
    dst_ptr[2] = bit_byte_pool64.byte_buf[0];
    dst_ptr[10] = bit_byte_pool64.byte_buf[3];
    dst_ptr[18] = bit_byte_pool64.byte_buf[6];
    bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask1;
    dst_ptr[5] = bit_byte_pool64.byte_buf[1];
    dst_ptr[13] = bit_byte_pool64.byte_buf[4];
    dst_ptr[21] = bit_byte_pool64.byte_buf[7] | (((uint8_t)src32 & 3u) << 1u);
    src32 = src32 >> 2u;
    bit_byte_pool32.bit_buf = src32 & bit_mask0;
    dst_ptr[22] = bit_byte_pool32.byte_buf[0];
    dst_ptr[30] = bit_byte_pool32.byte_buf[3];
    bit_byte_pool32.bit_buf = (src32 >> 1u) & bit_mask1;
    dst_ptr[25] = bit_byte_pool32.byte_buf[1];
    bit_byte_pool32.bit_buf = (src32 >> 2u) & bit_mask2;
    dst_ptr[28] = bit_byte_pool32.byte_buf[2];
    bit_byte_pool32.bit_buf = (src32 >> 3u) & bit_mask0;
    dst_ptr[23] = bit_byte_pool32.byte_buf[0];
    dst_ptr[31] = bit_byte_pool32.byte_buf[3];
    bit_byte_pool32.bit_buf = (src32 >> 4u) & bit_mask1;
    dst_ptr[26] = bit_byte_pool32.byte_buf[1];
    bit_byte_pool32.bit_buf = (src32 >> 5u) & bit_mask2;
    dst_ptr[29] = bit_byte_pool32.byte_buf[2];
    bit_byte_pool32.bit_buf = (src32 >> 6u) & bit_mask0;
    dst_ptr[24] = bit_byte_pool32.byte_buf[0];
    bit_byte_pool32.bit_buf = (src32 >> 7u) & bit_mask1;
    dst_ptr[27] = bit_byte_pool32.byte_buf[1];
    dst_ptr += 32u;
    num_values -= 32u;
  }
  if (num_values > 16u) {
    bit_byte_pool48_t bit_byte_pool48;
    uint32_t* tmp_src32 = (uint32_t*)src_ptr;
    bit_byte_pool48.dw_buf[0] = (*tmp_src32);
    src_ptr = src_ptr + sizeof(uint32_t);
    uint16_t* tmp_src16 = (uint16_t*)src_ptr;
    bit_byte_pool48.word_buf[2] = (*tmp_src16);
    src_ptr = src_ptr + sizeof(uint16_t);
    uint64_t src64 = bit_byte_pool48.bit_buf;
    bit_byte_pool48.bit_buf = src64 & bit_mask0;
    dst_ptr[0] = bit_byte_pool48.byte_buf[0];
    dst_ptr[8] = bit_byte_pool48.byte_buf[3];
    bit_byte_pool48.bit_buf = (src64 >> 1u) & bit_mask1;
    dst_ptr[3] = bit_byte_pool48.byte_buf[1];
    dst_ptr[11] = bit_byte_pool48.byte_buf[4];
    bit_byte_pool48.bit_buf = (src64 >> 2u) & bit_mask2;
    dst_ptr[6] = bit_byte_pool48.byte_buf[2];
    dst_ptr[14] = bit_byte_pool48.byte_buf[5];
    bit_byte_pool48.bit_buf = (src64 >> 3u) & bit_mask0;
    dst_ptr[1] = bit_byte_pool48.byte_buf[0];
    dst_ptr[9] = bit_byte_pool48.byte_buf[3];
    bit_byte_pool48.bit_buf = (src64 >> 4u) & bit_mask1;
    dst_ptr[4] = bit_byte_pool48.byte_buf[1];
    dst_ptr[12] = bit_byte_pool48.byte_buf[4];
    bit_byte_pool48.bit_buf = (src64 >> 5u) & bit_mask2;
    dst_ptr[7] = bit_byte_pool48.byte_buf[2];
    dst_ptr[15] = bit_byte_pool48.byte_buf[5];
    bit_byte_pool48.bit_buf = (src64 >> 6u) & bit_mask0;
    dst_ptr[2] = bit_byte_pool48.byte_buf[0];
    dst_ptr[10] = bit_byte_pool48.byte_buf[3];
    bit_byte_pool48.bit_buf = (src64 >> 7u) & bit_mask1;
    dst_ptr[5] = bit_byte_pool48.byte_buf[1];
    dst_ptr[13] = bit_byte_pool48.byte_buf[4];
    dst_ptr += 16u;
    num_values -= 16u;
  }
  if (num_values > 8u) {
    bit_byte_pool32_t bit_byte_pool32;
    uint16_t* tmp_src16 = (uint16_t*)src_ptr;
    bit_byte_pool32.word_buf[0] = (*tmp_src16);
    src_ptr = src_ptr + 2u;
    bit_byte_pool32.byte_buf[2] = (*src_ptr);
    src_ptr = src_ptr + 1u;
    uint32_t src32 = bit_byte_pool32.bit_buf;
    bit_byte_pool32.bit_buf = src32 & (uint32_t)bit_mask0;
    dst_ptr[0] = bit_byte_pool32.byte_buf[0];
    bit_byte_pool32.bit_buf = (src32 >> 3u) & (uint32_t)bit_mask0;
    dst_ptr[1] = bit_byte_pool32.byte_buf[0];
    bit_byte_pool32.bit_buf = (src32 >> 6u) & (uint32_t)bit_mask0;
    dst_ptr[2] = bit_byte_pool32.byte_buf[0];
    bit_byte_pool32.bit_buf = (src32 >> 1u) & (uint32_t)bit_mask1;
    dst_ptr[3] = bit_byte_pool32.byte_buf[1];
    bit_byte_pool32.bit_buf = (src32 >> 4u) & (uint32_t)bit_mask1;
    dst_ptr[4] = bit_byte_pool32.byte_buf[1];
    bit_byte_pool32.bit_buf = (src32 >> 7u) & (uint32_t)bit_mask1;
    dst_ptr[5] = bit_byte_pool32.byte_buf[1];
    bit_byte_pool32.bit_buf = (src32 >> 2u) & (uint32_t)bit_mask2;
    dst_ptr[6] = bit_byte_pool32.byte_buf[2];
    bit_byte_pool32.bit_buf = (src32 >> 5u) & (uint32_t)bit_mask2;
    dst_ptr[7] = bit_byte_pool32.byte_buf[2];
    dst_ptr += 8u;
    num_values -= 8u;
  }
  if (0u < num_values) {
    uint16_t mask = OWN_3_BIT_MASK;
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;
    while (0u != num_values) {
      if (3u > bits_in_buf) {
        next_byte = (uint16_t)(*src_ptr);
        src_ptr++;
        next_byte = next_byte << bits_in_buf;
        src = src | next_byte;
        bits_in_buf += OWN_BYTE_WIDTH;
      }
      *dst_ptr = (uint8_t)(src & mask);
      src = src >> 3u;
      bits_in_buf -= 3u;
      dst_ptr++;
      num_values--;
    }
  }

  return std::make_pair(src_ptr, num_values);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_4(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(4, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi32(
      60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0);
  __m512i mask = _mm512_set1_epi32(0x0000000f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi64(in64_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 4) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 4>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_5(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(5, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  //__m512i am =
  //_mm512_set_epi8(37,36,37,36,37,36,36,33,33,32,33,32,32,29,32,29,28,25,28,25,28,25,25,24,24,21,24,21,21,20,21,20,17,16,17,16,17,16,16,13,13,12,13,12,12,9,12,9,8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0);
  __m512i am = _mm512_set_epi8(
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      8,
      5,
      8,
      5,
      8,
      5,
      5,
      4,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0,
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0,
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0,
      59,
      59,
      38,
      38,
      17,
      17,
      4,
      4,
      55,
      55,
      34,
      34,
      21,
      21,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    //__m256i data = _mm256_loadu_epi8(in_pos);
    // data = _mm256_maskz_expand_epi8(0x1f1f1f1f,data);
    __m256i data = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 20;
    in_bytes -= (BATCH_SIZE * 5) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 5>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_6(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(6, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0,
      9,
      8,
      9,
      8,
      8,
      5,
      8,
      5,
      4,
      1,
      4,
      1,
      1,
      0,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0,
      58,
      58,
      36,
      36,
      22,
      22,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x0000003f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 24;
    in_bytes -= (BATCH_SIZE * 6) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 6>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_5(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m256i am =
_mm256_set_epi8(4,3,4,3,4,3,3,2,2,1,2,1,1,0,1,0,4,3,4,3,4,3,3,2,2,1,2,1,1,0,1,0);
  __m512i dm = _mm512_set_epi32(43,6,33,4,39,2,37,0,43,6,33,4,39,2,37,0);
  __m512i mask = _mm512_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x001f001f,in_pos);
    //__m256i data = _mm256_loadu_epi8(in_pos);
    //data = _mm256_maskz_expand_epi8(0x001f001f,data);
    __m256i bm = _mm256_shuffle_epi8(data, am);
    __m512i cm = _mm512_cvtepu16_epi32(bm);
    cm = _mm512_multishift_epi64_epi8(dm, cm);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_epi8(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 10;
    //in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}*/

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_5(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 8;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m128i am = _mm_set_epi8(4,3,4,3,4,3,3,2,2,1,2,1,1,0,1,0);
  __m256i dm = _mm256_set_epi32(43,6,33,4,39,2,37,0);
  __m256i mask = _mm256_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m128i bm = _mm_shuffle_epi8(data, am);
    __m256i cm = _mm256_cvtepu16_epi32(bm);
    cm = _mm256_multishift_epi64_epi8(dm, cm);
    cm = _mm256_maskz_and_epi32(0xff,cm, mask);
    _mm256_storeu_epi8(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 5;
    in_bytes -= (BATCH_SIZE * 5) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 5>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}*/

/*
template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_6(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 8;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m128i am = _mm_set_epi8(5,4,5,4,4,3,4,3,2,1,2,1,1,0,1,0);
  __m256i dm = _mm256_set_epi32(42,4,38,0,42,4,38,0);
  __m256i mask = _mm256_set1_epi32(0x0000003f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m128i bm = _mm_shuffle_epi8(data, am);
    __m256i cm = _mm256_cvtepu16_epi32(bm);
    cm = _mm256_multishift_epi64_epi8(dm, cm);
    cm = _mm256_maskz_and_epi32(0xff,cm, mask);
    _mm256_storeu_epi8(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 6;
    in_bytes -= (BATCH_SIZE * 6) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 6>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}*/

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_7(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(7, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0,
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0,
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0,
      12,
      9,
      12,
      9,
      9,
      8,
      8,
      5,
      5,
      4,
      4,
      1,
      1,
      0,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0,
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0,
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0,
      57,
      57,
      34,
      34,
      19,
      19,
      4,
      4,
      53,
      53,
      38,
      38,
      23,
      23,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x0000007f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 28;
    in_bytes -= (BATCH_SIZE * 7) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 7>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_8(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(8, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m512i data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_epi8(out_pos, data1);
    data = _mm_loadu_epi8(in_pos + 16);
    data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_epi8(out_pos + 16, data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 8) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 8>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_9(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(9, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      8,
      7,
      7,
      6,
      6,
      5,
      5,
      4,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0,
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0,
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0,
      63,
      55,
      46,
      38,
      29,
      21,
      12,
      4,
      59,
      51,
      42,
      34,
      25,
      17,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x000001ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x01ff01ff01ff01ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 36;
    in_bytes -= (BATCH_SIZE * 9) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 9>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_10(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(10, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0,
      9,
      8,
      8,
      7,
      7,
      6,
      6,
      5,
      4,
      3,
      3,
      2,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0,
      62,
      54,
      44,
      36,
      26,
      18,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x000003ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x03ff03ff03ff03ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 40;
    in_bytes -= (BATCH_SIZE * 10) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 10>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_11(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(11, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0,
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0,
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0,
      10,
      9,
      9,
      8,
      7,
      6,
      6,
      5,
      5,
      4,
      3,
      2,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0,
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0,
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0,
      61,
      53,
      42,
      34,
      31,
      23,
      12,
      4,
      57,
      49,
      46,
      38,
      27,
      19,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x000007ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x07ff07ff07ff07ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 44;
    in_bytes -= (BATCH_SIZE * 11) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 11>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_12(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(12, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      11,
      10,
      10,
      9,
      8,
      7,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0,
      60,
      52,
      40,
      32,
      28,
      20,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x00000fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x0fff0fff0fff0fff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 48;
    in_bytes -= (BATCH_SIZE * 12) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 12>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_13(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(13, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0,
      12,
      11,
      10,
      9,
      9,
      8,
      7,
      6,
      5,
      4,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i am2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      6,
      5,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i cm1 = _mm512_set_epi8(
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0,
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0,
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0,
      59,
      51,
      46,
      38,
      25,
      17,
      12,
      4,
      63,
      55,
      42,
      34,
      29,
      21,
      8,
      0);
  __m512i cm2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      55,
      0,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x00001fff);
  __m512i mask1 = _mm512_set_epi64(
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(
      0x0000000000000000,
      0x1f00000000000000,
      0x0000000000000000,
      0x1f00000000000000,
      0x0000000000000000,
      0x1f00000000000000,
      0x0000000000000000,
      0x1f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x1fff1fff1fff1fff, in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 52;
    in_bytes -= (BATCH_SIZE * 13) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 13>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_14(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(14, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      13,
      12,
      11,
      10,
      9,
      8,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i cm = _mm512_set_epi8(
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0,
      58,
      50,
      44,
      36,
      30,
      22,
      8,
      0);
  __m512i mask = _mm512_set1_epi32(0x00003fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x3fff3fff3fff3fff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 56;
    in_bytes -= (BATCH_SIZE * 14) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 14>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_15(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(15, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0,
      14,
      13,
      12,
      11,
      10,
      9,
      8,
      7,
      6,
      5,
      4,
      3,
      2,
      1,
      1,
      0);
  __m512i am2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      6,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i cm1 = _mm512_set_epi8(
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0,
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0,
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0,
      57,
      49,
      42,
      34,
      27,
      19,
      12,
      4,
      61,
      53,
      46,
      38,
      31,
      23,
      8,
      0);
  __m512i cm2 = _mm512_set_epi8(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      53,
      0,
      0,
      0,
      0,
      0,
      0,
      0);
  __m512i mask = _mm512_set1_epi32(0x00007fff);
  __m512i mask1 = _mm512_set_epi64(
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff,
      0xffffffffffffffff,
      0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(
      0x0000000000000000,
      0x7f00000000000000,
      0x0000000000000000,
      0x7f00000000000000,
      0x0000000000000000,
      0x7f00000000000000,
      0x0000000000000000,
      0x7f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x7fff7fff7fff7fff, in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 0));
    _mm512_storeu_epi8(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 1));
    _mm512_storeu_epi8(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 60;
    in_bytes -= (BATCH_SIZE * 15) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 15>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_16(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(16, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_loadu_epi8(in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    _mm512_storeu_epi8(out_pos, data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 16) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 16>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_17(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_18(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_19(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_20(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_21(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_22(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_23(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_24(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_25(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_26(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_27(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_28(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_29(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_30(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_31(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesQPL_32(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(
      IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint16_t* in16_pos = reinterpret_cast<const uint16_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi32(in16_pos[i], _mm512_set1_epi8(0x01));
    _mm512_storeu_epi8(out_pos, tmp);
    out_pos += BATCH_SIZE;
    // in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackAndDecodeValues(
    int bit_width,
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    OutType* __restrict__ dict,
    int64_t dict_len,
    int64_t num_values,
    OutType* __restrict__ out,
    int64_t stride,
    bool* __restrict__ decode_error) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case i:                                       \
    return UnpackAndDecodeValues<OutType, i>(   \
        in, in_bytes, dict, dict_len, num_values, out, stride, decode_error);

  switch (bit_width) {
    // Expand cases from 0 to MAX_DICT_BITWIDTH.
    BOOST_PP_REPEAT_FROM_TO(0, 33, UNPACK_VALUES_CASE, ignore);
    default:
      DCHECK(false);
      return std::make_pair(nullptr, -1);
  }
#pragma pop_macro("UNPACK_VALUES_CASE")
}
template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackAndDecodeValues(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    OutType* __restrict__ dict,
    int64_t dict_len,
    int64_t num_values,
    OutType* __restrict__ out,
    int64_t stride,
    bool* __restrict__ decode_error) {
  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read =
      NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  uint8_t* out_pos = reinterpret_cast<uint8_t*>(out);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in_pos = UnpackAndDecode32Values<OutType, BIT_WIDTH>(
        in_pos,
        in_bytes,
        dict,
        dict_len,
        reinterpret_cast<OutType*>(out_pos),
        stride,
        decode_error);
    out_pos += stride * BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }
  // Then unpack the final partial batch.
  if (remainder_values > 0) {
    in_pos = UnpackAndDecodeUpTo31Values<OutType, BIT_WIDTH>(
        in_pos,
        in_bytes,
        dict,
        dict_len,
        remainder_values,
        reinterpret_cast<OutType*>(out_pos),
        stride,
        decode_error);
  }
  return std::make_pair(in_pos, values_to_read);
}

// Loop body of unrolled loop that unpacks the value. BIT_WIDTH is the bit width
// of the packed values. 'in_buf' is the start of the input buffer and
// 'out_vals' is the start of the output values array. This function unpacks the
// VALUE_IDX'th packed value from 'in_buf'.
//
// This implements essentially the same algorithm as the (Apache-licensed) code
// in bpacking.c at https://github.com/lemire/FrameOfReference/, but is much
// more compact because it uses templates rather than source-level unrolling of
// all combinations.
//
// After the template parameters are expanded and constants are propagated, all
// branches and offset/shift calculations should be optimized out, leaving only
// shifts by constants and bitmasks by constants. Calls to this must be stamped
// out manually or with BOOST_PP_REPEAT_FROM_TO: experimentation revealed that
// the GCC 4.9.2 optimiser was not able to fully propagate constants and remove
// branches when this was called from inside a for loop with constant bounds
// with VALUE_IDX changed to a function argument.
//
// We compute how many 32 bit words we have to read, which is either 1, 2 or 3.
// If it is at least 2, the first two 32 bit words are read as one 64 bit word.
// Even if only one word needs to be read, we try to read 64 bits if it does not
// lead to buffer overflow because benchmarks show that it has a positive effect
// on performance.
//
// If 'FULL_BATCH' is true, this function call is part of unpacking 32 values,
// otherwise up to 31 values. This is needed to optimise the length of the reads
// (32 or 64 bits) and avoid buffer overflow (if we are unpacking 32 values, we
// can safely assume an input buffer of length 32 * BIT_WIDTH).

inline void print_bin(uint32_t number) {
  int bit = sizeof(uint32_t) * 8;
  int i;
  for (i = bit - 1; i >= 0; i--) {
    int bin = (number & (1 << i)) >> i;
    printf("%d", bin);
    if (i % 8 == 0) {
      printf(" ");
    }
  }
  printf("\n");
}

template <int BIT_WIDTH, int VALUE_IDX, bool FULL_BATCH>
inline uint64_t ALWAYS_INLINE UnpackValue(const uint8_t* __restrict__ in_buf) {
  if (BIT_WIDTH == 0)
    return 0;

  constexpr int FIRST_BIT_IDX = VALUE_IDX * BIT_WIDTH;
  constexpr int FIRST_WORD_IDX = FIRST_BIT_IDX / 32;
  constexpr int LAST_BIT_IDX = FIRST_BIT_IDX + BIT_WIDTH;
  constexpr int LAST_WORD_IDX = RoundUpNumi32(LAST_BIT_IDX);
  constexpr int WORDS_TO_READ = LAST_WORD_IDX - FIRST_WORD_IDX;
  static_assert(
      WORDS_TO_READ <= 3, "At most three 32-bit words need to be loaded.");

  constexpr int FIRST_BIT_OFFSET = FIRST_BIT_IDX - FIRST_WORD_IDX * 32;
  constexpr uint64_t mask = BIT_WIDTH == 64 ? ~0L : (1UL << BIT_WIDTH) - 1;
  const uint32_t* const in = reinterpret_cast<const uint32_t*>(in_buf);

  // Avoid reading past the end of the buffer. We can safely read 64 bits if we
  // know that this is a full batch read (so the input buffer is 32 * BIT_WIDTH
  // long) and there is enough space in the buffer from the current reading
  // point. We try to read 64 bits even when it is not necessary because the
  // benchmarks show it is faster.
  constexpr bool CAN_SAFELY_READ_64_BITS =
      FULL_BATCH && FIRST_BIT_IDX - FIRST_BIT_OFFSET + 64 <= BIT_WIDTH * 32;

  // We do not try to read 64 bits when the bit width is a power of two (unless
  // it is necessary) because performance benchmarks show that it is better this
  // way. This seems to be due to compiler optimisation issues, so we can
  // revisit it when we update the compiler version.
  constexpr bool READ_32_BITS =
      WORDS_TO_READ == 1 && (!CAN_SAFELY_READ_64_BITS || IsPowerOf2(BIT_WIDTH));

  if (READ_32_BITS) {
    uint32_t word = in[FIRST_WORD_IDX];
    word >>= FIRST_BIT_OFFSET < 32 ? FIRST_BIT_OFFSET : 0;
    return word & mask;
  }

  uint64_t word = *reinterpret_cast<const uint64_t*>(in + FIRST_WORD_IDX);
  word >>= FIRST_BIT_OFFSET;

  if (WORDS_TO_READ > 2) {
    constexpr int USEFUL_BITS =
        FIRST_BIT_OFFSET == 0 ? 0 : 64 - FIRST_BIT_OFFSET;
    uint64_t extra_word = in[FIRST_WORD_IDX + 2];
    word |= extra_word << USEFUL_BITS;
  }

  return word & mask;
}

template <typename OutType>
inline void ALWAYS_INLINE DecodeValue(
    OutType* __restrict__ dict,
    int64_t dict_len,
    uint32_t idx,
    OutType* __restrict__ out_val,
    bool* __restrict__ decode_error) {
  if (UNLIKELY(idx >= dict_len)) {
    *decode_error = true;
  } else {
    // Use memcpy() because we can't assume sufficient alignment in some cases
    // (e.g. 16 byte decimals).
    memcpy(out_val, &dict[idx], sizeof(OutType));
  }
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::Unpack32Values(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    OutType* __restrict__ out) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT)
      << "BIT_WIDTH too high for output";
  constexpr int BYTES_TO_READ = RoundUpNumBytes(32 * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);

  // Call UnpackValue for 0 <= i < 32.
#pragma push_macro("UNPACK_VALUE_CALL")
#define UNPACK_VALUE_CALL(ignore1, i, ignore2) \
  out[i] = static_cast<OutType>(UnpackValue<BIT_WIDTH, i, true>(in));

  BOOST_PP_REPEAT_FROM_TO(0, 32, UNPACK_VALUE_CALL, ignore);
  return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUE_CALL")
}

template <typename OutType>
const uint8_t* BitPacking::Unpack32Values(
    int bit_width,
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    OutType* __restrict__ out) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case i:                                       \
    return Unpack32Values<OutType, i>(in, in_bytes, out);

  switch (bit_width) {
    // Expand cases from 0 to 64.
    BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default:
      DCHECK(false);
      return in;
  }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackAndDecode32Values(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    OutType* __restrict__ dict,
    int64_t dict_len,
    OutType* __restrict__ out,
    int64_t stride,
    bool* __restrict__ decode_error) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  constexpr int BYTES_TO_READ = RoundUpNumBytes(32 * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  // TODO: this could be optimised further by using SIMD instructions.
  // https://lemire.me/blog/2016/08/25/faster-dictionary-decoding-with-simd-instructions/

  static_assert(
      BIT_WIDTH <= MAX_DICT_BITWIDTH,
      "Too high bit width for dictionary index.");

  // Call UnpackValue() and DecodeValue() for 0 <= i < 32.
#pragma push_macro("DECODE_VALUE_CALL")
#define DECODE_VALUE_CALL(ignore1, i, ignore2)                       \
  {                                                                  \
    uint32_t idx = UnpackValue<BIT_WIDTH, i, true>(in);              \
    uint8_t* out_pos = reinterpret_cast<uint8_t*>(out) + i * stride; \
    DecodeValue(                                                     \
        dict,                                                        \
        dict_len,                                                    \
        idx,                                                         \
        reinterpret_cast<OutType*>(out_pos),                         \
        decode_error);                                               \
  }

  BOOST_PP_REPEAT_FROM_TO(0, 32, DECODE_VALUE_CALL, ignore);
  return in + BYTES_TO_READ;
#pragma pop_macro("DECODE_VALUE_CALL")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackUpTo31Values(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    int num_values,
    OutType* __restrict__ out) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT)
      << "BIT_WIDTH too high for output";
  constexpr int MAX_BATCH_SIZE = 31;
  const int BYTES_TO_READ = RoundUpNumBytes(num_values * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  DCHECK_LE(num_values, MAX_BATCH_SIZE);

  // Make sure the buffer is at least 1 byte.
  constexpr int TMP_BUFFER_SIZE =
      BIT_WIDTH ? (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
  uint8_t tmp_buffer[TMP_BUFFER_SIZE];

  const uint8_t* in_buffer = in;
  // Copy into padded temporary buffer to avoid reading past the end of 'in' if
  // the last 32-bit load would go past the end of the buffer.
  if (RoundUp(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
    memcpy(tmp_buffer, in, BYTES_TO_READ);
    in_buffer = tmp_buffer;
  }

#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case 31 - i:                                  \
    out[30 - i] = static_cast<OutType>(         \
        UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer));

  // Use switch with fall-through cases to minimise branching.
  switch (num_values) {
    // Expand cases from 31 down to 1.
    BOOST_PP_REPEAT_FROM_TO(0, 31, UNPACK_VALUES_CASE, ignore);
    case 0:
      break;
    default:
      DCHECK(false);
  }
  return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackAndDecodeUpTo31Values(
    const uint8_t* __restrict__ in,
    int64_t in_bytes,
    OutType* __restrict__ dict,
    int64_t dict_len,
    int num_values,
    OutType* __restrict__ out,
    int64_t stride,
    bool* __restrict__ decode_error) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  constexpr int MAX_BATCH_SIZE = 31;
  const int BYTES_TO_READ = RoundUpNumBytes(num_values * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  DCHECK_LE(num_values, MAX_BATCH_SIZE);

  // Make sure the buffer is at least 1 byte.
  constexpr int TMP_BUFFER_SIZE =
      BIT_WIDTH ? (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
  uint8_t tmp_buffer[TMP_BUFFER_SIZE];

  const uint8_t* in_buffer = in;
  // Copy into padded temporary buffer to avoid reading past the end of 'in' if
  // the last 32-bit load would go past the end of the buffer.
  if (RoundUp(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
    memcpy(tmp_buffer, in, BYTES_TO_READ);
    in_buffer = tmp_buffer;
  }

#pragma push_macro("DECODE_VALUES_CASE")
#define DECODE_VALUES_CASE(ignore1, i, ignore2)                             \
  case 31 - i: {                                                            \
    uint32_t idx = UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer);        \
    uint8_t* out_pos = reinterpret_cast<uint8_t*>(out) + (30 - i) * stride; \
    DecodeValue(                                                            \
        dict,                                                               \
        dict_len,                                                           \
        idx,                                                                \
        reinterpret_cast<OutType*>(out_pos),                                \
        decode_error);                                                      \
  }

  // Use switch with fall-through cases to minimise branching.
  switch (num_values) {
    // Expand cases from 31 down to 1.
    BOOST_PP_REPEAT_FROM_TO(0, 31, DECODE_VALUES_CASE, ignore);
    case 0:
      break;
    default:
      DCHECK(false);
  }
  return in + BYTES_TO_READ;
#pragma pop_macro("DECODE_VALUES_CASE")
}
} // namespace impala
