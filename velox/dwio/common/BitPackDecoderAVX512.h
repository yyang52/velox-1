/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <folly/Range.h>
#include <iostream>
#include "extend_16u.h"
#include "extend_32u.h"
#include "extend_8u.h"
#include "unpack_16u.h"
#include "unpack_32u.h"
#include "unpack_8u.h"
#include "unpack_def.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {

typedef void (*unpackavx512func8)(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result);

typedef void (*unpackavx512func16)(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result);

typedef void (*unpackavx512func32)(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result);

static inline void unpack0(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_0u8u(inputBits, numValues, result);
}

static inline void unpack0(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_0u8u(inputBits, numValues, result);
}

static inline void unpack0(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_0u8u(inputBits, numValues, result);
}

static inline void unpack1(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_1u8u(inputBits, numValues, result);
  // uint64_t mask = kPdepMask8[1];
  // // __m256i masks = _mm256_set1_epi32(0x01010101);
  // while (numValues >= 64) {
  //   const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
  //   __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[0], _mm512_set1_epi8(0x01));

  //   __m256i out = _mm512_extracti64x4_epi64(tmp, 0);
  //   // __m512i out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   // out = _mm256_and_si256(out, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
  //   result += 32;

  //   out = _mm512_extracti64x4_epi64(tmp, 1);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
  //   // out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   result += 32;

  //   // tmp1 = _mm512_extracti32x4_epi32(tmp, 2);
  //   // out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   // result += 16;

  //   // tmp1 = _mm512_extracti32x4_epi32(tmp, 3);
  //   // out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   // result += 16;

  //   inputBits += 8;
  //   numValues -= 64;
  // }
  // uint64_t numBytes = (numValues * 1 + 7) / 8;
  // auto readEndOffset = inputBits + numBytes;
  // while (inputBits <= readEndOffset - 8) {
  //   // Using memcpy() here may result in non-optimized loops by clong.
  //   uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
  //   *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 1;
  //   result += 8;
  // }

  // // last batch of 8 values that is less than 8 bytes
  // uint64_t val = 0;
  // while (inputBits < readEndOffset) {
  //   std::memcpy(&val, inputBits, 1);

  //   *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 1;
  //   result += 8;
  // }
}

static inline void unpack1(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_1u8u(inputBits, numValues, result);
  // uint8_t bitWidth = 1;
  // uint64_t mask = kPdepMask8[1];
  // // __m256i masks = _mm256_set1_epi32(0x01010101);
  // while (numValues >= 64) {
  //   const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
  //   __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[0], _mm512_set1_epi8(0x01));

  //   __m128i tmp1 = _mm512_extracti32x4_epi32(tmp, 0);
  //   __m256i out = _mm256_cvtepu8_epi16(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   // out = _mm256_and_si256(out, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
  //   result += 16;

  //   tmp1 = _mm512_extracti32x4_epi32(tmp, 1);
  //   out = _mm256_cvtepu8_epi16(tmp1);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
  //   // out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   result += 16;

  //   tmp1 = _mm512_extracti32x4_epi32(tmp, 2);
  //   out = _mm256_cvtepu8_epi16(tmp1);
  //   // out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   result += 16;

  //   tmp1 = _mm512_extracti32x4_epi32(tmp, 3);
  //   out = _mm256_cvtepu8_epi16(tmp1);
  //   // out = _mm512_cvtepu8_epi32(tmp1);
  //   // _mm512_storeu_epi8(result, out);
  //   result += 16;

  //   inputBits += 8;
  //   numValues -= 64;
  // }
  // uint64_t numBytesPerTime = (bitWidth * 16 + 7) / 8;
  // uint64_t shift = bitWidth * 8;
  // uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  // alignas(16) uint64_t intermediateValues[2];
  // auto readEndOffset = inputBits + numBytes;
  // while (inputBits <= readEndOffset - 8) {
  //   uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

  //   intermediateValues[0] = _pdep_u64(val, mask);
  //   intermediateValues[1] = _pdep_u64(val >> shift, mask);
  //   __m256i out = _mm256_cvtepu8_epi16(
  //       _mm_load_si128(reinterpret_cast<const
  //       __m128i*>(intermediateValues)));
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

  //   inputBits += numBytesPerTime;
  //   result += 16;
  // }

  // // Finish the last batch which has < 8 bytes. Now Process 8 values a time.
  // uint64_t val = 0;
  // while (inputBits < readEndOffset) {
  //   std::memcpy(&val, inputBits, bitWidth);

  //   uint64_t intermediateValue = _pdep_u64(val, mask);
  //   __m256i out = _mm256_cvtepu8_epi16(_mm_loadl_epi64(
  //       (reinterpret_cast<const __m128i*>(&intermediateValue))));
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

  //   inputBits += bitWidth;
  //   result += 8;
  // }
}

static inline void unpack1(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_1u8u(inputBits, numValues, result);
  // uint64_t mask = kPdepMask8[1];
  // uint64_t numBytes = (numValues * 1 + 7) / 8;
  // auto readEndOffset = inputBits + numBytes;
  // __m256i masks = _mm256_set1_epi32(0x00000001);
  // // __m256i masks = _mm256_set1_epi32(0x07070707);
  // // __m512i masks = _mm512_set1_epi32(0x00000007);
  // // // // // __m512i shiftMask =
  // // // // //     _mm512_set_epi32(3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0,
  // 3,
  // // 0);
  // // __m512i shiftMask = _mm512_set_epi32(
  // //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi32(7, 6, 5, 4, 3, 2, 1, 0);

  // const uint64_t* in64_pos;
  // while (numValues >= 8) {
  //   in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
  //   __m256i data = _mm256_maskz_set1_epi64(0xFF, in64_pos[0]);
  //   __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
  //   out = _mm256_and_si256(out, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
  //   // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 1;
  //   result += 8;
  //   numValues -= 8;
  // }
  // // while (numValues >= 64) {
  // //   const uint64_t* in64_pos = reinterpret_cast<const
  // uint64_t*>(inputBits);
  // //   __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[0],
  // _mm512_set1_epi8(0x01));

  // //   __m128i tmp1 = _mm512_extracti32x4_epi32(tmp, 0);
  // //   __m512i out = _mm512_cvtepu8_epi32(tmp1);
  // //   _mm512_storeu_epi8(result, out);
  // //   result += 16;

  // //   tmp1 = _mm512_extracti32x4_epi32(tmp, 1);
  // //   out = _mm512_cvtepu8_epi32(tmp1);
  // //   _mm512_storeu_epi8(result, out);
  // //   result += 16;

  // //   tmp1 = _mm512_extracti32x4_epi32(tmp, 2);
  // //   out = _mm512_cvtepu8_epi32(tmp1);
  // //   _mm512_storeu_epi8(result, out);
  // //   result += 16;

  // //   tmp1 = _mm512_extracti32x4_epi32(tmp, 3);
  // //   out = _mm512_cvtepu8_epi32(tmp1);
  // //   _mm512_storeu_epi8(result, out);
  // //   result += 16;

  // //   numValues -= 64;
  // //   inputBits += 8;
  // // }

  // while (inputBits <= readEndOffset - 8) {
  //   // Using memcpy() here may result in non-optimized loops by clong.
  //   uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
  //   *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 1;
  //   result += 8;
  // }

  // // last batch of 8 values that is less than 8 bytes
  // uint64_t val = 0;
  // while (inputBits < readEndOffset) {
  //   std::memcpy(&val, inputBits, 1);

  //   *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 1;
  //   result += 8;
  // }
}

static inline void unpack2(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  // unpack_2u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[2];
  __m256i masks = _mm256_set1_epi32(0x03030303);
  __m256i shiftMask = _mm256_set_epi8(
      62,
      60,
      58,
      56,
      54,
      52,
      50,
      48,
      46,
      44,
      42,
      40,
      38,
      36,
      34,
      32,
      30,
      28,
      26,
      24,
      22,
      20,
      18,
      16,
      14,
      12,
      10,
      8,
      6,
      4,
      2,
      0);
  uint64_t numBytes = (numValues * 2 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  while (numValues >= 32) {
    // Using memcpy() here may result in non-optimized loops by clong.
    // __m512i val = _mm512_maskz_expandloadu_epi8(0x0707070707070707,
    //                                             inputBits);
    // __m256i val = _mm256_maskz_expandloadu_epi8(0x03030303, inputBits);
    const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m256i data = _mm256_maskz_set1_epi64(0xFF, in64_pos[0]);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
    out = _mm256_and_si256(out, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
    // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 8;
    result += 32;
    numValues -= 32;
  }

  // Process bitWidth bytes (8 values) a time. Note that for bitWidth 8, the
  // performance of direct memcpy is about the same as this solution.
  while (inputBits <= readEndOffset - 8) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 2;
    result += 8;
  }

  // last batch of 8 values that is less than 8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 2);

    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 2;
    result += 8;
  }
}

static inline void unpack2(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_2u8u(inputBits, numValues, result);
}

static inline void unpack2(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_2u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[2];
  __m512i shiftMask = _mm512_set_epi32(
      30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
  __m512i masks = _mm512_set1_epi32(0x00000003);
  // __m256i masks = _mm256_set1_epi32(0x03030303);
  // __m256i shiftMask = _mm256_set_epi8(
  //     14,
  //     12,
  //     10,
  //     8,
  //     6,
  //     4,
  //     2,
  //     0,
  //     14,
  //     12,
  //     10,
  //     8,
  //     6,
  //     4,
  //     2,
  //     0,
  //     14,
  //     12,
  //     10,
  //     8,
  //     6,
  //     4,
  //     2,
  //     0,
  //     14,
  //     12,
  //     10,
  //     8,
  //     6,
  //     4,
  //     2,
  //     0);
  uint64_t numBytes = (numValues * 2 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;
  const uint32_t* in32_pos;
  while (numValues >= 16) {
    in32_pos = reinterpret_cast<const uint32_t*>(inputBits);
    __m512i data = _mm512_set1_epi32(in32_pos[0]);
    __m512i cm = _mm512_multishift_epi64_epi8(shiftMask, data);
    cm = _mm512_and_epi32(cm, masks);
    _mm512_storeu_epi8(result, cm);
    result += 16;
    inputBits += 4;
    numValues -= 16;
  }

  // while (numValues >= 32) {
  //   // Using memcpy() here may result in non-optimized loops by clong.
  //   // __m512i val = _mm512_maskz_expandloadu_epi8(0x0707070707070707,
  //   //                                             inputBits);
  //   __m256i val = _mm256_maskz_expandloadu_epi8(0x03030303, inputBits);
  //   // __m512i val = _mm512_shuffle_epi8(data1, am);
  //   __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
  //   out = _mm256_and_si256(out, masks);

  //   __m512i out0 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 0));
  //   __m256i half = _mm512_extracti64x4_epi64(out0, 0);
  //   // half = _mm256_and_epi32(half, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   half = _mm512_extracti64x4_epi64(out0, 1);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   // _mm512_storeu_si512(result, out);
  //   // _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
  //   __m512i out1 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 1));
  //   half = _mm512_extracti64x4_epi64(out1, 0);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   half = _mm512_extracti64x4_epi64(out1, 1);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);
  //   inputBits += 8;
  //   result += 8;
  //   // result += 32;
  //   numValues -= 32;
  // }

  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 2;
    result += 8;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 2);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 2;
    result += 8;
  }
}

static inline void unpack3(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  // unpack_3u8u(inputBits, numValues, result);
  // uint64_t mask = kPdepMask8[3];
  // __m512i masks = _mm512_set1_epi32(0x07070707);
  uint64_t mask = kPdepMask8[3];
  __m256i masks = _mm256_set1_epi32(0x07070707);
  // __m512i shiftMask = _mm512_set_epi8(
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0);
  __m256i shiftMask = _mm256_set_epi8(
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0,
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0,
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0,
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0);
  uint64_t numBytes = (numValues * 3 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  // Process bitWidth bytes (8 values) a time. Note that for bitWidth 8, the
  // performance of direct memcpy is about the same as this solution.
  // while (numValues >= 64) {
  //   // Using memcpy() here may result in non-optimized loops by clong.
  //   __m512i val = _mm512_maskz_expandloadu_epi8(0x0707070707070707,
  //   inputBits);
  //   // uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
  //   // __m512i data = _mm512_set1_epi64(val);
  //   __m512i out = _mm512_multishift_epi64_epi8(shiftMask, val);
  //   out = _mm512_and_epi32(out, masks);
  //   _mm512_storeu_epi8(result, out);
  //   // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 3 * 8;
  //   result += 64;
  //   numValues -= 64;
  // }

  while (numValues >= 32) {
    // Using memcpy() here may result in non-optimized loops by clong.
    // __m512i val = _mm512_maskz_expandloadu_epi8(0x0707070707070707,
    //                                             inputBits);
    __m256i val = _mm256_maskz_expandloadu_epi8(0x07070707, inputBits);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
    out = _mm256_and_si256(out, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
    // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 12;
    result += 32;
    numValues -= 32;
  }

  // // last batch of multiple 8 values that is less than 24 bytes
  // // uint64_t value = 0;
  // // while (inputBits < readEndOffset) {
  // //   std::memcpy(&value, inputBits, 3);

  // //   __m512i val = _mm512_maskz_expandloadu_epi8(0x0707070707070707,
  // &value);
  // //   // uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
  // //   // __m512i data = _mm512_set1_epi64(val);
  // //   __m512i out = _mm512_multishift_epi64_epi8(shiftMask, val);
  // //   out = _mm512_and_epi32(out, mask);
  // //   _mm512_storeu_epi8(result, out);
  // //   // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  // //   inputBits += 3 * 8;
  // //   result += 64;
  // // }

  // Process bitWidth bytes (8 values) a time. Note that for bitWidth 8, the
  // performance of direct memcpy is about the same as this solution.
  while (inputBits <= readEndOffset - 8) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 3;
    result += 8;
  }

  // last batch of 8 values that is less than 8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 3);

    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 3;
    result += 8;
  }
}

static inline void unpack3(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  // unpack_3u8u(inputBits, numValues, result);
  uint8_t bitWidth = 3;
  // uint64_t pdepMask = kPdepMask8[bitWidth];
  uint64_t mask = kPdepMask8[3];
  // __m256i masks = _mm256_set1_epi32(0x00070007);
  __m256i masks = _mm256_set1_epi32(0x07070707);
  // __m512i masks = _mm512_set1_epi16(0x00070007);
  // __m512i shiftMask = _mm512_set_epi16(
  //   45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0,
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi16(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  __m256i shiftMask = _mm256_set_epi8(
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0,
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0,
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0,
      21,
      18,
      15,
      12,
      9,
      6,
      3,
      0);

  uint64_t numBytesPerTime = (bitWidth * 16 + 7) / 8;
  uint64_t shift = bitWidth * 8;
  uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  alignas(16) uint64_t intermediateValues[2];
  auto readEndOffset = inputBits + numBytes;

  while (numValues >= 32) {
    // const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m256i val = _mm256_maskz_expandloadu_epi8(0x07070707, inputBits);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
    out = _mm256_and_si256(out, masks);
    __m256i out0 = _mm256_cvtepu8_epi16(_mm256_extracti32x4_epi32(out, 0));
    // out0 = _mm256_and_epi32(out0, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
    __m256i out1 = _mm256_cvtepu8_epi16(_mm256_extracti32x4_epi32(out, 1));
    // out1 = _mm256_and_epi32(out1, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out1);
    inputBits += 12;
    result += 32;
    numValues -= 32;
  }

  // Process 2 * bitWidth bytes (16 values) a time.
  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    intermediateValues[0] = _pdep_u64(val, mask);
    intermediateValues[1] = _pdep_u64(val >> shift, mask);
    __m256i out = _mm256_cvtepu8_epi16(
        _mm_load_si128(reinterpret_cast<const __m128i*>(intermediateValues)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += numBytesPerTime;
    result += 16;
  }

  // Finish the last batch which has < 8 bytes. Now Process 8 values a time.
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, bitWidth);

    uint64_t intermediateValue = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi16(_mm_loadl_epi64(
        (reinterpret_cast<const __m128i*>(&intermediateValue))));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += bitWidth;
    result += 8;
  }

  // uint64_t numBytesPerTime = (bitWidth * 32 + 7) / 8;
  // // uint64_t shift1 = bitWidth * 8;
  // // uint64_t shift2 = bitWidth * 16;
  // // uint64_t shift3 = bitWidth * 24;
  // uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  // // alignas(32) uint64_t intermediateValues[4];
  // auto readEndOffset = inputBits + numBytes;
  // while (inputBits <= readEndOffset - 16) {
  //   // uint64_t val0 = reinterpret_cast<const uint64_t*>(inputBits)[0];
  //   // uint64_t val1 = reinterpret_cast<const uint64_t*>(inputBits)[1];

  //   // // 8 -> 512 (64 bit mask)
  //   // // 0f0f0f0f0f
  //   // // _mm512_maskz_compress_epi8

  //   // intermediateValues[0] = _pdep_u64(val0, pdepMask);
  //   // intermediateValues[1] = _pdep_u64(val0 >> shift1, pdepMask);

  //   // intermediateValues[2] = _pdep_u64(val1, pdepMask);
  //   // intermediateValues[3] = _pdep_u64(val1 >> shift1, pdepMask);
  //   // __m512i out = _mm512_cvtepu8_epi16(_mm256_load_si256(
  //   //     reinterpret_cast<const __m256i*>(intermediateValues)));
  //   // _mm512_storeu_si512(reinterpret_cast<__m512i*>(result), out);

  //   // Using memcpy() here may result in non-optimized loops by clong.
  //   uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
  //   __m512i data = _mm512_set1_epi64(val);
  //   __m512i cm = _mm512_multishift_epi64_epi8(shiftMask, data);
  //   cm = _mm512_and_epi16(cm, masks);
  //   _mm512_storeu_epi8(result, cm);
  //   // result = _mm256_cvtepu8_epi16(
  //   //     _mm_load_si128(reinterpret_cast<const
  //   //     __m128i*>(intermediateValues)));
  //   // _mm256_storeu_si256(reinterpret_cast<__m256i*>(outputBuffer), result);
  //   // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += bitWidth * 4;
  //   result += 32;
  // }
}

static inline void unpack3(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_3u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[3];
  // __m256i masks = _mm256_set1_epi32(0x00000007);
  // __m256i masks = _mm256_set1_epi32(0x07070707);
  __m512i masks = _mm512_set1_epi32(0x00000007);
  // // // // __m512i shiftMask =
  // // // //     _mm512_set_epi32(3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3,
  // 0);
  __m512i shiftMask = _mm512_set_epi32(
      45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi32(21, 18, 15, 12, 9, 6, 3, 0);
  // // __m256i shiftMask = _mm256_set_epi16(
  // //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi8(
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0,
  //     21,
  //     18,
  //     15,
  //     12,
  //     9,
  //     6,
  //     3,
  //     0);
  uint64_t numBytes = (numValues * 3 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;
  const uint64_t* in64_pos;
  while (numValues >= 16) {
    in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m512i data = _mm512_set1_epi64(in64_pos[0]);
    __m512i cm = _mm512_multishift_epi64_epi8(shiftMask, data);
    cm = _mm512_and_epi32(cm, masks);
    _mm512_storeu_epi8(result, cm);
    result += 16;
    inputBits += 6;
    numValues -= 16;
  }
  // while (numValues >= 32) {
  //   // const uint64_t* in64_pos = reinterpret_cast<const
  //   uint64_t*>(inputBits);
  //   __m256i val = _mm256_maskz_expandloadu_epi8(0x07070707, inputBits);
  //   // __m512i val = _mm512_shuffle_epi8(data1, am);
  //   __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
  //   out = _mm256_and_si256(out, masks);

  //   __m512i out0 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 0));
  //   __m256i half = _mm512_extracti64x4_epi64(out0, 0);
  //   // half = _mm256_and_epi32(half, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   half = _mm512_extracti64x4_epi64(out0, 1);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   // _mm512_storeu_si512(result, out);
  //   // _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
  //   __m512i out1 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 1));
  //   half = _mm512_extracti64x4_epi64(out1, 0);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   half = _mm512_extracti64x4_epi64(out1, 1);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);
  //   inputBits += 12;
  //   result += 8;
  //   // result += 32;
  //   numValues -= 32;
  // }

  // // // Process bitWidth * 8 bytes (64 values) a time.
  // while (numValues >= 8) {
  //   in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
  //   __m256i data = _mm256_maskz_set1_epi64(0xFF, in64_pos[0]);
  //   __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
  //   out = _mm256_and_si256(out, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);
  //   // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
  //   inputBits += 3;
  //   result += 8;
  //   numValues -= 8;
  // }
  // // uint64_t shift1 = 3 * 8;
  // // Process bitWidth bytes (16 values) a time.
  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 3;
    result += 8;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 3);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 3;
    result += 8;
  }
}

static inline void unpack4(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  // unpack_4u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[4];
  __m256i masks = _mm256_set1_epi32(0x0f0f0f0f);
  __m256i shiftMask = _mm256_set_epi8(
      60,
      56,
      52,
      48,
      44,
      40,
      36,
      32,
      28,
      24,
      20,
      16,
      12,
      8,
      4,
      0,
      60,
      56,
      52,
      48,
      44,
      40,
      36,
      32,
      28,
      24,
      20,
      16,
      12,
      8,
      4,
      0);
  uint64_t numBytes = (numValues * 4 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  while (numValues >= 32) {
    // Using memcpy() here may result in non-optimized loops by clong.
    // __m512i val = _mm512_maskz_expandloadu_epi8(0x0707070707070707,
    //                                             inputBits);
    // __m256i val = _mm256_maskz_expandloadu_epi8(0x0f0f0f0f, inputBits);
    const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m256i data = _mm256_maskz_set1_epi64(0xFF, in64_pos[0]);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
    out = _mm256_and_si256(out, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    in64_pos = reinterpret_cast<const uint64_t*>(inputBits + 8);
    data = _mm256_maskz_set1_epi64(0xFF, in64_pos[0]);
    out = _mm256_multishift_epi64_epi8(shiftMask, data);
    out = _mm256_and_si256(out, masks);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + 16), out);
    // *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 16;
    result += 32;
    numValues -= 32;
  }

  // Process bitWidth bytes (8 values) a time. Note that for bitWidth 8, the
  // performance of direct memcpy is about the same as this solution.
  while (inputBits <= readEndOffset - 8) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 4;
    result += 8;
  }

  // last batch of 8 values that is less than 8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 2);

    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 4;
    result += 8;
  }
}

static inline void unpack4(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_4u8u(inputBits, numValues, result);
}

static inline void unpack4(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_4u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[4];
  // __m256i masks = _mm256_set1_epi32(0x00000007);
  // __m256i masks = _mm256_set1_epi32(0x0f0f0f0f);
  // __m512i masks = _mm512_set1_epi32(0x00000007);
  // // // __m512i shiftMask =
  // // //     _mm512_set_epi32(3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0);
  // __m512i shiftMask = _mm512_set_epi32(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi32(21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi16(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi8(
  //     28,
  //     24,
  //     20,
  //     16,
  //     12,
  //     8,
  //     4,
  //     0,
  //     28,
  //     24,
  //     20,
  //     16,
  //     12,
  //     8,
  //     4,
  //     0,
  //     28,
  //     24,
  //     20,
  //     16,
  //     12,
  //     8,
  //     4,
  //     0,
  //     28,
  //     24,
  //     20,
  //     16,
  //     12,
  //     8,
  //     4,
  //     0);
  uint64_t numBytes = (numValues * 4 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;
  __m512i am = _mm512_set_epi32(
      60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0);
  __m512i masks = _mm512_set1_epi32(0x0000000f);
  // First unpack as many full batches as possible.
  while (numValues >= 16) {
    const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m512i data = _mm512_set1_epi64(in64_pos[0]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, masks);
    _mm512_storeu_epi8(result, cm);
    inputBits += 8;
    result += 16;
    numValues -= 16;
  }
  // while (numValues >= 32) {
  //   // const uint64_t* in64_pos = reinterpret_cast<const
  //   uint64_t*>(inputBits);
  //   __m256i val = _mm256_maskz_expandloadu_epi8(0x0f0f0f0f, inputBits);
  //   // __m512i val = _mm512_shuffle_epi8(data1, am);
  //   __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
  //   out = _mm256_and_si256(out, masks);

  //   __m512i out0 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 0));
  //   __m256i half = _mm512_extracti64x4_epi64(out0, 0);
  //   // half = _mm256_and_epi32(half, masks);
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   half = _mm512_extracti64x4_epi64(out0, 1);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   // _mm512_storeu_si512(result, out);
  //   // _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
  //   __m512i out1 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 1));
  //   half = _mm512_extracti64x4_epi64(out1, 0);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

  //   half = _mm512_extracti64x4_epi64(out1, 1);
  //   // half = _mm256_and_epi32(half, masks);
  //   result += 8;
  //   _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);
  //   inputBits += 16;
  //   result += 8;
  //   // result += 32;
  //   numValues -= 32;
  // }

  // Process bitWidth bytes (16 values) a time.
  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 4;
    result += 8;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 4);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 4;
    result += 8;
  }
}

static inline void unpack5(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  // unpack_5u8u(inputBits, numValues, result);
  uint8_t bitWidth = 5;
  uint64_t mask = kPdepMask8[bitWidth];

  uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  // __m256i masks = _mm256_set1_epi32(0x01ff01ff);
  // __m256i shuffleMask = _mm256_set_epi8(
  //     8,
  //     7,
  //     7,
  //     6,
  //     6,
  //     5,
  //     5,
  //     4,
  //     4,
  //     3,
  //     3,
  //     2,
  //     2,
  //     1,
  //     1,
  //     0,
  //     8,
  //     7,
  //     7,
  //     6,
  //     6,
  //     5,
  //     5,
  //     4,
  //     4,
  //     3,
  //     3,
  //     2,
  //     2,
  //     1,
  //     1,
  //     0);
  // __m256i shiftMask = _mm256_set_epi8(
  //     63,
  //     55,
  //     46,
  //     38,
  //     29,
  //     21,
  //     12,
  //     4,
  //     59,
  //     51,
  //     42,
  //     34,
  //     25,
  //     17,
  //     8,
  //     0,
  //     63,
  //     55,
  //     46,
  //     38,
  //     29,
  //     21,
  //     12,
  //     4,
  //     59,
  //     51,
  //     42,
  //     34,
  //     25,
  //     17,
  //     8,
  //     0);
  __m256i shiftMask = _mm256_set_epi8(
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0,
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0,
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0,
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0);
  __m256i masks = _mm256_set1_epi32(0x0000001f);
  while (numValues >= 32) {
    //   // const uint64_t* in64_pos = reinterpret_cast<const
    //   uint64_t*>(inputBits);
    __m256i data = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f, inputBits);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
    out = _mm256_and_si256(out, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 20;
    result += 32;
    numValues -= 32;
  }

  while (inputBits <= readEndOffset - 8) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 5;
    result += 8;
  }

  // last batch of 8 values that is less than 8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 5);

    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 5;
    result += 8;
  }
}

static inline void unpack5(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_5u8u(inputBits, numValues, result);
}

static inline void unpack5(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_5u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[5];
  // __m256i masks = _mm256_set1_epi32(0x00000007);
  __m256i masks = _mm256_set1_epi32(0x1f1f1f1f);
  // __m512i masks = _mm512_set1_epi32(0x00000007);
  // // // __m512i shiftMask =
  // // //     _mm512_set_epi32(3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0);
  // __m512i shiftMask = _mm512_set_epi32(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi32(21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi16(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  __m256i shiftMask = _mm256_set_epi8(
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0,
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0,
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0,
      35,
      30,
      25,
      20,
      15,
      10,
      5,
      0);
  uint64_t numBytes = (numValues * 5 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;
  while (numValues >= 32) {
    const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m256i val = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f, inputBits);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
    out = _mm256_and_si256(out, masks);

    __m512i out0 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 0));
    __m256i half = _mm512_extracti64x4_epi64(out0, 0);
    // half = _mm256_and_epi32(half, masks);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    half = _mm512_extracti64x4_epi64(out0, 1);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    // _mm512_storeu_si512(result, out);
    // _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
    __m512i out1 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 1));
    half = _mm512_extracti64x4_epi64(out1, 0);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    half = _mm512_extracti64x4_epi64(out1, 1);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);
    inputBits += 20;
    result += 8;
    // result += 32;
    numValues -= 32;
  }
  // __m512i am = _mm512_set_epi8(
  //     8,
  //     5,
  //     8,
  //     5,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     8,
  //     5,
  //     8,
  //     5,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     8,
  //     5,
  //     8,
  //     5,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     8,
  //     5,
  //     8,
  //     5,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0);
  // __m512i cm = _mm512_set_epi8(
  //     59,
  //     59,
  //     38,
  //     38,
  //     17,
  //     17,
  //     4,
  //     4,
  //     55,
  //     55,
  //     34,
  //     34,
  //     21,
  //     21,
  //     0,
  //     0,
  //     59,
  //     59,
  //     38,
  //     38,
  //     17,
  //     17,
  //     4,
  //     4,
  //     55,
  //     55,
  //     34,
  //     34,
  //     21,
  //     21,
  //     0,
  //     0,
  //     59,
  //     59,
  //     38,
  //     38,
  //     17,
  //     17,
  //     4,
  //     4,
  //     55,
  //     55,
  //     34,
  //     34,
  //     21,
  //     21,
  //     0,
  //     0,
  //     59,
  //     59,
  //     38,
  //     38,
  //     17,
  //     17,
  //     4,
  //     4,
  //     55,
  //     55,
  //     34,
  //     34,
  //     21,
  //     21,
  //     0,
  //     0);
  // __m512i masks = _mm512_set1_epi32(0x0000001f);
  // // First unpack as many full batches as possible.
  // while (numValues >= 32) {
  //   //__m256i data = _mm256_loadu_epi8(in_pos);
  //   // data = _mm256_maskz_expand_epi8(0x1f1f1f1f,data);
  //   __m256i data = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f, inputBits);
  //   __m512i data1 = _mm512_cvtepu16_epi32(data);
  //   __m512i bm = _mm512_shuffle_epi8(data1, am);
  //   __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
  //   __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
  //   _mm512_storeu_epi8(result, _mm512_and_epi32(out1, masks));
  //   __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
  //   _mm512_storeu_epi8(result + 16, _mm512_and_epi32(out2, masks));
  //   inputBits += 20;
  //   numValues -= 32;
  //   result += 32;
  // }

  // Process bitWidth bytes (16 values) a time.
  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 5;
    result += 8;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 5);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 5;
    result += 8;
  }
}

static inline void unpack6(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  // unpack_6u8u(inputBits, numValues, result);
  uint8_t bitWidth = 6;
  uint64_t mask = kPdepMask8[bitWidth];

  uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  __m256i shiftMask = _mm256_set_epi8(
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0,
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0,
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0,
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0);
  __m256i masks = _mm256_set1_epi32(0x0000003f);
  while (numValues >= 32) {
    //   // const uint64_t* in64_pos = reinterpret_cast<const
    //   uint64_t*>(inputBits);
    __m256i data = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f, inputBits);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
    out = _mm256_and_si256(out, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 24;
    result += 32;
    numValues -= 32;
  }

  while (inputBits <= readEndOffset - 8) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 6;
    result += 8;
  }

  // last batch of 8 values that is less than 8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 6);

    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 6;
    result += 8;
  }
}

static inline void unpack6(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_6u8u(inputBits, numValues, result);
}

static inline void unpack6(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_6u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[6];
  // __m256i masks = _mm256_set1_epi32(0x00000007);
  __m256i masks = _mm256_set1_epi32(0x3f3f3f3f);
  // __m512i masks = _mm512_set1_epi32(0x00000007);
  // // // __m512i shiftMask =
  // // //     _mm512_set_epi32(3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0);
  // __m512i shiftMask = _mm512_set_epi32(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi32(21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi16(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  __m256i shiftMask = _mm256_set_epi8(
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0,
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0,
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0,
      42,
      36,
      30,
      24,
      18,
      12,
      6,
      0);
  uint64_t numBytes = (numValues * 6 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;
  while (numValues >= 32) {
    const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m256i val = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f, inputBits);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
    out = _mm256_and_si256(out, masks);

    __m512i out0 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 0));
    __m256i half = _mm512_extracti64x4_epi64(out0, 0);
    // half = _mm256_and_epi32(half, masks);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    half = _mm512_extracti64x4_epi64(out0, 1);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    // _mm512_storeu_si512(result, out);
    // _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
    __m512i out1 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 1));
    half = _mm512_extracti64x4_epi64(out1, 0);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    half = _mm512_extracti64x4_epi64(out1, 1);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);
    inputBits += 24;
    result += 8;
    // result += 32;
    numValues -= 32;
  }
  // __m512i am = _mm512_set_epi8(
  //     9,
  //     8,
  //     9,
  //     8,
  //     8,
  //     5,
  //     8,
  //     5,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     9,
  //     8,
  //     9,
  //     8,
  //     8,
  //     5,
  //     8,
  //     5,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     9,
  //     8,
  //     9,
  //     8,
  //     8,
  //     5,
  //     8,
  //     5,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     9,
  //     8,
  //     9,
  //     8,
  //     8,
  //     5,
  //     8,
  //     5,
  //     4,
  //     1,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0);
  // __m512i cm = _mm512_set_epi8(
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0,
  //     58,
  //     58,
  //     36,
  //     36,
  //     22,
  //     22,
  //     0,
  //     0);
  // __m512i masks = _mm512_set1_epi32(0x0000003f);
  // // First unpack as many full batches as possible.
  // while (numValues >= 32) {
  //   __m256i data = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f, inputBits);
  //   __m512i data1 = _mm512_cvtepu16_epi32(data);
  //   __m512i bm = _mm512_shuffle_epi8(data1, am);
  //   __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
  //   __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
  //   _mm512_storeu_epi8(result, _mm512_and_epi32(out1, masks));
  //   __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
  //   _mm512_storeu_epi8(result + 16, _mm512_and_epi32(out2, masks));
  //   inputBits += 24;
  //   result += 32;
  //   numValues -= 32;
  // }

  // Process bitWidth bytes (16 values) a time.
  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 6;
    result += 8;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 6);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 6;
    result += 8;
  }
}

static inline void unpack7(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  // unpack_7u8u(inputBits, numValues, result);
  uint8_t bitWidth = 7;
  uint64_t mask = kPdepMask8[bitWidth];

  uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  __m256i shiftMask = _mm256_set_epi8(
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0,
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0,
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0,
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0);
  __m256i masks = _mm256_set1_epi32(0x0000007f);
  while (numValues >= 32) {
    //   // const uint64_t* in64_pos = reinterpret_cast<const
    //   uint64_t*>(inputBits);
    __m256i data = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f, inputBits);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, data);
    out = _mm256_and_si256(out, masks);
    // _mm512_storeu_si512(result, out);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 28;
    result += 32;
    numValues -= 32;
  }

  while (inputBits <= readEndOffset - 8) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 7;
    result += 8;
  }

  // last batch of 8 values that is less than 8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 7);

    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += 7;
    result += 8;
  }
}

static inline void unpack7(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_7u8u(inputBits, numValues, result);
}

static inline void unpack7(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_7u8u(inputBits, numValues, result);
  uint64_t mask = kPdepMask8[7];
  // __m256i masks = _mm256_set1_epi32(0x00000007);
  __m256i masks = _mm256_set1_epi32(0x7f7f7f7f);
  // __m512i masks = _mm512_set1_epi32(0x00000007);
  // // // __m512i shiftMask =
  // // //     _mm512_set_epi32(3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0, 3, 0);
  // __m512i shiftMask = _mm512_set_epi32(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi32(21, 18, 15, 12, 9, 6, 3, 0);
  // __m256i shiftMask = _mm256_set_epi16(
  //     45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  __m256i shiftMask = _mm256_set_epi8(
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0,
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0,
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0,
      49,
      42,
      35,
      28,
      21,
      14,
      7,
      0);
  uint64_t numBytes = (numValues * 7 + 7) / 8;
  auto readEndOffset = inputBits + numBytes;
  while (numValues >= 32) {
    const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(inputBits);
    __m256i val = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f, inputBits);
    // __m512i val = _mm512_shuffle_epi8(data1, am);
    __m256i out = _mm256_multishift_epi64_epi8(shiftMask, val);
    out = _mm256_and_si256(out, masks);

    __m512i out0 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 0));
    __m256i half = _mm512_extracti64x4_epi64(out0, 0);
    // half = _mm256_and_epi32(half, masks);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    half = _mm512_extracti64x4_epi64(out0, 1);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    // _mm512_storeu_si512(result, out);
    // _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out0);
    __m512i out1 = _mm512_cvtepu8_epi32(_mm256_extracti32x4_epi32(out, 1));
    half = _mm512_extracti64x4_epi64(out1, 0);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);

    half = _mm512_extracti64x4_epi64(out1, 1);
    // half = _mm256_and_epi32(half, masks);
    result += 8;
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), half);
    inputBits += 28;
    result += 8;
    // result += 32;
    numValues -= 32;
  }
  // __m512i am = _mm512_set_epi8(
  //     12,
  //     9,
  //     12,
  //     9,
  //     9,
  //     8,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     12,
  //     9,
  //     12,
  //     9,
  //     9,
  //     8,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     12,
  //     9,
  //     12,
  //     9,
  //     9,
  //     8,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0,
  //     12,
  //     9,
  //     12,
  //     9,
  //     9,
  //     8,
  //     8,
  //     5,
  //     5,
  //     4,
  //     4,
  //     1,
  //     1,
  //     0,
  //     1,
  //     0);
  // __m512i cm = _mm512_set_epi8(
  //     57,
  //     57,
  //     34,
  //     34,
  //     19,
  //     19,
  //     4,
  //     4,
  //     53,
  //     53,
  //     38,
  //     38,
  //     23,
  //     23,
  //     0,
  //     0,
  //     57,
  //     57,
  //     34,
  //     34,
  //     19,
  //     19,
  //     4,
  //     4,
  //     53,
  //     53,
  //     38,
  //     38,
  //     23,
  //     23,
  //     0,
  //     0,
  //     57,
  //     57,
  //     34,
  //     34,
  //     19,
  //     19,
  //     4,
  //     4,
  //     53,
  //     53,
  //     38,
  //     38,
  //     23,
  //     23,
  //     0,
  //     0,
  //     57,
  //     57,
  //     34,
  //     34,
  //     19,
  //     19,
  //     4,
  //     4,
  //     53,
  //     53,
  //     38,
  //     38,
  //     23,
  //     23,
  //     0,
  //     0);
  // __m512i masks = _mm512_set1_epi32(0x0000007f);
  // // First unpack as many full batches as possible.
  // while (numValues >= 32) {
  //   __m256i data = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f, inputBits);
  //   __m512i data1 = _mm512_cvtepu16_epi32(data);
  //   __m512i bm = _mm512_shuffle_epi8(data1, am);
  //   __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
  //   __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
  //   _mm512_storeu_epi8(result, _mm512_and_epi32(out1, masks));
  //   __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
  //   _mm512_storeu_epi8(result + 16, _mm512_and_epi32(out2, masks));
  //   result += 32;
  //   inputBits += 28;
  //   numValues -= 32;
  // }

  // Process bitWidth bytes (16 values) a time.
  while (inputBits <= readEndOffset - 8) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 7;
    result += 8;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBits < readEndOffset) {
    std::memcpy(&val, inputBits, 7);

    uint64_t intermediateVal = _pdep_u64(val, mask);
    __m256i out = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += 7;
    result += 8;
  }
}

static inline void unpack8(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_8u8u(inputBits, numValues, result);
}

static inline void unpack8(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_8u8u(inputBits, numValues, result);
}

static inline void unpack8u(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_8u8u(inputBits, numValues, result);
}

static inline void unpack9(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_9u16u(inputBits, numValues, result);
}

static inline void unpack9(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  // unpack_9u16u(inputBits, numValues, result);
  uint8_t bitWidth = 9;
  uint64_t pdepMask = kPdepMask16[bitWidth];

  uint8_t shift1 = bitWidth * 4;
  uint8_t shift2 = 64 - shift1;

  alignas(16) uint64_t values[2] = {0, 0};
  alignas(16) uint64_t intermediateValues[2];

  uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  auto readEndOffset = inputBits + numBytes;

  // __m256i masks = _mm256_set1_epi32(0x01ff01ff);
  // __m256i shuffleMask = _mm256_set_epi8(
  //     8,
  //     7,
  //     7,
  //     6,
  //     6,
  //     5,
  //     5,
  //     4,
  //     4,
  //     3,
  //     3,
  //     2,
  //     2,
  //     1,
  //     1,
  //     0,
  //     8,
  //     7,
  //     7,
  //     6,
  //     6,
  //     5,
  //     5,
  //     4,
  //     4,
  //     3,
  //     3,
  //     2,
  //     2,
  //     1,
  //     1,
  //     0);
  // __m256i shiftMask = _mm256_set_epi8(
  //     63,
  //     55,
  //     46,
  //     38,
  //     29,
  //     21,
  //     12,
  //     4,
  //     59,
  //     51,
  //     42,
  //     34,
  //     25,
  //     17,
  //     8,
  //     0,
  //     63,
  //     55,
  //     46,
  //     38,
  //     29,
  //     21,
  //     12,
  //     4,
  //     59,
  //     51,
  //     42,
  //     34,
  //     25,
  //     17,
  //     8,
  //     0);
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
  while (numValues >= 32) {
    //   // const uint64_t* in64_pos = reinterpret_cast<const
    //   uint64_t*>(inputBits);
    __m512i data = _mm512_maskz_expandloadu_epi8(0x01ff01ff01ff01ff, inputBits);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m256i out1 = _mm256_cvtepu16_epi32(_mm512_extracti32x4_epi32(dm, 0));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out1);

    out1 = _mm256_cvtepu16_epi32(_mm512_extracti32x4_epi32(dm, 1));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + 8), out1);

    out1 = _mm256_cvtepu16_epi32(_mm512_extracti32x4_epi32(dm, 2));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + 16), out1);

    out1 = _mm256_cvtepu16_epi32(_mm512_extracti32x4_epi32(dm, 3));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + 24), out1);

    // _mm512_storeu_epi8(result, _mm512_and_epi32(out1, mask));
    // __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    // _mm512_storeu_epi8(result + 16, _mm512_and_epi32(out2, mask));
    //   __m256i val = _mm256_maskz_expandloadu_epi8(0x01ff01ff, inputBits);
    //   __m256i shuffle = _mm256_shuffle_epi8(val, shuffleMask);
    //   // __m512i val = _mm512_shuffle_epi8(data1, am);
    //   __m256i out = _mm256_multishift_epi64_epi8(shiftMask, shuffle);
    //   out = _mm256_and_epi32(out, masks);

    inputBits += 36;
    result += 32;
    numValues -= 32;
  }

  // Process bitWidth bytes (8 values) a time.
  while (inputBits < readEndOffset) {
    std::memcpy(values, inputBits, bitWidth);

    intermediateValues[0] = _pdep_u64(values[0], pdepMask);
    intermediateValues[1] =
        _pdep_u64((values[0] >> shift1) | (values[1] << shift2), pdepMask);

    __m256i out = _mm256_cvtepu16_epi32(
        _mm_load_si128(reinterpret_cast<const __m128i*>(intermediateValues)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result), out);

    inputBits += bitWidth;
    result += 8;
  }
}

static inline void unpack10(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_10u16u(inputBits, numValues, result);
}

static inline void unpack10(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_10u16u(inputBits, numValues, result);
}

static inline void unpack11(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_11u16u(inputBits, numValues, result);
}

static inline void unpack11(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_11u16u(inputBits, numValues, result);
}

static inline void unpack12(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_12u16u(inputBits, numValues, result);
}

static inline void unpack12(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_12u16u(inputBits, numValues, result);
}

static inline void unpack13(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_13u16u(inputBits, numValues, result);
}

static inline void unpack13(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_13u16u(inputBits, numValues, result);
}

static inline void unpack14(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_14u16u(inputBits, numValues, result);
}

static inline void unpack14(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_14u16u(inputBits, numValues, result);
}

static inline void unpack15(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_15u16u(inputBits, numValues, result);
}

static inline void unpack15(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_15u16u(inputBits, numValues, result);
}

static inline void unpack16u(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_16u16u(inputBits, numValues, result);
}

static inline void unpack16u(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_16u16u(inputBits, numValues, result);
}

static inline void unpack17(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_17u32u(inputBits, numValues, result);
}

static inline void unpack18(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_18u32u(inputBits, numValues, result);
}

static inline void unpack19(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_19u32u(inputBits, numValues, result);
}

static inline void unpack20(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_20u32u(inputBits, numValues, result);
}

static inline void unpack21(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_21u32u(inputBits, numValues, result);
}

static inline void unpack22(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_22u32u(inputBits, numValues, result);
}

static inline void unpack23(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_23u32u(inputBits, numValues, result);
}

static inline void unpack24(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_24u32u(inputBits, numValues, result);
}

static inline void unpack25(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_25u32u(inputBits, numValues, result);
}

static inline void unpack26(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_26u32u(inputBits, numValues, result);
}

static inline void unpack27(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_27u32u(inputBits, numValues, result);
}

static inline void unpack28(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_28u32u(inputBits, numValues, result);
}

static inline void unpack29(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_29u32u(inputBits, numValues, result);
}

static inline void unpack30(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_30u32u(inputBits, numValues, result);
}

static inline void unpack31(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_31u32u(inputBits, numValues, result);
}

static inline void unpack32u(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_32u32u(inputBits, numValues, result);
}

static inline void unpack512(
    uint8_t bitWidth,
    const uint8_t* inputBuffer,
    uint64_t numValues,
    uint32_t* outputBuffer) {
  uint64_t pdepMask = kPdepMask8[bitWidth];

  uint64_t numBytesPerTime = (bitWidth * 16 + 7) / 8;
  uint64_t prefetchLine = 64 / numBytesPerTime;
  // 2, 4, 6, 8, 10, 12, 14
  uint64_t shift = bitWidth * 8;
  uint64_t numBytes = (numValues * bitWidth + 7) / 8;
  alignas(64) uint64_t intermediateValues[2];
  auto readEndOffset = inputBuffer + numBytes;

  // Process 2 * bitWidth bytes (16 values) a time.
  uint64_t i = 0;
  while (inputBuffer <= readEndOffset - 16) {
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBuffer);

    intermediateValues[0] = _pdep_u64(val, pdepMask);
    intermediateValues[1] = _pdep_u64(val >> shift, pdepMask);
    __m128i input =
        _mm_load_si128(reinterpret_cast<const __m128i*>(intermediateValues));
    if (!(i % prefetchLine)) {
      __builtin_prefetch(inputBuffer + 64);
    }
    __m512i result = _mm512_cvtepu8_epi32(input);
    _mm512_storeu_si512(outputBuffer, result);

    inputBuffer += numBytesPerTime;
    outputBuffer += 16;
    i++;
  }

  // last <8 bytes
  uint64_t val = 0;
  while (inputBuffer < readEndOffset) {
    std::memcpy(&val, inputBuffer, bitWidth);

    uint64_t intermediateVal = _pdep_u64(val, pdepMask);
    __m256i result = _mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(outputBuffer), result);

    inputBuffer += bitWidth;
    outputBuffer += 8;
  }
}

static unpackavx512func8 unpackFuncList8[] = {
    &unpack0,
    &unpack1,
    &unpack2,
    &unpack3,
    &unpack4,
    &unpack5,
    &unpack6,
    &unpack7,
    &unpack8};

static unpackavx512func16 unpackFuncList16[] = {
    &unpack0,
    &unpack1,
    &unpack2,
    &unpack3,
    &unpack4,
    &unpack5,
    &unpack6,
    &unpack7,
    &unpack8,
    &unpack9,
    &unpack10,
    &unpack11,
    &unpack12,
    &unpack13,
    &unpack14,
    &unpack15,
    &unpack16u};

static unpackavx512func32 unpackFuncList32[] = {
    &unpack0,  &unpack1,  &unpack2,  &unpack3,  &unpack4,   &unpack5,
    &unpack6,  &unpack7,  &unpack8u, &unpack9,  &unpack10,  &unpack11,
    &unpack12, &unpack13, &unpack14, &unpack15, &unpack16u, &unpack17,
    &unpack18, &unpack19, &unpack20, &unpack21, &unpack22,  &unpack23,
    &unpack24, &unpack25, &unpack26, &unpack27, &unpack28,  &unpack29,
    &unpack30, &unpack31, &unpack32u};

template <typename T>
inline void unpackAVX512_new(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    T* FOLLY_NONNULL& result) {
  unpackNaive<T>(inputBits, inputBufferLen, numValues, bitWidth, result);
}

template <>
inline void unpackAVX512_new<uint8_t>(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint8_t* FOLLY_NONNULL& result);

template <>
inline void unpackAVX512_new<uint16_t>(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint16_t* FOLLY_NONNULL& result);

template <>
inline void unpackAVX512_new<uint32_t>(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint32_t* FOLLY_NONNULL& result);

template <>
inline void unpackAVX512_new<uint8_t>(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint8_t* FOLLY_NONNULL& result) {
  VELOX_CHECK(bitWidth >= 1 && bitWidth <= 8);
  VELOX_CHECK((numValues & 0x7) == 0);
  VELOX_CHECK(inputBufferLen * 8 >= bitWidth * numValues);

  unpackavx512func8 unpackavx512 = unpackFuncList8[bitWidth];
  unpackavx512(inputBits, numValues, result);
}

template <>
inline void unpackAVX512_new<uint16_t>(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint16_t* FOLLY_NONNULL& result) {
  VELOX_CHECK(bitWidth >= 1 && bitWidth <= 16);
  VELOX_CHECK((numValues & 0x7) == 0);
  VELOX_CHECK(inputBufferLen * 8 >= bitWidth * numValues);

  unpackavx512func16 unpackavx512 = unpackFuncList16[bitWidth];
  unpackavx512(inputBits, numValues, result);
}

template <>
inline void unpackAVX512_new<uint32_t>(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint32_t* FOLLY_NONNULL& result) {
  VELOX_CHECK(bitWidth >= 1 && bitWidth <= 32);
  VELOX_CHECK((numValues & 0x7) == 0);
  VELOX_CHECK(inputBufferLen * 8 >= bitWidth * numValues);

  // unpackavx512func32 unpackavx512 = unpackFuncList32[bitWidth];
  // unpackavx512(inputBits, numValues, result);
  unpack512(bitWidth, inputBits, numValues, result);
}

} // namespace facebook::velox::dwio::common