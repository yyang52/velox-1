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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/BitPackDecoderUtil.h"
#ifdef VELOX_ENABLE_AVX512
#include "velox/dwio/common/BitPackDecoderUtilAVX512.h"
#endif

namespace facebook::velox::dwio::common {

using RowSet = folly::Range<const facebook::velox::vector_size_t*>;

/// Copies bit fields starting at 'bitOffset'th bit of 'bits' into
/// 'result'.  The indices of the fields are in 'rows' and their
/// bit-width is 'bitWidth'.  'rowBias' is subtracted from each
/// index in 'rows' before calculating the bit field's position. The
/// bit fields are considered little endian. 'bufferEnd' is the address of the
/// first undefined byte after the buffer containing the bits. If non-null,
/// extra-wide memory accesses will not be used at thee end of the range to
/// stay under 'bufferEnd'.
template <typename T>
void unpack(
    const uint64_t* FOLLY_NULLABLE bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* FOLLY_NULLABLE bufferEnd,
    T* FOLLY_NONNULL result);

/// Unpack numValues number of input values from inputBuffer. The results
/// will be written to result. numValues must be a multiple of 8. The
/// caller needs to make sure the inputBufferLen contains at least numValues
/// number of packed values. The inputBits and result pointers will be updated
/// to the next to read position after this call.
template <typename T>
void unpack(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    T* FOLLY_NONNULL& result) {
  unpackNaive<T>(inputBits, inputBufferLen, numValues, bitWidth, result);
}

#ifdef VELOX_ENABLE_AVX512
/// Unpack numValues number of input values from inputBuffer. The results
/// will be written to result. The
/// caller needs to make sure the inputBufferLen contains at least numValues
/// number of packed values. The inputBits and result pointers will be updated
/// to the next to read position after this call.
template <typename T>
void unpackAVX512(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    T* FOLLY_NONNULL& result);
#endif

/// Unpack numValues number of input values from inputBuffer. The results
/// will be written to result. The
/// caller needs to make sure the inputBufferLen contains at least numValues
/// number of packed values. The inputBits and result pointers will be updated
/// to the next to read position after this call.
template <typename T>
void unpackAVX2(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    T* FOLLY_NONNULL& result);

// Loads a bit field from 'ptr' + bitOffset for up to 'bitWidth' bits. makes
// sure not to access bytes past lastSafeWord + 7. The definition is put here
// because it's inlined.
inline uint64_t safeLoadBits(
    const char* FOLLY_NONNULL ptr,
    int32_t bitOffset,
    uint8_t bitWidth,
    const char* FOLLY_NONNULL lastSafeWord) {
  VELOX_DCHECK_GE(7, bitOffset);
  VELOX_DCHECK_GE(56, bitWidth);
  if (ptr < lastSafeWord) {
    return *reinterpret_cast<const uint64_t*>(ptr) >> bitOffset;
  }
  int32_t byteWidth =
      facebook::velox::bits::roundUp(bitOffset + bitWidth, 8) / 8;
  return facebook::velox::bits::loadPartialWord(
             reinterpret_cast<const uint8_t*>(ptr), byteWidth) >>
      bitOffset;
}

} // namespace facebook::velox::dwio::common
