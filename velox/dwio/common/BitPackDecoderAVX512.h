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

#include "extend_16u.h"
#include "extend_32u.h"
#include "extend_8u.h"
#include "unpack_16u.h"
#include "unpack_32u.h"
#include "unpack_8u.h"
#include "unpack_def.h"
#include "velox/common/base/Exceptions.h"

#include <folly/Range.h>

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
}

static inline void unpack1(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_1u8u(inputBits, numValues, result);
}

static inline void unpack1(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_1u8u(inputBits, numValues, result);
}

static inline void unpack2(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_2u8u(inputBits, numValues, result);
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
  unpack_2u8u(inputBits, numValues, result);
}

static inline void unpack3(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_3u8u(inputBits, numValues, result);
}

static inline void unpack3(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint16_t* FOLLY_NONNULL& result) {
  unpack_3u8u(inputBits, numValues, result);
}

static inline void unpack3(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint32_t* FOLLY_NONNULL& result) {
  unpack_3u8u(inputBits, numValues, result);
}

static inline void unpack4(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_4u8u(inputBits, numValues, result);
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
  unpack_4u8u(inputBits, numValues, result);
}

static inline void unpack5(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_5u8u(inputBits, numValues, result);
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
  unpack_5u8u(inputBits, numValues, result);
}

static inline void unpack6(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_6u8u(inputBits, numValues, result);
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
  unpack_6u8u(inputBits, numValues, result);
}

static inline void unpack7(
    const uint8_t* FOLLY_NONNULL& inputBits,
    uint64_t numValues,
    uint8_t* FOLLY_NONNULL& result) {
  unpack_7u8u(inputBits, numValues, result);
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
  unpack_7u8u(inputBits, numValues, result);
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
  unpack_9u16u(inputBits, numValues, result);
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

  unpackavx512func32 unpackavx512 = unpackFuncList32[bitWidth];
  unpackavx512(inputBits, numValues, result);
}

} // namespace facebook::velox::dwio::common