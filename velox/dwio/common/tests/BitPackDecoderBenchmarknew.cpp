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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/BitPackDecoder.h"
#include "velox/dwio/common/BitPackDecoderAVX512.h"
#include "velox/dwio/common/tests/Lemire/bmipacking32.h"
#include "velox/vector/TypeAliases.h"

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>
#include <fstream>
#include <iostream>

using namespace folly;
using namespace facebook::velox;

static const uint64_t kNumValues = 1024768 * 8;

// Array of bit packed representations of randomInts_u32. The array at index i
// is packed i bits wide and the values come from the low bits of
std::vector<std::vector<uint64_t>> bitPackedData;

uint8_t result8[kNumValues];
uint16_t result16[kNumValues];
uint32_t result32[kNumValues];

static size_t len_u32 = 0;
std::vector<uint32_t> randomInts_u32;
std::vector<uint64_t> randomInts_u32_result;

#define BYTES(numValues, bitWidth) (numValues * bitWidth + 7) / 8

template <typename T>
void unpackAVX512_new(uint8_t bitWidth, T* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());
  facebook::velox::dwio::common::unpackAVX512_new<T>(
      inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
}

template <typename T>
void veloxBitUnpack(uint8_t bitWidth, T* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());
  facebook::velox::dwio::common::unpack<T>(
      inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
}

template <typename T>
void unpackAVX512_new(uint8_t bitWidth, T* result, size_t iter) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());
  for (auto i = 0; i < iter; i++) {
    facebook::velox::dwio::common::unpackAVX512_new<T>(
        inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
  }
}

template <typename T>
void veloxBitUnpack(uint8_t bitWidth, T* result, size_t iter) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());
  for (auto i = 0; i < iter; i++) {
    facebook::velox::dwio::common::unpack<T>(
        inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
  }
}

void lemirebmi2(uint8_t bitWidth, uint32_t* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());

  bmiunpack32(inputIter, kNumValues, bitWidth, result);
}

#define BENCHMARK_UNPACK_FULLROWS_CASE_8(width)           \
  BENCHMARK(avx512_new_unpack_fullrows_##width##_8) {     \
    unpackAVX512_new<uint8_t>(width, result8);            \
  }                                                       \
  BENCHMARK_RELATIVE(velox_unpack_fullrows_##width##_8) { \
    veloxBitUnpack<uint8_t>(width, result8);              \
  }                                                       \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(width)                \
  BENCHMARK(velox_unpack_fullrows_switch_##width##_8) {               \
    veloxBitUnpack<uint8_t>(width, result8);                          \
  }                                                                   \
  BENCHMARK_RELATIVE(avx512_new_unpack_fullrows_switch_##width##_8) { \
    unpackAVX512_new<uint8_t>(width, result8);                        \
  }                                                                   \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_CASE_16(width)           \
  BENCHMARK(avx512_new_unpack_fullrows_##width##_16) {     \
    unpackAVX512_new<uint16_t>(width, result16);           \
  }                                                        \
  BENCHMARK_RELATIVE(velox_unpack_fullrows_##width##_16) { \
    veloxBitUnpack<uint16_t>(width, result16);             \
  }                                                        \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(width)                \
  BENCHMARK(velox_unpack_fullrows_switch_##width##_16) {               \
    veloxBitUnpack<uint16_t>(width, result16);                         \
  }                                                                    \
  BENCHMARK_RELATIVE(avx512_new_unpack_fullrows_switch_##width##_16) { \
    unpackAVX512_new<uint16_t>(width, result16);                       \
  }                                                                    \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_CASE_32(width)           \
  BENCHMARK(avx512_new_unpack_fullrows_##width##_32) {     \
    unpackAVX512_new<uint32_t>(width, result32);           \
  }                                                        \
  BENCHMARK_RELATIVE(velox_unpack_fullrows_##width##_32) { \
    veloxBitUnpack<uint32_t>(width, result32);             \
  }                                                        \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(width)                \
  BENCHMARK(velox_unpack_fullrows_switch_##width##_32) {               \
    veloxBitUnpack<uint32_t>(width, result32);                         \
  }                                                                    \
  BENCHMARK_RELATIVE(avx512_new_unpack_fullrows_switch_##width##_32) { \
    unpackAVX512_new<uint32_t>(width, result32);                       \
  }                                                                    \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_AVX512_CASE_8(width, iter)            \
  BENCHMARK(avx512_new_unpack_fullrows_##width##_##iter##_8) { \
    unpackAVX512_new<uint8_t>(width, result8, iter);           \
  }                                                            \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_VELOX_CASE_8(width, iter)        \
  BENCHMARK(velox_unpack_fullrows_##width##_##iter##_8) { \
    veloxBitUnpack<uint8_t>(width, result8, iter);        \
  }                                                       \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_AVX512_CASE_16(width, iter)            \
  BENCHMARK(avx512_new_unpack_fullrows_##width##_##iter##_16) { \
    unpackAVX512_new<uint16_t>(width, result16, iter);          \
  }                                                             \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_VELOX_CASE_16(width, iter)        \
  BENCHMARK(velox_unpack_fullrows_##width##_##iter##_16) { \
    veloxBitUnpack<uint16_t>(width, result16, iter);       \
  }                                                        \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_AVX512_CASE_32(width, iter)            \
  BENCHMARK(avx512_new_unpack_fullrows_##width##_##iter##_32) { \
    unpackAVX512_new<uint32_t>(width, result32, iter);          \
  }                                                             \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_VELOX_CASE_32(width, iter)        \
  BENCHMARK(velox_unpack_fullrows_##width##_##iter##_32) { \
    veloxBitUnpack<uint32_t>(width, result32, iter);       \
  }                                                        \
  BENCHMARK_DRAW_LINE();

BENCHMARK(warmup) {
  unpackAVX512_new<uint8_t>(1, result8);
  veloxBitUnpack<uint8_t>(1, result8);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(avx512_new_unpack_fullrows_2_8_iter) {
  for (auto i = 0; i < 5; i++) {
    unpackAVX512_new<uint8_t>(2, result8);
  }
}

BENCHMARK(avx512_new_unpack_fullrows_5_8_iter) {
  for (auto i = 0; i < 5; i++) {
    unpackAVX512_new<uint8_t>(5, result8);
  }
}

BENCHMARK(avx512_new_unpack_fullrows_1_16_iter) {
  for (auto i = 0; i < 5; i++) {
    unpackAVX512_new<uint16_t>(1, result16);
  }
}

BENCHMARK(avx512_new_unpack_fullrows_13_16_iter) {
  for (auto i = 0; i < 5; i++) {
    unpackAVX512_new<uint16_t>(13, result16);
  }
}

BENCHMARK(avx512_new_unpack_fullrows_10_32_iter) {
  for (auto i = 0; i < 5; i++) {
    unpackAVX512_new<uint32_t>(10, result32);
  }
}

BENCHMARK(avx512_new_unpack_fullrows_17_32_iter) {
  for (auto i = 0; i < 5; i++) {
    unpackAVX512_new<uint32_t>(17, result32);
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(velox_unpack_fullrows_2_8_iter) {
  for (auto i = 0; i < 5; i++) {
    veloxBitUnpack<uint8_t>(2, result8);
  }
}

BENCHMARK(velox_unpack_fullrows_5_8_iter) {
  for (auto i = 0; i < 5; i++) {
    veloxBitUnpack<uint8_t>(5, result8);
  }
}

BENCHMARK(velox_unpack_fullrows_1_16_iter) {
  for (auto i = 0; i < 5; i++) {
    veloxBitUnpack<uint16_t>(1, result16);
  }
}

BENCHMARK(velox_unpack_fullrows_13_16_iter) {
  for (auto i = 0; i < 5; i++) {
    veloxBitUnpack<uint16_t>(13, result16);
  }
}

BENCHMARK(velox_unpack_fullrows_10_32_iter) {
  for (auto i = 0; i < 5; i++) {
    veloxBitUnpack<uint32_t>(10, result32);
  }
}

BENCHMARK(velox_unpack_fullrows_17_32_iter) {
  for (auto i = 0; i < 5; i++) {
    veloxBitUnpack<uint32_t>(17, result32);
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_CASE_8(1)
BENCHMARK_UNPACK_FULLROWS_CASE_8(2)
BENCHMARK_UNPACK_FULLROWS_CASE_8(3)
BENCHMARK_UNPACK_FULLROWS_CASE_8(4)
BENCHMARK_UNPACK_FULLROWS_CASE_8(5)
BENCHMARK_UNPACK_FULLROWS_CASE_8(6)
BENCHMARK_UNPACK_FULLROWS_CASE_8(7)
BENCHMARK_UNPACK_FULLROWS_CASE_8(8)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(1)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(2)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(3)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(4)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(5)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(6)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(7)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_8(8)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_CASE_16(1)
BENCHMARK_UNPACK_FULLROWS_CASE_16(2)
BENCHMARK_UNPACK_FULLROWS_CASE_16(3)
BENCHMARK_UNPACK_FULLROWS_CASE_16(4)
BENCHMARK_UNPACK_FULLROWS_CASE_16(5)
BENCHMARK_UNPACK_FULLROWS_CASE_16(6)
BENCHMARK_UNPACK_FULLROWS_CASE_16(7)
BENCHMARK_UNPACK_FULLROWS_CASE_16(8)
BENCHMARK_UNPACK_FULLROWS_CASE_16(9)
BENCHMARK_UNPACK_FULLROWS_CASE_16(10)
BENCHMARK_UNPACK_FULLROWS_CASE_16(11)
BENCHMARK_UNPACK_FULLROWS_CASE_16(12)
BENCHMARK_UNPACK_FULLROWS_CASE_16(13)
BENCHMARK_UNPACK_FULLROWS_CASE_16(14)
BENCHMARK_UNPACK_FULLROWS_CASE_16(15)
BENCHMARK_UNPACK_FULLROWS_CASE_16(16)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(1)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(2)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(3)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(4)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(5)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(6)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(7)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(8)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(9)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(10)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(11)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(12)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(13)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(14)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(15)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_16(16)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_CASE_32(1)
BENCHMARK_UNPACK_FULLROWS_CASE_32(2)
BENCHMARK_UNPACK_FULLROWS_CASE_32(3)
BENCHMARK_UNPACK_FULLROWS_CASE_32(4)
BENCHMARK_UNPACK_FULLROWS_CASE_32(5)
BENCHMARK_UNPACK_FULLROWS_CASE_32(6)
BENCHMARK_UNPACK_FULLROWS_CASE_32(7)
BENCHMARK_UNPACK_FULLROWS_CASE_32(8)
BENCHMARK_UNPACK_FULLROWS_CASE_32(9)
BENCHMARK_UNPACK_FULLROWS_CASE_32(10)
BENCHMARK_UNPACK_FULLROWS_CASE_32(11)
BENCHMARK_UNPACK_FULLROWS_CASE_32(13)
BENCHMARK_UNPACK_FULLROWS_CASE_32(15)
BENCHMARK_UNPACK_FULLROWS_CASE_32(17)
BENCHMARK_UNPACK_FULLROWS_CASE_32(19)
BENCHMARK_UNPACK_FULLROWS_CASE_32(21)
BENCHMARK_UNPACK_FULLROWS_CASE_32(24)
BENCHMARK_UNPACK_FULLROWS_CASE_32(28)
BENCHMARK_UNPACK_FULLROWS_CASE_32(30)
BENCHMARK_UNPACK_FULLROWS_CASE_32(32)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(1)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(2)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(3)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(4)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(5)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(6)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(7)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(8)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(9)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(10)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(11)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(13)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(15)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(17)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(19)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(21)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(24)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(28)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(30)
BENCHMARK_UNPACK_FULLROWS_SWITCH_CASE_32(32)

void populateBitPacked() {
  bitPackedData.resize(33);
  for (auto bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    auto numWords = bits::roundUp(randomInts_u32.size() * bitWidth, 64) / 64;
    bitPackedData[bitWidth].resize(numWords);
    auto source = reinterpret_cast<uint64_t*>(randomInts_u32.data());
    auto destination =
        reinterpret_cast<uint64_t*>(bitPackedData[bitWidth].data());
    for (auto i = 0; i < randomInts_u32.size(); ++i) {
      bits::copyBits(source, i * 32, destination, i * bitWidth, bitWidth);
    }
  }
}

int32_t main(int32_t argc, char* argv[]) {
  folly::init(&argc, &argv);

  // Populate uint32 buffer
  for (int32_t i = 0; i < kNumValues; i++) {
    auto randomInt = folly::Random::rand32();
    randomInts_u32.push_back(randomInt);
  }

  randomInts_u32_result.resize(randomInts_u32.size());

  populateBitPacked();

  folly::runBenchmarks();
  return 0;
}
