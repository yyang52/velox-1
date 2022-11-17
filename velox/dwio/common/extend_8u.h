/*******************************************************************************
 * Copyright (C) 2022 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

/**
 * @brief Contains implementation of functions for vector packing byte integers
 * to 1...8-bit integers
 * @date 07/06/2020
 *
 * @details Function list:
 *          - @ref extend_8u1u
 *          - @ref extend_8u2u
 *          - @ref extend_8u3u
 *          - @ref extend_8u4u
 *          - @ref extend_8u5u
 *          - @ref extend_8u6u
 *          - @ref extend_8u7u
 *          - @ref extend_8u8u
 *          - @ref extend_8u16u
 *          - @ref extend_8u32u
 *
 */
#pragma once

#include "unpack_def.h"

inline void extend_8u16u_tail(
    const uint8_t* src_ptr,
    uint32_t num_values,
    uint8_t* dst_ptr) {
  __m256i srcmm;
  __m512i dstmm;

  __mmask32 tail_mask = OWN_BIT_MASK(num_values);
  srcmm = _mm256_maskz_loadu_epi8(tail_mask, (const __m256i*)src_ptr);
  dstmm = _mm512_maskz_cvtepu8_epi16(tail_mask, srcmm);
  _mm512_mask_storeu_epi16(dst_ptr, tail_mask, dstmm);
}

inline void extend_8u16u(const __m512i src, uint16_t* dst_ptr) {
  __m256i tmp = _mm512_extracti64x4_epi64(src, 0);
  __m512i result = _mm512_cvtepu8_epi16(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 32;

  tmp = _mm512_extracti64x4_epi64(src, 1);
  result = _mm512_cvtepu8_epi16(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 32;
}

inline void extend_8u32u_tail(
    const uint8_t* src_ptr,
    uint32_t num_values,
    uint8_t* dst_ptr) {
  __m128i srcmm;
  __m512i dstmm;

  __mmask16 tail_mask = OWN_BIT_MASK(num_values);
  srcmm = _mm_maskz_loadu_epi8(tail_mask, (const __m128i*)src_ptr);
  dstmm = _mm512_maskz_cvtepu8_epi32(tail_mask, srcmm);
  _mm512_mask_storeu_epi32(dst_ptr, tail_mask, dstmm);
}

inline void extend_8u32u(const __m512i src, uint32_t* dst_ptr) {
  __m128i tmp = _mm512_extracti32x4_epi32(src, 0);
  __m512i result = _mm512_cvtepu8_epi32(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;

  tmp = _mm512_extracti32x4_epi32(src, 1);
  result = _mm512_cvtepu8_epi32(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;

  tmp = _mm512_extracti32x4_epi32(src, 2);
  result = _mm512_cvtepu8_epi32(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;

  tmp = _mm512_extracti32x4_epi32(src, 3);
  result = _mm512_cvtepu8_epi32(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;
}

inline void extend_8u64u(const __m512i src, uint64_t* dst_ptr) {
  __m128i tmp = _mm512_extracti32x4_epi32(src, 0);
  __m512i presult = _mm512_cvtepu8_epi32(tmp);
  __m256i tmp1 = _mm512_extracti64x4_epi64(presult, 0);
  __m512i result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;
  tmp1 = _mm512_extracti64x4_epi64(presult, 1);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;

  tmp = _mm512_extracti32x4_epi32(src, 1);
  presult = _mm512_cvtepu8_epi32(tmp);
  tmp1 = _mm512_extracti64x4_epi64(presult, 0);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;
  tmp1 = _mm512_extracti64x4_epi64(presult, 1);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;

  tmp = _mm512_extracti32x4_epi32(src, 2);
  presult = _mm512_cvtepu8_epi32(tmp);
  tmp1 = _mm512_extracti64x4_epi64(presult, 0);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;
  tmp1 = _mm512_extracti64x4_epi64(presult, 1);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;

  tmp = _mm512_extracti32x4_epi32(src, 3);
  presult = _mm512_cvtepu8_epi32(tmp);
  tmp1 = _mm512_extracti64x4_epi64(presult, 0);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;
  tmp1 = _mm512_extracti64x4_epi64(presult, 1);
  result = _mm512_cvtepu32_epi64(tmp1);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 8u;
}
