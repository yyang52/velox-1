/*******************************************************************************
 * Copyright (C) 2022 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

/*------- pack_16u_k0.h -------*/

/**
 * @brief Contains implementation of functions for vector packing byte integers
 * to 9...16-bit integers
 * @date 16/02/2021
 *
 * @details Function list:
 *          - @ref extend_16u9u
 *          - @ref extend_16u10u
 *          - @ref extend_16u11u
 *          - @ref extend_16u12u
 *          - @ref extend_16u13u
 *          - @ref extend_16u14u
 *          - @ref extend_16u15u
 *          - @ref extend_16u32u
 *
 */
#pragma once

#include "unpack_def.h"

// ********************** 16u32u ****************************** //
inline void extend_16u32u(const __m512i src, uint32_t* dst_ptr) {
  __m256i tmp = _mm512_extracti64x4_epi64(src, 0);
  __m512i result = _mm512_cvtepu16_epi32(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 32;

  tmp = _mm512_extracti64x4_epi64(src, 1);
  result = _mm512_cvtepu16_epi32(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 32;
}

inline void extend_16u64u(const __m512i src, uint64_t* dst_ptr) {
  __m128i tmp = _mm512_extracti32x4_epi32(src, 0);
  __m512i result = _mm512_cvtepu16_epi64(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;

  tmp = _mm512_extracti32x4_epi32(src, 1);
  result = _mm512_cvtepu16_epi64(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;

  tmp = _mm512_extracti32x4_epi32(src, 2);
  result = _mm512_cvtepu16_epi64(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;

  tmp = _mm512_extracti32x4_epi32(src, 3);
  result = _mm512_cvtepu16_epi64(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 16u;
}
