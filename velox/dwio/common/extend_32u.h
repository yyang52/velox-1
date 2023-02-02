/*******************************************************************************
 * Copyright (C) 2022 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

/*------- pack_16u_k0.h -------*/

/**
 * @brief Contains implementation of functions for vector packing dword integers
 * to 17...32-bit integers
 * @date 22/03/2021
 *
 * @details Function list:
 */
#pragma once

#include "unpack_def.h"
inline void extend_32u64u(const __m512i src, uint64_t* dst_ptr) {
  __m256i tmp = _mm512_extracti64x4_epi64(src, 0);
  __m512i result = _mm512_cvtepu32_epi64(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 32;

  tmp = _mm512_extracti64x4_epi64(src, 1);
  result = _mm512_cvtepu32_epi64(tmp);
  _mm512_storeu_si512(dst_ptr, result);
  dst_ptr += 32;
}