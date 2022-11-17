#pragma once

#include <stdint.h>
#include <stdio.h>
#include "immintrin.h"

#define OWN_ALIGNED_ARRAY(array_declaration, alignment) \
  array_declaration __attribute__((aligned(alignment)))
#define OWN_ALIGNED_64_ARRAY(array_declaration) \
  OWN_ALIGNED_ARRAY(array_declaration, 64u)

/**
 * @brief Defines internal inline Intel core function
 */

#define OWN_MAX_16U 0xFFFF /**< Max value for uint16_t */
#define OWN_MAX_32U 0xFFFFFFFF /**< Max value for uint32_t */
#define OWN_1_BIT_MASK 1u /**< Mask for 1-bit integer */
#define OWN_2_BIT_MASK 3u /**< Mask for 2-bit integer */
#define OWN_3_BIT_MASK 7u /**< Mask for 3-bit integer */
#define OWN_4_BIT_MASK 0xfu /**< Mask for 4-bit integer */
#define OWN_5_BIT_MASK 0x1fu /**< Mask for 5-bit integer */
#define OWN_6_BIT_MASK 0x3fu /**< Mask for 6-bit integer */
#define OWN_7_BIT_MASK 0x7fu /**< Mask for 7-bit integer */
#define OWN_HIGH_BIT_MASK                            \
  0x80u /**< Mask for most significant bit in a byte \
         */
#define OWN_LOW_BIT_MASK 1u /**< Mask for least significant bit in a byte */
#define OWN_BYTE_WIDTH 8u /**< Byte width in bits */
#define OWN_WORD_WIDTH 16u /**< Word width in bits */
#define OWN_3_BYTE_WIDTH 24u /**< 3-byte width in bits */
#define OWN_DWORD_WIDTH 32u /**< Dword width in bits */
#define OWN_6_BYTE_WIDTH 48u /**< 6-byte width in bits */
#define OWN_7_BYTE_WIDTH 56u /**< 7-byte width in bits */
#define OWN_QWORD_WIDTH 64u /**< Qword width in bits */
#define OWN_RLE_BURST_MAX_COUNT \
  65535u /**< Maximum count for 32u rle_burst operation */
#define OWN_BIT_MASK(x) \
  (((1ULL) << (x)) - 1u) /**< Bit mask below bit position */
#define OWN_PARQUET_WIDTH 8u /**< Parquet size in elements (PRLE format) */
#define OWN_LITERAL_OCTA_GROUP 1u /**< PRLE format description */
#define OWN_VARINT_BYTE_1(x) \
  (((x)&OWN_7_BIT_MASK) << 6u) /**< 1st byte extraction for varint format */
#define OWN_VARINT_BYTE_2(x) \
  (((x)&OWN_7_BIT_MASK) << 13u) /**< 2nd byte extraction for varint format */
#define OWN_VARINT_BYTE_3(x) \
  (((x)&OWN_7_BIT_MASK) << 20u) /**< 3rd byte extraction for varint format */
#define OWN_VARINT_BYTE_4(x) \
  (((x)&OWN_5_BIT_MASK) << 27u) /**< 4th byte extraction for varint format */
#define OWN_PRLE_COUNT(x) \
  (((x)&OWN_7_BIT_MASK) >> 1u) /**< PRLE count field extraction */
#define OWN_MAX(a, b) (((a) > (b)) ? (a) : (b)) /**< Maximum from 2 values */
#define OWN_MIN(a, b) (((a) < (b)) ? (a) : (b)) /**< Minimum from 2 values */

#define OWN_BITS_2_WORD(x) \
  (((x) + 15u) >> 4u) /**< Convert a number of bits to a number of words */
#define OWN_BITS_2_DWORD(x) \
  (((x) + 31u) >>           \
   5u) /**< Convert a number of bits to a number of double words */
/**
 * @brief 64-bit union for simplifying arbitrary bit-width integers conversions
 * to/from standard types
 */
typedef union {
  uint64_t bit_buf;
  uint8_t byte_buf[8];
} bit_byte_pool64_t;

/**
 * @brief 32-bit union for simplifying arbitrary bit-width integers conversions
 * to/from standard types
 */
typedef union {
  uint32_t bit_buf;
  uint16_t word_buf[2];
  uint8_t byte_buf[4];
} bit_byte_pool32_t;

/**
 * @brief 16-bit union for simplifying arbitrary bit-width integers conversions
 * to/from standard types
 */
typedef union {
  uint16_t bit_buf;
  uint8_t byte_buf[2];
} bit_byte_pool16_t;

/**
 * @brief 48-bit union for simplifying arbitrary bit-width integers conversions
 * to/from standard types
 */
typedef union {
  uint8_t byte_buf[8];
  uint32_t dw_buf[2];
  uint16_t word_buf[4];
  uint64_t bit_buf;
} bit_byte_pool48_t;

inline uint32_t
own_get_align(uint32_t start_bit, uint32_t base, uint32_t bitsize) {
  uint32_t remnant = bitsize - start_bit;
  uint32_t ret_value = 0xFFFFFFFF;
  for (uint32_t i = 0u; i < bitsize; ++i) {
    uint32_t test_value = (i * base) % bitsize;
    if (test_value == remnant) {
      ret_value = i;
      break;
    }
  }
  return ret_value;
}

#define BitUnpackKernel(bit_width, ...)       \
  [&]() {                                     \
    switch (bit_width) {                      \
      case 0: {                               \
        return unpack_0u8u(__VA_ARGS__);      \
      }                                       \
      case 1: {                               \
        return unpack_1u8u(__VA_ARGS__);      \
      }                                       \
      case 2: {                               \
        return unpack_2u8u(__VA_ARGS__);      \
      }                                       \
      case 3: {                               \
        return unpack_3u8u(__VA_ARGS__);      \
      }                                       \
      case 4: {                               \
        return unpack_4u8u(__VA_ARGS__);      \
      }                                       \
      case 5: {                               \
        return unpack_5u8u(__VA_ARGS__);      \
      }                                       \
      case 6: {                               \
        return unpack_6u8u(__VA_ARGS__);      \
      }                                       \
      case 7: {                               \
        return unpack_7u8u(__VA_ARGS__);      \
      }                                       \
      case 8: {                               \
        return unpack_8u8u(__VA_ARGS__);      \
      }                                       \
      case 9: {                               \
        return unpack_9u16u(__VA_ARGS__);     \
      }                                       \
      case 10: {                              \
        return unpack_10u16u(__VA_ARGS__);    \
      }                                       \
      case 11: {                              \
        return unpack_11u16u(__VA_ARGS__);    \
      }                                       \
      case 12: {                              \
        return unpack_12u16u(__VA_ARGS__);    \
      }                                       \
      case 13: {                              \
        return unpack_13u16u(__VA_ARGS__);    \
      }                                       \
      case 14: {                              \
        return unpack_14u16u(__VA_ARGS__);    \
      }                                       \
      case 15: {                              \
        return unpack_15u16u(__VA_ARGS__);    \
      }                                       \
      case 16: {                              \
        return unpack_16u16u(__VA_ARGS__);    \
      }                                       \
      case 17: {                              \
        return unpack_17u32u(__VA_ARGS__);    \
      }                                       \
      case 18: {                              \
        return unpack_18u32u(__VA_ARGS__);    \
      }                                       \
      case 19: {                              \
        return unpack_19u32u(__VA_ARGS__);    \
      }                                       \
      case 20: {                              \
        return unpack_20u32u(__VA_ARGS__);    \
      }                                       \
      case 21: {                              \
        return unpack_21u32u(__VA_ARGS__);    \
      }                                       \
      case 22: {                              \
        return unpack_22u32u(__VA_ARGS__);    \
      }                                       \
      case 23: {                              \
        return unpack_23u32u(__VA_ARGS__);    \
      }                                       \
      case 24: {                              \
        return unpack_24u32u(__VA_ARGS__);    \
      }                                       \
      case 25: {                              \
        return unpack_25u32u(__VA_ARGS__);    \
      }                                       \
      case 26: {                              \
        return unpack_26u32u(__VA_ARGS__);    \
      }                                       \
      case 27: {                              \
        return unpack_27u32u(__VA_ARGS__);    \
      }                                       \
      case 28: {                              \
        return unpack_28u32u(__VA_ARGS__);    \
      }                                       \
      case 29: {                              \
        return unpack_29u32u(__VA_ARGS__);    \
      }                                       \
      case 30: {                              \
        return unpack_30u32u(__VA_ARGS__);    \
      }                                       \
      case 31: {                              \
        return unpack_31u32u(__VA_ARGS__);    \
      }                                       \
      case 32: {                              \
        return unpack_32u32u(__VA_ARGS__);    \
      }                                       \
      default: {                              \
        return (const unsigned char*)nullptr; \
      }                                       \
    }                                         \
  }()
