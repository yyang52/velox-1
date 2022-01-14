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
#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <vector>

#include "velox/omnisci/DataConvertor.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using facebook::velox::test::VectorMaker;
using namespace facebook::velox::cider;
using facebook::velox::cider::DataConvertor;

class ResultConvertTest : public testing::Test {
 public:
  template <typename T>
  FlatVectorPtr<T> makeNullableFlatVector(
      const std::vector<std::optional<T>>& data) {
    return vectorMaker_.flatVectorNullable(data);
  }

  RowVectorPtr makeRowVector(const std::vector<VectorPtr>& children) {
    return vectorMaker_.rowVector(children);
  }

 protected:
  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker vectorMaker_{pool_.get()};
};

template <typename T>
void testToCiderDirect(
    RowVectorPtr rowVector,
    const std::vector<std::optional<T>>& data,
    int numRows) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderResultSet crs = convertor->convertToCider(rowVector, numRows);
  EXPECT_EQ(numRows, crs.numRows);

  int8_t** colBuffer = crs.colBuffer;
  T* col_0 = reinterpret_cast<T*>(colBuffer[0]);
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      if (std::is_integral<T>::value) {
        EXPECT_EQ(inline_int_null_value<T>(), col_0[idx]);
      } else if (std::is_same<T, float>::value) {
        EXPECT_EQ(FLT_MIN, col_0[idx]);
      } else if (std::is_same<T, double>::value) {
        EXPECT_EQ(DBL_MIN, col_0[idx]);
      } else {
        VELOX_NYI("Conversion is not supported yet");
      }
    } else {
      EXPECT_EQ(data[idx], col_0[idx]);
    }
  }
}

template <>
void testToCiderDirect<bool>(
    RowVectorPtr rowVector,
    const std::vector<std::optional<bool>>& data,
    int numRows) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderResultSet crs = convertor->convertToCider(rowVector, numRows);
  EXPECT_EQ(numRows, crs.numRows);

  int8_t** colBuffer = crs.colBuffer;
  int8_t* col_0 = colBuffer[0];

  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      EXPECT_EQ(inline_int_null_value<int8_t>(), col_0[idx]);
    } else {
      EXPECT_EQ(data[idx].value(), static_cast<bool>(col_0[idx]));
    }
  }
}

template <>
void testToCiderDirect<Timestamp>(
    RowVectorPtr rowVector,
    const std::vector<std::optional<Timestamp>>& data,
    int numRows) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderResultSet crs = convertor->convertToCider(rowVector, numRows);
  EXPECT_EQ(numRows, crs.numRows);

  int8_t** colBuffer = crs.colBuffer;
  int64_t* col_0 = reinterpret_cast<int64_t*>(colBuffer[0]);

  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      EXPECT_EQ(inline_int_null_value<int64_t>(), col_0[idx]);
    } else {
      EXPECT_EQ(
          data[idx].value().getSeconds() * 1000000000 +
              data[idx].value().getNanos(),
          col_0[idx]);
    }
  }

  for (auto idx = 0; idx < numRows; idx++) {
    if (data_3[idx] == std::nullopt) {
      EXPECT_EQ(inline_int_null_value<int8_t>(), col_3[idx]);
    } else {
      EXPECT_EQ(data_3[idx].value(), static_cast<bool>(col_3[idx]));
    }
  }
}

TEST_F(ResultConvertTest, directToCiderIntegerOneCol) {
  int numRows = 10;
  std::vector<std::optional<int32_t>> data = {
      0, std::nullopt, 1, 3, std::nullopt, -1234, -99, -999, 1000, -1};
  auto col = makeNullableFlatVector<int32_t>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<int32_t>(rowVector, data, numRows);
}

TEST_F(ResultConvertTest, directToCiderBigintOneCol) {
  int numRows = 10;
  std::vector<std::optional<int64_t>> data = {
      0, 1, std::nullopt, 3, 1024, -123456, -99, -999, std::nullopt, -1};
  auto col = makeNullableFlatVector<int64_t>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<int64_t>(rowVector, data, numRows);
}

TEST_F(ResultConvertTest, directToCiderDoubleOneCol) {
  int numRows = 10;
  std::vector<std::optional<double>> data = {
      0.5,
      1,
      std::nullopt,
      3.14,
      1024,
      -123456,
      -99.99,
      -999,
      std::nullopt,
      -1};
  auto col = makeNullableFlatVector<double>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<double>(rowVector, data, numRows);
}

TEST_F(ResultConvertTest, directToCiderBoolOneCol) {
  int numRows = 10;
  std::vector<std::optional<bool>> data = {
      true,
      false,
      std::nullopt,
      false,
      true,
      true,
      false,
      std::nullopt,
      false,
      true,
  };
  auto col = makeNullableFlatVector<bool>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<bool>(rowVector, data, numRows);
}

TEST_F(ResultConvertTest, directToCiderTimestampOneCol) {
  int numRows = 10;
  std::vector<std::optional<Timestamp>> data = {
      Timestamp(28800, 10),
      Timestamp(946713600, 0),
      Timestamp(0, 0),
      std::nullopt,
      Timestamp(946758116, 20),
      Timestamp(-21600, 0),
      std::nullopt,
      Timestamp(957164400, 30),
      Timestamp(946729316, 0),
      Timestamp(7200, 0),
  };
  auto col = makeNullableFlatVector<Timestamp>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<Timestamp>(rowVector, data, numRows);
}

TEST_F(ResultConvertTest, VeloxToCiderArrowConvert) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::ARROW);
  RowVectorPtr input;
  int num_rows;
  // CiderResultSet crs = convertor->convertToCider(input, num_rows);
  // CHECK EQUAL
}

template <typename T>
void testToVeloxDirect(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    std::vector<int32_t> dimens,
    memory::MemoryPool* pool,
    int num_rows) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(
      col_buffer, col_names, col_types, dimens, num_rows, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<T>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  T* col_0 = reinterpret_cast<T*>(col_buffer[0]);
  for (auto idx = 0; idx < num_rows; idx++) {
    if (std::is_integral<T>::value) {
      if (col_0[idx] == inline_int_null_value<T>()) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else if (std::is_same<T, float>::value) {
      if (col_0[idx] == FLT_MIN) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else if (std::is_same<T, double>::value) {
      if (col_0[idx] == DBL_MIN) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else {
      VELOX_NYI("Conversion is not supported yet");
    }
  }
}

template <>
void testToVeloxDirect<bool>(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    std::vector<int32_t> dimens,
    memory::MemoryPool* pool,
    int num_rows) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(
      col_buffer, col_names, col_types, dimens, num_rows, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<bool>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  int8_t* col_0 = col_buffer[0];
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == inline_int_null_value<int8_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(childVal_0->valueAt(idx), col_0[idx]);
    }
  }
}

template <>
void testToVeloxDirect<Timestamp>(
    int8_t** col_buffer,
    std::vector<std::string> col_names,
    std::vector<std::string> col_types,
    std::vector<int32_t> dimens,
    memory::MemoryPool* pool,
    int num_rows) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(
      col_buffer, col_names, col_types, dimens, num_rows, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<Timestamp>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  int64_t* col_0 = reinterpret_cast<int64_t*>(col_buffer[0]);
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == inline_int_null_value<int64_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(
          childVal_0->valueAt(idx),
          Timestamp(col_0[idx] / 1000000, (col_0[idx] % 1000000) * 1000));
    }
  }
}

TEST_F(ResultConvertTest, directToVeloxIntegerOneCol) {
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*));
  int32_t* col_0 = (int32_t*)std::malloc(sizeof(int32_t) * 10);
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = inline_int_null_value<int32_t>();
  }
  col_buffer[0] = reinterpret_cast<int8_t*>(col_0);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<std::string> col_types = {"INT"};
  std::vector<int32_t> dimens = {0};
  testToVeloxDirect<int32_t>(
      col_buffer, col_names, col_types, dimens, pool_.get(), num_rows);
  std::free(col_buffer[0]);
  std::free(col_buffer);
}

TEST_F(ResultConvertTest, directToVeloxBigintOneCol) {
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*));
  int64_t* col_0 = (int64_t*)std::malloc(sizeof(int64_t) * 10);
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 123;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = inline_int_null_value<int64_t>();
  }
  col_buffer[0] = reinterpret_cast<int8_t*>(col_0);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<std::string> col_types = {"BIGINT"};
  std::vector<int32_t> dimens = {0};
  testToVeloxDirect<int64_t>(
      col_buffer, col_names, col_types, dimens, pool_.get(), num_rows);
  std::free(col_buffer[0]);
  std::free(col_buffer);
}

TEST_F(ResultConvertTest, directToVeloxDoubleOneCol) {
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*));
  double* col_0 = (double*)std::malloc(sizeof(double) * 10);
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 3.14;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = DBL_MIN;
  }
  col_buffer[0] = reinterpret_cast<int8_t*>(col_0);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<std::string> col_types = {"DOUBLE"};
  std::vector<int32_t> dimens = {0};
  testToVeloxDirect<double>(
      col_buffer, col_names, col_types, dimens, pool_.get(), num_rows);
  std::free(col_buffer[0]);
  std::free(col_buffer);
}

TEST_F(ResultConvertTest, directToVeloxBoolOneCol) {
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*));
  int8_t* col_0 = (int8_t*)std::malloc(sizeof(int8_t) * 10);
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i % 2 ? true : false;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = inline_int_null_value<int8_t>();
  }
  col_buffer[0] = col_0;

  std::vector<std::string> col_names = {"col_0"};
  std::vector<std::string> col_types = {"BOOL"};
  std::vector<int32_t> dimens = {0};
  testToVeloxDirect<bool>(
      col_buffer, col_names, col_types, dimens, pool_.get(), num_rows);
  std::free(col_buffer[0]);
  std::free(col_buffer);
}

TEST_F(ResultConvertTest, directToVeloxTimestampOneCol) {
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*));
  int64_t* col_0 = (int64_t*)std::malloc(sizeof(int64_t) * 10);
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i + 86400000000;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = inline_int_null_value<int64_t>();
  }
  col_buffer[0] = reinterpret_cast<int8_t*>(col_0);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<std::string> col_types = {"TIMESTAMP"};
  std::vector<int32_t> dimens = {6};
  testToVeloxDirect<Timestamp>(
      col_buffer, col_names, col_types, dimens, pool_.get(), num_rows);
  std::free(col_buffer[0]);
  std::free(col_buffer);
}

TEST_F(ResultConvertTest, CiderToVeloxArrowConvert) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::ARROW);
  int8_t** col_buffer;
  int num_rows;
  // RowVectorPtr rvp = convertor->convertToRowVector(col_buffer, num_rows);
  // CHECK EQUAL
}
