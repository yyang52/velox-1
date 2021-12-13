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
#include "velox/vector/FlatVector.h"
#include "velox/omnisci/RawDataConvertor.h"

namespace facebook::velox::omnisci {
  // implement type by type (could be refined as template)
  void convert(VectorPtr& child, int idx, int8_t*** col_buffer_ptr, int num_rows) {
    int8_t** col_buffer = *col_buffer_ptr;
    switch (child->typeKind()) {
      case TypeKind::BOOLEAN:
        {
          auto childVal = child->asFlatVector<bool>();
          auto* rawValues = childVal->mutableRawValues();
          bool* column = (bool*)std::malloc(sizeof(bool) * num_rows);
          if (child->mayHaveNulls()) {
            auto nulls = child->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<bool>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(bool) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      case TypeKind::TINYINT:
        {
          int8_t* column = (int8_t*)std::malloc(sizeof(int8_t) * num_rows);
          auto childVal = child->asFlatVector<int8_t>();
          auto* rawValues = childVal->mutableRawValues();
          if (child->mayHaveNulls()) {
            auto nulls = child->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<int8_t>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(int8_t) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      case TypeKind::SMALLINT:
        {
          int16_t* column = (int16_t*)std::malloc(sizeof(int16_t) * num_rows);       
          auto childVal = child->asFlatVector<int16_t>();
          auto* rawValues = childVal->mutableRawValues();
          if (childVal->mayHaveNulls()) {
            auto nulls = child->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<int16_t>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(int16_t) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      case TypeKind::INTEGER:
        {
          int32_t* column = (int32_t*)std::malloc(sizeof(int32_t) * num_rows);
          auto childVal = child->asFlatVector<int32_t>();
          auto* rawValues = childVal->mutableRawValues();
          if (child->mayHaveNulls()) {
            auto nulls = child->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<int32_t>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(int32_t) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      case TypeKind::BIGINT:
        {
          int64_t* column = (int64_t*)std::malloc(sizeof(int64_t) * num_rows);
          auto childVal = child->asFlatVector<int64_t>();
          auto* rawValues = childVal->mutableRawValues();
          if (child->mayHaveNulls()) {
            auto nulls = child->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<int64_t>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(int64_t) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      case TypeKind::REAL:
        {
          float* column = (float*)std::malloc(sizeof(float) * num_rows);
          auto childVal = child->asFlatVector<float>();
          auto* rawValues = childVal->mutableRawValues();
          if (child->mayHaveNulls()) {
            auto nulls = childVal->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<float>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(float) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      case TypeKind::DOUBLE:
        {
          double* column = (double*)std::malloc(sizeof(double) * num_rows);
          auto childVal = child->asFlatVector<double>();
          auto* rawValues = childVal->mutableRawValues();
          if (child->mayHaveNulls()) {
            auto nulls = childVal->rawNulls();
            for (auto pos = 0; pos < num_rows; pos++) {
              if (bits::isBitNull(nulls, pos)) {
                rawValues[pos] = std::numeric_limits<double>::min() + 1;
              } 
            }
          }
          memcpy(column, rawValues, sizeof(double) * num_rows);
          col_buffer[idx] = reinterpret_cast<int8_t*>(column);
          break;
        }
      default:
        VELOX_NYI(" {}: conversion is not supported yet", child->typeKind());
        break;
    }
  }
    
  CiderResultSet RawDataConvertor::convertToCider(RowVectorPtr input, int num_rows) {
    RowVector* row = input.get();
    auto* rowVector = row->as<RowVector>();
    auto size = rowVector->childrenSize();
    int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*) * size);
    auto col_buffer_ptr = &col_buffer;
    for (auto idx = 0; idx < size; idx++) {
      VectorPtr& child = rowVector->childAt(idx);
      switch (child->encoding()) {
        case VectorEncoding::Simple::FLAT:  
          convert(child, idx, col_buffer_ptr, num_rows);
          break;
        default:
          VELOX_NYI(" {} conversion is not supported yet", child->encoding());  
      }
    }
    return CiderResultSet(col_buffer, num_rows);
  };

  TypePtr getVeloxType(std::string typeName) {
    if (typeName == "BOOLEAN") return BOOLEAN();
    if (typeName == "TINYINT") return TINYINT();
    if (typeName == "SMALLINT") return SMALLINT();
    if (typeName == "INTEGER") return INTEGER();
    if (typeName == "BIGINT") return BIGINT();
    if (typeName == "FLOAT") return REAL();
    if (typeName == "DOUBLE") return DOUBLE();
    else {
      VELOX_NYI(" {} conversion is not supported yet", typeName); 
    } 
  }

  VectorPtr convertColumn(const TypePtr& vType, int8_t* data_buffer, int num_rows, memory::MemoryPool* pool) {
    switch (vType->kind()) {
      case TypeKind::BOOLEAN:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<bool>>();
          auto rawValues = flatResult->mutableRawValues();
          bool* dataBuffer = reinterpret_cast<bool*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(bool));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<bool>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      case TypeKind::TINYINT:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<int8_t>>();
          auto rawValues = flatResult->mutableRawValues();
          int8_t* dataBuffer = reinterpret_cast<int8_t*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(int8_t));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<int8_t>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      case TypeKind::SMALLINT:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<int16_t>>();
          auto rawValues = flatResult->mutableRawValues();
          int16_t* dataBuffer = reinterpret_cast<int16_t*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(int16_t));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<int16_t>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      case TypeKind::INTEGER:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<int32_t>>();
          auto rawValues = flatResult->mutableRawValues();
          int32_t* dataBuffer = reinterpret_cast<int32_t*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(int32_t));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<int32_t>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      case TypeKind::BIGINT:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<int64_t>>();
          auto rawValues = flatResult->mutableRawValues();
          int64_t* dataBuffer = reinterpret_cast<int64_t*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(int64_t));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<int64_t>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      case TypeKind::REAL:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<float>>();
          auto rawValues = flatResult->mutableRawValues();
          float* dataBuffer = reinterpret_cast<float*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(float));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<float>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      case TypeKind::DOUBLE:
        {
          auto result = BaseVector::create(vType, num_rows, pool);
          auto flatResult = result->as<FlatVector<double>>();
          auto rawValues = flatResult->mutableRawValues();
          double* dataBuffer = reinterpret_cast<double*>(data_buffer);
          memcpy(rawValues, dataBuffer, num_rows * sizeof(double));
          for (auto pos = 0; pos < num_rows; pos++) {
            if (dataBuffer[pos] == std::numeric_limits<double>::min() + 1) {
              result->setNull(pos, true);
            }
          }
          return result;
        }
      default:
        VELOX_NYI(" {} conversion is not supported yet", vType->kind());
        break;
    }
  }

  RowVectorPtr RawDataConvertor::convertToRowVector(int8_t** col_buffer, std::vector<std::string> col_names, 
                                                    std::vector<std::string> col_types, int num_rows, 
                                                    memory::MemoryPool* pool) {
    std::shared_ptr<const RowType> rowType;
    std::vector<VectorPtr> columns;
    
    // get row type from cider result
    std::vector<TypePtr> types;
    int num_cols = col_types.size();
    types.reserve(num_cols);
    for (int i = 0; i < num_cols; i++) {
      types.push_back(getVeloxType(col_types[i]));
    }
    
    // convert col buffer to vector ptr
    columns.reserve(num_cols);
    for (int i = 0; i < num_cols; i++) {
      columns.push_back(convertColumn(types[i], col_buffer[i], num_rows, pool));
    }
    rowType = std::make_shared<RowType>(move(col_names), move(types));
    return std::make_shared<RowVector>(
      pool,
      rowType,
      BufferPtr(nullptr),
      num_rows,
      columns);
  };
} // namespace facebook::velox::omnisci