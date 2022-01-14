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

#include "velox/exec/Operator.h"

#include "HybridExecOperator.h"

namespace facebook::velox::exec {
Operator::PlanNodeTranslator HybridExecOperator::planNodeTranslator =
    [](DriverCtx* ctx,
       int32_t id,
       const std::shared_ptr<const core::PlanNode>& node)
    -> std::unique_ptr<HybridExecOperator> {
  if (auto hybridOp = std::dynamic_pointer_cast<
          const facebook::velox::core::HybridPlanNode>(node)) {
    // TODO: translate from RelAlgExecutionUnit into kernel
    return std::make_unique<HybridExecOperator>(id, ctx, hybridOp);
  }
  return nullptr;
};

bool HybridExecOperator::needsInput() const {
  // TODO: just for agg, what about other case
  return !isFinishing_;
}

void HybridExecOperator::addInput(RowVectorPtr input) {
  input_ = std::move(input);
  process();
  // we should have new return data
  if (!hasData_) {
    hasData_ = true;
  }
}

VectorPtr HybridExecOperator::convertColumn(
    const TypePtr& vType,
    int8_t* data_buffer,
    int num_rows,
    memory::MemoryPool* pool) {
  switch (vType->kind()) {
    case TypeKind::BOOLEAN: {
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
    case TypeKind::TINYINT: {
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
    case TypeKind::SMALLINT: {
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
    case TypeKind::INTEGER: {
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
    case TypeKind::BIGINT: {
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
    case TypeKind::REAL: {
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
    case TypeKind::DOUBLE: {
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

RowVectorPtr HybridExecOperator::getOutput() {
  if (isGroupBy_) {
    throw std::runtime_error("not support now!");
  } else if (isAgg_) {
    if (finished_ || (!isFinishing_)) {
      return nullptr;
    }
    if (!hasData_) {
      return nullptr;
    }

    // convert partial agg Result to RowVector
    for (int i = 0; i < columns_.size(); i++) {
      columns_[i] = convertColumn(
          rowType_->childAt(i),
          (int8_t*)(partialAggResult_.data() + sizeof(int64_t) * i),
          1,
          pool());
    }
    auto aggRes = std::make_shared<RowVector>(
        pool(), rowType_, BufferPtr(nullptr), 1, columns_);
    // we return data, so no data left.
    hasData_ = false;
    return aggRes;
  } else if (isFilter_) {
    return tmpOut;
  }

  return std::move(result_);
}

void HybridExecOperator::process() {
  if (isGroupBy_) {
    throw std::runtime_error("not support now!");
  }
  if (isAgg_) {
    // 1. convert input data.
    auto startConversion = std::chrono::system_clock::now();

    int64_t numRows = input_->size();
    auto ciderBuffer = dataConvertor_->convertToCider(input_, numRows, &convertorInternalCounter);

    int32_t colNum = partialAggResult_.size(); // FIXME:
    int64_t** outBuffers = (int64_t**)std::malloc(sizeof(int64_t*) * colNum);
    outBuffers[0] =
        (int64_t*)std::malloc(sizeof(int64_t) * 1); // agg only return 1 row

    int32_t matchedRows = 0;
    int32_t errCode = 0;

    auto endConversion = std::chrono::system_clock::now();
    dataConversionCounter += std::chrono::duration_cast<std::chrono::microseconds>(
        endConversion - startConversion);

    // 2. convert partial result
    int64_t* aggInitValue =
        partialAggResult_.data(); // TODO: verify, this will be overwrite?

    // 3. call run method
    auto startCompute = std::chrono::system_clock::now();

    ciderKernel_->runWithData(
        (const int8_t**)ciderBuffer.colBuffer,
        &numRows,
        outBuffers,
        &matchedRows,
        &errCode,
        aggInitValue);

    auto endCompute = std::chrono::system_clock::now();
    computeCounter += std::chrono::duration_cast<std::chrono::microseconds>(
        endCompute - startCompute);

    // 4. convert result into partial agg result
    partialAggResult_[0] = outBuffers[0][0];
  } else if (isFilter_) {
    // NOTE: not ready yet!!
    // 1. convert input data.
    int64_t numRows = input_->size();
    auto ciderBuffer = dataConvertor_->convertToCider(input_, numRows, nullptr);

    int32_t colNum = 1; // FIXME:
    int64_t** outBuffers = (int64_t**)std::malloc(sizeof(int64_t*) * colNum);

    int32_t matchedRows = 0;
    int32_t errCode = 0;

    // 2. call run method
    ciderKernel_->runWithData(
        (const int8_t**)ciderBuffer.colBuffer,
        &numRows,
        outBuffers,
        &matchedRows,
        &errCode,
        nullptr);

    // 3. convert result into tmpOut/ RowVectorPtr
    // TODO: seems a lot overhead here
  }

  if (isSort_) {
    // TODO: impl
    throw std::runtime_error("not support now!");
  }
  // TODO: remove this part
  // for now, we just steal input_
  totalRowsProcessed_ += input_->size();
  result_ = std::move(input_);
}
} // namespace facebook::velox::exec
