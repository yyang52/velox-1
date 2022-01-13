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

#include "velox/core/HybridPlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/omnisci/DataConvertor.h"
#include "velox/vector/FlatVector.h"

// FIXME: include order matters since omnisci header file is not clean yet and
// may define some dirty macro which will influence velox code base, so we put
// it at the end of include chain. This is just a work around, if some further
// code change have similar issue, best way is make header file cleaner.
#include "Cider/CiderExecutionKernel.h"
#include "QueryEngine/InputMetadata.h"
#include "QueryEngine/RelAlgExecutionUnit.h"

using namespace facebook::velox::cider;

namespace facebook::velox::exec {
class HybridExecOperator : public Operator {
 public:
  HybridExecOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::HybridPlanNode>& hybridPlanNode)
      : Operator(
            driverCtx,
            hybridPlanNode->outputType(),
            operatorId,
            hybridPlanNode->id(),
            "hybrid"),
        relAlgExecUnit_(hybridPlanNode->getCiderParamContext()
                            ->getExeUnitBasedOnContext()) {
    assert(relAlgExecUnit_);
    // TODO: we should know the query type
    isGroupBy_ =
        hybridPlanNode->getCiderParamContext()->nodeProperty_.hasGroupBy;
    isAgg_ = hybridPlanNode->getCiderParamContext()->nodeProperty_.hasAgg;
    isFilter_ = hybridPlanNode->getCiderParamContext()->nodeProperty_.hasFilter;

    if (isAgg_) {
      int numCols = relAlgExecUnit_->target_exprs.size();
      partialAggResult_.resize(numCols);
      std::vector<TypePtr> types;
      std::vector<std::string> names;
      for (int i = 0; i < numCols; i++) {
        types.push_back(getVeloxType(
            relAlgExecUnit_->target_exprs[i]->get_type_info().get_type()));
        names.push_back(relAlgExecUnit_->target_exprs[i]->toString());
      }
      rowType_ = std::make_shared<RowType>(std::move(names), std::move(types));

      columns_.resize(numCols);
    }

    // construct and compile a kernel here.
    ciderKernel_ = CiderExecutionKernel::create();
    // todo: we don't have input yet.
    ciderKernel_->compileWorkUnit(*relAlgExecUnit_, buildInputTableInfo());
//    std::cout << "IR: " << ciderKernel_->getLlvmIR() << std::endl;

    // hardcode, init a DataConvertor here.
    dataConvertor_ = DataConvertor::create(CONVERT_TYPE::DIRECT);
  };

  static PlanNodeTranslator planNodeTranslator;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  };

  RowVectorPtr getOutput() override;

  void finish() override {
    Operator::finish();
    std::cout << "conversion takes " << dataConversionCounter.count()  << "us" << std::endl;
    std::cout << "compute takes " << computeCounter.count()  << "us" << std::endl;
    std::cout << "load vector takes " << convertorInternalCounter.count()  << "us" << std::endl;
  }

 private:
  int64_t totalRowsProcessed_ = 0;
  std::shared_ptr<CiderExecutionKernel> ciderKernel_;
  RowVectorPtr result_;

  bool isFilter_ = false;
  bool isAgg_ = false;
  bool isGroupBy_ = false;
  bool isSort_ = false;
  bool isJoin_ = false;
  bool hasData_ = false;

  bool finished_ = false;

  std::shared_ptr<const RowType> rowType_;
  std::vector<VectorPtr> columns_;

  const std::shared_ptr<RelAlgExecutionUnit> relAlgExecUnit_;

  // init this according to input expressions.
  std::vector<int64_t> partialAggResult_;
  RowVectorPtr tmpOut;

  std::shared_ptr<DataConvertor> dataConvertor_;

  std::chrono::microseconds dataConversionCounter;
  std::chrono::microseconds computeCounter;
  std::chrono::microseconds convertorInternalCounter;

  void process();

  std::vector<InputTableInfo> buildInputTableInfo() {
    std::vector<InputTableInfo> query_infos;
    Fragmenter_Namespace::FragmentInfo fi_0;
    fi_0.fragmentId = 0;
    fi_0.shadowNumTuples = 1024;
    //    fi_0.physicalTableId = relAlgExecUnit_->input_descs[0].getTableId();
    fi_0.physicalTableId = 100; // FIXME
    fi_0.setPhysicalNumTuples(1024);

    Fragmenter_Namespace::TableInfo ti_0;
    ti_0.fragments = {fi_0};
    ti_0.setPhysicalNumTuples(1024);

    //    InputTableInfo iti_0{relAlgExecUnit_->input_descs[0].getTableId(),
    //    ti_0};
    InputTableInfo iti_0{100, ti_0};
    query_infos.push_back(iti_0);

    return query_infos;
  }

  VectorPtr convertColumn(
      const TypePtr& vType,
      int8_t* data_buffer,
      int num_rows,
      memory::MemoryPool* pool);

  TypePtr getVeloxType(SQLTypes oType) {
    switch (oType) {
      case SQLTypes::kBOOLEAN:
        return BOOLEAN();
      case SQLTypes::kINT:
        return INTEGER();
      case SQLTypes::kBIGINT:
        return BIGINT();
      case SQLTypes::kFLOAT:
        return REAL();
      case SQLTypes::kDOUBLE:
        return DOUBLE();
      default: {
        std::cerr << "invalid type" << std::endl;
        return nullptr;
      }
    }
  }
};
} // namespace facebook::velox::exec
