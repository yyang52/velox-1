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

// FIXME: include order matters since omnisci header file is not clean yet and
// may define some dirty macro which will influence velox code base, so we put
// it at the end of include chain. This is just a work around, if some further
// code change have similar issue, best way is make header file cleaner.
#include "Cider/CiderExecutionKernel.h"
#include "QueryEngine/RelAlgExecutionUnit.h"

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
            "hybrid") {
    // construct and compile a kernel here.
    auto exeUnit =
        hybridPlanNode->getCiderParamContext()->getExeUnitBasedOnContext();
    ciderKernel_ = CiderExecutionKernel::create();
    // todo: we don't have input yet.
    // ciderKernel_->compileWorkUnit();
  };

  static PlanNodeTranslator planNodeTranslator;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  };

  RowVectorPtr getOutput() override;

 private:
  int64_t totalRowsProcessed_ = 0;
  std::shared_ptr<CiderExecutionKernel> ciderKernel_;
  RowVectorPtr result_;

  void process();
};
} // namespace facebook::velox::exec
