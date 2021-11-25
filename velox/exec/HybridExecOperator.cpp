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
    return std::make_unique<HybridExecOperator>(id, ctx, hybridOp);
  }
  return nullptr;
};

bool HybridExecOperator::needsInput() const {
  // TODO
  return !input_;
}

void HybridExecOperator::addInput(RowVectorPtr input) {
  // TODO
  input_ = std::move(input);
  process();
}

RowVectorPtr HybridExecOperator::getOutput() {
  return std::move(result_);
}

void HybridExecOperator::process() {
  // 1. convert data
  // Convertor.V2O

  // 2. call ciderKernel_ run
  // ciderKernel_->runWithData();

  // 3. convert data
  // result_ = Convertor.O2V()

  // for now, we just steal input_
  totalRowsProcessed_ += input_->size();
  result_ = std::move(input_);

}
} // namespace facebook::velox::exec