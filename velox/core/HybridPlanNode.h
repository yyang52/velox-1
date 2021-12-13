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

#include "velox/core/PlanNode.h"

#include "QueryEngine/RelAlgExecutionUnit.h"

namespace facebook::velox::core {
/**
 * indicate Ops in HybridNode
 */
struct NodeProperty {
  bool hasFilter;
  bool hasProject;
  bool hasAgg;
  bool hasGroupBy;
  bool hasOrderBy;
};

class HybridPlanNode : public PlanNode {
 public:
  HybridPlanNode(
      const PlanNodeId& id,
      const RowTypePtr& outputType,
      const std::shared_ptr<RelAlgExecutionUnit> relAlgExecUnit,
      const std::shared_ptr<NodeProperty> nodeProperty,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id),
        outputType_(outputType),
        relAlgExecUnit_(relAlgExecUnit),
        nodeProperty_(nodeProperty),
        sources_{source} {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const {
    return sources_;
  };

  std::string_view name() const {
    return "hybrid";
  }

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const RowTypePtr outputType_;
  const std::shared_ptr<RelAlgExecutionUnit> relAlgExecUnit_;
  const std::shared_ptr<NodeProperty> nodeProperty_;
};
} // namespace facebook::velox::core
