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

#include "QueryEngine/RelAlgExecutionUnit.h"
#include "velox/core/PlanNode.h"

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

struct CiderParamContext {
  CiderParamContext(RowTypePtr rowType, PlanNodeId id)
      : rowType_(rowType),
        id_(id),
        inputDescs_{},
        inputColDescs_{},
        simpleQuals_{},
        quals_{},
        targetExprMap_{},
        groupByExprMap_{},
        orderByCollation_{},
        limit_(0),
        offset_(0),
        nodeProperty_({false, false, false, false, false}) {}

  RowTypePtr rowType_;
  PlanNodeId id_;
  std::vector<InputDescriptor> inputDescs_;
  std::list<std::shared_ptr<const InputColDescriptor>> inputColDescs_;
  std::list<std::shared_ptr<Analyzer::Expr>> simpleQuals_;
  std::list<std::shared_ptr<Analyzer::Expr>> quals_;
  std::vector<std::pair<std::string, std::shared_ptr<Analyzer::Expr>>>
      targetExprMap_;
  std::vector<std::pair<std::string, std::shared_ptr<Analyzer::Expr>>>
      groupByExprMap_;
  std::list<Analyzer::OrderEntry> orderByCollation_;
  size_t limit_;
  size_t offset_;
  NodeProperty nodeProperty_;
  SortInfo getSortInfoFromCtx() {
    if (orderByCollation_.size() == 0) {
      return {{}, SortAlgorithm::Default, 0, 0};
    } else {
      return {
          orderByCollation_,
          SortAlgorithm::SpeculativeTopN,
          limit_,
          offset_};
    }
  }
  std::shared_ptr<RelAlgExecutionUnit> getExeUnitBasedOnContext() {
    std::vector<Analyzer::Expr*> targetExprs;
    std::list<std::shared_ptr<Analyzer::Expr>> groupByExprs;
    for (auto it : targetExprMap_) {
      targetExprs.push_back(it.second.get());
    }
    if (targetExprs.size() == 0) {
      return nullptr;
    }
    for (auto it : groupByExprMap_) {
      groupByExprs.push_back(it.second);
    }
    if (groupByExprs.size() == 0) {
      std::shared_ptr<Analyzer::Expr> empty;
      groupByExprs.emplace_back(empty);
    }
    JoinQualsPerNestingLevel leftDeepJoinQuals;
    return std::make_shared<RelAlgExecutionUnit>(RelAlgExecutionUnit{
        inputDescs_,
        inputColDescs_,
        simpleQuals_,
        quals_,
        leftDeepJoinQuals,
        groupByExprs,
        targetExprs,
        nullptr,
        getSortInfoFromCtx(),
        0,
        RegisteredQueryHint::defaults(),
        EMPTY_QUERY_PLAN,
        {},
        {},
        false,
        std::nullopt,
        nullptr});
  }
};

class HybridPlanNode : public PlanNode {
 public:
  HybridPlanNode(
      const PlanNodeId& id,
      const RowTypePtr& outputType,
      const std::shared_ptr<CiderParamContext> ciderParamContext,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id),
        outputType_(outputType),
        ciderParamContext_(ciderParamContext),
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

  const std::shared_ptr<CiderParamContext> getCiderParamContext() const {
    return ciderParamContext_;
  }

 private:
  const RowTypePtr outputType_;
  const std::shared_ptr<CiderParamContext> ciderParamContext_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
};
} // namespace facebook::velox::core
