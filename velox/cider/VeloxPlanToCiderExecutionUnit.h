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

#include "core/PlanNode.h"
#include "core/HybridPlanNode.h"

#include "cider/VeloxToCiderExpr.h"

#include "QueryEngine/RelAlgExecutionUnit.h"

namespace facebook::velox::cider {

constexpr char const* EMPTY_QUERY_PLAN = "";
// size_t g_default_max_groups_buffer_entry_guess{16384};

/**
 * To keep state of the last node, assuming the output type should be the same
 * as the 1st node
 */
struct CiderParamContext {
  CiderParamContext(RowTypePtr rowType, PlanNodeId id)
      : rowType_(rowType), id_(id) {}

  RowTypePtr rowType_;
  PlanNodeId id_;
};

class CiderExecutionUnitGenerator {
 public:
  explicit CiderExecutionUnitGenerator()
      : ciderExprConverter_(), exprMaskMap_{} {}

  /**
   *   Keep original output node and source node from Velox
   *   Translate other plan node into RelAlg as much as possible
   *   FIXME(Cheng): what if one node failed to be converted, do we still need
   * jump to source
   */
  std::shared_ptr<velox::core::PlanNode> transformPlan(
      const std::shared_ptr<velox::core::PlanNode> planNode);

  std::shared_ptr<RelAlgExecutionUnit> createExecutionUnit(
      const std::shared_ptr<const velox::core::PlanNode>& node);

 private:
  std::shared_ptr<const velox::core::PlanNode> transformPlanInternal(
      std::shared_ptr<CiderParamContext> ctx,
      const std::shared_ptr<const velox::core::PlanNode> current,
      std::shared_ptr<RelAlgExecutionUnit> execUnit);

  void createOrUpdateExecutionUnit(
      const std::shared_ptr<const velox::core::TableScanNode>& node,
      std::shared_ptr<RelAlgExecutionUnit>& exe_unit);

  void createOrUpdateExecutionUnit(
      const std::shared_ptr<const velox::core::ValuesNode>& node,
      std::shared_ptr<RelAlgExecutionUnit>& exe_unit);

  void createOrUpdateExecutionUnit(
      const std::shared_ptr<const velox::core::FilterNode>& node,
      std::shared_ptr<RelAlgExecutionUnit>& exe_unit);

  void createOrUpdateExecutionUnit(
      const std::shared_ptr<const velox::core::ProjectNode>& node,
      std::shared_ptr<RelAlgExecutionUnit>& exe_unit);

  void createOrUpdateExecutionUnit(
      const std::shared_ptr<const velox::core::AggregationNode>& node,
      std::shared_ptr<RelAlgExecutionUnit>& exe_unit);

  VeloxToCiderExprConverter ciderExprConverter_;
  std::vector<std::pair<std::string, std::shared_ptr<Analyzer::Expr>>>
      exprMaskMap_;
};

} // namespace facebook::velox::cider
