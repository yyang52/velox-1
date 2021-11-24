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

#include "QueryEngine/RelAlgExecutionUnit.h"
#include "cider/VeloxToCiderExpr.h"
#include "core/PlanNode.h"

namespace facebook::velox::cider {

constexpr char const* EMPTY_QUERY_PLAN = "";
// size_t g_default_max_groups_buffer_entry_guess{16384};

class CiderExecutionUnitGenerator {
 public:
  explicit CiderExecutionUnitGenerator()
      : ciderExprConverter_(), exprMaskMap_{} {}

  std::shared_ptr<RelAlgExecutionUnit> createExecutionUnit(
      const std::shared_ptr<const velox::core::PlanNode>& node);

 private:
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
