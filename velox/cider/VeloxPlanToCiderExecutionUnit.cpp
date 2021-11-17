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

#include "cider/VeloxPlanToCiderExecutionUnit.h"
#include "QueryEngine/Descriptors/ColSlotContext.h"
#include "QueryEngine/Descriptors/InputDescriptors.h"
#include "QueryEngine/ExpressionRewrite.h"
#include "QueryEngine/QueryHint.h"
#include "QueryEngine/RelAlgTranslator.h"

using namespace facebook::velox;

namespace facebook::velox::cider {

namespace {
std::vector<Analyzer::Expr*> getTargetExprsFromTableScanNode(
    const RowTypePtr& row_type,
    VeloxToCiderExprConverter& ciderExprConverter) {
  auto output_names = row_type->names();
  auto output_types = row_type->children();
  CHECK_GE(output_names.size(), 1);
  std::vector<Analyzer::Expr*> target_exprs;
  std::unordered_map<std::string, int> col_info;
  for (int i = 0; i < output_names.size(); i++) {
    col_info.emplace(output_names[i], i);
  }
  for (int i = 0; i < output_names.size(); i++) {
    auto col_expr = std::make_shared<FieldAccessTypedExpr>(
        output_types[i], output_names[i]);

    Analyzer::Expr* target_expr =
        ciderExprConverter
            .toCiderExpr(
                std::dynamic_pointer_cast<ITypedExpr>(col_expr), col_info)
            .get();
    target_exprs.emplace_back((Analyzer::Expr*)&target_expr);
  }
  return target_exprs;
}
} // namespace

std::shared_ptr<RelAlgExecutionUnit>
CiderExecutionUnitGenerator::createExecutionUnit(
    const std::shared_ptr<const velox::core::PlanNode>& node) {
  std::shared_ptr<RelAlgExecutionUnit> exe_unit;
  // put plan nodes into a vector and analyze each of them
  std::vector<std::shared_ptr<const velox::core::PlanNode>> nodes;
  std::shared_ptr<const velox::core::PlanNode> temp = node;
  // not considering multi-source such as join
  while (node->sources().size() != 0 && temp ->sources().size() != 0) {
    nodes.emplace_back(temp);
    const auto sources = temp->sources();
    // TODO: join case?
    //CHECK_EQ(sources.size(), 1);
    temp = sources[0];
  }
  nodes.emplace_back(node);
  for (int i = nodes.size() - 1; i > 0; i--) {
    if (const auto table_scan =
            std::dynamic_pointer_cast<const velox::core::TableScanNode>(
                nodes[i])) {
      createOrUpdateExecutionUnit(table_scan, exe_unit);
    }
    if (const auto filter =
            std::dynamic_pointer_cast<const velox::core::FilterNode>(
                nodes[i])) {
      createOrUpdateExecutionUnit(filter, exe_unit);
    }
    if (const auto project =
            std::dynamic_pointer_cast<const velox::core::ProjectNode>(
                nodes[i])) {
      createOrUpdateExecutionUnit(project, exe_unit);
    }
    if (const auto aggregation =
            std::dynamic_pointer_cast<const velox::core::AggregationNode>(
                nodes[i])) {
      createOrUpdateExecutionUnit(aggregation, exe_unit);
    }
  }
  return exe_unit;
}
void CiderExecutionUnitGenerator::createOrUpdateExecutionUnit(
    const std::shared_ptr<const velox::core::TableScanNode>& node,
    std::shared_ptr<RelAlgExecutionUnit>& exe_unit) {
  // get input_descs and input_column_descs from velox TableScanNode
  std::vector<InputDescriptor> input_descs;
  // give a dummy table_id and nest_level
  input_descs.emplace_back(0, 0);
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  // we can get column info(name, columnIndex, type) from tableScan
  // outputvariables but here for omnisci, the index is default using
  // 0, 1...output_variables.size()-1 column type will be tracked in exprs
  auto rowType = node->outputType();
  for (int i = 0; i < rowType->size(); i++) {
    input_col_descs.emplace_back(
        std::make_shared<const InputColDescriptor>(i, 0, 0));
  }
  // TableScanNode should firstly create exec_unit in velox PlanFragment
  VELOX_CHECK_NULL(exe_unit);
  auto quals_cf = QualsConjunctiveForm{};
  std::shared_ptr<Analyzer::Expr> empty;
  std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
  groupby_exprs.emplace_back(empty);
  // generate target_exprs which may update in later nodes
  auto target_exprs =
      getTargetExprsFromTableScanNode(node->outputType(), ciderExprConverter_);
  JoinQualsPerNestingLevel left_deep_join_quals;
  // use nullptr for query_state_;
  exe_unit = std::make_shared<RelAlgExecutionUnit>(RelAlgExecutionUnit{
      input_descs,
      input_col_descs,
      quals_cf.simple_quals,
      quals_cf.quals,
      left_deep_join_quals,
      groupby_exprs,
      target_exprs,
      nullptr,
      {{}, SortAlgorithm::Default, 0, 0},
      0,
      RegisteredQueryHint::defaults(),
      EMPTY_QUERY_PLAN,
      {},
      {},
      false,
      std::nullopt,
      nullptr});
}

void CiderExecutionUnitGenerator::createOrUpdateExecutionUnit(
    const std::shared_ptr<const velox::core::FilterNode>& node,
    std::shared_ptr<RelAlgExecutionUnit>& exe_unit) {
  CHECK_NOTNULL(exe_unit);
  // get column info from source node, for velox::TableScanNode, outputed
  // columns are actually tracked in its outputType
  auto sources = node->sources();
  CHECK_EQ(sources.size(), 1);
  std::unordered_map<std::string, int> col_info;
  // current only support TableScan as filter's source
  if (auto table_scan =
          std::dynamic_pointer_cast<const TableScanNode>(sources[0])) {
    auto row_type = table_scan->outputType();
    auto col_names = row_type->names();
    // auto col_types = row_type ->children();
    for (int i = 0; i < col_names.size(); i++) {
      col_info.emplace(col_names[i], i);
    }
  } else if (auto values_node = std::dynamic_pointer_cast<const ValuesNode>(sources[0])) {
    auto row_type = values_node->outputType();
    auto col_names = row_type->names();
    for (int i = 0; i < col_names.size(); i++) {
      col_info.emplace(col_names[i], i);
    }
  } else {
    throw std::runtime_error(
        "current only support TableScan/ValuesNode as filter's source");
  }
  auto cider_expr = ciderExprConverter_.toCiderExpr(node->filter(), col_info);
  CHECK_NOTNULL(cider_expr);
  const auto quals_cf = qual_to_conjunctive_form(fold_expr(cider_expr.get()));
  exe_unit->simple_quals = quals_cf.simple_quals;
  exe_unit->quals = quals_cf.quals;
}

void CiderExecutionUnitGenerator::createOrUpdateExecutionUnit(
    const std::shared_ptr<const velox::core::ProjectNode>& node,
    std::shared_ptr<RelAlgExecutionUnit>& exe_unit) {
  std::cout << "just for test";
}

void CiderExecutionUnitGenerator::createOrUpdateExecutionUnit(
    const std::shared_ptr<const velox::core::AggregationNode>& node,
    std::shared_ptr<RelAlgExecutionUnit>& exe_unit) {
  std::cout << "just for test";
}

} // namespace facebook::velox::cider
