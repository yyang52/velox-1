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

namespace facebook::velox::cider {

namespace {

std::list<std::shared_ptr<const InputColDescriptor>> getInputColDescs(
    const std::shared_ptr<const velox::core::PlanNode>& node) {
  std::list<std::shared_ptr<const InputColDescriptor>> inputColDescs;
  // we can get column info(name, columnIndex, type) from parent node
  // outputvariables but here for omnisci, the index is default using
  // 0, 1...output_variables.size()-1 column type will be tracked in exprs
  // TODO: need handle complex outputType, like struct
  auto rowType = node->outputType();
  for (int i = 0; i < rowType->size(); i++) {
    // TODO: we use table_id 100(avoid 0 which used in groupby Var)
    // and nest_level 0 for input
    // table, which may need further evaluation
    inputColDescs.emplace_back(
        std::make_shared<const InputColDescriptor>(i, 100, 0));
  }
  return inputColDescs;
}

std::unordered_map<std::string, int> getColInfoFromSource(
    const std::shared_ptr<const velox::core::PlanNode>& node) {
  auto colNames = node->outputType()->names();

  std::unordered_map<std::string, int> colInfo;
  // auto col_types = rowType ->children();
  for (int i = 0; i < colNames.size(); i++) {
    colInfo.emplace(colNames[i], i);
  }
  return colInfo;
}

bool isSourceNode(const std::shared_ptr<const velox::core::PlanNode>& node) {
  if (std::dynamic_pointer_cast<const velox::core::TableScanNode>(node) ||
      std::dynamic_pointer_cast<const velox::core::ValuesNode>(node)) {
    return true;
  }
  return false;
}

} // namespace

std::shared_ptr<velox::core::PlanNode>
CiderExecutionUnitGenerator::transformPlan(
    const std::shared_ptr<velox::core::PlanNode> planNode) {
  // For output node, keep it as it is and update its source
  if (const auto paritionedOutputNode =
          std::dynamic_pointer_cast<const PartitionedOutputNode>(planNode)) {
    // Hybrid cider node should have same output as outputNode's source, with
    // planId "0"
    auto ciderParamContext = std::make_shared<CiderParamContext>(
        paritionedOutputNode->sources()[0]->outputType(), "0");
    // Deep Copy Output node with planId "1"
    return std::make_shared<PartitionedOutputNode>(
        "1",
        paritionedOutputNode->keys(),
        paritionedOutputNode->numPartitions(),
        paritionedOutputNode->isBroadcast(),
        paritionedOutputNode->isReplicateNullsAndAny(),
        paritionedOutputNode->partitionFunctionFactory(),
        paritionedOutputNode->outputType(),
        transformPlanInternal(
            ciderParamContext, paritionedOutputNode->sources()[0]));
  }
  throw std::runtime_error("Unsupported output node");
}

std::shared_ptr<const velox::core::PlanNode>
CiderExecutionUnitGenerator::transformPlanInternal(
    std::shared_ptr<CiderParamContext> ctx,
    const std::shared_ptr<const PlanNode> current) {
  // TODO (Cheng): need to support join nodes???

  // For source nodes, keep it as it is but still needs its meta info for
  // previous generated metadata. And one new hybrid plan node is built with
  // this updated exec unit info.
  if (isSourceNode(current)) {
    if (ctx->targetExprMap_.size() != 0) {
      // fake table with id 100
      std::vector<InputDescriptor> inputDescs;
      inputDescs.emplace_back(100, 0);
      ctx->inputDescs_ = inputDescs;
      // FIXME: How to update plan ID info?
      // Still requiring table scan info to generate enough for
      // RelAlgExecutionUnit
      return std::dynamic_pointer_cast<const facebook::velox::core::PlanNode>(
          std::make_shared<velox::core::HybridPlanNode>(
              ctx->id_, ctx->rowType_, ctx, current));
    }
    return current;
  }

  // For supported nodes, create or update its meta info
  if (const auto filter =
          std::dynamic_pointer_cast<const velox::core::FilterNode>(current)) {
    updateCiderParamCtx(filter, ctx);
    if (!translatable_) {
      throw std::runtime_error("Failed to translate filter condition.");
    }
    return transformPlanInternal(ctx, current->sources()[0]);
  }
  if (const auto project =
          std::dynamic_pointer_cast<const velox::core::ProjectNode>(current)) {
    updateCiderParamCtx(project, ctx);
    if (!translatable_) {
      throw std::runtime_error("Failed to translate projects.");
    }
    return transformPlanInternal(ctx, current->sources()[0]);
  }
  if (const auto aggregation =
          std::dynamic_pointer_cast<const velox::core::AggregationNode>(
              current)) {
    updateCiderParamCtx(aggregation, ctx);
    if (!translatable_) {
      throw std::runtime_error("Failed to translate aggregates.");
    }
    return transformPlanInternal(ctx, current->sources()[0]);
  }
  if (const auto orderBy =
          std::dynamic_pointer_cast<const velox::core::OrderByNode>(current)) {
    updateCiderParamCtx(orderBy, ctx);
    if (!translatable_) {
      throw std::runtime_error("Failed to translate orderBy.");
    }
    return transformPlanInternal(ctx, current->sources()[0]);
  }
  // Fallback part: if non-source, non-supported ops, just return. if execUnit
  // exists, build up a execUnit for now.
  // TODO: just throw an exception here for now
  throw std::runtime_error("Unsupported Plan Node");
}

void CiderExecutionUnitGenerator::updateCiderParamCtx(
    const std::shared_ptr<const velox::core::FilterNode>& node,
    std::shared_ptr<CiderParamContext> ctx) {
  ctx->nodeProperty_.hasFilter = true;
  // update inputColDescriptors based on source node
  ctx->inputColDescs_ = getInputColDescs(node->sources()[0]);
  // get column info from source node, for velox::TableScanNode, outputed
  // columns are actually tracked in its outputType
  auto sources = node->sources();
  CHECK_EQ(sources.size(), 1);
  std::unordered_map<std::string, int> colInfo;
  // current only support TableScan as filter's source
  if (std::dynamic_pointer_cast<const TableScanNode>(sources[0]) ||
      std::dynamic_pointer_cast<const ValuesNode>(sources[0])) {
    colInfo = getColInfoFromSource(sources[0]);
  } else {
    throw std::runtime_error(
        "current only support TableScan/ValuesNode as filter's source");
  }
  auto ciderExpr = ciderExprConverter_.toCiderExpr(node->filter(), colInfo);
  if (!ciderExpr) {
    translatable_ = false;
    return;
  }
  auto qualsCf = qual_to_conjunctive_form(fold_expr(ciderExpr.get()));
  ctx->simpleQuals_ = qualsCf.simple_quals;
  ctx->quals_ = qualsCf.quals;
  // add data source columns as targetExprs if there is no followed plan node
  if (ctx->targetExprMap_.size() == 0) {
    auto outputType = node->outputType();
    for (int i = 0; i < outputType->size(); i++) {
      auto colExpr = std::make_shared<FieldAccessTypedExpr>(
          outputType->childAt(i), outputType->nameOf(i));
      auto ciderExpr = ciderExprConverter_.toCiderExpr(
          std::dynamic_pointer_cast<ITypedExpr>(colExpr), colInfo);
      ctx->targetExprMap_.emplace_back(outputType->nameOf(i), ciderExpr);
    }
  }
}

void CiderExecutionUnitGenerator::updateCiderParamCtx(
    const std::shared_ptr<const velox::core::ProjectNode>& node,
    std::shared_ptr<CiderParamContext> ctx) {
  ctx->nodeProperty_.hasProject = true;
  // update inputColDescriptors based on source node
  ctx->inputColDescs_ = getInputColDescs(node->sources()[0]);
  // in project+agg case, need construct a map targetExprs since
  // omnisci doesn't use project masks so that we need pass down the real exprs
  // instead, such as c1*c2 as e1, we will put generate both c1*c2 and e1
  // Ananlyzer::Expr collect colInfo for expr translation

  // using a map<mask, expr> for tracking targetExprs which will need in later
  // nodes
  auto colInfo = getColInfoFromSource(node->sources()[0]);
  for (int i = 0; i < node->names().size(); i++) {
    auto targetExpr =
        ciderExprConverter_.toCiderExpr(node->projections()[i], colInfo);
    if (!targetExpr) {
      translatable_ = false;
      return;
    }
    auto exprName = node->names()[i];
    if (std::dynamic_pointer_cast<const CallTypedExpr>(
            node->projections()[i])) {
      // case sum(abs(x/y)), project need CAST info based on project Op in
      // Omnisci for callTypedExpr
      auto wrappedExpr = ciderExprConverter_.wrapExprWithCast(
          targetExpr, node->outputType()->childAt(i));
      updateExprMap(ctx, exprName, wrappedExpr, i);
    } else {
      updateExprMap(ctx, exprName, targetExpr, i);
    }
  }
}

void CiderExecutionUnitGenerator::updateCiderParamCtx(
    const std::shared_ptr<const velox::core::AggregationNode>& node,
    std::shared_ptr<CiderParamContext> ctx) {
  ctx->nodeProperty_.hasAgg = true;
  // update inputColDescriptors based on source node
  ctx->inputColDescs_ = getInputColDescs(node->sources()[0]);
  // TODO: group-key not covered yet, which should update groupbyExprs
  // TODO: agg mask not covered since PlanBuilder can't build such a
  // semantic yet
  // TODO: how to handle ignoreNullKeys(true except in distinct case)? what are
  // the aggregateNames? agg node will actually do update on the targetExprs

  auto colInfo = getColInfoFromSource(node->sources()[0]);
  CHECK_EQ(node->step(), core::AggregationNode::Step::kPartial);
  // add group keys(FieldAccessTypedExpr) as target_exprs
  int i = 1;
  for (auto& key : node->groupingKeys()) {
    ctx->nodeProperty_.hasGroupBy = true;
    auto field =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(key);
    VELOX_CHECK(field, "Grouping key must be a field reference");
    auto ciderExpr = ciderExprConverter_.toCiderExpr(
        std::dynamic_pointer_cast<const ITypedExpr>(field), colInfo);
    // will need update this if groupby key is original column of input table
    // for magic number here, refer var_ref() in Analyzer.h
    // here groupbyExpr use default table ID 0
    auto groupbyExpr = std::make_shared<Analyzer::Var>(
        ciderExpr->get_type_info(), 0, 0, -1, Analyzer::Var::kGROUPBY, i++);
    // groupByExprMap need columnVar while targetExpr need Var
    ctx->groupByExprMap_.push_back(std::make_pair(field->name(), ciderExpr));
    ctx->targetExprMap_.push_back(std::make_pair(field->name(), groupbyExpr));
  }
  for (auto aggregate : node->aggregates()) {
    auto expr = std::dynamic_pointer_cast<const ITypedExpr>(aggregate);
    auto ciderAgg = ciderExprConverter_.toCiderExpr(expr, colInfo);
    if (!ciderAgg) {
      translatable_ = false;
      return;
    }
    if (auto aggExpr = std::dynamic_pointer_cast<Analyzer::AggExpr>(ciderAgg)) {
      // store the aggExpr and columnVar name in the map
      if (auto aggField = std::dynamic_pointer_cast<const FieldAccessTypedExpr>(
              aggregate->inputs()[0])) {
        ctx->targetExprMap_.push_back(
            std::make_pair(aggField->name(), aggExpr));
      }
    } else {
      std::runtime_error("Error happened in agg target expr update");
    }
  }
}

void CiderExecutionUnitGenerator::updateCiderParamCtx(
    const std::shared_ptr<const velox::core::OrderByNode>& node,
    std::shared_ptr<CiderParamContext> ctx) {
  ctx->nodeProperty_.hasOrderBy = true;
  // TODO: how to handle isPartial?
  auto colInfo = getColInfoFromSource(node->sources()[0]);
  // refer get_order_entries() in RelAlgExecutor.cpp
  std::list<Analyzer::OrderEntry> result;
  for (size_t i = 0; i < node->sortingKeys().size(); ++i) {
    const auto sortField = node->sortingKeys()[i];
    const auto sortOrder = node->sortingOrders()[i];
    result.emplace_back(
        colInfo[sortField->name()] + 1,
        !sortOrder.isAscending(),
        sortOrder.isNullsFirst());
  }
  ctx->orderByCollation_ = result;
}

// update the exprMaps: ctx->targetExprMap_, ctx->groupByExprMap_,
// orderByExprMap
void CiderExecutionUnitGenerator::updateExprMap(
    std::shared_ptr<CiderParamContext> ctx,
    const std::string exprKey,
    std::shared_ptr<Analyzer::Expr>& expr,
    int index) {
  // replace child expr based on the string key, such as sum(e1) -> sum(x+y),
  // type order of exprs is important, need matched with final output
  // update ctx->targetExprMap_
  bool hasMatched = false;
  for (auto it = ctx->targetExprMap_.begin(); it != ctx->targetExprMap_.end();
       ++it) {
    if (it->first == exprKey) {
      if (auto aggExpr =
              std::dynamic_pointer_cast<Analyzer::AggExpr>(it->second)) {
        it->second = std::make_shared<Analyzer::AggExpr>(
            aggExpr->get_type_info(),
            aggExpr->get_aggtype(),
            expr,
            false,
            aggExpr->get_arg1());
        hasMatched = true;
        continue;
      }
      // if this project related with groupby keys, update targetExprMap_ only
      // when the groupby key is col
      auto varExpr = std::dynamic_pointer_cast<Analyzer::Var>(it->second);
      auto colExpr = std::dynamic_pointer_cast<Analyzer::ColumnVar>(expr);
      if (varExpr && colExpr) {
        it->second = std::make_shared<Analyzer::Var>(
            varExpr->get_type_info(),
            colExpr->get_table_id(),
            colExpr->get_column_id(),
            colExpr->get_rte_idx(),
            varExpr->get_which_row(),
            varExpr->get_varno());
        hasMatched = true;
        continue;
      }
      if (varExpr && !colExpr) {
        hasMatched = true;
        continue;
      }
      throw std::runtime_error(
          "detected replacable expr but need further support.");
    }
  }
  // insert this new target expr
  if (!hasMatched) {
    ctx->targetExprMap_.insert(
        ctx->targetExprMap_.begin() + index, std::make_pair(exprKey, expr));
  }
  // update ctx->groupByExprMap_
  for (auto it = ctx->groupByExprMap_.begin(); it != ctx->groupByExprMap_.end();
       ++it) {
    if (it->first == exprKey) {
      // for groupby key expression, use new expr since it's closer to input
      // cols
      it->second = expr;
    }
  }
  // update orderByExprMap
}
} // namespace facebook::velox::cider
