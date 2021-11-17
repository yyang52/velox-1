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
#include "cider/VeloxToCiderExpr.h"
#include <cstdint>
#include "Shared/sqltypes.h"

using namespace facebook::velox::core;
using facebook::velox::TypeKind;

namespace facebook::velox::cider {

namespace {
// convert velox sqltype to omnisci SQLTypeInfo(SQLTypes t, int d/p, int s,
// bool n, EncodingType c, int p, SQLTypes st)
SQLTypeInfo getCiderType(
    const std::shared_ptr<const velox::Type>& expr_type,
    bool isNullable) {
  switch (expr_type->kind()) {
    case TypeKind::BOOLEAN:
      return SQLTypeInfo(SQLTypes::kBOOLEAN, isNullable);
    case TypeKind::DOUBLE:
      return SQLTypeInfo(SQLTypes::kDOUBLE, isNullable);
    case TypeKind::INTEGER:
      return SQLTypeInfo(SQLTypes::kINT, isNullable);
    case TypeKind::BIGINT:
      return SQLTypeInfo(SQLTypes::kBIGINT, isNullable);
    case TypeKind::TIMESTAMP:
      return SQLTypeInfo(SQLTypes::kTIMESTAMP, isNullable);
    default:
      throw std::runtime_error(
          expr_type->toString() + " is not yet supported.");
  }
}

SQLOps getCiderSqlOps(const std::string op) {
  if (op == "lt") {
    return SQLOps::kLT;
  } else if (op == "gt") {
    return SQLOps::kGT;
  } else if (op == "eq") {
    return SQLOps::kEQ;
  } else if (op == "gte") {
    return SQLOps::kGE;
  } else if (op == "multiply") {
    return SQLOps::kMULTIPLY;
  } else {
    throw std::runtime_error(op + " is not yet supported");
  }
}
} // namespace

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const ITypedExpr> v_expr,
    std::unordered_map<std::string, int> col_info) const {
  if (auto constant =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(v_expr)) {
    return toCiderExpr(constant);
  }
  if (auto field_expr =
          std::dynamic_pointer_cast<const FieldAccessTypedExpr>(v_expr)) {
    return toCiderExpr(field_expr, col_info);
  }
  if (auto call_expr = std::dynamic_pointer_cast<const CallTypedExpr>(v_expr)) {
    return toCiderExpr(call_expr, col_info);
  }
  if (auto cast_expr = std::dynamic_pointer_cast<const CastTypedExpr>(v_expr)) {
    return toCiderExpr(cast_expr, col_info);
  }
  return nullptr;
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const ConstantTypedExpr> v_expr) const {
  auto type_kind = v_expr->type()->kind();
  auto cider_type = getCiderType(v_expr->type(), false);
  auto value = v_expr->value();
  Datum constant_value;
  switch (type_kind) {
    case TypeKind::BOOLEAN:
      constant_value.boolval = value.value<TypeKind::BOOLEAN>();
      return std::make_shared<Analyzer::Constant>(
          cider_type, false, constant_value);
    case TypeKind::DOUBLE:
      constant_value.doubleval = value.value<TypeKind::DOUBLE>();
      return std::make_shared<Analyzer::Constant>(
          cider_type, false, constant_value);
    case TypeKind::INTEGER:
      constant_value.intval = value.value<TypeKind::INTEGER>();
      return std::make_shared<Analyzer::Constant>(
          cider_type, false, constant_value);
    default:
      throw std::runtime_error(
          v_expr->type()->toString() + " is not yet supported.");
  }
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const FieldAccessTypedExpr> v_expr,
    std::unordered_map<std::string, int> col_info) const {
  // inputs for FieldAccessTypedExpr is not useful for omnisci?
  // TODO: no table id info from ConnectorTableHandle, so use 0 for now
  auto it = col_info.find(v_expr->name());
  if (it == col_info.end()) {
    throw std::runtime_error(
        "can't get column index for column " + v_expr->name());
  }
  auto col_index = it->second;
  // TODO: how to determine isNullable? use true for now
  auto col_type = getCiderType(v_expr->type(), true);
  return std::make_shared<Analyzer::ColumnVar>(col_type, 0, col_index, 0);
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const CastTypedExpr> v_expr,
    std::unordered_map<std::string, int> col_info) const {
  // currently only support one expr cast, see Analyzer::UOper
  auto inputs = v_expr->inputs();
  CHECK_EQ(inputs.size(), 1);
  return std::make_shared<Analyzer::UOper>(
    getCiderType(v_expr->type(), false),
    false,
    SQLOps::kCAST,
    toCiderExpr(inputs[0], col_info));
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const CallTypedExpr> v_expr,
    std::unordered_map<std::string, int> col_info) const {
  if (v_expr->name() == "gt" || v_expr->name() == "lt" ||
      v_expr->name() == "eq") {
    auto type = getCiderType(v_expr->type(), false);
    auto inputs = v_expr->inputs();
    CHECK_EQ(inputs.size(), 2);
    SQLQualifier qualifier = SQLQualifier::kONE;
    return std::make_shared<Analyzer::BinOper>(
        type,
        false,
        getCiderSqlOps(v_expr->name()),
        qualifier,
        toCiderExpr(inputs[0], col_info),
        toCiderExpr(inputs[1], col_info));
  }
  // between needs change from between(ROW["c1"],0.6,1.6)
  // to AND(GE(ROW['c1'], 0.6), LE(ROW['c1'], 1.6))
  // TODO: what about timestamp type?
  if (v_expr->name() == "between") {
    auto type = getCiderType(v_expr->type(), false);
    // should have 3 inputs
    auto inputs = v_expr->inputs();
    CHECK_EQ(inputs.size(), 3);
    SQLQualifier qualifier = SQLQualifier::kONE;
    return std::make_shared<Analyzer::BinOper>(
        type,
        false,
        SQLOps::kAND,
        qualifier,
        std::make_shared<Analyzer::BinOper>(
            type,
            false,
            SQLOps::kGE,
            qualifier,
            toCiderExpr(inputs[0], col_info),
            toCiderExpr(inputs[1], col_info)),
        std::make_shared<Analyzer::BinOper>(
            type,
            false,
            SQLOps::kLE,
            qualifier,
            toCiderExpr(inputs[0], col_info),
            toCiderExpr(inputs[2], col_info)));
  }
  return nullptr;
}
} // namespace facebook::velox::cider
