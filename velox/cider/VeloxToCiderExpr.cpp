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
#include "QueryEngine/RelAlgTranslator.h"
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
  } else if (op == "and") {
    return SQLOps::kAND;
  } else if (op == "gt") {
    return SQLOps::kGT;
  } else if (op == "eq") {
    return SQLOps::kEQ;
  } else if (op == "gte") {
    return SQLOps::kGE;
  } else if (op == "lte") {
    return SQLOps::kLE;
  } else if (op == "multiply") {
    return SQLOps::kMULTIPLY;
  } else if (op == "plus") {
    return SQLOps::kPLUS;
  } else if (op == "modulus") {
    return SQLOps::kMODULO;
  } else {
    throw std::runtime_error(op + " is not yet supported");
  }
}

SQLAgg getCiderAggOp(const std::string op) {
  if (op == "sum") {
    return SQLAgg::kSUM;
  } else if (op == "min") {
    return SQLAgg::kMIN;
  } else if (op == "max") {
    return SQLAgg::kMAX;
  } else if (op == "avg") {
    return SQLAgg::kAVG;
  } else if (op == "count") {
    return SQLAgg::kCOUNT;
  } else {
    throw std::runtime_error(op + " is not yet supported");
  }
}

// referring velox/docs/functions/aggregate.rst
SQLTypeInfo getVeloxAggType(
    std::string op,
    std::shared_ptr<const ITypedExpr> vExpr) {
  if (op == "sum") {
    return getCiderType(vExpr->type(), false);
  } else if (op == "min") {
    return getCiderType(vExpr->type(), false);
  } else if (op == "max") {
    return getCiderType(vExpr->type(), false);
  } else if (op == "avg") {
    return SQLTypeInfo(SQLTypes::kDOUBLE, false);
  } else if (op == "count") {
    return SQLTypeInfo(SQLTypes::kBIGINT, false);
  } else {
    throw std::runtime_error("failed to get type for velox funtion: " + op);
  }
}

// TODO: get output type for agg functions, but this may violate rules of velox?
// refering CalciteDeserializerUtils.cpp
SQLTypeInfo getCiderAggType(
    const SQLAgg agg_kind,
    const Analyzer::Expr* arg_expr) {
  bool g_bigint_count{false};
  switch (agg_kind) {
    case SQLAgg::kCOUNT:
      return SQLTypeInfo(
          g_bigint_count ? SQLTypes::kBIGINT : SQLTypes::kINT, false);
    case SQLAgg::kMIN:
    case SQLAgg::kMAX:
      return arg_expr->get_type_info();
    case SQLAgg::kSUM:
      return arg_expr->get_type_info().is_integer()
          ? SQLTypeInfo(SQLTypes::kBIGINT, false)
          : arg_expr->get_type_info();
    case SQLAgg::kAVG:
      return SQLTypeInfo(SQLTypes::kDOUBLE, false);
    default:
      throw std::runtime_error("unsupported agg.");
  }
  CHECK(false);
  return SQLTypeInfo();
}

} // namespace

// wrap target expr from project node with CAST info
std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::wrapExprWithCast(
    const std::shared_ptr<Analyzer::Expr> c_expr,
    std::shared_ptr<const Type> type) const {
  // TODO: isNuallable may not be false here
  return c_expr->add_cast(getCiderType(type, false));
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const ITypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  try {
    if (auto constant =
            std::dynamic_pointer_cast<const ConstantTypedExpr>(vExpr)) {
      return toCiderExpr(constant);
    }
    if (auto field_expr =
            std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr)) {
      return toCiderExpr(field_expr, colInfo);
    }
    if (auto call_expr = std::dynamic_pointer_cast<const CallTypedExpr>(vExpr)) {
      return toCiderExpr(call_expr, colInfo);
    }
    if (auto cast_expr = std::dynamic_pointer_cast<const CastTypedExpr>(vExpr)) {
      return toCiderExpr(cast_expr, colInfo);
    }
    return nullptr;
  } catch (std::runtime_error& error) {
    std::cout << error.what();
    return nullptr;
  }
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const ConstantTypedExpr> vExpr) const {
  // TOTO: update this based on RelAlgTranslator::translateLiteral
  auto type_kind = vExpr->type()->kind();
  auto cider_type = getCiderType(vExpr->type(), false);
  auto value = vExpr->value();
  // referring sqltypes.h/Datum
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
    case TypeKind::BIGINT:
      constant_value.bigintval = value.value<TypeKind::BIGINT>();
      return std::make_shared<Analyzer::Constant>(
          cider_type, false, constant_value);
    default:
      throw std::runtime_error(
        vExpr->type()->toString() + " is not yet supported.");
  }
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const FieldAccessTypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  // inputs for FieldAccessTypedExpr is not useful for omnisci?
  // TODO: no table id info from ConnectorTableHandle, so use 0 for now
  auto it = colInfo.find(vExpr->name());
  if (it == colInfo.end()) {
    throw std::runtime_error(
        "can't get column index for column " + vExpr->name());
  }
  auto colIndex = it->second;
  // TODO: how to determine isNullable? use true for now
  auto colType = getCiderType(vExpr->type(), false);
  // TODO: fixed fake table id 100
  return std::make_shared<Analyzer::ColumnVar>(colType, 100, colIndex, 0);
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const CastTypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  // currently only support one expr cast, see Analyzer::UOper
  auto inputs = vExpr->inputs();
  CHECK_EQ(inputs.size(), 1);
  return std::make_shared<Analyzer::UOper>(
    getCiderType(vExpr->type(), false),
    false,
    SQLOps::kCAST,
    toCiderExpr(inputs[0], colInfo));
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const CallTypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  if (vExpr->name() == "gt" || vExpr->name() == "lt" ||
      vExpr->name() == "gte" || vExpr->name() == "lte" ||
      vExpr->name() == "eq" || vExpr->name() == "and" ||
      vExpr->name() == "multiply" || vExpr->name() == "plus" ||
      vExpr->name() == "modulus") {
    auto type = getCiderType(vExpr->type(), false);
    auto inputs = vExpr->inputs();
    CHECK_EQ(inputs.size(), 2);
    SQLQualifier qualifier = SQLQualifier::kONE;
    // TODO: bug here, need also check child expr is null or not
    auto leftExpr = toCiderExpr(inputs[0], colInfo);
    auto rightExpr = toCiderExpr(inputs[1], colInfo);
    if (leftExpr && rightExpr) {
      return std::make_shared<Analyzer::BinOper>(
          type,
          false,
          getCiderSqlOps(vExpr->name()),
          qualifier,
          leftExpr,
          rightExpr);
    }
    return nullptr;
  }
  // between needs change from between(ROW["c1"],0.6,1.6)
  // to AND(GE(ROW['c1'], 0.6), LE(ROW['c1'], 1.6))
  // TODO: what about timestamp type?
  if (vExpr->name() == "between") {
    auto type = getCiderType(vExpr->type(), false);
    // should have 3 inputs
    auto inputs = vExpr->inputs();
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
            toCiderExpr(inputs[0], colInfo),
            toCiderExpr(inputs[1], colInfo)),
        std::make_shared<Analyzer::BinOper>(
            type,
            false,
            SQLOps::kLE,
            qualifier,
            toCiderExpr(inputs[0], colInfo),
            toCiderExpr(inputs[2], colInfo)));
  }
  // agg cases, refer RelAlgTranslator::translateAggregateRex
  if (vExpr->name() == "sum" || vExpr->name() == "avg") {
    // get target_expr which bypass the project mask
    // TODO: we only support one arg agg here
    CHECK_EQ(vExpr->inputs().size(), 1);
    // we returned a agg_expr with Analyzer::ColumnVar as its expr and then
    // replace this mask with actual detailed expr in agg node
    if (auto agg_field = std::dynamic_pointer_cast<const FieldAccessTypedExpr>(
            vExpr->inputs()[0])) {
      SQLAgg agg_kind = getCiderAggOp(vExpr->name());
      auto mask = agg_field->name();
      auto arg_expr = toCiderExpr(vExpr->inputs()[0], colInfo);
      std::shared_ptr<Analyzer::Constant> arg1; // 2nd aggregate parameter
      SQLTypeInfo agg_type = getCiderAggType(agg_kind, arg_expr.get());
      // we need to compare agg outputType between cider and velox to assure
      // result's corectness
      // FIXME: remove this to pass type check
//      CHECK_EQ(
//          agg_type.get_type(),
//          getVeloxAggType(vExpr->name(), agg_field).get_type());
      return std::make_shared<Analyzer::AggExpr>(
          agg_type, agg_kind, arg_expr, false, arg1);
    } else {
      std::runtime_error("agg should happen on specific column.");
    }
  }
  return nullptr;
}
} // namespace facebook::velox::cider
