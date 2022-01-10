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

#include <folly/init/Init.h>
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

// FIXME: Workaround dependency issue from omnisci and velox integrations
#include "velox/cider/VeloxPlanToCiderExecutionUnit.h"
#include "velox/exec/HybridExecOperator.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class HybridExecOperatorTest : public OperatorTestBase {
 protected:
  void assertHybridQuery(
      const std::shared_ptr<core::PlanNode>& planNode,
      const std::string& duckDBSql) {
    auto startCider = std::chrono::system_clock::now();

    Operator::registerOperator(HybridExecOperator::planNodeTranslator);
    facebook::velox::cider::CiderExecutionUnitGenerator generator;
    auto hybridPlan = generator.transformPlan(planNode);
    // TODO: we should verify whether this hybridPlan is valid.
    assertQuery(hybridPlan, duckDBSql);

    auto endCider = std::chrono::system_clock::now();
    auto durationCider = std::chrono::duration_cast<std::chrono::microseconds>(
        endCider - startCider);
    std::cout << "Cider compute takes " << durationCider.count() << " us "
              << std::endl;
  }

  void assertVeloxQuery(
      const std::shared_ptr<core::PlanNode>& planNode,
      const std::string& duckDBSql) {
    auto startVelox = std::chrono::system_clock::now();

    assertQuery(planNode, duckDBSql);

    auto endVelox = std::chrono::system_clock::now();
    auto durationVelox = std::chrono::duration_cast<std::chrono::microseconds>(
        endVelox - startVelox);
    std::cout << "Velox compute takes " << durationVelox.count() << " us "
              << std::endl;
  }

  // without null value
  std::vector<RowVectorPtr> createIncreaseInputData() {
    std::vector<RowVectorPtr> vectors;
    int batchNum = 1000;
    int batchSize = 2000;
    for (int32_t i = 0; i < batchNum; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createIncreaseBatch(rowType_, batchSize, *pool_));
      vectors.push_back(vector);
    }
    return std::move(vectors);
  }

  std::vector<RowVectorPtr> createRandomInputData() {
    std::vector<RowVectorPtr> vectors;
    int batchNum = 1000;
    int batchSize = 4000;
    for (int32_t i = 0; i < batchNum; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(rowType_, batchSize, *pool_));
      vectors.push_back(vector);
    }
    return std::move(vectors);
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {INTEGER(), DOUBLE(), INTEGER(), INTEGER(), BIGINT(), BIGINT()})};
};

// simple agg test

TEST_F(HybridExecOperatorTest, sum_int) {
  auto vectors = createIncreaseInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c0) from tmp where c2 < 50";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 50)")
          .aggregation(
              {}, {"sum(c0)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();

  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}

TEST_F(HybridExecOperatorTest, sum_double) {
  auto vectors = createIncreaseInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c1) from tmp where c2 < 1000";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 1000)")
          .aggregation(
              {}, {"sum(c1)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();

  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}

TEST_F(HybridExecOperatorTest, sum_bigint) {
  auto vectors = createIncreaseInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c4) from tmp where c2 < 100";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 100)")
          .aggregation(
              {}, {"sum(c4)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();

  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}

// agg + project test
TEST_F(HybridExecOperatorTest, sum_int_product_double) {
  auto vectors = createIncreaseInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c0 * c1) from tmp where c2 < 50";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 50)")
          .project(
              std::vector<std::string>{"c0 * c1"},
              std::vector<std::string>{"e1"})
          .aggregation(
              {}, {"sum(e1)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();
  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}

// Currently, int * int will cause some type issue, cause some check failed in
// omnisci. TEST_F(HybridExecOperatorTest, sum_int_product_int) {
//   Operator::registerOperator(HybridExecOperator::planNodeTranslator);
//   std::vector<RowVectorPtr> vectors;
//   for (int32_t i = 0; i < 1; ++i) {
//     auto vector = std::dynamic_pointer_cast<RowVector>(
//         BatchMaker::createIncreaseBatch(rowType_, 100, *pool_));
//     vectors.push_back(vector);
//   }
//   createDuckDbTable(vectors);
//
//   auto plan =
//       PlanBuilder()
//           .values(vectors)
//           .filter("(c2 < 50)")
//           .project(
//               std::vector<std::string>{"c0 * c3"},
//               std::vector<std::string>{"e1"})
//           .aggregation(
//               {}, {"sum(e1)"}, {}, core::AggregationNode::Step::kPartial,
//               false)
//           //          .partitionedOutput({}, 1)
//           .planNode();
//
//   std::cout << plan->toString() << std::endl;
//
//   assertHybridQuery(plan, "SELECT SUM(c0 * c3) from tmp where c2 < 50");
// }

// TODO: verify data nith Null value

TEST_F(HybridExecOperatorTest, sum_int_null) {
  auto vectors = createRandomInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c0) from tmp where c2 < 50";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 50)")
          .aggregation(
              {}, {"sum(c0)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();
  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}

TEST_F(HybridExecOperatorTest, sum_int_null_) {
  auto vectors = createRandomInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c0) from tmp where c4 < 50";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c4 < 50)")
          .aggregation(
              {}, {"sum(c0)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();
  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}

// this test may fail due to duckDB overflow check issue.
TEST_F(HybridExecOperatorTest, sum_long_null) {
  auto vectors = createRandomInputData();
  createDuckDbTable(vectors);
  std::string duckDbSql = "SELECT SUM(c5) from tmp where c4 < 50";

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c4 < 50)")
          .aggregation(
              {}, {"sum(c5)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();
  assertVeloxQuery(plan, duckDbSql);
  assertHybridQuery(plan, duckDbSql);
}
