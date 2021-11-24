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

#include "velox/exec/HybridExecOperator.h"
#include <folly/init/Init.h>
#include "velox/core/HybridPlanNode.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class HybridExecOperatorTest : public OperatorTestBase {
 protected:
  // current hybrid node just return input, so use SQL select *
  void assertHybrid(
      std::vector<RowVectorPtr>&& vectors,
      const std::string& duckDBSql = "",
      const std::string& hybridCondition = "") {
    auto plan =
        PlanBuilder().values(vectors).hybrid("just for test").planNode();
    assertQuery(plan, "SELECT * FROM tmp");
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()})};
};

TEST_F(HybridExecOperatorTest, hybrid) {
  Operator::registerOperator(HybridExecOperator::planNodeTranslator);
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertHybrid(std::move(vectors));
}
