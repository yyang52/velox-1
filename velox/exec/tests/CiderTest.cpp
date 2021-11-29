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
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"
#include "cider/VeloxPlanToCiderExecutionUnit.h"

using namespace facebook::velox;
using namespace facebook::velox::cider;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class CiderTest : public OperatorTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {INTEGER(), DOUBLE(), INTEGER(), INTEGER()})};
};

// Test for consecutive filter and project fusion.
TEST_F(CiderTest, filterProjectFused) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  const auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 1000) and (c1 between 0.6 and 1.6) and (c0 >= 100)")
          .project(
              std::vector<std::string>{"c0 * c1"},
              std::vector<std::string>{"e1"})
          .aggregation(
                    {},
                    {"sum(e1)"},
                    {"revenue"},
                    core::AggregationNode::Step::kPartial,
                    false)
          .planNode();
  auto content = plan->toString(true, true);
  std::cout << content;
  CiderExecutionUnitGenerator generator;
  // for test
  auto exe_unit = generator.createExecutionUnit(plan);
}
