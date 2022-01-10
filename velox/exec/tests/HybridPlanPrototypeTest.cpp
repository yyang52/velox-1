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

#include <chrono>

#include "velox/common/base/test_utils/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/exec/tests/Cursor.h"
#include "velox/exec/tests/HiveConnectorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

#include <boost/filesystem.hpp>
using namespace boost::filesystem;

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

// FIXME: Workaround dependency issue from omnisci and velox integrations
#include "velox/cider/VeloxPlanToCiderExecutionUnit.h"
#include "velox/exec/HybridExecOperator.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

class HybridPlanPrototypeTest : public virtual HiveConnectorTestBase,
                                public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useAsyncCache_ = GetParam();
    HiveConnectorTestBase::SetUp();
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const std::shared_ptr<const RowType>& rowType) {
    return HiveConnectorTestBase::makeVectors(rowType, count, rowsPerVector);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::shared_ptr<HiveConnectorSplit>& hiveSplit,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {hiveSplit}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const exec::Split&& split,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {split}, duckDbSql);
  }

  static std::shared_ptr<facebook::velox::core::PlanNode> tableScanNode(
      const std::shared_ptr<const RowType>& outputType) {
    return PlanBuilder().tableScan(outputType).planNode();
  }

  bool EndsWith(const std::string& data, const std::string& suffix) {
    return data.find(suffix, data.size() - suffix.size()) != std::string::npos;
  }

  std::vector<exec::Split> createSplits() {
    std::string current_path = fs::current_path().c_str();
//    std::string dir = "/tmp/dev/velox/velox/exec/tests/data/result10g/";
    std::string dir = "/tmp/dev/velox/velox/exec/tests/data/lineitem_orcs/";

    path file_path(dir);
    std::vector<std::string> file_list;
    for (auto i = directory_iterator(file_path); i != directory_iterator();
         i++) {
      if (!is_directory(i->path())) {
        std::string single_file_path = i->path().filename().string();
        if (EndsWith(single_file_path, ".orc")) {
          //          std::cout << single_file_path << std::endl;
          file_list.push_back(dir + single_file_path);
        }
      } else {
        continue;
      }
    }
    std::vector<std::shared_ptr<connector::ConnectorSplit>> connectorSplits;
    for (auto single_file_path : file_list) {
      auto split = std::make_shared<HiveConnectorSplit>(
          kHiveConnectorId,
          single_file_path,
          facebook::velox::dwio::common::FileFormat::ORC,
          0,
          fs::file_size(single_file_path));
      connectorSplits.push_back(split);
    }

    std::vector<exec::Split> splits;
    splits.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      splits.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
    }
    return splits;
  }
};

// prototype test based on tpc-h Q6

TEST_P(HybridPlanPrototypeTest, prototypeTestCider) {
  auto rowType =
      ROW({"l_quantity", "l_extendedprice", "l_discount", "l_shipdate_new"},
          {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  const auto op =
      PlanBuilder()
          .tableScan(rowType)
          .filter(
              "l_shipdate_new >= 8765.666666666667 and l_shipdate_new < 9130.666666666667 and l_discount between 0.05 and 0.07 and l_quantity < 24.0")
          .project(
              std::vector<std::string>{"l_extendedprice * l_discount"},
              std::vector<std::string>{"e0"})
          .aggregation(
              {}, {"sum(e0)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();

  // get data/exec splits
  auto splits = createSplits();

  // define lambda function as readCursor parameters.
  bool noMoreSplits = false;
  std::function<void(exec::Task*)> addSplits = [&](Task* task) {
    if (noMoreSplits) {
      return;
    }
    for (auto& split : splits) {
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
    noMoreSplits = true;
  };

  //  CursorParameters paramsVelox;
  //  paramsVelox.planNode = op;
  //
  //  auto startVelox = std::chrono::system_clock::now();
  //
  //  auto resultPairVelox = readCursor(paramsVelox, addSplits);
  //
  //  auto endVelox = std::chrono::system_clock::now();
  //  auto durationVelox =
  //  std::chrono::duration_cast<std::chrono::microseconds>(
  //      endVelox - startVelox);
  //  std::vector<RowVectorPtr> resultsVelox = resultPairVelox.second;
  //  std::cout << "Velox result size: " << resultsVelox.size() << std::endl;
  //  std::cout << "Velox compute takes " << durationVelox.count() << " us "
  //  << std::endl;
  //  std::cout <<
  //  resultsVelox[0]->childAt(0)->asFlatVector<double>()->valueAt(0)
  //  << std::endl;
  //
  //  // re-create new Splits. previous splits are all consumed
  //  splits = createSplits();
  //  noMoreSplits = false;

  // build cider runtime paramters.
  Operator::registerOperator(HybridExecOperator::planNodeTranslator);
  facebook::velox::cider::CiderExecutionUnitGenerator generator;
  auto hybridPlan = generator.transformPlan(op);

  CursorParameters paramsCider;
  paramsCider.planNode = hybridPlan;
  auto startCider = std::chrono::system_clock::now();

  auto resultPairCider = readCursor(paramsCider, addSplits);

  auto endCider = std::chrono::system_clock::now();
  auto durationCider = std::chrono::duration_cast<std::chrono::microseconds>(
      endCider - startCider);
  std::vector<RowVectorPtr> resultsCider = resultPairCider.second;
  std::cout << "Cider result size: " << resultsCider.size() << std::endl;
  std::cout << "Cider compute takes " << durationCider.count() << " us "
            << std::endl;
  std::cout.precision(17);
  std::cout << std::fixed
            << resultsCider[0]->childAt(0)->asFlatVector<double>()->valueAt(0)
            << std::endl;

  //  ASSERT_TRUE(resultsVelox[0]->equalValueAt(resultsCider[0].get(), 0, 0));
}

TEST_P(HybridPlanPrototypeTest, prototypeTest) {
  auto rowType =
      ROW({"l_quantity", "l_extendedprice", "l_discount", "l_shipdate_new"},
          {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  const auto op =
      PlanBuilder()
          .tableScan(rowType)
          .filter(
              "l_shipdate_new >= 8765.666666666667 and l_shipdate_new < 9130.666666666667 and l_discount between 0.05 and 0.07 and l_quantity < 24.0")
          .project(
              std::vector<std::string>{"l_extendedprice * l_discount"},
              std::vector<std::string>{"e0"})
          .aggregation(
              {}, {"sum(e0)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();

  // get data/exec splits
  auto splits = createSplits();

  // define lambda function as readCursor parameters.
  bool noMoreSplits = false;
  std::function<void(exec::Task*)> addSplits = [&](Task* task) {
    if (noMoreSplits) {
      return;
    }
    for (auto& split : splits) {
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
    noMoreSplits = true;
  };

  CursorParameters paramsVelox;
  paramsVelox.planNode = op;

  auto startVelox = std::chrono::system_clock::now();

  auto resultPairVelox = readCursor(paramsVelox, addSplits);

  auto endVelox = std::chrono::system_clock::now();
  auto durationVelox = std::chrono::duration_cast<std::chrono::microseconds>(
      endVelox - startVelox);
  std::vector<RowVectorPtr> resultsVelox = resultPairVelox.second;
  std::cout << "Velox result size: " << resultsVelox.size() << std::endl;
  std::cout << "Velox compute takes " << durationVelox.count() << " us "
            << std::endl;
  std::cout.precision(17);
  std::cout << std::fixed
            << resultsVelox[0]->childAt(0)->asFlatVector<double>()->valueAt(0)
            << std::endl;

  //  // re-create new Splits. previous splits are all consumed
  //  splits = createSplits();
  //  noMoreSplits = false;
  //
  //  // build cider runtime paramters.
  //  Operator::registerOperator(HybridExecOperator::planNodeTranslator);
  //  facebook::velox::cider::CiderExecutionUnitGenerator generator;
  //  auto hybridPlan = generator.transformPlan(op);
  //
  //  CursorParameters paramsCider;
  //  paramsCider.planNode = hybridPlan;
  //  auto startCider = std::chrono::system_clock::now();
  //
  //  auto resultPairCider = readCursor(paramsCider, addSplits);
  //
  //  auto endCider = std::chrono::system_clock::now();
  //  auto durationCider =
  //  std::chrono::duration_cast<std::chrono::microseconds>(
  //      endCider - startCider);
  //  std::vector<RowVectorPtr> resultsCider = resultPairCider.second;
  //  std::cout << "Cider result size: " << resultsCider.size() << std::endl;
  //  std::cout << "Cider compute takes " << durationCider.count() << " us "
  //            << std::endl;
  //  std::cout <<
  //  resultsCider[0]->childAt(0)->asFlatVector<double>()->valueAt(0)
  //            << std::endl;
  //
  //  ASSERT_TRUE(resultsVelox[0]->equalValueAt(resultsCider[0].get(), 0, 0));
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HybridPlanPrototypeTests,
    HybridPlanPrototypeTest,
    testing::Values(false));
