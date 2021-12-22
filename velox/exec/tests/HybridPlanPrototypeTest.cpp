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
};

// prototype test based on tpc-h Q6
TEST_P(HybridPlanPrototypeTest, prototypeTest) {
  auto rowType =
      ROW({"l_quantity", "l_extendedprice", "l_discount", "l_shipdate"},
          {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
  // get data splits
  std::string current_path = fs::current_path().c_str();
  std::string dir = current_path + "/velox/exec/tests/data/result/";
  path file_path(dir);
  std::vector<std::string> file_list;
  for (auto i = directory_iterator(file_path); i != directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string single_file_path = i->path().filename().string();
      if (EndsWith(single_file_path, ".orc")) {
        std::cout << single_file_path << std::endl;
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
  const auto op =
      PlanBuilder()
          .tableScan(rowType)
          //.filter("l_shipdate >= 757353600 and l_shipdate < 788889600")
          .filter(
              "l_shipdate >= 8765.666666666667 and l_shipdate < 9130.666666666667 and l_discount between 0.05 and 0.07 and l_quantity < 24.0")
          .project(
              std::vector<std::string>{"l_extendedprice * l_discount"},
              std::vector<std::string>{"e0"})
          .aggregation(
              {}, {"sum(e0)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();
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

  CursorParameters params;
  params.planNode = op;
  std::cout << "start read" << std::endl;
  auto result_pair = readCursor(params, addSplits);
  std::cout << "finish read" << std::endl;
  std::vector<RowVectorPtr> results = result_pair.second;
  std::cout << "result size: " << results.size() << std::endl;
  int64_t num_rows = 0;
  for (auto outputVector : results) {
    num_rows += outputVector->size();
  }
  std::cout << "num_rows: " << num_rows << std::endl;
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HybridPlanPrototypeTests,
    HybridPlanPrototypeTest,
    testing::Values(true, false));
