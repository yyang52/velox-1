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
#include "velox/type/Type.h"
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

class QuerySimulationTest : public virtual HiveConnectorTestBase,
                            public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useAsyncCache_ = GetParam();
    HiveConnectorTestBase::SetUp();
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

  static OperatorStats getTableScanStats(const std::shared_ptr<Task>& task) {
    return task->taskStats().pipelineStats[0].operatorStats[0];
  }

  static int64_t getSkippedStridesStat(const std::shared_ptr<Task>& task) {
    return getTableScanStats(task).runtimeStats["skippedStrides"].sum;
  }

  static int64_t getSkippedSplitsStat(const std::shared_ptr<Task>& task) {
    return getTableScanStats(task).runtimeStats["skippedSplits"].sum;
  }

  void testNonPartitionedTableWithPlanBuilder(
      const std::vector<std::string>& file_list) {
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
    std::cout << "splits size: " << splits.size() << std::endl;

    std::string c_0 = "l_quantity";
    std::string c_1 = "l_extendedprice";
    std::string c_2 = "l_discount";
    std::string c_3 = "l_shipdate_new";

    auto outputType = ROW({c_1, c_2}, {DOUBLE(), DOUBLE()});

    SubfieldFilters filters;
    filters[common::Subfield(c_3)] = std::make_unique<common::DoubleRange>(
        8766.0, false, false, 9131.0, false, true, false);
    filters[common::Subfield(c_0)] = std::make_unique<common::DoubleRange>(
        0, true, false, 24, false, true, false);
    filters[common::Subfield(c_2)] = std::make_unique<common::DoubleRange>(
        0.05, false, false, 0.07, false, false, false);
    auto tableHandle = makeTableHandle(std::move(filters), nullptr);

    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;
    assignments[c_1] = regularColumn(c_1);
    assignments[c_2] = regularColumn(c_2);

    auto op = PlanBuilder()
                  .tableScan(outputType, tableHandle, assignments)
                  // .filter("not(is_null(l_shipdate_new)) and
                  // not(is_null(l_discount)) and not(is_null(l_quantity)) and
                  // (l_shipdate_new >= 8766.0) and (l_shipdate_new < 9131.0)
                  // and (l_discount >= 0.05) and (l_discount <= 0.07) and
                  // (l_quantity < 24.00)")
                  .project(
                      std::vector<std::string>{"l_extendedprice * l_discount"},
                      std::vector<std::string>{"mul_res"})
                  .aggregation(
                      {},
                      {"sum(mul_res)"},
                      {},
                      core::AggregationNode::Step::kPartial,
                      false)
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
      // std::cout << "out size: " << outputVector->size() << std::endl;
      num_rows += outputVector->size();
      // for (size_t i = 0; i < outputVector->size(); ++i) {
      //   std::cout << outputVector->toString(i) << std::endl;
      // }
    }
    std::cout << "num_rows: " << num_rows << std::endl;
  }

  void testNonPartitionedTableWithPlanNode(
      const std::vector<std::string>& file_list) {
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
    std::cout << "splits size: " << splits.size() << std::endl;

    std::string c_0 = "l_quantity";
    std::string c_1 = "l_extendedprice";
    std::string c_2 = "l_discount";
    std::string c_3 = "l_shipdate_new";

    auto outputType = ROW({c_1, c_2}, {DOUBLE(), DOUBLE()});

    SubfieldFilters filters;
    filters[common::Subfield(c_3)] = std::make_unique<common::DoubleRange>(
        8766.0, false, false, 9131.0, false, true, false);
    filters[common::Subfield(c_0)] = std::make_unique<common::DoubleRange>(
        0, true, false, 24, false, true, false);
    filters[common::Subfield(c_2)] = std::make_unique<common::DoubleRange>(
        0.05, false, false, 0.07, false, false, false);
    auto tableHandle = makeTableHandle(std::move(filters), nullptr);

    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;
    assignments[c_1] = regularColumn(c_1);
    assignments[c_2] = regularColumn(c_2);

    std::shared_ptr<core::PlanNode> planNode;
    planNodeId_ = 0;
    // TableScanNode
    planNode = std::make_shared<core::TableScanNode>(
        nextPlanNodeId(), outputType, tableHandle, assignments);
    // ProjectNode
    // Fields
    std::vector<std::shared_ptr<const core::ITypedExpr>> scan_params;
    scan_params.reserve(2);
    auto field_0 = std::make_shared<const core::FieldAccessTypedExpr>(
        DOUBLE(), "l_extendedprice");
    auto field_1 = std::make_shared<const core::FieldAccessTypedExpr>(
        DOUBLE(), "l_discount");
    scan_params.emplace_back(field_0);
    scan_params.emplace_back(field_1);
    // Expressions
    std::vector<std::string> projectNames;
    projectNames.push_back("mul_res");
    std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
    auto mulExpr = std::make_shared<const core::CallTypedExpr>(
        DOUBLE(), std::move(scan_params), "multiply");
    expressions.emplace_back(mulExpr);
    planNode = std::make_shared<core::ProjectNode>(
        nextPlanNodeId(),
        std::move(projectNames),
        std::move(expressions),
        planNode);
    // Aggregate
    bool ignoreNullKeys = false;
    std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> groupingExpr;
    std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregateExprs;
    aggregateExprs.reserve(1);
    std::vector<std::shared_ptr<const core::ITypedExpr>> agg_params;
    agg_params.reserve(1);
    auto field_agg =
        std::make_shared<const core::FieldAccessTypedExpr>(DOUBLE(), "mul_res");
    agg_params.emplace_back(field_agg);
    auto aggExpr = std::make_shared<const core::CallTypedExpr>(
        DOUBLE(), std::move(agg_params), "sum");
    aggregateExprs.emplace_back(aggExpr);
    std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
        aggregateMasks(aggregateExprs.size());
    auto aggNames = makeNames("a", aggregateExprs.size());
    planNode = std::make_shared<core::AggregationNode>(
        nextPlanNodeId(),
        core::AggregationNode::Step::kPartial,
        groupingExpr,
        aggNames,
        aggregateExprs,
        aggregateMasks,
        ignoreNullKeys,
        planNode);

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
    params.planNode = planNode;
    std::cout << "start read" << std::endl;
    auto result_pair = readCursor(params, addSplits);
    std::cout << "finish read" << std::endl;
    std::vector<RowVectorPtr> results = result_pair.second;
    std::cout << "result size: " << results.size() << std::endl;
    int64_t num_rows = 0;
    for (auto outputVector : results) {
      // std::cout << "out size: " << outputVector->size() << std::endl;
      num_rows += outputVector->size();
      // for (size_t i = 0; i < outputVector->size(); ++i) {
      //   std::cout << outputVector->toString(i) << std::endl;
      // }
    }
    std::cout << "num_rows: " << num_rows << std::endl;
  }

  bool EndsWith(const std::string& data, const std::string& suffix) {
    return data.find(suffix, data.size() - suffix.size()) != std::string::npos;
  }

  std::string orc_dir = "/tmp/dev/velox/velox/exec/tests/data/lineitem_orcs/";

 private:
  int planNodeId_ = 0;
  std::string nextPlanNodeId() {
    auto id = fmt::format("{}", planNodeId_);
    planNodeId_++;
    return id;
  }
  std::vector<std::string> makeNames(const std::string& prefix, int size) {
    std::vector<std::string> names;
    for (int i = 0; i < size; i++) {
      names.push_back(fmt::format("{}{}", prefix, i));
    }
    return names;
  }
};

TEST_P(QuerySimulationTest, nonPartitionedTableWithPlanBuilder) {
  path file_path(orc_dir);
  std::vector<std::string> file_list;
  for (auto i = directory_iterator(file_path); i != directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string single_file_path = i->path().filename().string();
      if (EndsWith(single_file_path, ".orc")) {
        std::cout << single_file_path << std::endl;
        file_list.push_back(orc_dir + single_file_path);
      }
    } else {
      continue;
    }
  }
  testNonPartitionedTableWithPlanBuilder(file_list);
}

TEST_P(QuerySimulationTest, nonPartitionedTableWithPlanNode) {
  path file_path(orc_dir);
  std::vector<std::string> file_list;
  for (auto i = directory_iterator(file_path); i != directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string single_file_path = i->path().filename().string();
      if (EndsWith(single_file_path, ".orc")) {
        std::cout << single_file_path << std::endl;
        file_list.push_back(orc_dir + single_file_path);
      }
    } else {
      continue;
    }
  }
  testNonPartitionedTableWithPlanNode(file_list);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    QuerySimulationTests,
    QuerySimulationTest,
    testing::Values(true, false));
