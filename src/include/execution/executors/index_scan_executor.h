//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  const IndexInfo *index_info_;
  const TableInfo *table_info_;
  //计划中索引对象的类型始终为 BPlusTreeIndexForOneIntegerColumn。安全地将其转换并存储在执行器对象中
  BPlusTreeIndexForOneIntegerColumn *tree_;  //B+树
  BPlusTreeIndexIteratorForOneIntegerColumn iter_;//IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>
  std::vector<RID> rids_;
  std::vector<RID>::const_iterator rid_iter_{};
};
}  // namespace bustub
