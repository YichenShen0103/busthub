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

#include <memory>
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

  BPlusTreeIndexForOneIntegerColumn *tree_{nullptr};  // The B+ tree index
  TableHeap *table_{nullptr};                         // The table heap associated with the index
  std::unique_ptr<BPlusTreeIndexIteratorForOneIntegerColumn> index_iter_{nullptr};
};
}  // namespace bustub
