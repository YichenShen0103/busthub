//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  found_match_ = false;  // Reset the match found flag
  loop_done_ = true;     // Start with loop_done_ = true so we fetch the first left tuple
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Iterate over all tuples from the left executor
  while (!loop_done_ || left_executor_->Next(&left_tuple_, rid)) {
    if (loop_done_) {
      found_match_ = false;     // Reset the match found flag
      loop_done_ = false;       // Reset the loop done flag for the next left tuple
      right_executor_->Init();  // Reinitialize the right executor
    }

    // For each left tuple, iterate over all tuples from the right executor
    while (right_executor_->Next(&right_tuple_, rid)) {
      // Evaluate the join predicate
      auto value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuple_, right_schema_);
      // If the predicate evaluates to true, combine the left and right tuples into a new tuple
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); ++i) {
          values.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); ++i) {
          values.push_back(right_tuple_.GetValue(&right_schema_, i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        found_match_ = true;  // Set the found match flag
        return true;
      }
    }

    // We've exhausted all right tuples for this left tuple
    loop_done_ = true;  // Set the loop done flag to true for the next left tuple

    if (!found_match_ && plan_->GetJoinType() == JoinType::LEFT) {
      // If no match was found and it's a LEFT JOIN, we still need to produce a tuple with NULLs for the right side
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema_.GetColumnCount(); ++i) {
        values.push_back(left_tuple_.GetValue(&left_schema_, i));
      }
      for (uint32_t i = 0; i < right_schema_.GetColumnCount(); ++i) {
        // Create a NULL value with the correct type from the right schema
        auto col_type = right_schema_.GetColumn(i).GetType();
        values.push_back(ValueFactory::GetNullValueByType(col_type));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }

  return false;  // No more tuples to produce
}

}  // namespace bustub
