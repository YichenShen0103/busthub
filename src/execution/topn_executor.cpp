#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), n_(plan->GetN()) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  // Clear any existing data
  while (!topn_pq_.empty()) {
    topn_pq_.pop();
  }

  // Create a comparator for the priority queue
  // We want to maintain a min-heap of size N, where the "smallest" elements according to our order criteria
  // are kept. The comparator should return true if a should come AFTER b in the final result.
  auto comparator = [this](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &[order_by_type, expr] : plan_->GetOrderBy()) {
      auto val_a = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      auto val_b = expr->Evaluate(&b, child_executor_->GetOutputSchema());

      bool is_ascending = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);

      if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
        // a < b: if ascending, a should come before b, so return false (a has higher priority)
        // if descending, a should come after b, so return true (a has lower priority)
        return is_ascending;
      }
      if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
        // a > b: if ascending, a should come after b, so return true (a has lower priority)
        // if descending, a should come before b, so return false (a has higher priority)
        return !is_ascending;
      }
      // a == b, continue to next ordering criterion
    }
    // All order criteria are equal, maintain stable order
    return false;
  };

  // Initialize priority queue with comparator
  topn_pq_ =
      std::priority_queue<Tuple, std::vector<Tuple>, std::function<bool(const Tuple &, const Tuple &)>>(comparator);

  // Read all tuples from child executor
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (topn_pq_.size() < n_) {
      // If we haven't reached N tuples yet, just add it
      topn_pq_.push(tuple);
    } else {
      // If we have N tuples, compare with the "worst" (top of min-heap)
      // If this tuple is better than the worst, replace it
      if (comparator(tuple, topn_pq_.top())) {
        topn_pq_.pop();
        topn_pq_.push(tuple);
      }
    }
  }

  // Convert priority queue to vector for easy iteration
  // Since it's a min-heap, we need to reverse to get correct order
  result_tuples_.clear();
  while (!topn_pq_.empty()) {
    result_tuples_.push_back(topn_pq_.top());
    topn_pq_.pop();
  }

  // Reverse to get the correct order (best tuples first)
  std::reverse(result_tuples_.begin(), result_tuples_.end());

  // Initialize iterator
  result_iterator_ = result_tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iterator_ != result_tuples_.end()) {
    *tuple = *result_iterator_;
    *rid = RID();  // TopN doesn't maintain RIDs
    ++result_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
