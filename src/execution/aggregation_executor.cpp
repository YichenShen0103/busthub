//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  // Clear the aggregation hash table to handle multiple Init() calls
  aht_.Clear();
  successful_ = false;  // Reset the successful flag

  // Initialize the aggregation hash table
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey key = MakeAggregateKey(&tuple);
    AggregateValue value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema schema(plan_->OutputSchema());
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> value(aht_iterator_.Key().group_bys_);
    for (const auto &aggregate : aht_iterator_.Val().aggregates_) {
      value.push_back(aggregate);
    }
    *tuple = {value, &schema};
    ++aht_iterator_;
    successful_ = true;
    return true;
  }
  // 空表执行 select count(*) from t1;
  // 对 varchar 类型的 v1 执行 select min(v1) from t1;
  if (!successful_) {
    successful_ = true;
    if (plan_->group_bys_.empty()) {
      std::vector<Value> value;
      for (auto aggregate : plan_->agg_types_) {
        switch (aggregate) {
          case AggregationType::CountStarAggregate:
            value.push_back(ValueFactory::GetIntegerValue(0));
            break;
          case AggregationType::CountAggregate:
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      *tuple = {value, &schema};
      successful_ = true;
      return true;
    }
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
