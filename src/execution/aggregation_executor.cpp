//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
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
      aht_(plan->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    const auto &aggregate_key = aht_iterator_.Key();
    const auto &aggregate_val = aht_iterator_.Val();
    ++aht_iterator_;
    // order matters!
    if ((plan_->GetHaving() == nullptr) ||
        (plan_->GetHaving()->EvaluateAggregate(aggregate_key.group_bys_, aggregate_val.aggregates_).GetAs<bool>())) {
      std::vector<Value> result;
      for (auto &column : GetOutputSchema()->GetColumns()) {
        result.push_back(column.GetExpr()->EvaluateAggregate(aggregate_key.group_bys_, aggregate_val.aggregates_));
      }
      *tuple = Tuple(result, GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
