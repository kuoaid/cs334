//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <vector>
#include "execution/executors/nested_loop_join_executor.h"


namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_(std::move(left_executor)), right_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  Tuple left_tuple, right_tuple, result_tuple;
  RID rid, left_rid, right_rid;
  while (left_->Next(&left_tuple, &left_rid)) {
    // LOG_INFO("left loop");
    right_->Init();  // do this to reset right pointer to its original position each time.
    while (right_->Next(&right_tuple, &right_rid)) {
      // LOG_INFO("right loop");
      if (plan_->Predicate()
              ->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                             plan_->GetRightPlan()->OutputSchema())
              .GetAs<bool>()) {
        LOG_INFO("need to join");
        std::vector<Value> result_vector;
        for (size_t index = 0; index < plan_->GetLeftPlan()->OutputSchema()->GetColumnCount(); index++) {
          // LOG_INFO("1st for with index = %li", index);
          // LOG_INFO("1st for with size = %ui", left_tuple.GetLength());
          result_vector.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), index));
          // LOG_INFO("passed push_back for 1st loop");
        }
        for (size_t index = 0; index < plan_->GetRightPlan()->OutputSchema()->GetColumnCount(); index++) {
          // LOG_INFO("2nd for");
          result_vector.push_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), index));
        }
        result_tuples.push_back(Tuple(result_vector, GetOutputSchema()));
        LOG_INFO("size %li", result_tuples.size());
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_tuples.size() == 0) {
    return false;
  }
  *tuple = result_tuples.back();
  result_tuples.pop_back();
  return true;
}

}  // namespace bustub
