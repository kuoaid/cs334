//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_meta_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_heap_(table_meta_->table_.get()),
      ite_(table_heap_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  // LOG_INFO("LOOK AT ME: init entered");
  table_heap_ = table_meta_->table_.get();
  TableIterator ite_ = table_heap_->Begin(exec_ctx_->GetTransaction());
  // LOG_INFO("LOOK AT ME: finish initted");
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // LOG_INFO("entered Next");
  while (ite_ != table_heap_->End()) {
    auto tuple_tested = *(ite_++);
    auto eval_result = true;
    if (plan_->GetPredicate() != nullptr) {
      eval_result = plan_->GetPredicate()->Evaluate(&tuple_tested, GetOutputSchema()).GetAs<bool>();
    }
    if (eval_result) {
      *tuple = Tuple(tuple_tested);
      // LOG_INFO("LOOK AT ME: returnin true");
      return true;
    }
  }
  // LOG_INFO("LOOK AT ME: returnin false");
  return false;
}

}  // namespace bustub
