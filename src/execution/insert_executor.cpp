//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_exe_(std::move(child_executor)),
      table_meta_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_heap_(table_meta_->table_.get()) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_exe_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Schema *schema = &GetExecutorContext()->GetCatalog()->GetTable(this->plan_->GetTableOid())->schema_;
  if (plan_->IsRawInsert()) {
    auto size = plan_->RawValues().size();
    for (size_t index = 0; index < size; index++) {
      Tuple to_be_inserted = Tuple(plan_->RawValuesAt(index), schema);
      bool successfulInsert = table_heap_->InsertTuple(to_be_inserted, rid, GetExecutorContext()->GetTransaction());
      if (!successfulInsert) {
        return false;
      }
    }
    LOG_INFO("LOOK AT ME: finish initted");
    return false;
  }
  // not rawinsert.
  while (child_exe_->Next(tuple, rid)) {
    bool successfulInsert = table_heap_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction());
    if (!successfulInsert) {
      return false;
    }
  }
  return false;
}

}  // namespace bustub
