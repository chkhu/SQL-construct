//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"
#include "storage/table_heap.h"
#include "storage/table_iterator.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
        : AbstractExecutor(exec_ctx),
          plan_(plan), is_init(false) {
}

void SeqScanExecutor::Init() {
    if (!is_init) {
        auto result = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
        if (result != DB_SUCCESS) {
            throw MyException("SeqScanExecutor init failed, table not found");
        }
        table_heap_ = table_info_->GetTableHeap();
        table_iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
        is_init = true;
    }
}


bool SeqScanExecutor::Next(Row *row, RowId *rid) {
    if (!is_init) {
        throw MyException("SeqScanExecutor not initialized");
    } else {
        if (table_iterator_ == table_heap_->End()) {
            is_init = false;
            return false;
        } else {
            while (table_iterator_ != table_heap_->End()) {
                *row = *table_iterator_;
                if (plan_->GetPredicate() == nullptr) {
                    Row new_row;
                    row->GetKeyFromRow(table_info_->GetSchema(), plan_->OutputSchema(), new_row);
                    new_row.SetRowId(row->GetRowId());
                    *row = new_row;
                    *rid = row->GetRowId();
                    table_iterator_++;
                    return true;
                }
                auto eva_res = plan_->GetPredicate()->Evaluate(row);
                if (eva_res.CompareEquals(Field(kTypeInt, 1)) == CmpBool::kTrue) {

                    Row new_row;
                    row->GetKeyFromRow(table_info_->GetSchema(), plan_->OutputSchema(), new_row);
                    new_row.SetRowId(row->GetRowId());
                    *row = new_row;
                    *rid = row->GetRowId();
                    table_iterator_++;
                    return true;
                } else {
                    table_iterator_++;
                }
            }
            return false;
        }
    }
}
