//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"
#include <exception>
#include "common/rowid.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    child_executor_->Init();
    auto result = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
    if (result != DB_SUCCESS) {
        throw MyException("InsertExecutor init failed, table not found");
    }
    result = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), table_indexes_);
    if (result != DB_SUCCESS && result != DB_INDEX_NOT_FOUND) {
        throw MyException("InsertExecutor init failed, index info initialization failed");
    }
    if (!GetInsertTuples()) {
        throw MyException("InsertExecutor init failed, unique check failed");
    }
    for (auto &row: insert_rows_) {
        ASSERT(table_info_->GetTableHeap()->InsertTuple(row, exec_ctx_->GetTransaction()) == true,
               "InsertExecutor init failed, insert tuple failed");
    }
    ASSERT(UpdateIndexes(), "InsertExecutor init failed, update index failed");
    insert_size = insert_rows_.size();
    is_init_ = true;
}

bool InsertExecutor::CheckUniqueInvalid(const Row &row) {
    for (auto &index_info: table_indexes_) {
        auto index = index_info->GetIndex();
        vector<RowId> check_result;
        Row copy_row = Row(row);
        Row key_row(row.GetRowId());
        copy_row.GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key_row);
        index->ScanKey(key_row, check_result, nullptr);
        if (!check_result.empty()) {
            return false;
        }
    }
    return true;
}

bool InsertExecutor::GetInsertTuples() {
    Row insert_row;
    RowId insert_row_id;
    insert_rows_.clear();
    while (child_executor_->Next(&insert_row, &insert_row_id)) {
        if (CheckUniqueInvalid(insert_row)) {
            insert_rows_.emplace_back(insert_row);
        } else {
            insert_rows_.clear();
            return false;
        }
    }
    return true;
}

bool InsertExecutor::UpdateIndexes() {
    for (auto &index_info: table_indexes_) {
        auto index = index_info->GetIndex();
        auto key_schema = index_info->GetIndexKeySchema();
        auto table_schema = table_info_->GetSchema();
        for (auto &row: insert_rows_) {
            auto key_row = new Row(row.GetRowId());
            row.GetKeyFromRow(table_schema, key_schema, *key_row);
            if (index->InsertEntry(*key_row, row.GetRowId(), exec_ctx_->GetTransaction()) != DB_SUCCESS) {
                return false;
            }
        }
    }
    return true;
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    if (!is_init_) {
        return false;
    } else {
        while (insert_size--) {
            return true;
        }
        is_init_ = false;
        return false;
    }
}