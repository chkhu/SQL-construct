//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"
#include <vector>

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    //first init child
    child_executor_->Init();
    //find table
    auto result = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
    if (result != DB_SUCCESS) {
        throw MyException("DeleteExecutor init failed, table not found");
    }
    //find index
    result = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), table_indexes_);
    if (result != DB_SUCCESS && result != DB_INDEX_NOT_FOUND) {
        throw MyException("DeleteExecutor init failed, index info initialization failed");
    }
    //find tuples
    GetDeleteTuples();
    for (auto &row: delete_rows_) {
        ASSERT(table_info_->GetTableHeap()->MarkDelete(row.GetRowId(), exec_ctx_->GetTransaction()) == true,
               "DeleteExecutor init failed, delete tuple failed");
    }
    ASSERT(UpdateIndexes(), "DeleteExecutor init failed, update index failed");
    delete_size = delete_rows_.size();
    is_init_ = true;
}

[[maybe_unused]] bool DeleteExecutor::CheckTupleExist(const Row &row) {
    for (auto &index_info: table_indexes_) {
        auto index = index_info->GetIndex();
        vector<RowId> check_result;
        index->ScanKey(row, check_result, nullptr);
        if (!check_result.empty()) {
            return true;
        }
    }
    return false;
}

bool DeleteExecutor::GetDeleteTuples() {
    Row delete_row;
    RowId delete_row_id;
    delete_rows_.clear();
    //loop delete
    while (child_executor_->Next(&delete_row, &delete_row_id)) {
        delete_rows_.emplace_back(delete_row);
    }
    return true;
}

bool DeleteExecutor::UpdateIndexes() {
    for (auto &index_info: table_indexes_) {
        auto index = index_info->GetIndex();
        auto key_schema = index_info->GetIndexKeySchema();
        auto table_schema = table_info_->GetSchema();
        for (auto &row: delete_rows_) {
            auto key_row = new Row(row.GetRowId());
            row.GetKeyFromRow(table_schema, key_schema, *key_row);
            if (index->RemoveEntry(*key_row, row.GetRowId(), exec_ctx_->GetTransaction()) != DB_SUCCESS) {
                throw MyException("DeleteExecutor init failed, remove index failed");
            }
        }
    }
    return true;
}
//      next method
bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    if (!is_init_) {
        return false;
    } else {
        while (delete_size--) {
            return true;
        }
        is_init_ = false;
        return false;
    }
}