//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "common/macros.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
    child_executor_->Init();
    auto result = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
    if (result != DB_SUCCESS) {
        throw MyException("UpdateExecutor init failed, table not found");
    }
    result = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), table_indexes_);
    if (result != DB_SUCCESS && result != DB_INDEX_NOT_FOUND) {
        throw MyException("UpdateExecutor init failed, index info initialization failed");
    }
    GetUpdateTuples();
    for (auto &row: new_rows_) {
        ASSERT(table_info_->GetTableHeap()->UpdateTuple(row, row.GetRowId(), exec_ctx_->GetTransaction()) == true,
               "UpdateExecutor failed, update tuple failed");
    }
    ASSERT(UpdateIndexes(), "UpdateExecutor failed, update index failed");
    update_size = update_rows_.size();
    is_init_ = true;
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    if (!is_init_) {
        return false;
    } else {
        while (update_size--) {
            return true;
        }
        is_init_ = false;
        return false;
    }
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
    auto UpdateAttr = plan_->GetUpdateAttr();
    vector<Field> new_fields;
    for (unsigned long i = 0; i < src_row.GetFieldCount(); i++) {
        if (UpdateAttr.count(i) != 0) {
            new_fields.emplace_back(UpdateAttr[i]->Evaluate(&src_row));
        } else {
            new_fields.emplace_back(*src_row.GetField(i));
        }
    }
    Row new_row(new_fields);
    new_row.SetRowId(src_row.GetRowId());
    return new_row;
}

[[maybe_unused]] bool UpdateExecutor::CheckTupleExist(const Row &row) {
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

bool UpdateExecutor::GetUpdateTuples() {
    Row update_row;
    RowId update_row_id;
    update_rows_.clear();
    while (child_executor_->Next(&update_row, &update_row_id)) {
        update_rows_.emplace_back(update_row);
        new_rows_.emplace_back(GenerateUpdatedTuple(update_row));
    }
    return true;
}

bool UpdateExecutor::UpdateIndexes() {
    for (auto &index_info: table_indexes_) {
        auto index = index_info->GetIndex();
        auto key_schema = index_info->GetIndexKeySchema();
        auto table_schema = table_info_->GetSchema();
        for (auto &row: update_rows_) {
            auto key_row = new Row(row.GetRowId());
            row.GetKeyFromRow(table_schema, key_schema, *key_row);
            ASSERT(index->RemoveEntry(*key_row, row.GetRowId(), exec_ctx_->GetTransaction()) == DB_SUCCESS,
                   "UpdateExecutor failed, delete index failed");
        }
        for (auto &row: new_rows_) {
            auto key_row = new Row(row.GetRowId());
            row.GetKeyFromRow(table_schema, key_schema, *key_row);
            ASSERT(index->InsertEntry(*key_row, row.GetRowId(), exec_ctx_->GetTransaction()) == DB_SUCCESS,
                   "UpdateExecutor failed, insert index failed");
        }
    }
    return true;
}