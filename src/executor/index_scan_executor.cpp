#include "executor/executors/index_scan_executor.h"
#include <memory>
#include <vector>
#include <algorithm>
#include "planner/expressions/comparison_expression.h"
#include "planner/expressions/constant_value_expression.h"
#include <iostream>

/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
        : AbstractExecutor(exec_ctx), plan_(plan), is_init_(false) {}

//只需要实现and的情况
void IndexScanExecutor::Init() {
    if (!is_init_) {
        index_info_map_.clear();
        not_matched_exprs_.clear();
        auto result = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
        if (result != DB_SUCCESS) {
            throw MyException("SeqScanExecutor init failed, table not found");
        }
        GetLeafComparisons(plan_->GetPredicate());
        InitGetRows();
        rows_iterator_ = rows_.begin();
        is_init_ = true;
    }
}

void IndexScanExecutor::InitGetRows() {
    IndexFilterRows();
    if (not_matched_exprs_.empty()) {
        return;
    }
    SeqFilterRows();
}

void IndexScanExecutor::IndexFilterRows() {
    bool first_flag = true;
    for (auto map_pair: index_info_map_) {
        auto center_expr = std::dynamic_pointer_cast<ComparisonExpression>(map_pair.first);
        auto op = center_expr->GetComparisonType();
        auto value_expression = std::dynamic_pointer_cast<ConstantValueExpression>(center_expr->GetChildAt(1));
        auto match_index = map_pair.second->GetIndex();
        auto key_row_fields = std::vector<Field>(1, value_expression->val_);
        std::vector<RowId> row_ids;
        match_index->ScanKey(Row(key_row_fields), row_ids, nullptr, op);
        vector<Row> step_rows;
        for (auto row_id: row_ids) {
            Row *row = new Row(row_id);
            table_info_->GetTableHeap()->GetTuple(row, exec_ctx_->GetTransaction());
            step_rows.emplace_back(*row);
        }
        if (first_flag) {
            first_flag = false;
            rows_ = vector<Row>(step_rows);
            continue;
        }
        vector<Row> intersection;
        std::set_intersection(rows_.begin(), rows_.end(),
                              step_rows.begin(), step_rows.end(),
                              std::back_inserter(intersection));
        rows_.clear();
        step_rows.clear();
        rows_ = vector<Row>(intersection);
    }
}

void IndexScanExecutor::SeqFilterRows() {
    for (auto expr: not_matched_exprs_) {
        vector<Row> intersection;
        for (auto row: rows_) {
            if (expr->Evaluate(&row).CompareEquals(Field(kTypeInt, 1)) == true) {
                intersection.push_back(row);
            }
        }
        rows_.clear();
        rows_ = vector<Row>(intersection);
    }
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
    if (!is_init_) {
        throw MyException("IndexScanExecutor not initialized");
    } else {
        if (rows_iterator_ != rows_.end()) {
            *row = *rows_iterator_;
            Row new_row;
            row->GetKeyFromRow(table_info_->GetSchema(), plan_->OutputSchema(), new_row);
            new_row.SetRowId(row->GetRowId());
            *row = new_row;
            *rid = row->GetRowId();
            rows_iterator_++;
            return true;
        } else {
            return false;
        }
    }
}

void IndexScanExecutor::GetLeafComparisons(AbstractExpressionRef expr) {
    if (expr->GetType() == ExpressionType::ComparisonExpression) {
        auto column_expression = std::dynamic_pointer_cast<ColumnValueExpression>(expr->GetChildAt(0));
        auto match_indexes = this->plan_->indexes_;
        bool is_match = false;
        for (auto match_index: match_indexes) {
            if (match_index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == column_expression->GetColIdx()) {
                index_info_map_.emplace_back(expr, match_index);
                is_match = true;
            }
        }
        if (!is_match) {
            not_matched_exprs_.emplace_back(expr);
        }
    } else {
        auto children = expr->GetChildren();
        for (const auto &child: children) {
            GetLeafComparisons(child);
        }
    }
}
