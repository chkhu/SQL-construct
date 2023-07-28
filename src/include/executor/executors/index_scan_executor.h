#pragma once

#include <utility>
#include <vector>

#include "catalog/indexes.h"
#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/index_scan_plan.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/comparison_expression.h"
#include <vector>
#include <unordered_map>
#include <utility>

/**
 * The IndexScanExecutor executor can over a table.
 */
class IndexScanExecutor : public AbstractExecutor {
public:
    /**
     * Construct a new SeqScanExecutor instance.
     * @param exec_ctx The executor context
     * @param plan The sequential scan plan to be executed
     */
    IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan);

    /** Initialize the sequential scan */
    void Init() override;

    /**
     * Yield the next row from the sequential scan.
     * @param[out] row The next row produced by the scan
     * @param[out] rid The next row RID produced by the scan
     * @return `true` if a row was produced, `false` if there are no more rows
     */
    bool Next(Row *row, RowId *rid) override;

    /** @return The output schema for the sequential scan */
    const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

private:

    void GetLeafComparisons(AbstractExpressionRef expr);
    void InitGetRows();

    /** The sequential scan plan node to be executed */
    const IndexScanPlanNode *plan_;
    bool is_init_ = false;
    TableInfo *table_info_{};
    std::vector<Row> rows_;
    std::vector<Row>::iterator rows_iterator_;
    std::vector<std::pair<AbstractExpressionRef, IndexInfo * >> index_info_map_;
    std::vector<AbstractExpressionRef> not_matched_exprs_;


    void IndexFilterRows();

    void SeqFilterRows();
};