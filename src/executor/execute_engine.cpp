#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

#include <set>
bool IsExecuteFile = false;
ExecuteEngine::ExecuteEngine() {
    char path[] = "./databases";
    DIR *dir;
    if ((dir = opendir(path)) == nullptr) {
        mkdir("./databases", 0777);
        dir = opendir(path);
    }
    struct dirent *stdir;
    while ((stdir = readdir(dir)) != nullptr) {
        if (strcmp(stdir->d_name, ".") == 0 ||
            strcmp(stdir->d_name, "..") == 0 ||
            stdir->d_name[0] == '.')
            continue;
        dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
    }
    closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
    switch (plan->GetType()) {
        // Create a new sequential scan executor
        case PlanType::SeqScan: {
            return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
        }
            // Create a new index scan executor
        case PlanType::IndexScan: {
            return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
        }
            // Create a new update executor
        case PlanType::Update: {
            auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
            return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
        }
            // Create a new delete executor
        case PlanType::Delete: {
            auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
            return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
        }
        case PlanType::Insert: {
            auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
            return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
        }
        case PlanType::Values: {
            return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
        }
        default:
            throw std::logic_error("Unsupported plan type.");
        //unsupported
        case PlanType::Aggregation:
            break;
        case PlanType::Limit:
            break;
        case PlanType::Distinct:
            break;
        case PlanType::NestedLoopJoin:
            break;
    }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
    // Construct the executor for the abstract plan node
    auto executor = CreateExecutor(exec_ctx, plan);

    try {
        executor->Init();
        RowId rid{};
        Row row{};
        while (executor->Next(&row, &rid)) {
            if (result_set != nullptr) {
                result_set->push_back(row);
            }
        }
    } catch (const exception &ex) {
        std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
        if (result_set != nullptr) {
            result_set->clear();
        }
        return DB_FAILED;
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
    if (ast == nullptr) {
        return DB_FAILED;
    }
    auto start_time = std::chrono::system_clock::now();
    unique_ptr<ExecuteContext> context(nullptr);
    if (!current_db_.empty())
        context = dbs_[current_db_]->MakeExecuteContext(nullptr);
    switch (ast->type_) {
        case kNodeCreateDB:
            return ExecuteCreateDatabase(ast, context.get());
        case kNodeDropDB:
            return ExecuteDropDatabase(ast, context.get());
        case kNodeShowDB:
            return ExecuteShowDatabases(ast, context.get());
        case kNodeUseDB:
            return ExecuteUseDatabase(ast, context.get());
        case kNodeShowTables:
            return ExecuteShowTables(ast, context.get());
        case kNodeCreateTable:
            return ExecuteCreateTable(ast, context.get());
        case kNodeDropTable:
            return ExecuteDropTable(ast, context.get());
        case kNodeShowIndexes:
            return ExecuteShowIndexes(ast, context.get());
        case kNodeCreateIndex:
            return ExecuteCreateIndex(ast, context.get());
        case kNodeDropIndex:
            return ExecuteDropIndex(ast, context.get());
        case kNodeTrxBegin:
            return ExecuteTrxBegin(ast, context.get());
        case kNodeTrxCommit:
            return ExecuteTrxCommit(ast, context.get());
        case kNodeTrxRollback:
            return ExecuteTrxRollback(ast, context.get());
        case kNodeExecFile:
            return ExecuteExecfile(ast, context.get());
        case kNodeQuit:
            return ExecuteQuit(ast, context.get());
        default:
            break;
    }
    // Plan the query.
    Planner planner(context.get());
    std::vector<Row> result_set{};
    try {
        planner.PlanQuery(ast);
        // Execute the query.
        ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
    } catch (const exception &ex) {
        std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
        return DB_FAILED;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
            double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    // Return the result set as string.
    std::stringstream ss;
    ResultWriter writer(ss);

    if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
        auto schema = planner.plan_->OutputSchema();
        auto num_of_columns = schema->GetColumnCount();
        if (!result_set.empty()) {
            // find the max width for each column
            vector<int> data_width(num_of_columns, 0);
            for (const auto &row: result_set) {
                for (uint32_t i = 0; i < num_of_columns; i++) {
                    data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
                }
            }
            int k = 0;
            for (const auto &column: schema->GetColumns()) {
                data_width[k] = max(data_width[k], int(column->GetName().length()));
                k++;
            }
            // Generate header for the result set.
            writer.Divider(data_width);
            k = 0;
            writer.BeginRow();
            for (const auto &column: schema->GetColumns()) {
                writer.WriteHeaderCell(column->GetName(), data_width[k++]);
            }
            writer.EndRow();
            writer.Divider(data_width);

            // Transforming result set into strings.
            for (const auto &row: result_set) {
                writer.BeginRow();
                for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
                    writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
                }
                writer.EndRow();
            }
            writer.Divider(data_width);
        }
        writer.EndInformation(result_set.size(), duration_time, true);
    } else {
        writer.EndInformation(result_set.size(), duration_time, false);
    }
    if (!IsExecuteFile)
        std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
    switch (result) {
        case DB_ALREADY_EXIST:
            cout << "Database already exists." << endl;
            break;
        case DB_NOT_EXIST:
            cout << "Database not exists." << endl;
            break;
        case DB_TABLE_ALREADY_EXIST:
            cout << "Table already exists." << endl;
            break;
        case DB_TABLE_NOT_EXIST:
            cout << "Table not exists." << endl;
            break;
        case DB_INDEX_ALREADY_EXIST:
            cout << "Index already exists." << endl;
            break;
        case DB_INDEX_NOT_FOUND:
            cout << "Index not exists." << endl;
            break;
        case DB_COLUMN_NAME_NOT_EXIST:
            cout << "Column not exists." << endl;
            break;
        case DB_KEY_NOT_FOUND:
            cout << "Key not exists." << endl;
            break;
        case DB_QUIT:
            cout << "Bye." << endl;
            break;
        default:
            break;
    }
}

//context maybe empty, so we must not use context->current_db_ directly
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    //TODO: CHECK RIGHT
    ast = ast->child_;
    string database_name = ast->val_;
    auto it = dbs_.find(database_name);
    if (it == dbs_.end()) {
        DBStorageEngine *new_db = new DBStorageEngine(database_name, true);
        dbs_.emplace(database_name, new_db);
        endTime = clock();
        cout << "Query OK, 1 row affected (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return DB_SUCCESS;
    } else return DB_ALREADY_EXIST;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    //TODO: CHECK RIGHT
    ast = ast->child_;
    string database_name = ast->val_;
    auto it = dbs_.find(database_name);
    if (it != dbs_.end()) {
        dbs_.erase(database_name);
        remove(("./databases/" + database_name).c_str());
        endTime = clock();
        cout << "Query OK, 1 row affected (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return DB_SUCCESS;
    } else {
        cout << "ERROR 1008 (HY000): Can't drop database \'" << database_name << "\'; database doesn't exist" << endl;
        return DB_NOT_EXIST;
    }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    std::stringstream ss;
    ResultWriter show(ss);
    vector<int> database_width;
    int name_length = 15;
    for (auto &db: dbs_) {
        if (db.first.length() > name_length) name_length = db.first.length();
    }
    database_width.push_back(name_length);
    show.Divider(database_width);
    show.BeginRow();
    show.WriteHeaderCell("show databases", name_length);
    show.EndRow();
    show.Divider(database_width);
    int database_num = 0;
    for (auto &db: dbs_) {
        show.BeginRow();
        show.WriteCell(db.first, name_length);
        show.EndRow();
        database_num++;
    }
    show.Divider(database_width);
    std::cout << show.stream_.rdbuf();
    std::cout << database_num << " rows in set.";
    endTime = clock();
    cout << "(" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    ast = ast->child_;  //语法树孩子节点找db
    string database_name = ast->val_;
    auto it = dbs_.find(database_name);
    //找到
    if (it != dbs_.end()) {
        current_db_ = database_name;
        endTime = clock();
        cout << "Query OK (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return DB_SUCCESS;
    }
    //没找到
    return DB_NOT_EXIST;

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    auto it = dbs_.find(current_db_);
    if (it == dbs_.end()) return DB_NOT_EXIST;
    DBStorageEngine *db_used = dbs_[current_db_];
    vector<TableInfo *> all_tables;
    db_used->catalog_mgr_->GetTables(all_tables);
    std::stringstream s;
    ResultWriter show(s);
    vector<int> table_width;
    int name_length = 15 + current_db_.length() + 1;
    //找最长
    for (int i = 0; i < all_tables.size(); i++) {
        if (all_tables[i]->GetTableName().length() > name_length)
            name_length = all_tables[i]->GetTableName().length();
    }
    table_width.push_back(name_length);
    show.Divider(table_width);
    show.BeginRow();
    show.WriteHeaderCell("show tables in " + current_db_, name_length);
    show.EndRow();
    show.Divider(table_width);
    //写表
    for (int i = 0; i < all_tables.size(); i++) {
        show.BeginRow();
        show.WriteCell(all_tables[i]->GetTableName(), name_length);
        show.EndRow();
    }
    show.Divider(table_width);
    std::cout << show.stream_.rdbuf();
    std::cout << all_tables.size() << " rows in set.";
    endTime = clock();
    cout << "(" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //记时开始
    ast = ast->child_;
    auto it = dbs_.find(current_db_);
    if (it == dbs_.end()) return DB_NOT_EXIST;
    DBStorageEngine *db_used = dbs_[current_db_];
    string new_table_name = ast->val_;
    ast = ast->next_->child_;  //找孩子
    vector<Column *> table_columns;
    vector<string> columns_order;
    unordered_map<string, bool> unique;
    unordered_map<string, TypeId> type;
    unordered_map<string, int> length;
    while (ast != NULL) {
        //column
        if (ast->type_ == kNodeColumnDefinition) {
            bool node_unique = false;
            if (ast->val_ != NULL && strcmp(ast->val_, "unique") == 0) node_unique = true;
            TypeId node_type = kTypeInvalid;
            pSyntaxNode column_node = ast->child_;
            string column_name = column_node->val_;
            columns_order.push_back(column_name);
            column_node = column_node->next_;
            //不同域类型
            if (strcmp(column_node->val_, "int") == 0) {
                node_type = kTypeInt;
                type.emplace(column_name, node_type);
                length.emplace(column_name, 0);
            } else if (strcmp(column_node->val_, "float") == 0) {
                node_type = kTypeFloat;
                type.emplace(column_name, node_type);
                length.emplace(column_name, 0);
            } else if (strcmp(column_node->val_, "char") == 0) {
                node_type = kTypeChar;
                type.emplace(column_name, node_type);
                column_node = column_node->child_;
                unsigned int node_length = 0;
                for (int i = 0; i < strlen(column_node->val_); i++) {
                    if (column_node->val_[i] == '-' || column_node->val_[i] == '.') return DB_FAILED;
                    node_length = node_length * 10 + column_node->val_[i] - '0';
                }
                length.emplace(column_name, node_length);
            }
            unique.emplace(column_name, node_unique);
        }
            //to do
        else if (ast->type_ == kNodeColumnList) {
            //check primary key
            if (ast->val_ != NULL && strcmp(ast->val_, "primary keys") == 0) {
                pSyntaxNode primary_keys = ast->child_;
                vector<string> primary_keys_names;
                while (primary_keys != NULL) {
                    primary_keys_names.push_back(primary_keys->val_);
                    auto find_column = unique.find(primary_keys->val_);
                    if (find_column == unique.end()) return DB_COLUMN_NAME_NOT_EXIST;
                    unique[primary_keys->val_] = true;
                    primary_keys = primary_keys->next_;
                }
            }
        }
        ast = ast->next_;
    }
    for (int i = 0; i < columns_order.size(); i++) {
        Column *new_column;
        if (length[columns_order[i]] > 0)
            new_column = new Column(columns_order[i], type[columns_order[i]], length[columns_order[i]],
                                    i, false, unique[columns_order[i]]);
        else
            new_column = new_column = new Column(columns_order[i], type[columns_order[i]], i, false,
                                                 unique[columns_order[i]]);
        table_columns.push_back(new_column);
    }
    Schema *table_schema = new Schema(table_columns, true);
    TableInfo *tableInfo;
    dberr_t result = db_used->catalog_mgr_->CreateTable(new_table_name, table_schema, nullptr, tableInfo);
    if (result == DB_SUCCESS) {
        endTime = clock();
        cout << "Query OK (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    }
    return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock();
    ast = ast->child_;
    auto it = dbs_.find(current_db_);
    if (it == dbs_.end()) return DB_NOT_EXIST;
    DBStorageEngine *db_used = dbs_[current_db_];
    dberr_t result = db_used->catalog_mgr_->DropTable(ast->val_);
    if (result == DB_SUCCESS) {
        endTime = clock();
        cout << "Query OK (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    }
    return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock();
    auto it = dbs_.find(current_db_);
    if (it == dbs_.end()) return DB_NOT_EXIST;
    DBStorageEngine *db_used = dbs_[current_db_];
    uint32_t index_num = 0;
    vector<IndexInfo *> all_indexes;
    if (ast->child_ == NULL) {
        vector<TableInfo *> all_table;
        db_used->catalog_mgr_->GetTables(all_table);
        for (auto i = 0; i < all_table.size(); i++) {
            db_used->catalog_mgr_->GetTableIndexes(all_table[i]->GetTableName(), all_indexes);
        }
    } else {
        ast = ast->child_;
        string table_name = ast->val_;
        db_used->catalog_mgr_->GetTableIndexes(table_name, all_indexes);
    }

    std::stringstream ss;
    ResultWriter show(ss);
    vector<int> width;
    int index_name_length = 8;
    for (int i = 0; i < all_indexes.size(); i++) {
        if (all_indexes[i]->GetIndexName().length() > index_name_length)
            index_name_length = all_indexes[i]->GetIndexName().length();
    }
    width.push_back(index_name_length);
    show.Divider(width);
    show.BeginRow();
    show.WriteHeaderCell("indexes", index_name_length);
    show.EndRow();
    show.Divider(width);
    for (int i = 0; i < all_indexes.size(); i++) {
        show.BeginRow();
        show.WriteCell(all_indexes[i]->GetIndexName(), index_name_length);
        show.EndRow();
    }
    show.Divider(width);
    std::cout << show.stream_.rdbuf();
    endTime = clock();
    std::cout << all_indexes.size() << "rows in set(" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)"
              << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock();  //计时开始
    ast = ast->child_;
    auto it = dbs_.find(current_db_);
    if (it == dbs_.end()) return DB_NOT_EXIST;
    string index_name = ast->val_;
    ast = ast->next_;
    string table_name = ast->val_;
    ast = ast->next_;
    TableInfo *tableInfo;
    if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tableInfo) == DB_TABLE_NOT_EXIST) {
        return DB_TABLE_NOT_EXIST;
    }
    // compare string
    if (strcmp(ast->val_, "index keys") == 0) {
        ast = ast->child_;
        vector<string> index_keys;
        while (ast != NULL) {
            index_keys.push_back(ast->val_);
            ast = ast->next_;
        }
        IndexInfo *indexInfo_;
        dberr_t result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr,
                                                                      indexInfo_,

                                                                      "bptree");
        if (result != DB_SUCCESS) return result;
        endTime = clock();  //计时结束
        cout << "Query OK (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return result;

    }

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
    //#ifdef ENABLE_EXECUTE_DEBUG
    //    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    //#endif
    //    return DB_FAILED;
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    ast = ast->child_;
    auto it = dbs_.find(current_db_);
    if (it == dbs_.end()) return DB_NOT_EXIST;
    DBStorageEngine *db_used = dbs_[current_db_];
    vector<TableInfo *> tables;
    db_used->catalog_mgr_->GetTables(tables);
    for (auto it: tables) {
        db_used->catalog_mgr_->DropIndex(it->GetTableName(), ast->val_);
    }
    endTime = clock();
    cout << "Query OK (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
    return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
    clock_t startTime, endTime;
    startTime = clock(); //计时开始
    auto fileNode = ast->child_;
    ifstream ifs;
    ifs.open(fileNode->val_, ios::in);
    if (!ifs.is_open()) {//无法打开文件
        endTime = clock();
        cout<<"The file does not exist. ("<< (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return DB_FAILED;
    }
    const int buf_size = 1024;
    char cmd[buf_size];
    // for print syntax tree
    TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
    uint32_t syntax_tree_id = 0;
    IsExecuteFile = true;
    while (1) {
        // read from buffer
        int i = 0;
        char ch;
        memset(cmd, 0, sizeof(cmd));
        while ((ch = ifs.get()) != ';' && ch != EOF) {
            cmd[i++] = ch;
        }
        if (ch == EOF) break;//退出
        cmd[i] = ch;  // ;
        ifs.get();      // remove enter    // create buffer for sql input
        YY_BUFFER_STATE bp = yy_scan_string(cmd);
        if (bp == nullptr) {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);

        // init parser module
        MinisqlParserInit();

        // parse
        yyparse();

        // parse result handle
        if (MinisqlParserGetError()) {
            // error
            printf("%s\n", MinisqlParserGetErrorMessage());
        } else {
            // Comment them out if you don't need to debug the syntax tree
//            printf("[INFO] Sql syntax parse ok!\n");
//            SyntaxTreePrinter printer(MinisqlGetParserRootNode());
//            printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
        }

        auto result = Execute(MinisqlGetParserRootNode());//这里要自己处理，否则会循环定义

        // clean memory after parse
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();

        // quit condition
        ExecuteInformation(result);
        if (result == DB_QUIT) {
            break;
        } else if (result != DB_SUCCESS) {
            IsExecuteFile = false;
            return result;
        }
    }
    endTime = clock();
    cout << "Query OK (" << (double) (endTime - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    IsExecuteFile = false;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    return DB_QUIT;
}


