#include "catalog/catalog.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "utils/utils.h"

static string db_file_name = "catalog_test.db";

TEST(CatalogTest, CatalogMetaTest) {
    char *buf = new char[PAGE_SIZE];
    CatalogMeta *meta = CatalogMeta::NewInstance();
    // fill data
    const int table_nums = 100;
    const int index_nums = 200;
    for (auto i = 0; i < table_nums; i++) {
        meta->GetTableMetaPages()->emplace(i, RandomUtils::RandomInt(0, 1 << 16));
    }
    meta->GetTableMetaPages()->emplace(table_nums, INVALID_PAGE_ID);
    for (auto i = 0; i < index_nums; i++) {
        meta->GetIndexMetaPages()->emplace(i, RandomUtils::RandomInt(0, 1 << 16));
    }
    meta->GetIndexMetaPages()->emplace(index_nums, INVALID_PAGE_ID);
    // serialize
    meta->SerializeTo(buf);
    // deserialize
    CatalogMeta *other = CatalogMeta::DeserializeFrom(buf);
    ASSERT_NE(nullptr, other);
    ASSERT_EQ(table_nums + 1, other->GetTableMetaPages()->size());
    ASSERT_EQ(index_nums + 1, other->GetIndexMetaPages()->size());
    ASSERT_EQ(INVALID_PAGE_ID, other->GetTableMetaPages()->at(table_nums));
    ASSERT_EQ(INVALID_PAGE_ID, other->GetIndexMetaPages()->at(index_nums));
    for (auto i = 0; i < table_nums; i++) {
        EXPECT_EQ(meta->GetTableMetaPages()->at(i), other->GetTableMetaPages()->at(i));
    }
    for (auto i = 0; i < index_nums; i++) {
        EXPECT_EQ(meta->GetIndexMetaPages()->at(i), other->GetIndexMetaPages()->at(i));
    }
    delete meta;
    delete other;
    auto db_file_name_ = "./databases/" + db_file_name;
    remove(db_file_name_.c_str());

}

TEST(CatalogTest, CatalogTableTest) {
    /** Stage 2: Testing simple operation */
    auto db_01 = new DBStorageEngine(db_file_name, true);
    auto &catalog_01 = db_01->catalog_mgr_;
    TableInfo *table_info = nullptr;
    ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_01->GetTable("table-1", table_info));
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    auto schema = std::make_shared<Schema>(columns);
    Transaction txn;
    catalog_01->CreateTable("table-1", schema.get(), &txn, table_info);
    ASSERT_TRUE(table_info != nullptr);
    TableInfo *table_info_02 = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog_01->GetTable("table-1", table_info_02));
    ASSERT_EQ(table_info, table_info_02);
    auto *table_heap = table_info->GetTableHeap();
    ASSERT_TRUE(table_heap != nullptr);
    ASSERT_EQ(DB_TABLE_ALREADY_EXIST, catalog_01->CreateTable("table-1", schema.get(), &txn, table_info));
    string new_table_name = "0";
    for (int i = 0; i < 100; i++) {
        new_table_name += "1";
        TableInfo *table_info_03 = nullptr;
        catalog_01->CreateTable(new_table_name, schema.get(), &txn, table_info);
        ASSERT_EQ(DB_SUCCESS, catalog_01->GetTable(new_table_name, table_info_03));
        ASSERT_EQ(table_info, table_info_03);
        auto *new_table_heap = table_info->GetTableHeap();
        ASSERT_TRUE(new_table_heap != nullptr);
    }
    vector<TableInfo *> all_table_info;
    catalog_01->GetTables(all_table_info);
    for (int i = 0; i < 101; i++) {
        ASSERT_EQ(all_table_info[i]->GetSchema()->GetColumnCount(), table_info->GetSchema()->GetColumnCount());
    }
    delete db_01;
    /** Stage 2: Testing catalog loading */
    auto db_02 = new DBStorageEngine(db_file_name, false);
    auto &catalog_02 = db_02->catalog_mgr_;
    TableInfo *table_info_04 = nullptr;
    ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_02->GetTable("table-2", table_info_04));
    ASSERT_EQ(DB_SUCCESS, catalog_02->GetTable("table-1", table_info_04));
    string old_table_name = "0";
    for (int i = 0; i < 100; i++) {
        TableInfo *table_info_05 = nullptr;
        old_table_name += "1";
        ASSERT_EQ(DB_SUCCESS, catalog_02->GetTable(old_table_name, table_info_05));
        catalog_02->DropTable(old_table_name);
        ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_02->GetTable(old_table_name, table_info_05));
    }
    delete db_02;
    auto db_file_name_ = "./databases/" + db_file_name;
    remove(db_file_name_.c_str());
}

TEST(CatalogTest, CatalogIndexTest) {
    /** Stage 1: Testing simple operation */
    auto db_01 = new DBStorageEngine(db_file_name, true);
    auto &catalog_01 = db_01->catalog_mgr_;
    TableInfo *table_info = nullptr;
    ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_01->GetTable("table-1", table_info));
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    auto schema = std::make_shared<Schema>(columns);
    Transaction txn;
    catalog_01->CreateTable("table-1", schema.get(), &txn, table_info);
    ASSERT_TRUE(table_info != nullptr);

    IndexInfo *index_info = nullptr;
    std::vector<std::string> bad_index_keys{"id", "age", "name"};
    std::vector<std::string> index_keys{"id", "name"};
    auto r1 = catalog_01->CreateIndex("table-0", "index-0", index_keys, &txn, index_info, "bptree");
    ASSERT_EQ(DB_TABLE_NOT_EXIST, r1);
    auto r2 = catalog_01->CreateIndex("table-1", "index-1", bad_index_keys, &txn, index_info, "bptree");
    ASSERT_EQ(DB_COLUMN_NAME_NOT_EXIST, r2);
    auto r3 = catalog_01->CreateIndex("table-1", "index-1", index_keys, &txn, index_info, "bptree");
    ASSERT_EQ(DB_SUCCESS, r3);
    for (int i = 0; i < 10; i++) {
        std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                                  Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
        Row row(fields);
        RowId rid(1000, i);
        ASSERT_EQ(DB_SUCCESS, index_info->GetIndex()->InsertEntry(row, rid, nullptr));
    }
    // Scan Key
    std::vector<RowId> ret;
    for (int i = 0; i < 10; i++) {
        std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                                  Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
        Row row(fields);
        RowId rid(1000, i);
        ASSERT_EQ(DB_SUCCESS, index_info->GetIndex()->ScanKey(row, ret, &txn));
        ASSERT_EQ(rid.Get(), ret[i].Get());
    }
    string new_index_name = "0";
    for (int i = 0; i < 100; i++) {
        auto r4 = catalog_01->CreateIndex("table-1", new_index_name, index_keys, &txn, index_info, "bptree");
        ASSERT_EQ(DB_SUCCESS, r4);
        new_index_name += "1";
    }
    vector<IndexInfo *> all_index_info;
    auto r5 = catalog_01->GetTableIndexes("table-1", all_index_info);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(all_index_info[i]->GetIndexKeySchema()->GetColumnCount(), 2);
    }
    delete db_01;
    /** Stage 2: Testing catalog loading */
    auto db_02 = new DBStorageEngine(db_file_name, false);
    auto &catalog_02 = db_02->catalog_mgr_;
    auto r4 = catalog_02->CreateIndex("table-1", "index-1", index_keys, &txn, index_info, "bptree");
    ASSERT_EQ(DB_INDEX_ALREADY_EXIST, r4);
    IndexInfo *index_info_02 = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog_02->GetIndex("table-1", "index-1", index_info_02));
    std::vector<RowId> ret_02;
    for (int i = 0; i < 10; i++) {
        std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                                  Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
        Row row(fields);
        RowId rid(1000, i);
        ASSERT_EQ(DB_SUCCESS, index_info_02->GetIndex()->ScanKey(row, ret_02, &txn));
        ASSERT_EQ(rid.Get(), ret_02[i].Get());
    }
    string old_index_name = "0";
    for (int i = 0; i < 100; i++) {
        auto r7 = catalog_02->DropIndex("table-1", old_index_name);
        ASSERT_EQ(DB_SUCCESS, r7);
        old_index_name += "1";
    }
    vector<IndexInfo *> now_all_index;
    catalog_02->GetTableIndexes("table-1", now_all_index);
    ASSERT_EQ(1, now_all_index.size());
    delete db_02;
    auto db_file_name_ = "./databases/" + db_file_name;
    remove(db_file_name_.c_str());
}