#include "index/b_plus_tree.h"

#include "common/config.h"
#include "common/instance.h"
#include "common/macros.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"
#include <iostream>
#include <algorithm>
#include <vector>

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP);
    TreeFileManagers mgr("tree_");
    // Prepare data
    const int n = 50000;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.push_back(RowId(i + 1));
        delete_seq.push_back(key);
    }
    vector<GenericKey *> keys_copy(keys);
    // Shuffle data
    ShuffleArray(keys);
    ShuffleArray(values);
    ShuffleArray(delete_seq);
//    reverse(keys_copy.begin(), keys_copy.end());
//    reverse(values.begin(), values.end());
//    reverse(delete_seq.begin(), delete_seq.end());

    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
        std::cout << "Inserted " << i + 1 << std::endl;
//        tree.LdsPrintTree();
//        std::cout << std::endl;
    }
    ASSERT_TRUE(tree.Check());
    // Print tree
    tree.PrintTree(mgr[0]);
    // Search keys
    vector<RowId> ans;
    for (int i = 0; i < n; i++) {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Delete half keys
    for (int i = 0; i < n / 2; i++) {
        tree.Remove(delete_seq[i]);
        std::cout << "Removed " << i + 1 << std::endl;
//        tree.LdsPrintTree();
//        std::cout << std::endl;
    }
    tree.PrintTree(mgr[1]);
    // Check valid
    ans.clear();
    for (int i = 0; i < n / 2; i++) {
        ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
    }
    for (int i = n / 2; i < n; i++) {
        ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
        ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
    }
    remove(("./databases/" + db_name).c_str());
}

TEST(BPlusTreeTests, EmptyCreateBPlusTree) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP);
    tree.LdsPrintTree();
    ASSERT(tree.IsEmpty() == true, "CreateBPlusTree should be empty");
    remove(("./databases/" + db_name).c_str());

}

TEST(BPlusTreeTests, SingleLayerInsertFromLeft) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    tree.LdsPrintTree();
    // Prepare data
    const int n = 4;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.emplace_back(i + 1);
    }
    vector<GenericKey *> keys_copy(keys);
//    reverse(keys_copy.begin(), keys_copy.end());
//    reverse(values.begin(), values.end());
//    reverse(delete_seq.begin(), delete_seq.end());

    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
        std::cout << "Inserted " << i + 1 << std::endl;
        tree.LdsPrintTree();
        std::cout << std::endl;
    }
    auto page = reinterpret_cast<LeafPage *>( tree.FindLeafPage(nullptr, INVALID_PAGE_ID, true));
    ASSERT(page->GetPageId() == 2, "the page should be star from 2");
    // test the page is the single page
    ASSERT(page->IsLeafPage() == true, "the page should be leaf page");
    ASSERT(page->IsRootPage() == true, "the page should be root page");
    ASSERT(page->GetNextPageId() == INVALID_PAGE_ID, "the page should not have the next page");
    for (int i = 0; i < 4; i++) {
        auto key = page->KeyAt(i);
        auto rid = page->ValueAt(i);
        auto result = tree.GetKeyManager().CompareKeys(key, keys[i]);
        ASSERT(result == 0, "key should be equal");
        ASSERT(rid == values[i], "rid should be equal");
    }
    remove(("./databases/" + db_name).c_str());

}

TEST(BPlusTreeTests, SingleLayerInsertFromRight) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    tree.LdsPrintTree();
    // Prepare data
    const int n = 4;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.emplace_back(i + 1);
    }
    vector<GenericKey *> keys_copy(keys);
    vector<RowId> values_copy(values);
    reverse(keys.begin(), keys.end());
    reverse(values.begin(), values.end());

    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
        std::cout << "Inserted " << i + 1 << std::endl;
        tree.LdsPrintTree();
        std::cout << std::endl;
    }
    auto page = reinterpret_cast<LeafPage *>( tree.FindLeafPage(nullptr, INVALID_PAGE_ID, true));
    ASSERT(page->GetPageId() == 2, "the page should be star from 2");
    // test the page is the single page
    ASSERT(page->IsLeafPage() == true, "the page should be leaf page");
    ASSERT(page->IsRootPage() == true, "the page should be root page");
    ASSERT(page->GetNextPageId() == INVALID_PAGE_ID, "the page should not have the next page");
    for (int i = 0; i < 4; i++) {
        auto key = page->KeyAt(i);
        auto rid = page->ValueAt(i);
        auto result = tree.GetKeyManager().CompareKeys(key, keys_copy[i]);
        ASSERT(result == 0, "key should be equal");
        ASSERT(rid == values_copy[i], "rid should be equal");
    }
    remove(("./databases/" + db_name).c_str());
}

TEST(BPlusTreeTests, LeafSplit) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    // Prepare data
    const int n = 5;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.push_back(RowId(i + 1));
        delete_seq.push_back(key);
    }
    vector<GenericKey *> keys_copy(keys);


    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
        std::cout << "Inserted " << i + 1 << std::endl;
        tree.LdsPrintTree();
        std::cout << std::endl;
    }
    ASSERT_TRUE(tree.Check());
    vector<RowId> ans;
    for (int i = 0; i < n; i++) {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    remove(("./databases/" + db_name).c_str());

}

TEST(BPlusTreeTests, InternalSplit) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    // Prepare data
    const int n = 11;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.push_back(RowId(i + 1));
        delete_seq.push_back(key);
    }
    vector<GenericKey *> keys_copy(keys);


    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
        std::cout << "Inserted " << i + 1 << std::endl;
        if (i == n - 2 || i == n - 1)
            tree.LdsPrintTree();
        std::cout << std::endl;
    }
    ASSERT_TRUE(tree.Check());
    vector<RowId> ans;
    for (int i = 0; i < n; i++) {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    remove(("./databases/" + db_name).c_str());

}

TEST(BPlusTreeTests, DeleteToEmpty) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    TreeFileManagers mgr("tree_");
    // Prepare data
    const int n = 100;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.push_back(RowId(i + 1));
        delete_seq.push_back(key);
    }
    vector<GenericKey *> keys_copy(keys);
    // Shuffle data
    ShuffleArray(keys);
    ShuffleArray(values);
    ShuffleArray(delete_seq);

    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Print tree
    tree.PrintTree(mgr[0]);
    // Search keys
    vector<RowId> ans;
    for (int i = 0; i < n; i++) {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Delete half keys
    for (int i = 0; i < n; i++) {
        tree.Remove(delete_seq[i]);
        std::cout << "Removed " << i + 1 << std::endl;
    }
    // Check valid
    ans.clear();
    for (int i = 0; i < n; i++) {
        ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
    }
    remove(("./databases/" + db_name).c_str());

}


TEST(BPlusTreeTests, LeafRedistributionToRight) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    TreeFileManagers mgr("tree_");
    // Prepare data
    const int n = 6;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.push_back(RowId(i + 1));
        delete_seq.push_back(key);
    }
    vector<GenericKey *> keys_copy(keys);

    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Search keys
    vector<RowId> ans;
    for (int i = 0; i < n; i++) {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Delete half keys
    tree.LdsPrintTree();
    tree.Remove(keys[0], nullptr);
    tree.LdsPrintTree();
    remove(("./databases/" + db_name).c_str());

}

TEST(BPlusTreeTests, LeafRedistributionToLeaf) {
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 16);
    BPlusTree tree(0, engine.bpm_, KP, 4, 4);
    TreeFileManagers mgr("tree_");
    // Prepare data
    const int n = 7;
    vector<GenericKey *> keys;
    vector<RowId> values;
    vector<GenericKey *> delete_seq;
    map<GenericKey *, RowId> kv_map;
    for (int i = 0; i < n; i++) {
        GenericKey *key = KP.InitKey();
        std::vector<Field> fields{Field(TypeId::kTypeInt, i + 1)};
        KP.SerializeFromKey(key, Row(fields), table_schema);
        keys.push_back(key);
        values.push_back(RowId(i + 1));
        delete_seq.push_back(key);
    }
    vector<GenericKey *> keys_copy(keys);
    reverse(keys.begin(), keys.end());
    reverse(values.begin(), values.end());
    // Map key value
    for (int i = 0; i < n; i++) {
        kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
        tree.Insert(keys[i], values[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Search keys
    vector<RowId> ans;
    for (int i = 0; i < n; i++) {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Delete half keys
    tree.LdsPrintTree();
    tree.Remove(keys[0], nullptr);
    tree.Remove(keys[1], nullptr);
    tree.LdsPrintTree();
    remove(("./databases/" + db_name).c_str());
}
