#include "catalog/catalog.h"
#include "index/b_plus_tree.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);  // magic num
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter: table_meta_pages_) {    //page info
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter: index_meta_pages_) {    //index
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}
//deserialize
CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);  //magic num
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
    //ASSERT(false, "Not Implemented yet");
    //size = magic_num + table_mata_page_size + index_meta_page_size + table_meta_page + index_meta_page
    // 8 bytes for size, 12 = 4 + 4 + 4
    uint32_t size = 12 + 8 * (table_meta_pages_.size() + index_meta_pages_.size());
    return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
//constructor
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    //init == true, create a new CM
    if (init) {
        catalog_meta_ = new CatalogMeta;
        next_index_id_.store(0);
        next_table_id_.store(0);
    } else {   //read from old
        catalog_meta_ = new CatalogMeta;
        Page *meta_page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = catalog_meta_->DeserializeFrom(meta_page_->GetData());
        next_index_id_.store(0);
        next_table_id_.store(0);
        for (auto iter: catalog_meta_->table_meta_pages_) {
            if (next_table_id_ <= iter.first) next_table_id_.store(iter.first + 1);
            TableMetadata *old_table_meta_data;
            Page *old_table_page = buffer_pool_manager_->FetchPage(iter.second);
            old_table_meta_data = nullptr;
            //copy table_metadata
            old_table_meta_data->DeserializeFrom(old_table_page->GetData(), old_table_meta_data);
            table_names_.emplace(old_table_meta_data->GetTableName(), iter.first);
            TableHeap *old_table_heap = nullptr;
            old_table_heap = old_table_heap->Create(buffer_pool_manager_, old_table_meta_data->GetFirstPageId(),
                                                    old_table_meta_data->schema_, nullptr, nullptr);
            TableInfo *old_table_info = old_table_info->Create();
            old_table_info->Init(old_table_meta_data, old_table_heap);
            tables_.emplace(iter.first, old_table_info);
        }
        for (auto iter: catalog_meta_->index_meta_pages_) {
            if (next_index_id_ <= iter.first) next_index_id_.store(iter.first + 1);
            IndexMetadata *old_index_meta_data = nullptr;
            Page *old_index_page = buffer_pool_manager_->FetchPage(iter.second);
            //copy index_metadata
            old_index_meta_data->DeserializeFrom(old_index_page->GetData(), old_index_meta_data);
            //find table
            string old_table_name;
            for (auto it: table_names_) {
                if (it.second == old_index_meta_data->GetTableId()) {
                    old_table_name = it.first;
                    break;
                }
            }
            string old_index_name = old_index_meta_data->GetIndexName();
            index_names_[old_table_name][old_index_name] = iter.first;
            IndexInfo *old_index_info = old_index_info->Create();
            old_index_info->Init(old_index_meta_data, tables_[old_index_meta_data->GetTableId()], buffer_pool_manager_);
            indexes_.emplace(iter.first, old_index_info);
        }
    }
}
//destructor
CatalogManager::~CatalogManager() {
    FlushCatalogMetaPage();
    if (catalog_meta_ != NULL) {
        delete catalog_meta_;
        catalog_meta_ = NULL;
    }
    for (auto iter: tables_) {
        delete iter.second;
    }
    for (auto iter: indexes_) {
        delete iter.second;
    }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const std::string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
    auto it = table_names_.find(table_name);
    if (it != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
    table_id_t new_table_id_;
    // using std::atomic
    do {
        new_table_id_ = next_table_id_.load(std::memory_order_seq_cst);  // get value atomically
    } while (0);
    table_names_.emplace(table_name, new_table_id_);
    page_id_t new_table_page_id_;
    buffer_pool_manager_->NewPage(new_table_page_id_);
    table_info = table_info->Create();
    //create tableheap
    TableHeap *new_table_heap = new_table_heap->Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
    page_id_t new_heap_root_id = new_table_heap->GetFirstPageId();
    //create table_metadata
    TableMetadata *new_table_meta_data = new_table_meta_data->Create(new_table_id_, table_name, new_heap_root_id,
                                                                     schema);
    //write table_metadata to disk
    new_table_meta_data->SerializeTo(buffer_pool_manager_->FetchPage(new_table_page_id_)->GetData());
    buffer_pool_manager_->FlushPage(new_table_page_id_);

    //new_table_meta_data->root_page_id_ = new_table_heap->GetFirstPageId();
    table_info->Init(new_table_meta_data, new_table_heap);
    tables_.emplace(new_table_id_, table_info);

    catalog_meta_->table_meta_pages_.emplace(new_table_id_, new_table_page_id_);
    //tables_[new_table_id_] = table_info;
    next_table_id_.store(next_table_id_ + 1);
    FlushCatalogMetaPage();
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const std::string &table_name, TableInfo *&table_info) {
    auto find_table_name = table_names_.find(table_name);
    if (find_table_name == table_names_.end()) return DB_TABLE_NOT_EXIST;
    auto find_table_info = tables_.find(find_table_name->second);
    table_info = find_table_info->second;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
    for (auto it = tables_.begin(); it != tables_.end(); it++) {
        tables.push_back(it->second);
    }
    return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const std::string &table_name) {
    auto find_table = table_names_.find(table_name);
    if (find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
    table_id_t drop_id = table_names_[table_name];
    buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[drop_id]);
    catalog_meta_->table_meta_pages_.erase(drop_id);
    table_names_.erase(table_name);
    tables_.erase(drop_id);

    auto check_index = index_names_.find(table_name);
    if (check_index == index_names_.end()) return DB_SUCCESS;
    std::unordered_map<std::string, index_id_t> drop_index = index_names_[table_name];
    for (auto it = drop_index.begin(); it != drop_index.end(); it++) {
        DropIndex(table_name, it->first);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const std::string &index_type) {
    auto find_table = table_names_.find(table_name);
    if (find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
    auto find_index = index_names_.find(table_name);
    if (find_index != index_names_.end()) {
        auto find_index_2 = find_index->second.find(index_name);
        if (find_index_2 != find_index->second.end()) return DB_INDEX_ALREADY_EXIST;
    }
    //construct schema map
    Schema *table_schema_used = tables_[find_table->second]->GetSchema();
    std::vector<uint32_t> new_key_map_;
    for (auto it = index_keys.begin(); it != index_keys.end(); it++) {
        uint32_t i;
        for (i = 0; i < table_schema_used->GetColumnCount(); i++) {
            const Column *check_column = table_schema_used->GetColumn(i);
            if (check_column->GetName() == *it) {
                //may change
                new_key_map_.push_back(i);
                break;
            }
        }
        if (i == table_schema_used->GetColumnCount()) return DB_COLUMN_NAME_NOT_EXIST;
    }
    //using std::atomic
    index_id_t new_index_id_;
    do {
        new_index_id_ = next_index_id_.load(std::memory_order_seq_cst);  // get value atomically
    } while (0);
    index_names_[table_name][index_name] = new_index_id_;
    index_info = index_info->Create();
    //write index_metadata to disk
    page_id_t new_index_page_id_;
    buffer_pool_manager_->NewPage(new_index_page_id_);
    IndexMetadata *new_index_meta = new_index_meta->Create(new_index_id_, index_name, find_table->second, new_key_map_);
    catalog_meta_->index_meta_pages_.emplace(new_index_id_, new_index_page_id_);
    new_index_meta->SerializeTo(buffer_pool_manager_->FetchPage(new_index_page_id_)->GetData());
    buffer_pool_manager_->FlushPage(new_index_page_id_);
    //init info
    index_info->Init(new_index_meta, tables_[find_table->second], buffer_pool_manager_);
    indexes_.emplace(new_index_id_, index_info);
    next_index_id_.store(next_index_id_ + 1);
    auto this_index = index_info->GetIndex();
    auto table_heap = tables_[find_table->second]->GetTableHeap();
    auto table_iterator = table_heap->Begin(txn);
    while (table_iterator != table_heap->End()) {
        RowId rid = table_iterator->GetRowId();
        Row row = *table_iterator;
        Row index_row(rid);
        row.GetKeyFromRow(tables_[find_table->second]->GetSchema(), index_info->GetIndexKeySchema(), index_row);
        this_index->InsertEntry(index_row, rid, txn);
        table_iterator++;
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    // ASSERT(false, "Not Implemented yet");
    //return DB_FAILED;
    auto find_table = table_names_.find(table_name);
    if (find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
    auto check_index = index_names_.find(table_name);
    if (check_index == index_names_.end()) return DB_INDEX_NOT_FOUND;
    std::unordered_map<std::string, index_id_t> find_table_index = index_names_.at(table_name);
    if (find_table_index.size() == 0) return DB_INDEX_NOT_FOUND;
    auto find_index = find_table_index.find(index_name);
    if (find_index == find_table_index.end()) return DB_INDEX_NOT_FOUND;
    auto get_index = indexes_.find(find_table_index[index_name]);
    index_info = get_index->second;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    auto find_table = table_names_.find(table_name);
    if (find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
    auto find_table_index = index_names_.find(table_name);
    if (find_table_index == index_names_.end()) return DB_INDEX_NOT_FOUND;
    std::unordered_map<std::string, index_id_t> check_index_id;
    check_index_id = index_names_.at(table_name);
    for (auto insert = check_index_id.begin(); insert != check_index_id.end(); insert++) {
        indexes.push_back(indexes_.at(insert->second));
    }
    if (indexes.size() == 0) return DB_INDEX_NOT_FOUND;
    else return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const std::string &table_name, const std::string &index_name) {
    auto check_table = index_names_.find(table_name);
    if (check_table == index_names_.end()) return DB_SUCCESS;
    std::unordered_map<std::string, index_id_t> droped_index = index_names_[table_name];
    index_names_.erase(table_name);
    index_id_t drop_id;
    auto check_index = droped_index.find(index_name);
    if (check_index != droped_index.end()) {
        drop_id = droped_index[index_name];
        catalog_meta_->index_meta_pages_.erase((drop_id));
        indexes_.erase(drop_id);
        droped_index.erase(index_name);
    }
    if (droped_index.size() > 0) index_names_[table_name] = droped_index;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
    Page *tem_page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(tem_page_->GetData());
    buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto find_table = tables_.find(table_id);
    if (find_table == tables_.end()) return DB_TABLE_NOT_EXIST;
    table_info = tables_[table_id];
    return DB_SUCCESS;
}