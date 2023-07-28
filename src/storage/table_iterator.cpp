#include "storage/table_iterator.h"
#include "common/macros.h"
#include "common/rowid.h"
#include "record/row.h"
#include "storage/table_heap.h"

/**
 * TODO: test
 */
TableIterator::TableIterator(const TableHeap &table_heap, const Row &row) {
    table_page_id_ = row.GetRowId().GetPageId();
    ASSERT(table_page_id_ != INVALID_PAGE_ID, "table_page_ is INVALID_PAGE_ID");
    slot_id_ = row.GetRowId().GetSlotNum();
    ASSERT(slot_id_ != INVALID_LSN, "slot_id_ is INVALID_SLOT_NUM");
    table_heap_ = const_cast<TableHeap *>(&table_heap);
}

TableIterator::TableIterator(const TableHeap &table_heap, const RowId &row_id) {
    table_page_id_ = row_id.GetPageId();
    ASSERT(table_page_id_ != INVALID_PAGE_ID, "table_page_ is INVALID_PAGE_ID");
    slot_id_ = row_id.GetSlotNum();
    ASSERT(slot_id_ != INVALID_LSN, "slot_id_ is INVALID_SLOT_NUM");
    table_heap_ = const_cast<TableHeap *>(&table_heap);
}

TableIterator::TableIterator(const TableIterator &other) {
    table_page_id_ = other.table_page_id_;
    slot_id_ = other.slot_id_;
    table_heap_ = other.table_heap_;
}

TableIterator::TableIterator() {
    table_page_id_ = INVALID_PAGE_ID;
    slot_id_ = INVALID_LSN;
    table_heap_ = nullptr;
}

TableIterator::~TableIterator() {}
//
//bool TableIterator::operator==(const TableIterator &itr) const {
//  return table_page_id_ == itr.table_page_id_ && slot_id_ == itr.slot_id_ && table_heap_ == itr.table_heap_;
//}
//
//bool TableIterator::operator!=(const TableIterator &itr) const {
//  return table_page_id_ != itr.table_page_id_ || slot_id_ != itr.slot_id_ || table_heap_ != itr.table_heap_;
//}

const Row &TableIterator::operator*() {
    if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
        // throw exception shows that the iterator is invalid
        throw std::out_of_range("iterator is invalid");
    }
    Row *row = new Row(RowId(table_page_id_, slot_id_));
    auto *table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
    // if you need multiple threads, need to add lock here
    table_page->GetTuple(row, table_heap_->schema_, nullptr, table_heap_->lock_manager_);
    // TODO: not sure if need to unpin page here
    table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
    return *row;
}

Row *TableIterator::operator->() {
    if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
        // throw exception shows that the iterator is invalid
        throw std::out_of_range("iterator is invalid");
    }
    Row *row = new Row(RowId(table_page_id_, slot_id_));
    auto *table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
    table_page->GetTuple(row, table_heap_->schema_, nullptr, table_heap_->lock_manager_);
    // TODO: not sure if need to unpin page here
    table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
    return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept = default;

// ++iter
TableIterator &TableIterator::operator++() {
    if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
        // throw exception shows that the iterator is invalid
        throw std::out_of_range("iterator is invalid");
    }
    auto table_page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
    RowId row_id(table_page_->GetPageId(), slot_id_);
    RowId *next_row_id = new RowId();
    if (table_page_->GetNextTupleRid(row_id, next_row_id)) {
        table_page_id_ = next_row_id->GetPageId();
        slot_id_ = next_row_id->GetSlotNum();
    } else {
        table_page_id_ = INVALID_PAGE_ID;
        slot_id_ = INVALID_LSN;
        while ((table_page_id_ = table_page_->GetNextPageId()) != INVALID_PAGE_ID) {
            table_page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
            if (table_page_->GetFirstTupleRid(next_row_id)) {
                slot_id_ = next_row_id->GetSlotNum();
                table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
                break;
            }
            table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
        }
        if(slot_id_ == INVALID_LSN) {
            table_page_id_ = INVALID_PAGE_ID;
        }
    }
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator ret = TableIterator(*this);
    if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
        // throw exception shows that the iterator is invalid
        throw std::out_of_range("iterator is invalid");
    }
    auto table_page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
    RowId row_id(table_page_->GetPageId(), slot_id_);
    RowId *next_row_id = new RowId();
    if (table_page_->GetNextTupleRid(row_id, next_row_id)) {
        table_page_id_ = next_row_id->GetPageId();
        slot_id_ = next_row_id->GetSlotNum();
    } else {
        table_page_id_ = INVALID_PAGE_ID;
        slot_id_ = INVALID_LSN;
        while ((table_page_id_ = table_page_->GetNextPageId()) != INVALID_PAGE_ID) {
            table_page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
            if (table_page_->GetFirstTupleRid(next_row_id)) {
                slot_id_ = next_row_id->GetSlotNum();
                table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
                break;
            }
            table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
        }
        if(slot_id_ == INVALID_LSN) {
            *this = TableIterator();
        }
    }
    return ret;
}
