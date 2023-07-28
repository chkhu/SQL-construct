#include "storage/table_heap.h"
#include "common/config.h"
#include "storage/table_iterator.h"

/**
 * TODO: Test
 * @param exec_ctx
 */

/*
 * InsertTuple()
 * Insert tuple into table heap
 * @param row: the row to be inserted
 * @param txn: the transaction that performs the insert
 * @return: true if insert successfully, false otherwise
 * 1. Find the first page with enough space, if no page has enough space, create a new page.
 * 2. Insert the tuple into the page.
 * 3. If the page is full, set the next page id of the current page to the new page.
 * ...
 * finally unpin the page
 */

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    // Step1: Find the first page with enough space, if no page has enough space, create a new page.
    if (last_page_id_ != INVALID_PAGE_ID) {
        auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
        if (last_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
            buffer_pool_manager_->UnpinPage(last_page_id_, true);
            return true;
        }
        page_id_t new_page_id = INVALID_PAGE_ID;
        auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
        ASSERT(new_page_id != INVALID_PAGE_ID, "new page fail in Insert tuple");
        new_page->Init(new_page_id, last_page_id_, log_manager_, txn);
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        last_page->SetNextPageId(new_page_id);
        //unpin the page
        buffer_pool_manager_->UnpinPage(last_page_id_, true);
        buffer_pool_manager_->UnpinPage(new_page_id, true);
        last_page_id_ = new_page_id;
        return true;
    } else {
        page_id_t traverse_page_id = first_page_id_;
        while (traverse_page_id != INVALID_PAGE_ID) {
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(traverse_page_id));
            // TODO: if need multiple threads, need to add WLatch here
            if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
                buffer_pool_manager_->UnpinPage(traverse_page_id, true);
                return true;
            }
            buffer_pool_manager_->UnpinPage(traverse_page_id, false);
            if (page->GetNextPageId() != INVALID_PAGE_ID)
                traverse_page_id = page->GetNextPageId();
            else
                break;
        }
        TablePage *new_page = nullptr;
        page_id_t new_page_id = INVALID_PAGE_ID;
        while (true) {
            // TODO: didn't understand why it's possible to have nullptr here
            new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
            if (new_page == nullptr) {
                sleep(1);
                continue;
            }
            break;
        }
        // if there is no page in the table_heap
        if (traverse_page_id == INVALID_PAGE_ID) {
            first_page_id_ = new_page_id;
            last_page_id_ = new_page_id;
        }
        // TODO: if need multiple threads, need to add lock here
        new_page->Init(new_page_id, traverse_page_id, log_manager_, txn);
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        auto rear_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(traverse_page_id));
        rear_page->SetNextPageId(new_page_id);
        buffer_pool_manager_->UnpinPage(traverse_page_id, true);
        buffer_pool_manager_->UnpinPage(new_page_id, true);
        return true;
    }
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the transaction.
    if (page == nullptr) {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
//  page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
//  page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

/**
 * TODO: Test
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
    // Step1: Find the page which contains the tuple.
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the transaction.
    if (page == nullptr) {
        return false;
    }
    // Step2: Update the tuple in the page.
//  page->WLatch();
    // step 2.0 get the old tuple
    Row old_tuple = Row(rid);
//  page->GetTuple(&old_tuple, schema_, txn, lock_manager_);
    uint8_t error_code = page->UpdateTuple(row, &old_tuple, schema_, txn, lock_manager_, log_manager_);
    if (error_code == 0) {
//    page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return true;
    } else if (error_code == 1) {
//    page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return false;
    }
    MarkDelete(rid, txn);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    InsertTuple(row, txn);
    return true;
}

/**
 * TODO: Test
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Otherwise, apply the tuple as deleted.
//  page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
//  page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Rollback to delete.
//  page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
//  page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Test
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    // If the page could not be found, then abort the transaction.
    if (page == nullptr) {
        return false;
    }
    auto ret = page->GetTuple(row, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return ret;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr) {
        return;
    }
    if (page->GetNextPageId() != INVALID_PAGE_ID) {
        DeleteTable(page->GetNextPageId());
    }
    buffer_pool_manager_->DeletePage(page_id);
}

/**
 * TODO: test
 */
TableIterator TableHeap::Begin(Transaction *txn) {
    RowId first_row_id;
    if (first_page_id_ == INVALID_PAGE_ID) {
        return TableIterator();
    }
    // create a begin ptr
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    // traverse
    while (page != nullptr && page->GetFirstTupleRid(&first_row_id) == false) {
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    }
    //not found
    if (page == nullptr) {
        return TableIterator();
    } else {  //found
        auto ret = TableIterator(*this, first_row_id);
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        return ret;
    }
}

/**
 * TODO: test
 */
TableIterator TableHeap::End() { return TableIterator(); }
