#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "buffer/buffer_pool_manager.h"
#include "common/rowid.h"
#include "record/row.h"
#include "storage/table_heap.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
    // you may define your own constructor based on your member variables
    TableIterator(const TableHeap &table_heap, const Row &row);

    TableIterator(const TableHeap &table_heap, const RowId &row_id);

    TableIterator(const TableIterator &other);

    TableIterator();

    virtual ~TableIterator();

    inline bool operator==(const TableIterator &itr) const {
        return table_page_id_ == itr.table_page_id_ && slot_id_ == itr.slot_id_ && table_heap_ == itr.table_heap_;
    }

    inline bool operator!=(const TableIterator &itr) const {
        return table_page_id_ != itr.table_page_id_ || slot_id_ != itr.slot_id_ || table_heap_ != itr.table_heap_;
    }

    const Row &operator*();

    Row *operator->();

    TableIterator &operator=(const TableIterator &itr) noexcept;

    TableIterator &operator++();

    TableIterator operator++(int);

private:
    // add your own private member variables here
    page_id_t table_page_id_;
    int slot_id_;
    TableHeap *table_heap_;
};

#endif  // MINISQL_TABLE_ITERATOR_H
