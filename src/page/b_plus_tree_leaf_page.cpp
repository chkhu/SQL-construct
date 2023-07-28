#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetSize(0);
    SetPageType(IndexPageType::LEAF_PAGE);
    SetKeySize(key_size);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const { return next_page_id_; }

void LeafPage::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
    if (next_page_id == 0) {
        LOG(INFO) << "Fatal error";
    }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 * note: if not found return -1
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    // right-1，是偏移量
    int left = 0, right = GetSize() - 1;
    int found = -1;
    while (left <= right) {
        int mid = (left + right) / 2;
        auto key_at_middle = KeyAt(mid);
        if (KM.CompareKeys(KeyAt(mid), key) >= 0) {
            found = mid;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return found;
}
//找key
int LeafPage::KeyFind(const GenericKey *key, const KeyManager &KM) {
    int idx = KeyIndex(key, KM);
    if (idx == -1 || KM.CompareKeys(KeyAt(idx), key) != 0) {
        return -1;
    } else {
        return idx;
    }
}


GenericKey *LeafPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
    return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
    *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) { return KeyAt(index); }

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
    memmove(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

void LeafPage::SetPairAt(int index, GenericKey *key, const RowId &value) {
    SetKeyAt(index, key);
    SetValueAt(index, value);
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
// TODO test
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    if (index < 0 || index >= GetSize()) {
        return std::make_pair(nullptr, RowId(0));
    }
    GenericKey *key = KeyAt(index);
    RowId value = ValueAt(index);
    return std::make_pair(key, value);
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 * Assume the input key & value pair has enough space in this page
 * Still insert even there are duplicates
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    int index = KeyIndex(key, KM);
    if (index == -1) {
        return InsertPairAtEnd(key, value);
    }
    return InsertPairBefore(index, key, value);
}
//最开始插
int LeafPage::InsertPairBefore(int index, GenericKey *new_key, const RowId &new_value) {
    PairCopy(PairPtrAt(index + 1), PairPtrAt(index), GetSize() - index);
    IncreaseSize(1);
    SetPairAt(index, new_key, new_value);
    return GetSize();
}

int LeafPage::InsertPairAtEnd(GenericKey *new_key, const RowId &new_value) {
    SetPairAt(GetSize(), new_key, new_value);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * Assume recipient page have enough space to store these pairs, the page is a new page
 * TODO: Understand this fucking meaning
 */
GenericKey *LeafPage::MoveHalfToEmpty(LeafPage *recipient) {
    ASSERT(recipient->GetSize() == 0, "recipient is not empty");
    int half = GetSize() / 2;
    recipient->CopyNFrom(PairPtrAt(half), GetSize() - half);
    SetSize(half);
    auto middle_key = recipient->KeyAt(0);
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
    return middle_key;
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 * TODO: Understand this fucking meaning
 */
void LeafPage::CopyNFrom(void *src, int size) {
    int old_size = GetSize();
    ASSERT(size + GetSize() <= GetMaxSize(), "no enough space for copy n from");
    IncreaseSize(size);
    PairCopy(PairPtrAt(old_size), src, size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
//查表
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    int found = KeyFind(key, KM);
    if (found == -1) {
        return false;
    } else {
        value = ValueAt(found);
        return true;
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    int found = KeyFind(key, KM);
    if (found == -1) {
        return GetSize();
    } else {
        PairCopy(KeyAt(found), KeyAt(found + 1), GetSize() - found - 1);
        IncreaseSize(-1);
        return GetSize();
    }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 * ld: this function just move without delete the page or the key
 */
void LeafPage::MoveAllToLeft(LeafPage *recipient) {
    recipient->CopyNFrom(PairPtrAt(0), GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 * things to do with the middle key should be handled by the caller
 */
GenericKey *LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    recipient->PushBack(KeyAt(0), ValueAt(0));
    Remove(0);
    return KeyAt(0);
}

void LeafPage::Remove(int index) {
    PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - 1 - index);
    SetSize(GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::PushBack(GenericKey *key, const RowId value) {
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
GenericKey *LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    recipient->PushFront(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
    Remove(GetSize() - 1);
    return recipient->KeyAt(0);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::PushFront(GenericKey *key, const RowId value) {
    PairCopy(KeyAt(1), KeyAt(0), GetSize());
    IncreaseSize(1);
    SetPairAt(0, key, value);
}