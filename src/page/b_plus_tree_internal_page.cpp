#include "page/b_plus_tree_internal_page.h"
#include <cstring>

#include "common/config.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_page.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeyAt(0, nullptr);
    SetMaxSize(max_size);
    SetSize(0);
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetKeySize(key_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
    auto address = pairs_off + index * pair_size + key_off;
    if (key == nullptr)
        memset(pairs_off + index * pair_size + key_off, 0, GetKeySize());
    else
        memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
    return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
    *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

void InternalPage::SetPairAt(int index, GenericKey *key, page_id_t value) {
    ASSERT(index < GetSize(), "index out of bound");
    SetKeyAt(index, key);
    SetValueAt(index, value);
}

int InternalPage::ValueIndex(const page_id_t &value) const {
    for (int i = 0; i < GetSize(); ++i) {
        if (ValueAt(i) == value) return i;
    }
    //没找到
    return -1;
}

void *InternalPage::PairPtrAt(int index) { return KeyAt(index); }
//移动pair
void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
    memmove(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    //牺牲一个指针，从1开始，right还得-1防止溢出
    int left = 1, right = GetSize() - 1;
    int found = 0;
    while (left <= right) {
        int mid = (left + right) / 2;
        if (KM.CompareKeys(KeyAt(mid), key) <= 0) {
            found = mid;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return ValueAt(found);
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * ld: because in the function can not compare the value, must input old->left and new->right
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetSize(2);
    SetKeyAt(0, nullptr);
    SetKeyAt(1, new_key);
    SetValueAt(0, old_value);
    SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 * ld: if developer want to insert the pair in the first place, input old_value = INVALID_PAGE_ID
 * ld: try to know how to know what old_value show be input(judge when call this function)
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    IncreaseSize(1);
    if (old_value == INVALID_PAGE_ID) {
        PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize() - 1);
        SetPairAt(0, new_key, new_value);
        return GetSize();
    }
    int index = ValueIndex(old_value);
    if (index == -1) return -1;
    PairCopy(PairPtrAt(index + 2), PairPtrAt(index + 1), GetSize() - 1 - (index + 1));
    SetPairAt(index + 1, new_key, new_value);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * move less than half amount to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页, 为啥要fetch, 因为要修改parent page id
 */
GenericKey *InternalPage::MoveHalfToEmpty(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    ASSERT(recipient->GetSize() == 0, "recipient not empty");
    int half = GetSize() / 2;
    recipient->CopyNFrom(PairPtrAt(half), GetSize() - half, buffer_pool_manager);
    SetSize(half);
    auto middle_key = recipient->KeyAt(0);
    return middle_key;
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int old_size = GetSize();
    ASSERT(old_size + size <= GetMaxSize(), "size out of bound");
    SetSize(size + old_size);
    PairCopy(PairPtrAt(old_size), src, size);
    for (int i = old_size; i < GetSize(); ++i) {
        page_id_t child_page_id = ValueAt(i);
        auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));
        ASSERT(child_page != nullptr, "fetch child page failed");
        child_page->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(child_page_id, true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - 1 - index);
    SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    auto only_value = ValueAt(0);
    SetSize(0);
    return only_value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * ld: maybe always should move the right one to the left one
 * ld: this function just move without delete the page or the key
 */
void InternalPage::MoveAllToLeft(InternalPage *recipient, GenericKey *middle_key,
                                 BufferPoolManager *buffer_pool_manager) {
    int old = GetSize();
    int recipient_old_size = recipient->GetSize();
    recipient->CopyNFrom(PairPtrAt(0), old, buffer_pool_manager);
    recipient->SetKeyAt(recipient_old_size, middle_key);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE(a.k.a borrow or 重分配)
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * note1: after this move the first key of the right page is the node middle key
 * instead of the invalid key
 * note2: to maintain the invariant, the recipient must be at the left of this page
 * note3: return the new middle key
 */
GenericKey *InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
    recipient->PushBack(middle_key, ValueAt(0), buffer_pool_manager);
    Remove(0);
    auto new_middle_key = KeyAt(0);
    return new_middle_key;
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::PushBack(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int old_size = GetSize();
    IncreaseSize(1);
    SetKeyAt(old_size, key);
    SetValueAt(old_size, value);
    page_id_t child_page_id = value;
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));
    ASSERT(child_page != nullptr, "fetch child page failed");
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
GenericKey *InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                            BufferPoolManager *buffer_pool_manager) {
    int old_size = GetSize();
    recipient->SetKeyAt(0, middle_key);
    recipient->PushFront(ValueAt(old_size - 1), buffer_pool_manager);
    auto new_middle_key = KeyAt(old_size - 1);
    Remove(old_size - 1);
    return new_middle_key;
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 * note:the first key should be replaced by middle key before call this function
 */
void InternalPage::PushFront(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int old_size = GetSize();
    SetSize(old_size + 1);
    PairCopy(PairPtrAt(1), PairPtrAt(0), old_size);
    SetPairAt(0, nullptr, value);
    page_id_t child_page_id = value;
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));
    ASSERT(child_page != nullptr, "fetch child page failed");
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
}