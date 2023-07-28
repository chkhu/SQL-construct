#include "index/b_plus_tree.h"

#include <string>

#include "common/config.h"
#include "common/rowid.h"
#include "glog/logging.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_page.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          processor_(KM),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size),
          root_page_id_(INVALID_PAGE_ID) {
    if (leaf_max_size_ == UNDEFINED_SIZE) {
        int DEFAULT_LEAF_MAX_SIZE = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId)) - 1;
        leaf_max_size_ = DEFAULT_LEAF_MAX_SIZE;
    }
    if (internal_max_size_ == UNDEFINED_SIZE) {
        int DEFAULT_INTERNAL_MAX_SIZE =
                (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t)) - 1;
        internal_max_size_ = DEFAULT_INTERNAL_MAX_SIZE;
    }
    auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    page_id_t root_page_id;
    if (index_root_page->GetRootId(index_id_, &root_page_id)) {
        root_page_id_ = root_page_id;
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    }
}

// destroy
void BPlusTree::DestroySubTree(page_id_t current_page_id) {
    auto current_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (current_page->IsLeafPage()) {
        buffer_pool_manager_->DeletePage(current_page_id);
    } else {
        auto internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
        for (int i = 0; i < internal_page->GetSize(); i++) {
            DestroySubTree(internal_page->ValueAt(i));
        }
        buffer_pool_manager_->DeletePage(current_page_id);
    }
}

void BPlusTree::Destroy() {
    auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    page_id_t root_page_id;
    if (index_root_page->GetRootId(index_id_, &root_page_id)) {
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    } else {
        DestroySubTree(root_page_id);
        index_root_page->Delete(index_id_);
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    }
    root_page_id_ = INVALID_PAGE_ID;
}


bool BPlusTree::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
    if (IsEmpty()) {
        return false;
    }
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false, transaction));
    RowId find_value;
    auto is_find = leaf_page->Lookup(key, find_value, processor_);
    if (is_find) {
        result.push_back(find_value);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
    if (IsEmpty()) {
        StartNewTree(key, value);
        return true;
    } else {
        auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false, transaction));
        if (leaf_page->GetSize() < leaf_max_size_) {
            leaf_page->Insert(key, value, processor_);
            buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
            return true;
        } else {
            leaf_page->Insert(key, value, processor_);
            GenericKey *middle_key = nullptr;
            auto new_leaf_page = Split(leaf_page, transaction, middle_key);
            InsertIntoParent(leaf_page, middle_key, new_leaf_page, transaction);
            buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
            return true;
        }
    }
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    auto root_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(root_page_id_));
    if (root_page == nullptr) {
        LOG(FATAL) << "out of memory";
    }
    root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    root_page->SetNextPageId(INVALID_PAGE_ID);
    auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    index_root_page->Insert(index_id_, root_page_id_);
    InsertIntoLeaf(key, value, nullptr);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false, transaction));
    if (leaf_page->KeyFind(key, processor_) != -1) {
        return false;
    }
    if (leaf_page->GetSize() < leaf_max_size_) {
        leaf_page->Insert(key, value, processor_);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        return true;
    } else {
        GenericKey *middle_key;
        auto new_leaf_page = Split(leaf_page, transaction, middle_key);
        InsertIntoParent(leaf_page, middle_key, new_leaf_page, transaction);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
        return true;
    }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction, GenericKey *&middle_key) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    // new page is on the right side of node
    auto new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(new_page_id));
    if (new_page == nullptr) {
        LOG(FATAL) << "out of memory";
    }
    new_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
    middle_key = node->MoveHalfToEmpty(new_page, buffer_pool_manager_);
    return new_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction, GenericKey *&middle_key) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(new_page_id));
    if (new_page == nullptr) {
        LOG(FATAL) << "out of memory";
    }
    new_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
    middle_key = node->MoveHalfToEmpty(new_page);
    return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
    // if no parent
    if (old_node->GetParentPageId() == INVALID_PAGE_ID) {
        page_id_t new_root_page_id = INVALID_PAGE_ID;
        auto root_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(new_root_page_id));
        ASSERT(root_page != nullptr, "out of memory");
        root_page->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
        root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(new_root_page_id);
        new_node->SetParentPageId(new_root_page_id);
        root_page_id_ = new_root_page_id;
        auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
        index_root_page->Update(index_id_, new_root_page_id);
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
        buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    } else {
        auto parent_page =
                reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
        if (parent_page->GetSize() < parent_page->GetMaxSize()) {
            parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        } else {
            parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            GenericKey *middle_key;
            auto parent_split_right_page = Split(parent_page, transaction, middle_key);
//            Row print_row(INVALID_ROWID);
//            processor_.DeserializeToKey(middle_key, print_row, processor_.GetSchema());
//            Row print_row2(INVALID_ROWID);
//            processor_.DeserializeToKey(key, print_row2, processor_.GetSchema());
            InsertIntoParent(parent_page, middle_key, parent_split_right_page, transaction);
            parent_split_right_page->SetKeyAt(0, nullptr);
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(parent_split_right_page->GetPageId(), true);
        }
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
    if (IsEmpty()) {
        return;
    }
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false, transaction));
    if (leaf_page == nullptr || leaf_page->GetSize() == 0) {
        return;
    }
    leaf_page->RemoveAndDeleteRecord(key, processor_);
    if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
        CoalesceOrRedistribute(leaf_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/* todo
 * 1. User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * understand index's meaning and usage
 * 1. redistribute
 * index is the index of the node
 * if the index is 0, then redistribute should move the right neighbor's first node to the left neighbor's last node
 * if the index is not 0, then redistribute should move the left neighbor's last node to the right neighbor's first node
 */

template<typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
    if (node->IsRootPage()) {
        return AdjustRoot(node);
    }
    auto parent_page =
            reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index = parent_page->ValueIndex(node->GetPageId());
    //index是需要接收结点的结点，所以如果index为0，代表index是最左边的结点，需要从右边的结点接收
    //1. find the recipient of the node
    int recipient_index = index == 0 ? 1 : index - 1;
    auto recipient_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(recipient_index)));
    //2. if the recipient can not merge with the node, then redistribute
    if (recipient_page->GetSize() + node->GetSize() > node->GetMaxSize()) {
        Redistribute(recipient_page, node, index);
        return false;
    } else {
        //if the be removed node index is zero
        Coalesce(recipient_page, node, parent_page, index, transaction);
        if (parent_page->GetSize() < parent_page->GetMinSize()) {
            CoalesceOrRedistribute(parent_page, transaction);
        }
        return true;
    }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * TODO: REPLACE BY YOUR OWN CODE
 * @param   sender_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 **** sender_node is the sender, node is the receiver
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
    if (index == 0) {
        neighbor_node->MoveAllToLeft(node);
        parent->Remove(index + 1);
        buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    } else {
        node->MoveAllToLeft(neighbor_node);
        parent->Remove(index);
        buffer_pool_manager_->DeletePage(node->GetPageId());
    }
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
    if (index == 0) {
        neighbor_node->MoveAllToLeft(node, parent->KeyAt(index + 1), buffer_pool_manager_);
        parent->Remove(index + 1);
        buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    } else {
        node->MoveAllToLeft(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
        parent->Remove(index);
        buffer_pool_manager_->DeletePage(node->GetPageId());
    }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * the caller should make sure it's able to call the function without cause error in the page
 * Using template N to represent either internal page or leaf page.
 * @param   sender_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * the index is the node's index
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    //if the node's index is zero, then the node is the left one, need some borrow from the right neighbor
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(
            node->GetParentPageId()));
    if (index == 0) {
        auto middle_key_index = parent_page->ValueIndex(neighbor_node->GetPageId());
        auto new_middle_key = neighbor_node->MoveFirstToEndOf(node);
        parent_page->SetKeyAt(middle_key_index, new_middle_key);
    } else {
        auto middle_key_index = parent_page->ValueIndex(node->GetPageId());
        auto new_middle_key = neighbor_node->MoveLastToFrontOf(node);
        parent_page->SetKeyAt(middle_key_index, new_middle_key);
    }
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    //if the node's index is zero, then the node is the left one, need some borrow from the right neighbor
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(
            node->GetParentPageId()));
    if (index == 0) {
        auto middle_key_index = parent_page->ValueIndex(neighbor_node->GetPageId());
        auto old_middle_key = parent_page->KeyAt(middle_key_index);
        auto new_middle_key = neighbor_node->MoveFirstToEndOf(node, old_middle_key, buffer_pool_manager_);
        parent_page->SetKeyAt(middle_key_index, new_middle_key);
        neighbor_node->SetKeyAt(0, nullptr);
    } else {
        auto middle_key_index = parent_page->ValueIndex(node->GetPageId());
        auto old_middle_key = parent_page->KeyAt(middle_key_index);
        auto new_middle_key = neighbor_node->MoveLastToFrontOf(node, old_middle_key, buffer_pool_manager_);
        parent_page->SetKeyAt(middle_key_index, new_middle_key);
    }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->GetSize() == 0) {
        auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
        index_root_page->Delete(index_id_);
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
        return true;
    } else if (old_root_node->GetSize() == 1) {
        root_page_id_ = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
        auto new_root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
        new_root_page->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        UpdateRootPageId();
        return true;
    } else {
        return false;
    }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
    if (IsEmpty()) {
        return IndexIterator();
    }
    auto leftest_leaf_page = FindLeafPage(nullptr, root_page_id_, true);
    return IndexIterator(leftest_leaf_page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
    if (IsEmpty()) {
        return IndexIterator();
    }
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *> (FindLeafPage(key, root_page_id_, false));
    if (leaf_page == nullptr) {
        return IndexIterator();
    }
    auto index = leaf_page->KeyIndex(key, processor_);
    if (index == -1) {
        return IndexIterator();
    }
    return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { return IndexIterator(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    if (IsEmpty()) {
        return nullptr;
    } else {
        auto root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
        auto traversed_page = root_page;
        while (!traversed_page->IsLeafPage()) {
            auto internal_page = reinterpret_cast<InternalPage *>(traversed_page);
            page_id_t next_page_id;
            if (leftMost) {
                next_page_id = internal_page->ValueAt(0);
            } else {
                next_page_id = INVALID_PAGE_ID;
//                int i = 0;
//                for (i = 0; i < internal_page->GetSize(); i++) {
//                    if (processor_.CompareKeys(internal_page->KeyAt(i), key) > 0) {
//                        break;
//                    }
//                }
//                next_page_id = internal_page->ValueAt(i - 1);
                next_page_id = internal_page->Lookup(key, processor_);
            }
            buffer_pool_manager_->UnpinPage(traversed_page->GetPageId(), false);
            traversed_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id));
        }
        return reinterpret_cast<Page *>(traversed_page);
    }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
    auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    if (insert_record != 0) {
        index_root_page->Insert(index_id_, root_page_id_);
    } else {
        index_root_page->Update(index_id_, root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
//画图，文件可以看
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
    std::string leaf_prefix("LEAF_");
    std::string internal_prefix("INT_");
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        // Print node name
        out << leaf_prefix << leaf->GetPageId();
        // Print node properties
        out << "[shape=plain color=green ";
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
            << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
            << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
            << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < leaf->GetSize(); i++) {
            out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Leaf node link if there is a next page
        if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
            out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
            out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
                << "};\n";
        }

        // Print parent links if there is a parent
        if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
                << leaf->GetPageId() << ";\n";
        }
    } else {
        auto *inner = reinterpret_cast<InternalPage *>(page);
        // Print node name
        out << internal_prefix << inner->GetPageId();
        // Print node properties
        out << "[shape=plain color=pink ";  // why not?
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
            << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
            << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
            << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < inner->GetSize(); i++) {
            out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
            if (i > 0) {
                out << inner->KeyAt(i);
            } else {
                out << " ";
            }
            out << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Parent link
        if (inner->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
                << internal_prefix
                << inner->GetPageId() << ";\n";
        }
        // Print leaves
        for (int i = 0; i < inner->GetSize(); i++) {
            auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
            ToGraph(child_page, bpm, out);
            if (i > 0) {
                auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
                if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
                    out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
                        << child_page->GetPageId() << "};\n";
                }
                bpm->UnpinPage(sibling_page->GetPageId(), false);
            }
        }
    }
    bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
                  << " next: " << leaf->GetNextPageId() << std::endl;
        for (int i = 0; i < leaf->GetSize(); i++) {
            std::cout << leaf->KeyAt(i) << ",";
        }
        std::cout << std::endl;
        std::cout << std::endl;
    } else {
        auto *internal = reinterpret_cast<InternalPage *>(page);
        std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
                  << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
        }
        std::cout << std::endl;
        std::cout << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
            bpm->UnpinPage(internal->ValueAt(i), false);
        }
    }
}

bool BPlusTree::Check() {
    bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
    if (!all_unpinned) {
        LOG(ERROR) << "problem in page unpin" << endl;
    }
    return all_unpinned;
}

