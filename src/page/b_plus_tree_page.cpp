#include "page/b_plus_tree_page.h"
#include <cmath>
/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }

/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

int BPlusTreePage::GetKeySize() const { return key_size_; }

void BPlusTreePage::SetKeySize(int size) { key_size_ = size; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const { return size_; }

void BPlusTreePage::SetSize(int size) { size_ = size; }

void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
/**
 * TODO: Student Implement
 */
int BPlusTreePage::GetMaxSize() const { return max_size_; }

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }


/**
 * TODO: Student Implement
 */

//min size计算
int BPlusTreePage::GetMinSize() const {
  // root
  if (IsRootPage() && !IsLeafPage()) {
    return 2;
  } else if (IsRootPage() && IsLeafPage()) {
    return 0;
  } else if (!IsRootPage() && !IsLeafPage()) {
    return std::ceil(max_size_ / 2.0);
  } else {
    return std::ceil((max_size_ - 1) / 2.0);
  }
}


/**
 * TODO: Student Implement
 */
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }


page_id_t BPlusTreePage::GetPageId() const { return page_id_; }

void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }