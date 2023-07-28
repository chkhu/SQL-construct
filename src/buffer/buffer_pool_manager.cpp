#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

// constructor
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}
// destructor
BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin fetch_page_it and return fetch_page_it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write fetch_page_it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if(page_id == INVALID_PAGE_ID)
    return nullptr;
  auto fetch_page_it = page_table_.find(page_id);
  if (fetch_page_it != page_table_.end()) {
    replacer_->Pin(fetch_page_it->second);
    return &pages_[fetch_page_it->second];
  } else {
    frame_id_t cache_page_frame_id;
    Page *cache_page = nullptr;
    if (!free_list_.empty()) {
      auto free_it = free_list_.begin();
      cache_page_frame_id = *free_it;
      cache_page = &pages_[cache_page_frame_id];
      free_list_.erase(free_it);
    } else {
      if (replacer_->UnpinSize() > 0) {
        replacer_->Victim(&cache_page_frame_id);
        cache_page = &pages_[cache_page_frame_id];
        if (cache_page->IsDirty()) {
          disk_manager_->WritePage(cache_page->page_id_, cache_page->data_);
        }
        page_table_.erase(cache_page->page_id_);
      } else
        return nullptr;
    }
    page_table_.emplace(page_id, cache_page_frame_id);
    disk_manager_->ReadPage(page_id, pages_[cache_page_frame_id].data_);
    pages_[cache_page_frame_id].page_id_ = page_id;
    pages_[cache_page_frame_id].is_dirty_ = false;
    if (pages_[cache_page_frame_id].pin_count_++ == 0) {
      replacer_->Pin(cache_page_frame_id);
    }
    return &pages_[cache_page_frame_id];
  }
  // return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t cache_page_frame_id;
  Page *cache_page = nullptr;
  if (!free_list_.empty()) {
    auto free_it = free_list_.begin();
    cache_page_frame_id = *free_it;
    cache_page = &pages_[cache_page_frame_id];
    free_list_.erase(free_it);
  } else {
    if (replacer_->UnpinSize() > 0) {
      replacer_->Victim(&cache_page_frame_id);
      cache_page = &pages_[cache_page_frame_id];
      page_table_.erase(cache_page->page_id_);
    } else
      return nullptr;
  }
  page_id_t new_page_id = AllocatePage();
  page_table_.emplace(new_page_id, cache_page_frame_id);
  pages_[cache_page_frame_id].ResetMemory();
  pages_[cache_page_frame_id].page_id_ = new_page_id;
  page_id = new_page_id;
  pages_[cache_page_frame_id].is_dirty_ = false;
  pages_[cache_page_frame_id].pin_count_ = 1;
  replacer_->Pin(cache_page_frame_id);
  return &pages_[cache_page_frame_id];
  // return nullptr;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto it = page_table_.find(page_id);
  if (it == page_table_.end())
    return true;
  else if (pages_[it->second].pin_count_ > 0)
    return false;
  frame_id_t cache_page_frame_id = it->second;
  Page *cache_page = &pages_[cache_page_frame_id];
  if (cache_page->IsDirty()) {
    FlushPage(page_id);
  }
  page_table_.erase(page_id);
  cache_page->page_id_ = INVALID_PAGE_ID;
  cache_page->pin_count_ = 0;
  cache_page->is_dirty_ = false;
  free_list_.push_back(it->second);
  DeallocatePage(page_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t cache_page_frame_id = it->second;
  Page *cache_page = &pages_[cache_page_frame_id];
  if (cache_page->pin_count_ > 0) {
    cache_page->pin_count_--;
    if (is_dirty) {
      cache_page->is_dirty_ = true;
    }
    if (cache_page->pin_count_ == 0) {
      replacer_->Unpin(cache_page_frame_id);
    }
  }
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) disk_manager_->WritePage(page_id, pages_[it->second].data_);
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// check unpin
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}