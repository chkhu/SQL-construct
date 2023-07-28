#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    // directory or file does not exist
    if (!db_io_.is_open()) {
        db_io_.clear();
        // create a new file
        std::filesystem::path p = db_file;
        if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
        db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
        db_io_.close();
        // reopen with original mode
        db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
        if (!db_io_.is_open()) {
            throw std::exception();
        }
    }
    ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    WritePhysicalPage(META_PAGE_ID, meta_data_);
    if (!closed) {
        db_io_.close();
        closed = true;
    }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
    ASSERT(logical_page_id >= 0, "Invalid page id.");
    ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
    ASSERT(logical_page_id >= 0, "Invalid page id.");
    WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 * 0. Read meta page
 * 1. Find a free page from bitmap pages
 * 2. Update bitmap page
 * 3. Write bitmap page back to disk
 * 4. Return page id
 * @return
 */
page_id_t DiskManager::AllocatePage() {
    DiskFileMetaPage *meta_data_ = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
    uint32_t num_allocated_pages_ = meta_data_->num_allocated_pages_;
    uint32_t num_extents_ = meta_data_->num_extents_;
    uint32_t allocated_extents_id = 0;
    for (allocated_extents_id = 0; allocated_extents_id < num_extents_; allocated_extents_id++) {
        if (meta_data_->extent_used_page_[allocated_extents_id] < BitmapPage<4096>::GetMaxSupportedSize()) {
            break;
        }
    }
    uint32_t allocated_extents_frame_id = 1 + allocated_extents_id * (BitmapPage<4096>::GetMaxSupportedSize() + 1);
    if (allocated_extents_id == num_extents_) {
        // no free page
        auto new_extent = new BitmapPage<4096>();
        WritePhysicalPage(allocated_extents_frame_id, reinterpret_cast<char *>(new_extent));
        meta_data_->extent_used_page_[num_extents_] = 0;
        meta_data_->num_extents_++;
        num_extents_++;
    }
    auto allocated_extent = new BitmapPage<4096>();
    ReadPhysicalPage(allocated_extents_frame_id, reinterpret_cast<char *>(allocated_extent));
    uint32_t allocated_page_extent_offset = -1;
    allocated_extent->AllocatePage(allocated_page_extent_offset);
    meta_data_->num_allocated_pages_++;
    meta_data_->extent_used_page_[allocated_extents_id]++;
    WritePhysicalPage(allocated_extents_frame_id, reinterpret_cast<char *>(allocated_extent));
    return allocated_extents_id * BitmapPage<4096>::GetMaxSupportedSize() + allocated_page_extent_offset;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    // ASSERT(false, "Not implemented yet.");
    DiskFileMetaPage *meta_data_ = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
    uint32_t num_allocated_pages_ = meta_data_->num_allocated_pages_;
    uint32_t num_extents_ = meta_data_->num_extents_;
    uint32_t deallocated_extents_id = logical_page_id / BitmapPage<4096>::GetMaxSupportedSize();
    //free a page not allocated
    if (deallocated_extents_id >= num_extents_) {
        ASSERT(false, "try free in invalid extent");
    }
    uint32_t deallocated_extents_frame_id = 1 + deallocated_extents_id * (BitmapPage<4096>::GetMaxSupportedSize() + 1);
    auto deallocated_extent = new BitmapPage<4096>();
    ReadPhysicalPage(deallocated_extents_frame_id, reinterpret_cast<char *>(deallocated_extent)); //read from disk
    ASSERT(deallocated_extent != nullptr, "bitmap_page is nullptr");
    deallocated_extent->DeAllocatePage(logical_page_id % BitmapPage<4096>::GetMaxSupportedSize());
//    if (meta_data_->num_allocated_pages_ - 1 == logical_page_id)
    meta_data_->num_allocated_pages_--;
    meta_data_->extent_used_page_[deallocated_extents_id]--;
    WritePhysicalPage(deallocated_extents_frame_id, reinterpret_cast<char *>(deallocated_extent));  //write back
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
    DiskFileMetaPage *meta_data_ = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
    uint32_t num_allocated_pages_ = meta_data_->num_allocated_pages_;  //not used
    uint32_t num_extents_ = meta_data_->num_extents_;
    uint32_t checked_extents_id = logical_page_id / BitmapPage<4096>::GetMaxSupportedSize();
    uint32_t checked_extents_frame_id = 1 + checked_extents_id * (BitmapPage<4096>::GetMaxSupportedSize() + 1);
    if (checked_extents_id >= num_extents_) {
        return true;
    }
    auto checked_extent = new BitmapPage<4096>();
    ReadPhysicalPage(checked_extents_frame_id, reinterpret_cast<char *>(checked_extent));
    ASSERT(checked_extent != nullptr, "bitmap_page is nullptr");
    return checked_extent->IsPageFree(logical_page_id % BitmapPage<4096>::GetMaxSupportedSize());
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
    uint32_t extent_meta_page_num = (logical_page_id / BitmapPage<4096>::GetMaxSupportedSize() + 1);
    uint32_t total_extra_meta_page_num = extent_meta_page_num + 1;
    return total_extra_meta_page_num + logical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
    int offset = physical_page_id * PAGE_SIZE;
    // check if read beyond file length
    if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "Read less than a page" << std::endl;
#endif
        memset(page_data, 0, PAGE_SIZE);
    } else {
        // set read cursor to offset
        db_io_.seekp(offset);
        db_io_.read(page_data, PAGE_SIZE);
        // if file ends before reading PAGE_SIZE
        int read_count = db_io_.gcount();
        if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
            LOG(INFO) << "Read less than a page" << std::endl;
#endif
            memset(page_data + read_count, 0, PAGE_SIZE - read_count);
        }
    }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
    size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
    // set write cursor to offset
    db_io_.seekp(offset);
    db_io_.write(page_data, PAGE_SIZE);
    // check for I/O error
    if (db_io_.bad()) {
        LOG(ERROR) << "I/O error while writing";
        return;
    }
    // needs to flush to keep disk file in sync
    db_io_.flush();
}