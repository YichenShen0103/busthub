//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (free_list_.empty()) {
    // No free frames available, try to evict a frame
    if (!replacer_->Evict(&frame_id)) {
      // No evictable frames available
      return nullptr;
    }
    // Evict the frame and reset it
    Page *page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_->Remove(page->GetPageId());
    page->ResetMemory();
  } else {
    // Get a free frame from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // Allocate a new page id
  *page_id = AllocatePage();
  if (*page_id == INVALID_PAGE_ID) {
    return nullptr;  // Allocation failed
  }

  Page *new_page = &pages_[frame_id];
  new_page->ResetMemory();
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;  // Pin the new page
  new_page->is_dirty_ = false;

  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);  // Pin the frame so it won't be evicted

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 1. Search the buffer pool for the page
  if (page_table_->Find(page_id, frame_id)) {
    // Page found, pin it and return
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    return page;
  }

  // 2. Page not found, try to evict a frame
  if (free_list_.empty()) {
    // No free frames available, try to evict a frame
    if (!replacer_->Evict(&frame_id)) {
      // No evictable frames available
      return nullptr;
    }
    // Evict the frame and reset it
    Page *page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_->Remove(page->GetPageId());
    page->ResetMemory();
  } else {
    // Get a free frame from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // 3. Read the page from disk
  Page *new_page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, new_page->GetData());
  new_page->page_id_ = page_id;
  new_page->pin_count_ = 1;  // Pin the new page
  new_page->is_dirty_ = false;
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);  // Pin the frame so it won't be evicted
  return new_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // Check if the page is in the buffer pool
  if (!page_table_->Find(page_id, frame_id)) {
    return false;  // Page not found
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() <= 0) {
    return false;  // Page is already unpinned
  }

  // Decrement pin count and set dirty flag if needed
  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  // If pin count reaches zero, make the frame evictable
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id)) {
    return false;  // Page not found or invalid page ID
  }
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    page_id_t page_id = page->GetPageId();
    FlushPgImp(page_id);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id)) {
    return true;  // Invalid page ID, nothing to delete
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;  // Page is pinned, cannot delete
  }

  replacer_->Remove(frame_id);       // Remove from replacer
  free_list_.push_back(frame_id);    // Add frame back to free list
  page_table_->Remove(page_id);      // Remove from page table
  page->ResetMemory();               // Reset the page memory
  page->page_id_ = INVALID_PAGE_ID;  // Mark the page as invalid
  page->pin_count_ = 0;              // Reset pin count
  page->is_dirty_ = false;           // Reset dirty flag
  DeallocatePage(page_id);           // Deallocate the page from disk
  return true;                       // Successfully deleted the page
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
