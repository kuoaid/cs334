//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer();

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guardo(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  if (!free_list_.empty() || replacer_->Size() != 0) {
    frame_id_t frame_id = FindFrameId();
    page_table_[page_id] = frame_id;
    InitNewPage(frame_id, page_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    return &pages_[frame_id];
  }

  return nullptr;

  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  //        If no page can be replaced, return nullptr
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guardo(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  if (page->pin_count_ <= 0) {
    return false;
  }
  if (page->pin_count_ > 0) {
    page->pin_count_--;
  }
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  page->is_dirty_ = page->is_dirty_ || is_dirty;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guardo(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPage(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guardo(latch_);
  auto new_page_id = disk_manager_->AllocatePage();
  if (free_list_.empty() && replacer_->Size() == 0) {
    printf("All pinned\n");
    return nullptr;
  }
  frame_id_t frame_id = FindFrameId();
  page_table_[new_page_id] = frame_id;
  InitNewPage(frame_id, new_page_id);
  pages_[frame_id].ResetMemory();

  *page_id = new_page_id;
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guardo(latch_);
  disk_manager_->DeallocatePage(page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  if (page->pin_count_ > 0) {
    return false;
  }
  replacer_->Unpin(frame_id);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // std::lock_guard<std::mutex> guardo(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPage(pages_[i].page_id_);
  }
}

}  // namespace bustub
