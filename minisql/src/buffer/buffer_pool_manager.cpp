#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // 1.     Search the page table for the requested page (P).
  try
  { // P exist
    frame_id_t P_frame_id = page_table_.at(page_id); // may raise err out_of_range
    replacer_->Pin(P_frame_id);
    return &pages_[P_frame_id];
  }
  catch(const std::out_of_range& e)
  { // P not exist

    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    frame_id_t frame_id; // the Page R's index in the BufferPool's pages_ array
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Victim(&frame_id)) {
        // LOG(ERROR) << "No free page available"; // for debug
        return nullptr;
      }
    }

    // 2.     If R is dirty, write it back to the disk.
    page_id_t R_page_id = page_table_[frame_id];
    if (pages_[frame_id].IsDirty()) 
      FlushPage(R_page_id);

    // 3.     Delete R from the page table and insert P.
    page_table_.erase(frame_id);
    page_table_[frame_id] = page_id;

    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    Page *P = &pages_[frame_id];
    // update metadata
    P->page_id_ = page_id;
    P->pin_count_ = 0;
    P->is_dirty_ = false;
    // read in page content
    disk_manager_->ReadPage(page_id, P->GetData());
    return P;
  }
}

// return nullptr if failed
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // 0.   Make sure you call AllocatePage!
  page_id_t P_page_id = AllocatePage();

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool isAllPinned = true;
  for (uint32_t i = 0; i < pool_size_; i++)
  {
    if (pages_[i].GetPinCount() == 0)
    {
      isAllPinned = false;
      break;
    }
  }
  if (isAllPinned == true)
    return nullptr;

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id; // the Page's index in the BufferPool's pages_ array
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      // LOG(ERROR) << "No free page available"; // for debug
      return nullptr;
    }
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *P = &pages_[frame_id];
  // Update P's metadata
  P->page_id_ = P_page_id;
  P->pin_count_ = 0;
  P->is_dirty_ = false;
  // Zero out memory
  P->ResetMemory();
  // Add P to the page table
  page_table_[P_page_id] = frame_id;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = P_page_id;
  return P;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);

  // 1.   Search the page table for the requested page (P).
  frame_id_t P_frame_id;
  try
  { 
    P_frame_id = page_table_.at(page_id); // may raise err out_of_range
  }
  catch(const std::out_of_range& e)
  { // P not exist, return true.
    return true;
  }
  
  // P exist 
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pages_[P_frame_id].GetPinCount()) // non-zero
    return false;

  // 3.   Otherwise, P can be deleted. Remove P from the page table, 
  //      reset its metadata and return it to the free list.
  // Remove P from the page table
  page_table_.erase(page_id);
  // Reset P's metadata
  Page *P = &pages_[P_frame_id];
  P->page_id_ = page_id;
  // P->pin_count_ = 0; // already zero
  P->is_dirty_ = false;
  // Add P to the free list
  free_list_.push_back(P_frame_id);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = page_table_[page_id];
  pages_[frame_id].pin_count_--;
  replacer_->Unpin(frame_id);
  if (is_dirty)
    pages_[frame_id].is_dirty_ = true;
  return true;

  // return false; // impossible for now
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false; // reset dirty bit
  return true;

  // return false; // impossible for now
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
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