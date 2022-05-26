#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
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

page_id_t DiskManager::AllocatePage() {
  //元数据页
  DiskFileMetaPage* meta_page_information = reinterpret_cast<DiskFileMetaPage*>(this->meta_data_); 
  page_id_t result_logic_page = 0;    //返回的page逻辑页号
  uint32_t temp_offest_in_exetent = 0;
  char bit_map[PAGE_SIZE];
  int is_find = 0;
  
  for(uint32_t i = 0;i<meta_page_information->GetExtentNums();i++)
  {
    if(meta_page_information->GetExtentUsedPage(i)<BITMAP_SIZE)
    {
      //算物理页号（位图页）
      uint32_t bit_map_physical_page = i*(this->BITMAP_SIZE + 1) + 1;
      //读到自己定义的字符串里，强制类型转换成bitmap，调用其函数
      this->ReadPhysicalPage(bit_map_physical_page,bit_map);
      BitmapPage<PAGE_SIZE>* Bitmap_Page_information = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bit_map); 
      Bitmap_Page_information->AllocatePage(temp_offest_in_exetent);
      result_logic_page = i*this->BITMAP_SIZE + temp_offest_in_exetent;
      //该分区所用page数++
      meta_page_information->extent_used_page_[i]++;
      //所用的总page数++
      meta_page_information->num_allocated_pages_++;
      is_find = 1;
      this->WritePhysicalPage(bit_map_physical_page,bit_map);
      break;
    }
  }
   //如果前面已用的分区没有空页，开一个新的页。
  if(is_find==0)
  {
    uint32_t bit_map_physical_page = meta_page_information->num_extents_*(this->BITMAP_SIZE + 1) + 1;
    this->ReadPhysicalPage(bit_map_physical_page,bit_map);
    BitmapPage<PAGE_SIZE>* Bitmap_Page_information = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bit_map); 
    Bitmap_Page_information->AllocatePage(temp_offest_in_exetent);
    result_logic_page = meta_page_information->num_extents_*this->BITMAP_SIZE + temp_offest_in_exetent;
    
    //该分区所用page数++
    meta_page_information->extent_used_page_[meta_page_information->num_extents_]++;
    //所用的总page数++
    meta_page_information->num_allocated_pages_++;
    //分区数++
    meta_page_information->num_extents_ ++;
    this->WritePhysicalPage(bit_map_physical_page,bit_map);
  }
  return result_logic_page;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  //元数据页
  DiskFileMetaPage* meta_page_information = reinterpret_cast<DiskFileMetaPage*>(this->meta_data_); 
  uint32_t temp_offest_in_exetent = logical_page_id % BITMAP_SIZE;
  uint32_t temp_exetent = logical_page_id / BITMAP_SIZE;

  char bit_map[PAGE_SIZE];
  //算物理页号（位图页）
  uint32_t bit_map_physical_page = temp_exetent*(this->BITMAP_SIZE + 1) + 1;
  //读到自己定义的字符串里，强制类型转换成bitmap，调用其函数
  this->ReadPhysicalPage(bit_map_physical_page,bit_map);
  BitmapPage<PAGE_SIZE>* Bitmap_Page_information = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bit_map); 
  Bitmap_Page_information->DeAllocatePage(temp_offest_in_exetent);
  //所用的总page数--
  meta_page_information->num_allocated_pages_--;
  //该分区所用page数--
  meta_page_information->extent_used_page_[temp_exetent]--;
  this->WritePhysicalPage(bit_map_physical_page,bit_map);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  //元数据页
  // DiskFileMetaPage* meta_page_information = reinterpret_cast<DiskFileMetaPage*>(this->meta_data_); 
  uint32_t temp_offest_in_exetent = logical_page_id % BITMAP_SIZE;
  uint32_t temp_exetent = logical_page_id / BITMAP_SIZE;

  char bit_map[PAGE_SIZE];
  //算物理页号（位图页）
  uint32_t bit_map_physical_page = temp_exetent*(this->BITMAP_SIZE + 1) + 1;
  //读到自己定义的字符串里，强制类型转换成bitmap，调用其函数
  this->ReadPhysicalPage(bit_map_physical_page,bit_map);
  BitmapPage<PAGE_SIZE>* Bitmap_Page_information = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bit_map);
  return Bitmap_Page_information->IsPageFree(temp_offest_in_exetent);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  uint32_t fixed_page = logical_page_id/BITMAP_SIZE + 1; //已用位图页数量
  page_id_t result = fixed_page + logical_page_id + 1;//位图页 + 数据页 + 元数据页
  return result;
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