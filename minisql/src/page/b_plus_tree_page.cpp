#include "page/b_plus_tree_page.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const {
  if(page_type_ == IndexPageType::LEAF_PAGE){
    return true;
  }
  else{
    return false;
  }
}

bool BPlusTreePage::IsRootPage() const {
  if(parent_page_id_ == INVALID_PAGE_ID){
    return true;
  }
  else{
    return false;
  }
}

void BPlusTreePage::SetPageType(IndexPageType page_type) {
  this->page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
  return this->size_;
}

void BPlusTreePage::SetSize(int size) {
  this->size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount) {
  this->size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const {
  return this->max_size_;
}

void BPlusTreePage::SetMaxSize(int size) {
  this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const {
  if(IsRootPage()){
    if(IsLeafPage()){
      return 1;   //|INVALID|p|
    }
    else{
      return 2;     // at least two sub tree |INVALID|p|k|p|
    }
  }
  return (this->max_size_ +1 ) /2;  // ceil 
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const {
  return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
  this->parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {
  return this->page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) {
  this->page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) {
  lsn_ = lsn;
}