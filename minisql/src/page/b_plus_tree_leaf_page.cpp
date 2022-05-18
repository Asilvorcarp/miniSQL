#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  //assert(sizeof(BPlusTreeLeafPage) == 28);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size); //minus 1 for insert first then split

}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return this->next_page_id_;   //ok
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this->next_page_id_ = next_page_id;   //ok
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() >= 0);
  int start = 0, end = GetSize() - 1;
  //find the last key in array <= input
  while (start <= end) { 
    int mid = (end + start) / 2;
    if (comparator(this->array_[mid].first,key) >= 0) {
      end = mid - 1;
    }
    else start = mid + 1;
  }
  return end + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  return this->array_[index].first;   //ok
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  return this->array_[index];       //ok
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int temp = KeyIndex(key,comparator); // the first index temp so that array_[temp].first >= key
  assert(temp >= 0);
  IncreaseSize(1);
  //move one space back from temp to end
  for (int i = GetSize()-1; i > temp; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[temp].first = key;
  array_[temp].second = value;
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, 
                            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  // the unused parameter is for "node->MoveHalfTo(new_node, buffer_pool_manager_);" in b_plus_tree.cpp
  // use when depart one leaf page to two ,so the recipient should be empty
  assert(recipient != nullptr);
  int max = GetMaxSize();
  int half_id = (max + 1)/2;    // ceil
  // move 
  for(int i = half_id;i<max;i++){    
    recipient->array_[i-half_id].first = array_[i].first;
    recipient->array_[i-half_id].second = array_[i].second;
  }
  // insert into the list
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  // renew the size
  this->SetSize(half_id);
  recipient->SetSize(max - half_id + 1);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int now_temp = this->GetSize();
  for(int i = 0;i<size;i++){
    array_[i+now_temp].first = items[i].first;
    array_[i+now_temp].second = items[i].second;
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  int get = KeyIndex(key,comparator);
  if (get < GetSize() && comparator(array_[get].first, key) == 0) {
    value = array_[get].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int get = KeyIndex(key,comparator);
  if (get >= GetSize() || comparator(array_[get].first, key) != 0) {
    return this->GetSize();
  }
  for(int i = get;i<GetSize()-1;i++){
    array_[i] = array_[i+1];
  }
  IncreaseSize(-1);
  //if delete the first one, renew the parent ,but there is no buffer manager
  // 
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  //use when merge, so recipient should not be empty, suppose the later merge to former
  assert(recipient != nullptr);

  int recipient_start = recipient->GetSize();
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[recipient_start + i].first = array_[i].first;
    recipient->array_[recipient_start + i].second = array_[i].second;
  }
  recipient->SetNextPageId(GetNextPageId());  //update the next_page id
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  MappingType pair = GetItem(0);    // the first item
  for(int i = 0;i<GetSize()-1;i++){
    array_[i].first = array_[i+1].first;    //move the later item
    array_[i].second = array_[i+1].second;
  }
  IncreaseSize(-1);
  recipient->CopyLastFrom(pair);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int temp_size = this->GetSize();
  array_[temp_size] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int temp_end_index = GetSize()-1;
  MappingType pair = GetItem(temp_end_index);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  IncreaseSize(1);
  for(int i = GetSize()-1;i>0;i--){   //move one space 
    array_[i] = array_[i-1];
  }
  array_[0] = item;
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;