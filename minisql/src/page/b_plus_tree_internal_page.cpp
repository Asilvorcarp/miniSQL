#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  //lack range judge
  return this->array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  //lack range judge
  this->array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i = 0; i< GetSize();i++){
    if(value == ValueAt(i)){
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  //lack range judge
  return this->array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int start = 1;
  int end = GetSize() - 1;
  while(start<=end){
    int middle = (end + start) / 2;
    if(comparator(array_[middle].first,key)<=0){
      start = middle + 1;
    }
    else{
      end = middle - 1 ;
    }
  }
  return array_[start-1].second;  
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int temp_index = ValueIndex(old_value) + 1;   //find the old index' right space
  for(int i = GetSize();i>temp_index;i--){    // make the right space empty, laters move to right one space
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[temp_index].first = new_key;   //insert 
  array_[temp_index].second = new_value;
  IncreaseSize(1);    //renew the size
  return GetSize();   //return new size
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int max = GetMaxSize();
  int half_id = max / 2 + 1;
  page_id_t recipient_id = recipient->GetPageId();
  for(int i = half_id;i<=max;i++){    //the recipient->array_[0].first should be invalid, but it is not important, we can store |k|p|k|p|,but don't use k(0)
    recipient->array_[i-half_id].first = array_[i].first;
    recipient->array_[i-half_id].second = array_[i].second;
    auto temp_page = buffer_pool_manager->FetchPage(array_[i].second);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(temp_page->GetData());
    childTreePage->SetParentPageId(recipient_id);
    buffer_pool_manager->UnpinPage(array_[i].second,true);
  }
  SetSize(half_id);
  recipient->SetSize(max - half_id + 1);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int now_size = GetSize();
  for(int i = 0;i<size;i++){
    this->array_[now_size+i] = items[i];
    page_id_t childPageId = items[i].second;
    Page *page = buffer_pool_manager->FetchPage(childPageId);
    assert (page != nullptr);
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(GetPageId());
    assert(child->GetParentPageId() == GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType result = ValueAt(0);
  IncreaseSize(-1);
  // assert(GetSize() == 0);
  return result;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int temp_recipient_size = recipient->GetSize();   //record old size
  page_id_t recipient_id = recipient->GetPageId();
  // first find parent page
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId()); //fatch page
  assert (page != nullptr);
  BPlusTreeInternalPage *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  // the separation key from parent
  SetKeyAt(0, middle_key);

  buffer_pool_manager->UnpinPage(node->GetPageId(), false);
  
  for(int i = 0;i<GetSize();i++){
    recipient->array_[temp_recipient_size + i].first = array_[i].first;
    recipient->array_[temp_recipient_size + i].second = array_[i].second;
    //update children's parent page
    auto temp_page = buffer_pool_manager->FetchPage(array_[i].second);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(temp_page->GetData());
    childTreePage->SetParentPageId(recipient_id);
    buffer_pool_manager->UnpinPage(array_[i].second,true);
  }
  recipient->SetSize(temp_recipient_size + GetSize());
  assert(recipient->GetSize() <= GetMaxSize());
  SetSize(0); 
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType temp_pair{KeyAt(0), ValueAt(0)};
  for(int i = 0;i<this->GetSize()-1;i++){
    array_[i].first = array_[i+1].first;
    array_[i].second = array_[i+1].second;
  }
  IncreaseSize(-1);
  recipient->CopyLastFrom(temp_pair, buffer_pool_manager);
  // update child parent page id
  page_id_t childPageId = temp_pair.second;
  Page *page = buffer_pool_manager->FetchPage(childPageId);
  assert (page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());
  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  //update relavent key & value pair in its parent page.
  page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  //! not sure ! depend on what is middle_key
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), array_[0].first);
  //middle_key = array_[0].first;

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType pair {KeyAt(GetSize() - 1),ValueAt(GetSize() - 1)};
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, buffer_pool_manager);

  // update child's parent page id
  page_id_t childPageId = pair.second;
  Page *page = buffer_pool_manager->FetchPage(childPageId);   
  assert (page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());    //get the childPage
  child->SetParentPageId(recipient->GetPageId());
  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  //update relavent key & value pair in recipient's parent page.
  page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());    //the recipient
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());   //get the recipient's parentpage
  //! not sure ! depend on what is middle_key
  parent->SetKeyAt(parent->ValueIndex(recipient->GetPageId()), array_[0].first);
  //middle_key = array_[0].first;
  buffer_pool_manager->UnpinPage(recipient->GetParentPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  // memmove(array + 1, array, GetSize()*sizeof(MappingType));
  for(int i = 0;i<GetSize();i++){
    array_[i+1].first = array_[i].first;
    array_[i+1].second = array_[i].second;
  }
  IncreaseSize(1);
  array_[0] = pair;
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;