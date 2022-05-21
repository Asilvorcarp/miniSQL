#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bpm)
  :leaf_(leaf), bpm_(bpm), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
    if (leaf_ != nullptr) {
        bpm_->UnpinPage(leaf_->GetPageId(), false);
        bpm_->UnpinPage(leaf_->GetPageId(), false);
    }
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_++;
  if (index_ >= leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      bpm_->UnpinPage(leaf_->GetPageId(), false);
      leaf_ = nullptr;
    } else {
      // update leaf to next_page
      Page *next_page = bpm_->FetchPage(next_page_id);
      bpm_->UnpinPage(leaf_->GetPageId(), false);
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      index_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if (leaf_ == itr.leaf_ && index_ == itr.index_ && bpm_ == itr.bpm_) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
