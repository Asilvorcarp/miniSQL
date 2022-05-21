#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          root_page_id_(INVALID_PAGE_ID),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  // destroy by removing all keys //todo: not sure
  while (!IsEmpty())
  {
    LeafPage *leftmost_leaf = FindLeafPage(KeyType(), true);
    Remove(leftmost_leaf->KeyAt(0)); // maybe replace with Begin() later
  }

  // or:
  // // destroy by traversing the tree
  // page_id_t node_pid = root_page_id_;
  // Page *node_page = buffer_pool_manager_->FetchPage(node_pid);
  // BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(node_page);
  // // end traversing when the node is a leaf
  // if (node->IsLeafPage()) {
  //   buffer_pool_manager_->UnpinPage(node_pid, true);
  //   buffer_pool_manager_->DeletePage(node_pid);
  //   return;
  // }
  // // DestroyChilds()
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  LeafPage *leaf = FindLeafPage(key);
  if (leaf == nullptr) 
  { // not found
    return false;
  }
  ValueType value;
  bool ifNoError = leaf->Lookup(key, value, comparator_);
  // append to result vector
  if (ifNoError)
    result.push_back(value);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return ifNoError;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // ask for new page from buffer pool manager
  page_id_t new_root_pid;
  Page *new_root_page = buffer_pool_manager_->NewPage(new_root_pid);
  if (new_root_page == nullptr) {
    throw std::bad_alloc();
  }
  // init root as leaf
  LeafPage *root_as_leaf = \
      reinterpret_cast<LeafPage *>(new_root_page->GetData());
  root_as_leaf->Init(new_root_pid, INVALID_PAGE_ID, leaf_max_size_);
  // update root page id
  root_page_id_ = new_root_pid;
  UpdateRootPageId(true);
  // insert entry into leaf
  InsertIntoLeaf(key, value);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LeafPage *leaf_page = FindLeafPage(key);
  assert(leaf_page != nullptr); // for debug
  ValueType value_discard;
  bool ifExist = leaf_page->Lookup(key, value_discard, comparator_);
  if (ifExist) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  if (leaf_page->GetSize() < leaf_page->GetMaxSize())
  { // dont need to split
    leaf_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true); // dirty now
    return true;
  }

  assert(leaf_page->GetSize() == leaf_page->GetMaxSize());
  // split leaf page
  leaf_page->Insert(key, value, comparator_);
  LeafPage *new_leaf = Split(leaf_page);
  InsertIntoParent(leaf_page, new_leaf->KeyAt(0), new_leaf);

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // ask for new page from buffer pool manager
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
      throw std::bad_alloc();
  }

  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  // different max size for internal and leaf (internal_max_size_)
  int max_size;
  if (node->IsLeafPage()) {
    max_size = leaf_max_size_;
  } else {
    max_size = internal_max_size_;
  }
  new_node->Init(new_page_id, node->GetParentPageId(), max_size);
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  
  return new_node;
} 
/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // todo: check
  if (old_node->IsRootPage()) {
    // old_node is root
    page_id_t new_root_pid;
    Page *new_root_page = buffer_pool_manager_->NewPage(new_root_pid);
    if (new_root_page == nullptr) {
        throw std::bad_alloc();
    }
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(new_root_pid, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // maintain parents
    old_node->SetParentPageId(new_root_pid);
    new_node->SetParentPageId(new_root_pid);

    // update root page id
    // LOG(INFO) << "new root pid: " << new_root_pid;
    root_page_id_ = new_root_pid;
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(new_root_pid, true);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  }
  // not root:
  page_id_t parent_pid = old_node->GetParentPageId();
  InternalPage *parent = \
    reinterpret_cast<InternalPage *>(GetPageWithPid(parent_pid)->GetData());

  // maintain parents
  new_node->SetParentPageId(parent_pid);

  if (parent->GetSize() < parent->GetMaxSize()) 
  { // parent not full
    int sizeNow = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    assert(sizeNow <= parent->GetMaxSize());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  } else {
    // parent split
    assert(parent->GetSize() == parent->GetMaxSize());

    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    InternalPage *parent_new_sibling = Split(parent);
    assert(parent->GetSize() < parent->GetMaxSize());
    // recursively
    InsertIntoParent(parent, parent_new_sibling->KeyAt(0), parent_new_sibling, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_pid, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // todo: may need to modify
  // assert(transaction != nullptr);
  LeafPage *target_leaf = FindLeafPage(key);
  if (target_leaf == nullptr) {
    return;
  }
  page_id_t target_pid = target_leaf->GetPageId();
  int size_after_delete = target_leaf->RemoveAndDeleteRecord(key, comparator_);
  if (size_after_delete < target_leaf->GetMinSize()) {
    CoalesceOrRedistribute(target_leaf, transaction);
  }
  buffer_pool_manager_->UnpinPage(target_pid, true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // todo: doing
  assert(node->GetSize() < node->GetMinSize());

  if (node->IsRootPage()) {
    // LOG(INFO) << "adjust root page " << node->GetPageId(); // for debug
    return AdjustRoot(node);
  }
  Page *node_page = GetPageWithPid(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(node_page->GetData());
  int nodeIndexInParent = parent->ValueIndex(node->GetPageId());

  // get sibling
  int siblingIndex;
  // bool isLeftSibling; //todo remove
  if (nodeIndexInParent == 0) {
    siblingIndex = nodeIndexInParent + 1;
    // isLeftSibling = false;
  } else {
    siblingIndex = nodeIndexInParent - 1;
    // isLeftSibling = true;
  }
  Page *sibling_page = GetPageWithPid(parent->ValueAt(siblingIndex));
  decltype(node) sibling = reinterpret_cast<decltype(node)>(sibling_page->GetData());

  if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) 
  { // merge
    Coalesce(&sibling, &node, &parent, nodeIndexInParent);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return true;
  } else 
  { // redistribute
    Redistribute(sibling, node, nodeIndexInParent);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  assert((*node)->GetSize() + (*neighbor_node)->GetSize() <= (*node)->GetMaxSize());
  page_id_t nodeId = (*node)->GetPageId();
  page_id_t neighborId = (*neighbor_node)->GetPageId();

  if (index != 0) { // left sibling
    (*node)->MoveAllTo((*neighbor_node), index, buffer_pool_manager_);

    // remove node from parent
    buffer_pool_manager_->UnpinPage(nodeId, true);
    if (!buffer_pool_manager_->DeletePage(nodeId)) {
      LOG(ERROR) << "buffer_pool_manager_ delete failed, pin_count != 0";
    }
    buffer_pool_manager_->UnpinPage(neighborId, true);
    (*parent)->Remove(index);
  } else {
    (*neighbor_node)->MoveAllTo((*node), index, buffer_pool_manager_);

    // remove neighbor from parent
    buffer_pool_manager_->UnpinPage(nodeId, true);
    buffer_pool_manager_->UnpinPage(neighborId, true);
    if (!buffer_pool_manager_->DeletePage(neighborId)) {
      LOG(ERROR) << "buffer_pool_manager_ delete failed, pin_count != 0";
    }
    (*parent)->Remove(index + 1);
  }

  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    // recursively if not enough size for parent
    return CoalesceOrRedistribute((*parent), transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    KeyType placeholder(0);
    neighbor_node->MoveFirstToEndOf(node, placeholder, buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    assert(old_root_node->GetSize() == 0);
    root_page_id_ = INVALID_PAGE_ID;

    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    if (!buffer_pool_manager_->DeletePage(old_root_node->GetPageId())) {
      LOG(ERROR) << "buffer_pool_manager_ delete failed, pin_count != 0";
    }
    return true;
  }

  assert(old_root_node->GetSize() == 1);
  InternalPage *old_root = static_cast<InternalPage *>(old_root_node);
  root_page_id_ = old_root->ValueAt(0);
  UpdateRootPageId();
  Page *new_root_page = GetPageWithPid(root_page_id_);
  BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
  new_root->SetParentPageId(INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

  if (!buffer_pool_manager_->DeletePage(old_root_node->GetPageId())) {
    LOG(ERROR) << "buffer_pool_manager_ delete failed, pin_count != 0";
  }
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType placeholder = KeyType();
  auto left_most_leaf = FindLeafPage(placeholder, true);
  return INDEXITERATOR_TYPE(left_most_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // todo: test
  LeafPage *start_leaf = FindLeafPage(key);
  int start_index = 0;
  if (start_leaf != nullptr) {
    // KeyIndex() is the index which key inserts
    int index = start_leaf->KeyIndex(key, comparator_);
    if (start_leaf->GetSize() > 0 && index < start_leaf->GetSize() &&
        comparator_(key, start_leaf->GetItem(index).first) == 0) {
      // key exists in leaf page
      start_index = index;
    } else {
      // not exists
      start_index = start_leaf->GetSize();
    }
  }
  return INDEXITERATOR_TYPE(start_leaf, start_index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  // todo: test
  INDEXITERATOR_TYPE iter = Begin();
  INDEXITERATOR_TYPE lastIter = iter; // last valid one
  while (iter.isValid())
  {
    lastIter = iter;
    ++iter;
  }
  return lastIter;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // todo: test
  if (IsEmpty()) {
    return nullptr;
  }
  // state init: currently on root
  page_id_t node_pid = root_page_id_;
  auto node_page = GetPageWithPid(node_pid);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(node_page->GetData());

  while (!node->IsLeafPage()) {
    // get next
    InternalPage *internalPage = static_cast<InternalPage *>(node);
    page_id_t next_pid;
    if (leftMost) {
      next_pid = internalPage->ValueAt(0);
    } else {
      next_pid = internalPage->Lookup(key, comparator_);
    }
    // temp store
    Page *last_page = node_page;
    // state transfer
    node_pid = next_pid;
    node_page = GetPageWithPid(next_pid);
    node = reinterpret_cast<BPlusTreePage *>(node_page->GetData());
    // unpin last page
    buffer_pool_manager_->UnpinPage(last_page->GetPageId(), false);
  }
  // node pinned and returned
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  /**
   * todo: where is the GOD DAMN header_page?
   * maybe this method is not needed.
   **/
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

/**
 * My definition.
 * Remember to UNPIN after using this method!
 **/
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::GetPageWithPid(page_id_t page_id) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    LOG(ERROR) << "Get page with pid failed" << endl;
    throw exception();
  }
  return page;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
