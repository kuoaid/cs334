//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * @return true if there is nothing stored in the b+ tree, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Add the value that is associated with parameter key to the vector result
 * if key exists.
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key, false, 1, transaction);
  BPlusTreePage *bppage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  LeafPage *leaf = reinterpret_cast<LeafPage *>(bppage);
  ValueType *container = new ValueType();
  bool res = leaf->Lookup(key, container, comparator_);
  if (res) {
    result->push_back(*container);
  }
  delete container;
  if (transaction != nullptr) {
    UnLatchPageSet(transaction, false);
  } else {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user tries to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    // if it's empty
    StartNewTree(key, value);
    root_id_mutex_.unlock();
    // Print(buffer_pool_manager_);
    return InsertIntoLeaf(key, value, transaction);
  }

  root_id_mutex_.unlock();
  // not empty, insert into leaf.
  // Print(buffer_pool_manager_);
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * You should first ask for new page from buffer pool manager (NOTICE: throw
 * an std::bad_alloc exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t newId;
  // LOG_INFO("getting to newroot");
  Page *newRoot = buffer_pool_manager_->NewPage(&newId);

  if (newRoot == nullptr) {
    printf("StartNewTree\n");
    throw std::bad_alloc();
  }
  // accessing the root
  LeafPage *root = reinterpret_cast<LeafPage *>(newRoot->GetData());

  // init
  root->Init(newId, INVALID_PAGE_ID, leaf_max_size_);
  buffer_pool_manager_->UnpinPage(newId, false);
  // update tree info
  root_page_id_ = newId;
  UpdateRootPageId(true);

  // finish accessing the root
}

/*
 * Insert constant key & value pair into leaf page
 * You should first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exists or not. If it exists, return
 * immdiately, otherwise insert entry. Remember to deal with a split if necessary.
 * @return: since we only support unique keys, if user tries to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // getting the leaf page.
  Page *page = FindLeafPage(key, false, 0, transaction);
  BPlusTreePage *bppage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  LeafPage *leaf = reinterpret_cast<LeafPage *>(bppage);
  ValueType v = value;
  // check if value exists
  if (leaf->Lookup(key, &v, comparator_)) {  // value exists?
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    UnLatchPageSet(transaction, 0);
    return false;
  }

  // Now value DNE. Insert.
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  } else {
    leaf->Insert(key, value, comparator_);
    LeafPage *splitted = reinterpret_cast<LeafPage *>(Split(leaf));
    splitted->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(splitted->GetPageId());
    splitted->SetParentPageId(leaf->GetParentPageId());

    InsertIntoParent(leaf, splitted->KeyAt(0), splitted, transaction);
  }
  UnLatchPageSet(transaction, 0);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager (NOTICE: throw
 * an std::bad_alloc exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::Split(BPlusTreePage *node) {
  // allocate a new page
  page_id_t newId;
  Page *newPage = buffer_pool_manager_->NewPage(&newId);

  if (newPage == nullptr) {
    printf("Split\n");
    throw std::bad_alloc();
  }

  if (node->IsLeafPage()) {
    // treat as leaf page
    // initialize its metadata
    LeafPage *newLeaf = reinterpret_cast<LeafPage *>(newPage->GetData());
    newLeaf->Init(newId, node->GetParentPageId(), leaf_max_size_);

    // move half of the entries in node to the new node.
    LeafPage *nodeAsLeaf = reinterpret_cast<LeafPage *>(node);
    nodeAsLeaf->MoveHalfTo(newLeaf);

    // Return the new node.
    return newLeaf;
  }
  // Now it's an internal page, initialize its metadata
  InternalPage *newInternal = reinterpret_cast<InternalPage *>(newPage->GetData());
  newInternal->Init(newId, node->GetParentPageId(), internal_max_size_);

  // move half of the entries in node to the new node.
  InternalPage *nodeAsInternal = reinterpret_cast<InternalPage *>(node);
  nodeAsInternal->MoveHalfTo(newInternal, buffer_pool_manager_);

  // Return the new node.
  return newInternal;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * You first needs to find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to recursively
 * insert in parent if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    // make a new root.
    page_id_t newRootId;
    Page *newRootPage = buffer_pool_manager_->NewPage(&newRootId);
    InternalPage *newRootNode = reinterpret_cast<InternalPage *>(newRootPage->GetData());

    newRootNode->Init(newRootId, INVALID_PAGE_ID, internal_max_size_);
    newRootNode->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = newRootId;

    old_node->SetParentPageId(newRootId);  // there's a new root in town.
    new_node->SetParentPageId(newRootId);

    UpdateRootPageId(false);

    root_id_mutex_.unlock();

    buffer_pool_manager_->UnpinPage(newRootId, true);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  } else {  // begin insertion
    page_id_t parentId = old_node->GetParentPageId();
    InternalPage *parentNode =
        reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(parentId))->GetData());  // make a copy
    new_node->SetParentPageId(parentId);
    // test if parent node has at least 1 spot left.
    if (parentNode->GetSize() < parentNode->GetMaxSize()) {  // does not exceed
      printf("old_node->GetPageId: %i\n", old_node->GetPageId());
      parentNode->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // insert into copy
      buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    } else {
      // parent node has less than 1 spot left after insertion
      InternalPage *splittedParent = reinterpret_cast<InternalPage *>(Split(parentNode));
      splittedParent->SetParentPageId(parentNode->GetParentPageId());
      if (comparator_(key, splittedParent->KeyAt(0)) < 0) {
        printf("old_node->GetPageId: %i\n", old_node->GetPageId());
        parentNode->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parentNode->GetPageId());
      } else {
        printf("old_node->GetPageId: %i\n", old_node->GetPageId());
        splittedParent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(splittedParent->GetPageId());
      }
      InsertIntoParent(parentNode, splittedParent->KeyAt(0), splittedParent, transaction);

      buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parentId, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, you first need to find the right leaf page as deletion target, then
 * delete entry from leaf page.
 * Remember to call CoalesceOrRedistribute if necessary.
 * @param key                  the key to remove
 * @param transaction          the current Transaction object, use to record pages
 *                             you latch, and pages that should be deleted.
 *                             This method should unlatch and delete before returning.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto *deletingPage = FindLeafPage(key, false, 1);
  BPlusTreePage *deletingBPTPage = reinterpret_cast<BPlusTreePage *>(deletingPage->GetData());
  LeafPage *deletingLeafPage = reinterpret_cast<LeafPage *>(deletingBPTPage);

  deletingLeafPage->Remove(key, comparator_);
  buffer_pool_manager_->UnpinPage(deletingPage->GetPageId(), false);
}

/*
 * You first need to find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, coalesce.
 * Using template N to represent either internal page or leaf page.
 * @param node                 the node that had a key removed
 * @param transaction          the current Transaction object, use to record pages for deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(BPlusTreePage *node, Transaction *transaction) {}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method CoalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              index of pointer to "node" within "parent"
 * @param   transaction        the current Transaction object, use to record pages for deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(BPlusTreePage *sibling, BPlusTreePage *node, InternalPage *parent, int index,
                              Transaction *transaction) {}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair to the front of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   sibling            sibling page of input "node"
 * @param   node               input from method CoalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              index of pointer to "node" within "parent"
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *sibling, BPlusTreePage *node, InternalPage *parent, int index) {}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  // KeyType key;
  // auto start_leaf = FindLeafPage(key, true);
  // BPlusTreePage *start_leaf_bp = reinterpret_cast<BPlusTreePage *>(start_leaf->GetData());
  // LeafPage *start_leaf_lf = reinterpret_cast<LeafPage *>(start_leaf_bp);
  // return INDEXITERATOR_TYPE(start_leaf_lf, 0, buffer_pool_manager_);
  KeyType key{};
  auto *start_leaf = FindLeafPage(key, false, 1);
  LeafPage *start_leaf_lf;
  if (start_leaf != nullptr) {
    BPlusTreePage *start_leaf_bp = reinterpret_cast<BPlusTreePage *>(start_leaf->GetData());
    start_leaf_lf = reinterpret_cast<LeafPage *>(start_leaf_bp);
  } else {
    start_leaf_lf = nullptr;
  }
  return INDEXITERATOR_TYPE(start_leaf_lf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto start_leaf = FindLeafPage(key, false, 1);
  BPlusTreePage *start_leaf_bp = reinterpret_cast<BPlusTreePage *>(start_leaf->GetData());
  LeafPage *start_leaf_lf = reinterpret_cast<LeafPage *>(start_leaf_bp);
  int start_index = 0;
  if (start_leaf_lf != nullptr) {
    //
    int index = start_leaf_lf->KeyIndex(key, comparator_);
    if (start_leaf_lf->GetSize() > 0 && index < start_leaf_lf->GetSize() &&
        comparator_(key, start_leaf_lf->GetItem(index).first) == 0) {
      //
      start_index = index;
    } else {
      start_index = start_leaf_lf->GetSize();
    }
  }
  return INDEXITERATOR_TYPE(start_leaf_lf, start_index, buffer_pool_manager_);
  // auto leaf_page = FindLeafPage(key, false);
  // auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  // auto leaf_lf = reinterpret_cast<LeafPage *>(leaf);
  // int index = leaf_lf->KeyIndex(key, comparator_);
  // //auto page_id = leaf->GetPageId();
  // //UnlockPage(leaf_page, nullptr, Operation::SEARCH);
  // return INDEXITERATOR_TYPE(leaf_lf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  page_id_t newId;
  Page *page = buffer_pool_manager_->NewPage(&newId);

  if (page == nullptr) {
    throw std::bad_alloc();
  }
  // accessing the root
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(INVALID_PAGE_ID, INVALID_PAGE_ID, leaf_max_size_);
  buffer_pool_manager_->UnpinPage(newId, false);
  return INDEXITERATOR_TYPE(leaf, 0, buffer_pool_manager_);
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, int indicator, Transaction *transaction) {
  root_id_mutex_.lock();
  if (IsEmpty()) {
    root_id_mutex_.unlock();
    return nullptr;
  }
  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreePage *bppage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (indicator == 1) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }

  page_id_t nextDest = 0;
  while (!bppage->IsLeafPage()) {
    InternalPage *internal = static_cast<InternalPage *>(bppage);
    if (leftMost) {
      nextDest = internal->ValueAt(0);
    } else {
      nextDest = internal->Lookup(key, comparator_);
    }
    Page *lastPage = page;
    BPlusTreePage *lastBp = bppage;
    page = buffer_pool_manager_->FetchPage(nextDest);
    bppage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (!root_id_mutex_.try_lock()) {
    } else {
      root_id_mutex_.unlock();
    }
    if (indicator == 1) {
      page->RLatch();
    } else {
      page->WLatch();
    }
    if (transaction != nullptr) {
      if (indicator == 1) {
        UnLatchPageSet(transaction, indicator);
      } else {
        bool isSafe;
        if (indicator == 1) {
          isSafe = true;
        } else if (indicator == 0) {
          isSafe = bppage->GetSize() < bppage->GetMaxSize();
        } else if (indicator == -1) {
          isSafe = bppage->GetSize() > bppage->GetMinSize();
        } else {
          isSafe = false;
        }
        if (isSafe) {
          UnLatchPageSet(transaction, indicator);
        }
      }
    } else {
      lastPage->RUnlatch();
      if (lastBp->IsRootPage()) {
        root_id_mutex_.unlock();
      }
      buffer_pool_manager_->UnpinPage(lastPage->GetPageId(), false);
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(page);
    }
    page_id = nextDest;
  }
  root_id_mutex_.unlock();
  buffer_pool_manager_->UnpinPage(nextDest, true);
  buffer_pool_manager_->UnpinPage(page_id, true);
  return page;
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
void BPLUSTREE_TYPE::UpdateRootPageId(bool insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
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
 * Tests depend on this function, DO NOT MODIFY.
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  std::stringstream result;
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    result << "Leaf Page: " << leaf->GetPageId() << " size: " << leaf->GetSize()
           << " parent: " << leaf->GetParentPageId() << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      result << leaf->KeyAt(i) << ",";
    }
    result << std::endl;
    result << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    result << "Internal Page: " << internal->GetPageId() << " size: " << internal->GetSize()
           << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      result << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    result << std::endl;
    result << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      result << ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
  return result.str();
}

// indicator: -1: delete, 0: insert, 1: search
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLatchPageSet(Transaction *transaction, int indicator) {
  while (!transaction->GetPageSet()->empty()) {
    Page *front = transaction->GetPageSet()->front();
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(front->GetData());
    transaction->GetPageSet()->pop_front();
    if (indicator == 1) {
      front->RUnlatch();
    } else {
      front->WUnlatch();
    }
    if (bp->IsRootPage()) {
      root_id_mutex_.unlock();
    }
    buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
