//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

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
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  //int max_size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1;
  SetMaxSize(max_size);
}

/**
 * Methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Method to find the first index i so that array[i].first >= key
 * NOTE: This method is primarily useful when constructing an index iterator
 *       that begins at a certain key.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize() - 1;
  int mid;
  int compareResult;
  // Binary search
  while (left <= right) {
      mid = left + (right - left) / 2;  //(left + right) / 2;有溢出风险
      compareResult = comparator(array[mid].first, key);
      if (compareResult == 0) {
          return mid;
      } else if (compareResult < 0) {
          left = mid + 1;
      } else {
          right = mid - 1;
      }
  }
  return left;
}

/*
 * Find and return the key stored at "index"
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code (fill in the curly braces with the appropriate key value)
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Find and return the key & value pair stored at "index"
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index];
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
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int index = KeyIndex(key, comparator);
  if (GetSize() > 0 && index < GetSize() && comparator(key, array[index].first) == 0) {
      *value = array[index].second;
      return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  assert(GetSize() < GetMaxSize() + 1);
  int targetIndex = KeyIndex(key, comparator);

  for (int i = GetSize() - 1; i >= targetIndex; i--) {
      array[i + 1].first = array[i].first;
      array[i + 1].second = array[i].second;
  }

  array[targetIndex].first = key;
  array[targetIndex].second = value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * Note: you might find it useful to assume recipient is a new, empty page
 *       and call MoveHalfTo accordingly in b_plus_tree.cpp.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  //assert(recipient != nullptr);
  //assert(GetSize() == GetMaxSize() + 1);

  // 维护next_page_id_
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());

  // 拷贝
  int lastIndex = GetSize() - 1;
  int copyStartIndex = lastIndex / 2 + 1;
  int i = 0;
  int j = copyStartIndex;
  while (j <= lastIndex) {
    recipient->array[i].first = array[j].first;
    recipient->array[i].second = array[j].second;
    i++;
    j++;
  }

  // 重新设置大小
  SetSize(copyStartIndex);
  recipient->SetSize(lastIndex - copyStartIndex + 1);
}

/*
 * Private helper method for MoveHalfTo.
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  assert(false);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: shift entries over to fill in removed slot
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {/*
  int index = KeyIndex(key, comparator);
  if (GetSize() > 0 && index < GetSize() && comparator(key, GetItem(index).first) == 0) {
      memmove(array + index, array + index + 1, static_cast<size_t>(GetSize() - 1 - index) * sizeof(MappingType));
      IncreaseSize(-1);
  }
  return GetSize();*/
  return 1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {/*
  assert(GetSize() + recipient->GetSize() <= GetMaxSize());
  assert(GetParentPageId() == recipient->GetParentPageId());
  assert(recipient->GetNextPageId() == GetPageId());

  recipient->CopyAllFrom(array, GetSize());
  IncreaseSize(-1 * GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(INVALID_PAGE_ID);*/
  assert(false);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  /*memmove(array + GetSize(), items, static_cast<size_t>(sizeof(MappingType) * size));
  IncreaseSize(size);*/
  assert(false);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  /*assert(GetParentPageId() == recipient->GetParentPageId());
  assert(recipient->GetNextPageId() == GetPageId());

  MappingType first = GetItem(0);
  recipient->CopyLastFrom(first);

  memmove(array, array + 1, static_cast<size_t>(GetSize() - 1) * sizeof(MappingType));
  IncreaseSize(-1);

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw BufferPoolManagerException(EXCEPTION_INFO);
  }
  BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());

  parent_page->SetKeyAt(parent_page->ValueIndex(GetPageId()), GetItem(0).first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  buffer_pool_manager->UnpinPage(GetPageId(), true);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);*/
  assert(false);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  /*array[GetSize()] = item;
  IncreaseSize(1);*/
  assert(false);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  /*assert(GetParentPageId() == recipient->GetParentPageId());
  assert(GetNextPageId() == recipient->GetPageId());

  MappingType last = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(last, parentIndex, buffer_pool_manager);

  buffer_pool_manager->UnpinPage(GetPageId(), true);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);*/
  assert(false);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  /*memmove(array + 1, array, static_cast<size_t>(GetSize() * sizeof(MappingType)));
  array[0] = item;
  IncreaseSize(1);

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw BufferPoolManagerException(EXCEPTION_INFO);
  }
  BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());
  parent_page->SetKeyAt(parentIndex, item.first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);*/
  assert(false);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub