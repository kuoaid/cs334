//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page.
 * Including set page type, set current size, set page id, set parent id and set
 * max page size.
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
 * Get the key stored at index.
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code (fill in the curly braces with the appropriate key value)
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Set the key stored at index.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index > 0 && index < GetMaxSize() + 1);
  array[index].first = key;
}

/*
 * Find and return array index so that its value equals to input "value".
 * Return -1 if the value is not found.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (value == ValueAt(i)) {
      return i;
    }
  }
  return -1;
}

/*
 * Get the value stored at index.
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that would contain input "key".
 * Start the search from the second key (the first key should always be invalid).
 * Remember that page_id at index i refers to a subtree in which all keys K satisfy:
 * K(i) < K <= K(i+1).
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() >= 2);
  int left = 1;
  int right = GetSize() - 1;
  int mid;
  int compareResult;
  int targetIndex;
  while (left <= right) {
    mid = left + (right - left) / 2;
    compareResult = comparator(array[mid].first, key);
    if (compareResult == 0) {
      left = mid;
      break;
    }
    if (compareResult < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  targetIndex = left;
  if (targetIndex >= GetSize()) {
    return array[GetSize() - 1].second;
  }

  if (comparator(array[targetIndex].first, key) == 0) {
    return array[targetIndex].second;
  }
  return array[targetIndex - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way up to the root
 * page, you will create a new root page and call this method to populate its elements.
 * NOTE: This method is only called on a new root node from InsertIntoParent()(b_plus_tree.cpp)
 * where old_value is page_id of old root, new_value is page_id of new split
 * page (successor sibling to old root) and new_key is the middle key.
 * NOTE: Make sure to update the size of this node.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value == old_value
 * You can assume old_value will be present in this node.
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int index = ValueIndex(old_value);
  assert(index != -1);

  int i;
  for (i = GetSize() - 1; i > index; i--) {
    array[i + 1].first = array[i].first;
    array[i + 1].second = array[i].second;
  }
  array[index + 1].first = new_key;
  array[index + 1].second = new_value;

  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page.
 * Note: you might find it useful to assume recipient is a new, empty page
 *       and call MoveHalfTo accordingly in b_plus_tree.cpp.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // assert(recipient != nullptr);
  // assert(GetSize() == GetMaxSize() + 1);
  int lastIndex = GetSize() - 1;
  int start = lastIndex / 2 + 1;
  int i = 0;
  int j = start;
  while (j <= lastIndex) {
    recipient->array[i].first = array[j].first;
    recipient->array[i].second = array[j].second;
    i++;
    j++;
  }

  // 维护size
  SetSize(start);
  recipient->SetSize(lastIndex - start + 1);

  // 维护孩子节点的parent_page_id
  for (int i = 0; i < recipient->GetSize(); i++) {
    auto page_id = recipient->ValueAt(i);
    auto page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(page->GetData());
    bp->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

/*
 * Private helper method for MoveHalfTo.
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 * (i.e., fetch each child page, update the parent page id, and unpin as dirty).
 *
 * To call this from MoveHalfTo, use array + offset to get a pointer to where you want to
 * start copying (the items parameter). offset would be the middle index.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(false);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page stored at index
 * NOTE: shift entries over to fill in removed slot
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(0 <= index && index < GetSize());
  for (int i = index; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
/*
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    IncreaseSize(-1);
    assert(GetSize() == 1);
    return ValueAt(0);
}
*/
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient. Remember to adjust node sizes.
 * Note: you might find it helpful to assume recipient is a predecessor of this
 *       node (i.e, insert middle key at the end of it, followed by everything from this node)
 *       and then call MoveAllTo accordingly in b_plus_tree.cpp.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  /*assert(GetSize() + recipient->GetSize() <= GetMaxSize());
  assert(GetParentPageId() == recipient->GetParentPageId());

  // 总是key大page的移动到key小的page
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw BufferPoolManagerException(EXCEPTION_INFO);
  }
  BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());

  assert(parent_page->ValueIndex(GetPageId()) > parent_page->ValueIndex(recipient->GetPageId()));
  array[0].first = parent_page->KeyAt(index_in_parent);
  buffer_pool_manager->UnpinPage(GetParentPageId(), false);

  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);

  // 调整子节点的父节点指针
  for (int i = 0; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
    page = buffer_pool_manager->FetchPage(child_page_id);
    if (page == nullptr) {
      throw BufferPoolManagerException(EXCEPTION_INFO);
    }
    BPInternalPage *child_page = reinterpret_cast<BPInternalPage *>(page->GetData());

    child_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }

  buffer_pool_manager->UnpinPage(GetPageId(), true);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);*/
  assert(false);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  /*
assert(GetSize() + size <= GetMaxSize());
int start = GetSize();
for (int i = 0; i < size; ++i) {
  array[start + i] = *items++;
}
IncreaseSize(size);
*/
  assert(false);
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
 * pages that are moved to the recipient. Remember to adjust node sizes.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  /*
assert(GetParentPageId() == recipient->GetParentPageId());

MappingType pair{KeyAt(1), ValueAt(0)};
page_id_t child_page_id = ValueAt(0);
array[0].second = ValueAt(1);
Remove(1);

recipient->CopyLastFrom(pair, buffer_pool_manager);

auto *page = buffer_pool_manager->FetchPage(child_page_id);
if (page == nullptr) {
throw BufferPoolManagerException(EXCEPTION_INFO);
}
auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
child->SetParentPageId(recipient->GetPageId());

buffer_pool_manager->UnpinPage(child->GetPageId(), true);
buffer_pool_manager->UnpinPage(GetPageId(), true);
buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
*/
  assert(false);
}

/*
 * Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  /*auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw BufferPoolManagerException(EXCEPTION_INFO);
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  auto index = parent->ValueIndex(GetPageId());
  auto key = parent->KeyAt(index + 1);

  array[GetSize()] = {key, pair.second};
  IncreaseSize(1);
  parent->SetKeyAt(index + 1, pair.first);

  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);*/
  assert(false);
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
  /*assert(GetParentPageId() == recipient->GetParentPageId());

  MappingType last = array[GetSize() - 1];
  IncreaseSize(-1);
  page_id_t child_id = last.second;

  recipient->CopyFirstFrom(last, parent_index, buffer_pool_manager);

  Page *page = buffer_pool_manager->FetchPage(child_id);
  BPInternalPage *child_page = reinterpret_cast<BPInternalPage *>(page->GetData());
  child_page->SetParentPageId(recipient->GetPageId());

  buffer_pool_manager->UnpinPage(child_id, true);
  buffer_pool_manager->UnpinPage(GetPageId(), true);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);*/
  assert(false);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  /*Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw BufferPoolManagerException(EXCEPTION_INFO);
  }
  BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());

  auto tmp = parent_page->KeyAt(parent_index);
  parent_page->SetKeyAt(parent_index, pair.first);

  InsertNodeAfter(array[0].second, tmp, array[0].second);
  array[0].second = pair.second;

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);*/
  assert(false);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
