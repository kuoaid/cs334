/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include <cstdio>

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bpm)
    : leaf_(leaf), index_(index), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    // Page *page = bpm_->FetchPage(leaf_->GetPageId());
    // page->RUnlatch();
    // bpm_->UnpinPage(leaf_->GetPageId(), false);
    // bpm_->UnpinPage(leaf_->GetPageId(), false);
    // delete leaf_;
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  // auto next_id = leaf_->GetNextPageId();
  // printf("index_: %i \n", index_);
  // printf("leaf_->GetSize(): %i \n", leaf_->GetSize());
  if (leaf_ == nullptr) {
    return true;
  }
  return (index_ >= leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  // if (leaf_ == nullptr && itr.index_ == 0 && itr.leaf_->GetPageId() == INVALID_PAGE_ID) {
  //   return true;
  // }
  return (leaf_ == nullptr && itr.index_ == 0 && itr.leaf_->GetPageId() == INVALID_PAGE_ID) ||
         (itr.index_ == index_ && itr.leaf_->GetPageId() == leaf_->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  // printf("itr.index_: %i\n", itr.index_);
  // printf("index_: %i\n", index_);
  // printf("!operator==(itr): %d\n", !operator==(itr));
  // printf("itr.index_: %d\n", itr.index_);
  // printf("index_: %i\n", index_);
  // printf("itr.leaf_->GetPageId(): %d\n", itr.leaf_->GetPageId());
  // printf("leaf_->GetPageId(): %d\n", leaf_->GetPageId());
  // printf("\n");
  return !operator==(itr);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    throw std::out_of_range("IndexIterator: out of range");
  }
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
int INDEXITERATOR_TYPE::getIndex() { return index_; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (++index_ >= leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      leaf_ = nullptr;
    } else {
      Page *next_page = bpm_->FetchPage(next_page_id);

      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
