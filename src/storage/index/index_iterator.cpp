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
    ;
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  if (leaf_ == nullptr) {
    printf("leaf == nullptr\n");
    return true;
  }
  printf("leaf != nullptr\n");
  printf("index_: %i\n", index_);
  printf("leaf_->GetSize(): %i\n", lear_->GetSize());
  printf("leaf_->GetNextPageId() == INVALID_PAGE_ID: %i\n", leaf_->GetNextPageId() == INVALID_PAGE_ID);
  return (index_ >= leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return (leaf_ == nullptr && itr.index_ == 0 && itr.leaf_->GetPageId() == INVALID_PAGE_ID) ||
         (itr.index_ == index_ && itr.leaf_->GetPageId() == leaf_->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !operator==(itr); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    printf("here;\n");
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
      bpm_->UnpinPage(next_page_id, false);
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
