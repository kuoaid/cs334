/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bpm):leaf_(leaf), index_(index), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // if (leaf_ != nullptr) {
  //   Page *page = bmp_->FetchPage(leaf_->GetPageId());
  //   page->RUnlatch();
  //   bmp_->UnpinPage(leaf_->GetPageId(), false);
  //   bmp_->UnpinPage(leaf_->GetPageId(), false);
  // }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return itr.index_ == index_ && itr.leaf_ == leaf_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return itr.index_ != index_ || itr.leaf_ != leaf_;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
int INDEXITERATOR_TYPE::getIndex() {
  return index_;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (++index_ >= leaf_->GetSize()) {

    page_id_t next_page_id = leaf_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      Page *page = bpm_->FetchPage(leaf_->GetPageId());
      page->RUnlatch();
      bpm_->UnpinPage(leaf_->GetPageId(), false);

      bpm_->UnpinPage(leaf_->GetPageId(), false);
      leaf_ = nullptr;
    } else {
      //更新leaf指向next_page_id对应的叶子节点
      Page *next_page = bpm_->FetchPage(next_page_id);
      next_page->RLatch();

      Page *page = bpm_->FetchPage(leaf_->GetPageId());
      page->RUnlatch();
      bpm_->UnpinPage(leaf_->GetPageId(), false);
      bpm_->UnpinPage(leaf_->GetPageId(), false);

      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      index_ = 0;
    }
  }
  // if (isEnd()) {
  //   return *this;
  // }
  // index_++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
