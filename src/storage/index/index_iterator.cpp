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
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() {

// }

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() {

// }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (++index_ >= leaf_->GetSize()) {

    page_id_t next_page_id = leaf_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      Page *page = bmp_->FetchPage(leaf_->GetPageId());
      page->RUnlatch();
      bmp_->UnpinPage(leaf_->GetPageId(), false);

      bmp_->UnpinPage(leaf_->GetPageId(), false);
      leaf_ = nullptr;
    } else {
      //更新leaf指向next_page_id对应的叶子节点
      Page *next_page = bmp_->FetchPage(next_page_id);
      next_page->RLatch();

      Page *page = bmp_->FetchPage(leaf_->GetPageId());
      page->RUnlatch();
      bmp_->UnpinPage(leaf_->GetPageId(), false);
      bmp_->UnpinPage(leaf_->GetPageId(), false);

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
