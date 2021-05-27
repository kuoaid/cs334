//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>
#include <iostream>
#include <list>
#include <mutex>  // NOLINT

namespace bustub {

LRUReplacer::LRUReplacer() = default;
<<<<<<< HEAD
=======

>>>>>>> 3f4e886bac77b68a823533807f66cca0ed71e3da
LRUReplacer::~LRUReplacer() = default;

std::mutex latch;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guardo(latch);
  if (unpinned.empty()) {
    *frame_id = -1;
    return false;
  }
  *frame_id = *unpinned.begin();
  unpinned.remove(*unpinned.begin());
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guardo(latch);
  bool unpinned_contains_frame_id = std::find(unpinned.begin(), unpinned.end(), frame_id) != unpinned.end();
  if (unpinned_contains_frame_id) {
    unpinned.remove(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guardo(latch);
  bool unpinned_contains_frame_id = std::find(unpinned.begin(), unpinned.end(), frame_id) == unpinned.end();
  if (unpinned_contains_frame_id) {
    unpinned.push_back(frame_id);
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guardo(latch);
  return unpinned.size();
}

}  // namespace bustub
