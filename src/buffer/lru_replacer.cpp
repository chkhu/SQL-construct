#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_pages = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_unpin_list.empty()) {
    return false;
  }
  *frame_id = lru_unpin_list.back();
  lru_unpin_set.erase(*frame_id);
  lru_unpin_list.pop_back();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if (lru_unpin_set.find(frame_id) != lru_unpin_set.end()) {
    lru_unpin_set.erase(frame_id);
    lru_unpin_list.remove(frame_id);
  } else if (Size() == max_pages)
    return;
  lru_pin_set.insert(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_pin_set.find(frame_id) != lru_pin_set.end()) {
    lru_pin_set.erase(frame_id);
  } else if (Size() == max_pages)
    return;
  if (lru_unpin_set.find(frame_id) == lru_unpin_set.end()) {
    lru_unpin_set.insert(frame_id);
    lru_unpin_list.push_front(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() { return lru_pin_set.size() + lru_unpin_set.size(); }
size_t LRUReplacer::PinSize() { return lru_pin_set.size(); }
size_t LRUReplacer::UnpinSize() { return lru_unpin_set.size(); }
