#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  //lru_list.resize(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list.empty()){
    return false;
  }
  *frame_id=lru_list.back();
  hash_map.erase(*frame_id);
  lru_list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(hash_map.count(frame_id)){
    lru_list.erase(hash_map[frame_id]);
    hash_map.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(hash_map.count(frame_id)){
    return;
  }
  lru_list.push_front(frame_id);
  hash_map[frame_id]=lru_list.begin();
}

size_t LRUReplacer::Size() {
  return lru_list.size();
}