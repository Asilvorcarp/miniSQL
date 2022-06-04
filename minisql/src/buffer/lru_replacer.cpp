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

// ClockReplacer::ClockReplacer(size_t num_pages){
//   //initialize
// }

// ClockReplacer::~ClockReplacer()=default;

// bool ClockReplacer::Victim(frame_id_t *frame_id){
//   if(this->clock_list.empty()){
//     return false;
//   }
//   while(this->use_bit.at(*iter)){
//     this->use_bit[*iter]=false;
//     iter++;
//     if(iter==this->clock_list.end()){
//       iter=this->clock_list.begin();
//     }
//   }
//   *frame_id=*iter;
//   this->Pin(*frame_id);
//   return true;
// }

// void ClockReplacer::Pin(frame_id_t frame_id){
//   if(this->clock_map.count(frame_id)){
//     if(*iter==frame_id){
//       iter++;
//       if(iter==this->clock_list.end()){
//         iter=this->clock_list.begin();
//       }
//     }
//     this->clock_list.erase(this->clock_map.at(frame_id));
//     this->clock_map.erase(frame_id);
//     this->use_bit.erase(frame_id);
//   }
// }

// void ClockReplacer::Unpin(frame_id_t frame_id){
//   if(this->clock_map.count(frame_id)){
//     this->use_bit[frame_id]=true;
//   }else{
//     if(this->clock_list.empty()){
//       this->clock_list.push_back(frame_id);
//       this->iter=this->clock_list.begin();
//       this->use_bit[frame_id]=true;
//       this->clock_map[frame_id]=iter;
//     }else{
//       this->clock_list.push_back(frame_id);
//       this->iter++;
//       this->use_bit[frame_id]=true;
//       this->clock_map[frame_id]=iter;
//     }
//   }
// }

// size_t ClockReplacer::Size(){
//   return this->clock_list.size();
// }

ClockReplacer::ClockReplacer(size_t num_pages){
  //initialize
  clock_hand = clock_list.end();
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id){
  if(clock_list.empty()){
    return false;
  }
  if (clock_hand == clock_list.end()) {
    clock_hand = clock_list.begin();
  }
  while (clock_hand->second) {
    clock_hand->second = false;
    clock_hand++;
    if (clock_hand == clock_list.end()) {
      clock_hand = clock_list.begin();
    }
  }
  *frame_id=clock_hand->first;
  Pin(*frame_id);
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id){
  auto iter = clock_map.find(frame_id);
  if(iter != clock_map.end()){
    if (clock_hand == iter->second) {
      clock_hand++;
      if (clock_hand == clock_list.end())
        clock_hand = clock_list.begin();
    }
    clock_list.erase(iter->second);
    clock_map.erase(iter);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id){
  auto iter = clock_map.find(frame_id);
  if(iter != clock_map.end()){
    iter->second->second = true;
  }else{
    clock_list.emplace_back(frame_id, true);
    clock_map[frame_id] = --clock_list.end();
  }
}

size_t ClockReplacer::Size(){
  return clock_map.size();
}