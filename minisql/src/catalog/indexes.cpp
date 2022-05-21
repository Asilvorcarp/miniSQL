#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf+ofs,INDEX_METADATA_MAGIC_NUM);
  ofs+=4;
  MACH_WRITE_INT32(buf+ofs,index_id_);
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs,index_name_.length());
  ofs+=4;
  MACH_WRITE_STRING(buf+ofs,index_name_);
  ofs+=index_name_.length();
  MACH_WRITE_UINT32(buf+ofs,table_id_);
  ofs+=4;
  MACH_WRITE_INT32(buf+ofs,key_map_.size());
  ofs+=4;
  for(uint32_t i=0;i<key_map_.size();i++){
    MACH_WRITE_UINT32(buf+ofs,key_map_[i]);
    ofs+=4;
  }
  return ofs;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)*(5+key_map_.size())+index_name_.size();
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t ofs=0;
  if(index_meta!=nullptr){
    std::cerr << "Pointer to IndexMetadata is not null in IndexMetadata deserialize."<< std::endl;
  }
  if(buf==NULL){
    return 0;
  }
  uint32_t magicNum=MACH_READ_FROM(uint32_t,buf+ofs);
  if(magicNum!=344528){
    std::cerr<<"INDEX_METADATA_MAGIC_NUM does not match"<<std::endl;
    return 0;
  }
  ofs+=4;
  index_id_t indexID=MACH_READ_FROM(index_id_t,buf+ofs);
  ofs+=4;
  uint32_t nameLength=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=4;
  std::string indexName;
  for(uint32_t i=0;i<nameLength;i++){
    indexName.push_back(buf[i+ofs]);
  }
  ofs+=nameLength;
  table_id_t tableID=MACH_READ_FROM(table_id_t,buf+ofs);
  ofs+=4;
  uint32_t keyMapSize=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=4;
  std::vector<uint32_t> keyMap;
  for(uint32_t i=0;i<keyMapSize;i++){
    uint32_t tmp=MACH_READ_FROM(uint32_t,buf+ofs);
    ofs+=4;
    keyMap.push_back(tmp);
  }
  ALLOC_P(heap,IndexMetadata)(indexID,indexName,tableID,keyMap);
  index_meta=new IndexMetadata(indexID,indexName,tableID,keyMap);
  return ofs;
}