#include "record/schema.h"
#include <iostream>
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t ofs = 0;
  std::vector<Column *> columns_ = this->GetColumns();
  MACH_WRITE_UINT32(buf+ofs, SCHEMA_MAGIC_NUM); //1-Write the MagicNum
  ofs += 4;
  MACH_WRITE_UINT32(buf+ofs, columns_.size());  //2-Write the number of columns
  ofs += 4;
  for(uint32_t i = 0;i<columns_.size();i++)
  {
    // MACH_WRITE_UINT32(buf+ofs,columns_[i]->GetSerializedSize()); //3-Write the size of one columns  //no need
    // ofs += 4;
    columns_[i]->SerializeTo(buf+ofs);  //4-Write each column one by one
    ofs += columns_[i]->GetSerializedSize();
  }
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  std::vector<Column *> columns_ = this->GetColumns();
  // if(this->columns_.size()==0){
  //   return 0;
  // }
  // else{
    uint32_t number = columns_.size();
    uint32_t result = 0;
    result += sizeof(uint32_t) * 2;
    for(uint32_t i = 0;i<number;i++){
      // result += sizeof(uint32_t);        //no need
      result += columns_[i]->GetSerializedSize();
    }
    return result;
  // }
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  if(buf==NULL){
    return 0;
  }
  uint32_t ofs = 0;
  std::vector<Column *> columns_;
  uint32_t temp_Magic_Number = MACH_READ_FROM(uint32_t, buf+ofs);  //1-Read the MagicNum
  ofs += 4;
  if(temp_Magic_Number!=SCHEMA_MAGIC_NUM)
  {
    // std::cerr<<"COLUMN_MAGIC_NUM does not match"<<std::endl;
    // todo temporarily
    return 0;
  }
  
  uint32_t column_number = MACH_READ_FROM(uint32_t, buf+ofs);   //2-Read the number of columns
  ofs += 4;
  
  for(uint32_t i = 0;i<column_number;i++){
    // temp_column_size = MACH_READ_FROM(uint32_t, buf+ofs);  //3-Read the size of one columns  //no need
    // ofs += 4;
    Column* temp_column = nullptr;
    ofs += Column::DeserializeFrom(buf+ofs,temp_column,heap);
    columns_.push_back(temp_column);
  }
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new (mem)Schema(columns_);
  return ofs;
}