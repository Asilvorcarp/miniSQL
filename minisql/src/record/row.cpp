#include "record/row.h"
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  std::vector<bool> null_bitmap;
  
  uint32_t ofs = 0;
  ofs += schema->SerializeTo(buf);//0-schema

  MACH_WRITE_UINT32(buf+ofs, this->rid_.GetPageId() );  //0.5-Write the row PageId
  ofs += 4;
  MACH_WRITE_UINT32(buf+ofs, this->rid_.GetSlotNum() ); //0.5-Write the row GetSlotNum
  ofs += 4;

  MACH_WRITE_UINT32(buf+ofs, this->fields_.size() );   //1-Write the Write the Field Nums
  ofs += 4;

  for(uint32_t i = 0;i<this->fields_.size();i++){   //2-Write the null_bitmap
    bool temp_bool = false;
    if(this->fields_[i]==nullptr){
      temp_bool = true;
    }
    else{
      temp_bool =this->fields_[i]->IsNull();
    }
    null_bitmap.push_back(temp_bool);
    MACH_WRITE_TO(bool,buf+ofs, null_bitmap[i]);
    ofs += sizeof(bool);
  }

  for(uint32_t i = 0;i<fields_.size();i++)  
  {
    if(null_bitmap[i]==false){
      MACH_WRITE_TO(TypeId,buf+ofs, this->fields_[i]->get_type_id()); //3-Write the field_type_id
      ofs += sizeof(TypeId);

      MACH_WRITE_TO(bool,buf+ofs, this->fields_[i]->IsNull());  //4-Write the field_is_null
      ofs += sizeof(bool);

      ofs += this->fields_[i]->SerializeTo(buf+ofs);    //5-Write the field
    }
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  if(buf==NULL){
    return 0;
  }
  uint32_t ofs = 0;
  ofs += schema->DeserializeFrom(buf,schema,heap_);//0-schema
  uint32_t temp_PageId = MACH_READ_FROM(uint32_t, buf+ofs);  //0.5-Read row PageId
  ofs += 4;
  uint32_t temp_SlotNum = MACH_READ_FROM(uint32_t, buf+ofs);  //0.5-Read SlotNum
  ofs += 4;
  this->rid_.Set(temp_PageId,temp_SlotNum);
  uint32_t temp_size = MACH_READ_FROM(uint32_t, buf+ofs);   //1-Read the size
  ofs += 4;

  std::vector<bool> null_bitmap;
  for(uint32_t i = 0;i<temp_size;i++){   //2-Read the null_bitmap
    bool temp_bool = MACH_READ_FROM(bool,buf+ofs);
    ofs += sizeof(bool);
    null_bitmap.push_back(temp_bool);
  }

  for(uint32_t i = 0;i<temp_size;i++)
  {
    if(null_bitmap[i]==false){
      TypeId temp_type_id = MACH_READ_FROM(TypeId, buf+ofs);    //3-Read the field_type_id
      ofs += sizeof(TypeId);

      bool temp_is_null = MACH_READ_FROM(bool, buf+ofs);   //4-Read the field_is_null
      ofs += sizeof(bool);
      Field* temp_field = nullptr;

      //inline static uint32_t DeserializeFrom(char *buf, const TypeId type_id, Field **field, bool is_null, MemHeap *heap)
      ofs += Field::DeserializeFrom(buf + ofs,temp_type_id,&temp_field,temp_is_null,heap_);   //5-Read the field
      this->fields_.push_back(temp_field);
    }
    else{
      Field* temp_field = nullptr;
      this->fields_.push_back(temp_field);
    }
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  if(this->fields_.size()==0){
    return 0;
  }
  uint32_t ofs = 0;
  ofs += schema->GetSerializedSize(); //0-schema

  ofs += 4 * 3; //1-Field Nums +row PageId +SlotNum

  std::vector<bool> null_bitmap;
  for(uint32_t i = 0;i<this->fields_.size();i++){   //2- the null_bitmap
    bool temp_bool = false;
    if(this->fields_[i]==nullptr){
      temp_bool = true;
    }
    else{
      temp_bool =this->fields_[i]->IsNull();
    }
    null_bitmap.push_back(temp_bool);
    ofs += sizeof(bool);
  }

  for(uint32_t i = 0;i<this->fields_.size();i++){
    if(null_bitmap[i]==false){    //not null
      ofs += sizeof(TypeId);   //3-field_type_id
      ofs += sizeof(bool);    //4 the field_is_null
      ofs += this->fields_[i]->GetSerializedSize(); //5-field
    }
  }
  return ofs;
}
