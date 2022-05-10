#include "record/column.h"
#include <iostream>

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf+ofs, COLUMN_MAGIC_NUM); //1-Write the MagicNum
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs, this->name_.length()); //2-Write the length for the Name(string)
  ofs+=4;
  MACH_WRITE_STRING(buf+ofs, this->name_);  //3-Write the name(string type) to the buf
  ofs+=this->name_.length(); 
  MACH_WRITE_TO(TypeId, (buf+ofs), (this->type_));  //4-Write the type_(enum type) to the buf
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs, this->len_); //5-Write the len_ to the buf
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs, this->table_ind_); //6-Write the table_ind_
  ofs+=4;
  //can only deal with (u)int,float,char
  MACH_WRITE_UINT32(buf+ofs, this->nullable_); //7-Write the nullable to the buf
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs, this->unique_); //8-Write the unique_ to the buf
  ofs+=4;
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  // if(this->name_.size()==0){
  //   return 0;
  // }
  // else{
    return sizeof(uint32_t) *7 + name_.size();
  // }
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  uint32_t ofs=0;
  if (column != nullptr) {
    std::cerr << "Pointer to column is not null in column deserialize."<< std::endl;
  }
  if(buf==NULL) return 0;
  /* deserialize field from buf */

  //Read From the buff
  uint32_t Magic_Number=MACH_READ_FROM(uint32_t, buf+ofs);//1-Read the Magic_Number
  if(Magic_Number!=210928)
  {
    std::cerr<<"COLUMN_MAGIC_NUM does not match"<<std::endl;
    return 0;
  }
  ofs+=4;
  uint32_t length=MACH_READ_FROM(uint32_t, buf+ofs);  //2-Read the length of the name_
  ofs+=4;
  std::string  column_name;
  for(uint32_t i=0;i<length;i++)    //3-Read the Name from the buf
  {
    column_name.push_back(buf[i+ofs]);
  }
  ofs+=length;
  TypeId type=MACH_READ_FROM(TypeId, buf+ofs);  //4-Read the type
  ofs+=4;
  uint32_t len_=MACH_READ_FROM(uint32_t,buf+ofs); //5-Read the len_
  ofs+=4;
  uint32_t col_ind=MACH_READ_FROM(uint32_t,buf+ofs);  //6-Read the col_ind
  ofs+=4;
  bool nullable=MACH_READ_FROM(uint32_t,buf+ofs); //7-Read the nullable
  ofs+=4;
  bool unique=MACH_READ_FROM(uint32_t,buf+ofs); //8-Read the unique
  ofs+=4;
  // can be replaced by: 
  //ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  
  void *mem = heap->Allocate(sizeof(Column));
  //type is the int or float
  if (type == kTypeInt || type == kTypeFloat) {
    column = new (mem) Column(column_name, type, col_ind, nullable, unique);
  } else if (type == kTypeChar) {
    column =new(mem)Column(column_name, type, len_, col_ind, nullable, unique);
  }
  return ofs;
}
