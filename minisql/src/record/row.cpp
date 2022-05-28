#include "record/row.h"

/**
 * The null_bitmap is chars,
 * one bit for each field, one char for 8 fields,
 * 1 for null, 0 for not null.
 **/

inline bool IsBitSetOfChars(char *bitmap, uint32_t bit) {
  return (bitmap[bit / 8] & (1 << (bit % 8))) != 0;
}

inline void SetBitOfChars(char *bitmap, uint32_t bit) {
  bitmap[bit / 8] |= (1 << (bit % 8));
}

inline void ClearAllChars(char *bitmap, uint32_t bitmap_len) {
  memset(bitmap, 0, bitmap_len);
}

inline uint32_t SerializeBitmap(char *buf, char *bitmap, uint32_t bitmap_len) {
  for (uint32_t i = 0; i < bitmap_len; i++)
  {
    MACH_WRITE_TO(char, buf + i, bitmap[i]);
  }
  return bitmap_len;
}

inline uint32_t DeserializeBitmap(char *buf, char *bitmap, uint32_t bitmap_len) {
  for (uint32_t i = 0; i < bitmap_len; i++)
  {
    bitmap[i] = *(buf + i);
  }
  return bitmap_len;
}


// todo remove things about IsNull

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t bitmap_len = fields_.size()/8 + 1;
  char* null_bitmap = new char[bitmap_len];
  ClearAllChars(null_bitmap, bitmap_len);
 
  uint32_t ofs = 0;

  // MACH_WRITE_UINT32(buf+ofs, this->rid_.GetPageId() );  // PageId
  // ofs += 4;
  // MACH_WRITE_UINT32(buf+ofs, this->rid_.GetSlotNum() ); // SlotNum
  // ofs += 4;

  MACH_WRITE_UINT32(buf+ofs, this->fields_.size());   // 1 - field_num
  ofs += 4;
  
  for(uint32_t i = 0; i < this->fields_.size(); i++){ // 2 - null_bitmap
    if(fields_[i]==nullptr || fields_[i]->IsNull()){
      SetBitOfChars(null_bitmap, i);
    }
  }
  ofs += SerializeBitmap(buf+ofs, null_bitmap, bitmap_len);

  for (auto &field : fields_){
    if( field!=nullptr ){
      ofs += field->SerializeTo(buf+ofs);             // 3 - fields
    }
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  if(buf==NULL){
    return 0;
  }
  uint32_t ofs = 0;

  // uint32_t temp_PageId = MACH_READ_FROM(uint32_t, buf+ofs);   // PageId
  // ofs += 4;
  // uint32_t temp_SlotNum = MACH_READ_FROM(uint32_t, buf+ofs);  // SlotNum
  // ofs += 4;
  // this->rid_.Set(temp_PageId,temp_SlotNum);

  uint32_t field_num = MACH_READ_FROM(uint32_t, buf+ofs);    // 1 - field_num
  ofs += 4;

  uint32_t bitmap_len = field_num/8 + 1;
  char* null_bitmap = new char[bitmap_len];                  // 2 - null_bitmap
  ofs += DeserializeBitmap(buf+ofs, null_bitmap, bitmap_len);

  for(uint32_t i = 0; i < field_num; i++)
  {
    Field* temp_field = nullptr;                             // 3 - fields
    ofs += Field::DeserializeFrom(buf + ofs, schema->GetColumn(i)->GetType(),
                                  &temp_field, IsBitSetOfChars(null_bitmap, i), heap_);  
    this->fields_.push_back(temp_field);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  if(this->fields_.size()==0){
    return 0;
  }

  uint32_t ofs = 0;
  ofs += 4;                                          // 1 - field_num

  uint32_t bitmap_len = fields_.size()/8 + 1;
  ofs += bitmap_len;                                 // 2 - null_bitmap

  for (auto &field : fields_){
    if( !(field==nullptr) ){
      ofs += field->GetSerializedSize();             // 3 - fields
    }
  }
  
  return ofs;
}
