#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf+ofs,TABLE_METADATA_MAGIC_NUM);
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs,table_id_);
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs,table_name_.length());
  ofs+=4;
  MACH_WRITE_STRING(buf+ofs,table_name_);
  ofs+=table_name_.length();
  MACH_WRITE_INT32(buf+ofs,root_page_id_);
  ofs+=4;
  ofs+=schema_->SerializeTo(buf+ofs);
  return ofs;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)*4+table_name_.size()+schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t ofs=0;
  if(table_meta!=nullptr){
    std::cerr << "Pointer to TableMetadata is not null in TableMetadata deserialize."<< std::endl;
  }
  if(buf==NULL){
    return 0;
  }
  uint32_t magicNum=MACH_READ_FROM(uint32_t,buf+ofs);
  if(magicNum!=344528){
    std::cerr<<"TABLE_METADATA_MAGIC_NUM does not match"<<std::endl;
    return 0;
  }
  ofs+=4;
  table_id_t tableID=MACH_READ_FROM(table_id_t,buf+ofs);
  ofs+=4;
  uint32_t length=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=4;
  std::string tableName;
  for(uint32_t i=0;i<length;i++){
    tableName.push_back(buf[i+ofs]);
  }
  ofs+=length;
  page_id_t rootPageID=MACH_READ_FROM(page_id_t,buf+ofs);
  ofs+=4;
  Schema *shc=nullptr;
  ofs+=shc->DeserializeFrom(buf+ofs,shc,heap);
  //void *mem=heap->Allocate(sizeof(TableMetadata));
  // ALLOC_P(heap,TableMetadata)(tableID,tableName,rootPageID,shc);
  // table_meta=new TableMetadata(tableID,tableName,rootPageID,shc);
  void *mem=heap->Allocate(sizeof(TableMetadata));
  table_meta=new(mem)TableMetadata(tableID,tableName,rootPageID,shc);
  return ofs;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
