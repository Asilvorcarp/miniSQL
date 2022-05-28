#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
                           this->index_id_=index_id;
                           this->index_name_=index_name;
                           this->table_id_=table_id;
                           this->key_map_=key_map;
                         }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    // Step2: mapping index key to key schema
    // Step3: call CreateIndex to create the index
    this->meta_data_=meta_data;
    //this->table_info_=TableInfo::Create(this->heap_);
    this->table_info_=table_info;
    this->key_schema_=Schema::ShallowCopySchema(this->table_info_->GetSchema(),this->meta_data_->GetKeyMapping(),this->heap_);
    //key_schema_=Schema::ShallowCopySchema(table_info->GetSchema(),meta_data_->key_map_,heap_);
    this->index_=CreateIndex(buffer_pool_manager);
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    //ASSERT(false, "Not Implemented yet.");
    vector<Column *> tmp=this->key_schema_->GetColumns();
    uint32_t maxLength=0;
    for(uint32_t i=0;i<tmp.size();i++){
      maxLength=max(maxLength,tmp[i]->GetSerializedSize());
    }
    uint32_t tempSize=4;
    while(maxLength>tempSize){
      tempSize<<=1;
    }
    void *buf;
    switch(tempSize){
      case 4:
      //BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> *tmp;
      buf=this->heap_->Allocate(sizeof(BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>));
      return new(buf)BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);
      case 8:
      //BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> *tmp=NULL;
      buf=this->heap_->Allocate(sizeof(BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>));
      return new(buf)BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);
      case 16:
      //BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> *tmp=NULL;
      buf=this->heap_->Allocate(sizeof(BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>));
      return new(buf)BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);
      case 32:
      //BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> *tmp=NULL;
      buf=this->heap_->Allocate(sizeof(BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>));
      return new(buf)BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);
    }
    return new BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
