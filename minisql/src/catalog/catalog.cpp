#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf+ofs,CATALOG_METADATA_MAGIC_NUM);
  ofs+=4;
  MACH_WRITE_UINT32(buf+ofs,table_meta_pages_.size());
  ofs+=4;
  for(auto iter=table_meta_pages_.begin();iter!=table_meta_pages_.end();iter++){
    MACH_WRITE_UINT32(buf+ofs,iter->first);
    ofs+=4;
    MACH_WRITE_INT32(buf+ofs,iter->second);
    ofs+=4;
  }
  MACH_WRITE_UINT32(buf+ofs,index_meta_pages_.size());
  ofs+=4;
  for(auto iter=index_meta_pages_.begin();iter!=index_meta_pages_.end();iter++){
    MACH_WRITE_UINT32(buf+ofs,iter->first);
    ofs+=4;
    MACH_WRITE_INT32(buf+ofs,iter->second);
    ofs+=4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t ofs=0;
  if(buf==NULL){
    return NULL;
  }
  uint32_t magicNum=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=4;
  if(magicNum!=89849){
    std::cerr<<"CATALOG_METADATA_MAGIC_NUM does not match"<<std::endl;
    return NULL;
  }
  uint32_t tableSize=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=4;
  std::map<table_id_t, page_id_t> tableMetaPages;
  for(uint32_t i=0;i<tableSize;i++){
    table_id_t ti=MACH_READ_FROM(table_id_t,buf+ofs);
    ofs+=4;
    page_id_t pi=MACH_READ_FROM(page_id_t,buf+ofs);
    ofs+=4;
    tableMetaPages[ti]=pi;
  }
  uint32_t indexSiez=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=4;
  std::map<index_id_t, page_id_t> indexMetaPages;
  for(uint32_t i=0;i<indexSiez;i++){
    index_id_t ii=MACH_READ_FROM(index_id_t,buf+ofs);
    ofs+=4;
    page_id_t pi=MACH_READ_FROM(page_id_t,buf+ofs);
    ofs+=4;
    indexMetaPages[ii]=pi;
  }
  ALLOC_P(heap,CatalogMeta)(tableMetaPages,indexMetaPages);
  CatalogMeta *ans=new CatalogMeta(tableMetaPages,indexMetaPages);
  return ans;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t)*(3+2*(table_meta_pages_.size()+index_meta_pages_.size()));
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if(init==true){
    this->catalog_meta_=CatalogMeta::NewInstance(this->heap_);
  }
  else{
    Page *pge=buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    this->catalog_meta_=CatalogMeta::NewInstance(this->heap_);
    this->catalog_meta_=CatalogMeta::DeserializeFrom(pge->GetData(),this->heap_);
    auto iter=this->catalog_meta_->table_meta_pages_.begin();
    while(iter!=this->catalog_meta_->table_meta_pages_.end()){
      LoadTable(iter->first,iter->second);
      iter++;
    }
    iter=this->catalog_meta_->index_meta_pages_.begin();
    while(iter!=this->catalog_meta_->index_meta_pages_.end()){
      LoadIndex(iter->first,iter->second);
      iter++;
    }
  }     
  // ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  Page *pge=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(pge->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  delete heap_;
}

// new: added primaryKeyIndexs (default: empty)
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info,
                                    vector<uint32_t> primaryKeyIndexs) {
  if(table_names_.count(table_name)!=0){
    return DB_TABLE_ALREADY_EXIST;
  } 
  page_id_t pageID;
  Page *pge=buffer_pool_manager_->NewPage(pageID);
  TableHeap *th=TableHeap::Create(this->buffer_pool_manager_,schema,txn,this->log_manager_,this->lock_manager_,this->heap_);
  table_id_t tableID=this->catalog_meta_->GetNextTableId();
  TableMetadata *tm=TableMetadata::Create(tableID,table_name,pageID,schema,this->heap_,primaryKeyIndexs);
  table_info=TableInfo::Create(this->heap_);
  table_info->Init(tm,th);
  this->table_names_[table_name]=tableID;
  this->tables_[tableID]=table_info;
  this->catalog_meta_->table_meta_pages_[tableID]=pageID;
  tm->SerializeTo(pge->GetData());
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(this->table_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }
  table_info=this->tables_[this->table_names_[table_name]];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  auto iter=this->table_names_.begin();
  while(iter!=this->table_names_.end()){
    tables.push_back(this->tables_.at(iter->second));
    iter++;
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  if(this->table_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }else{
    if(this->index_names_.count(table_name)&&this->index_names_.at(table_name).count(index_name)!=0){
      return DB_INDEX_ALREADY_EXIST;
    }else{
      index_id_t nextIndexID=this->catalog_meta_->GetNextIndexId();
      TableInfo *tf=this->tables_.at(this->table_names_.at(table_name));          
      page_id_t pageID; 
      Page *pge=buffer_pool_manager_->NewPage(pageID);
      vector<uint32_t> tmp;
      vector<Column *> col=tf->GetSchema()->GetColumns();
      for(uint32_t i=0;i<index_keys.size();i++){
        uint32_t index_id;
        if(tf->GetSchema()->GetColumnIndex(index_keys[i],index_id)==DB_COLUMN_NAME_NOT_EXIST){
          return DB_COLUMN_NAME_NOT_EXIST;
        }else{
          tmp.push_back(index_id);
        }
      }
      IndexMetadata *im=IndexMetadata::Create(this->catalog_meta_->GetNextIndexId(),index_name,this->table_names_.at(table_name),tmp,this->heap_); 
      im->SerializeTo(pge->GetData());
      index_info=IndexInfo::Create(this->heap_);
      index_info->Init(im,tf,this->buffer_pool_manager_);
      unordered_map<std::string, index_id_t> tmpMap;
      if(this->index_names_.count(table_name)){
        tmpMap=this->index_names_.at(table_name);
      }
      tmpMap[index_name]=nextIndexID;
      this->index_names_[table_name]=tmpMap;
      this->indexes_[nextIndexID]=index_info;
      this->catalog_meta_->index_meta_pages_[nextIndexID]=pge->GetPageId();
      return DB_SUCCESS;
    }
  }
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  if(this->index_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }
  if(this->index_names_.at(table_name).count(index_name)==0){
    return DB_INDEX_NOT_FOUND;
  }
  index_info=this->indexes_.at(this->index_names_.at(table_name).at(index_name));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  unordered_map<std::string, index_id_t> tmp=this->index_names_.at(table_name);
  auto iter=tmp.begin();
  while(iter!=tmp.end()){
    indexes.push_back(this->indexes_.at(iter->second));
    iter++;
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  if(this->table_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }
  //get the page store the table
  //attention:the table may be stored in more than one page, which causes the more 
  Page *pge=buffer_pool_manager_->FetchPage(this->catalog_meta_->table_meta_pages_.at(this->table_names_.at(table_name)));
  buffer_pool_manager_->DeletePage(pge->GetPageId());
  this->catalog_meta_->table_meta_pages_.erase(this->table_names_.at(table_name));
  this->tables_[this->table_names_.at(table_name)]->GetTableHeap()->FreeHeap();
  this->tables_.erase(this->table_names_.at(table_name));
  this->table_names_.erase(table_name);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  if(this->index_names_.at(table_name).count(index_name)==0){
    return DB_INDEX_NOT_FOUND;
  }
  //get the page store the index
  Page *pge=buffer_pool_manager_->FetchPage(this->catalog_meta_->index_meta_pages_.at(this->index_names_.at(table_name).at(index_name)));
  buffer_pool_manager_->DeletePage(pge->GetPageId());
  this->catalog_meta_->index_meta_pages_.erase(this->index_names_.at(table_name).at(index_name));
  this->indexes_.erase(this->index_names_.at(table_name).at(index_name));
  this->index_names_.at(table_name).erase(index_name);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return buffer_pool_manager_->DeletePage(CATALOG_META_PAGE_ID)?DB_SUCCESS:DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Impleme
  TableInfo *table_info=TableInfo::Create(this->heap_);
  Page *pge=buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *tm=nullptr;
  TableMetadata::DeserializeFrom(pge->GetData(),tm,this->heap_);
  this->table_names_[tm->GetTableName()]=table_id;
  TableHeap *th=TableHeap::Create(this->buffer_pool_manager_,tm->GetSchema(),nullptr,this->log_manager_,this->lock_manager_,table_info->GetMemHeap());
  table_info->Init(tm,th);
  this->tables_[table_id]=table_info;
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  Page *pge=buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *im=nullptr;
  IndexMetadata::DeserializeFrom(pge->GetData(),im,this->heap_);
  IndexInfo *ii=IndexInfo::Create(this->heap_);
  ii->Init(im,this->tables_.at(im->GetTableId()),this->buffer_pool_manager_);
  unordered_map<std::string, index_id_t> tmp;
  if(index_names_.count(this->tables_.at(ii->GetTableInfo()->GetTableId())->GetTableName())){
    this->index_names_.at(this->tables_.at(ii->GetTableInfo()->GetTableId())->GetTableName());
  }
  tmp[ii->GetIndexName()]=index_id;
  this->index_names_[this->tables_.at(ii->GetTableInfo()->GetTableId())->GetTableName()]=tmp;
  this->indexes_[index_id]=ii;
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}