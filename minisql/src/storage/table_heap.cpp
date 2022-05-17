#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t i=first_page_id_;
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
  //auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(i));
  if(page->GetNextPageId()!=INVALID_PAGE_ID){
    page->Init(i,INVALID_PAGE_ID,log_manager_,txn);
  }
  if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    return true;
  }
  int lastI=i;
  i=page->GetNextPageId();
  while(i!=INVALID_PAGE_ID){
    auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
    if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
      return true;
    }
    lastI=i;
    i=page->GetNextPageId();
  }
  //need to allocate a new page,i=invalid page id
  page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(i));
  page->Init(i,lastI,log_manager_,txn);
  if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    return true;
  }
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_ ->FetchPage(rid.GetPageId()));//get the page
  if(page==nullptr){
    return false;
  }
  Row *oldRow=new Row(rid);//get the old row
  Row newRow=row;
  int type=page->UpdateTuple(row,oldRow,schema_,txn,lock_manager_,log_manager_);//get the update result
  switch(type){
    case 1:return false;
    case 2:return false;//invalid update
    case 0:return true;
    //not enough space for update
    case 3:if(page->MarkDelete(rid,txn,lock_manager_,log_manager_)){
      TablePage *oldPage=page;
      int i=first_page_id_;
      int preI=i;
      while(i!=INVALID_PAGE_ID){
        page=reinterpret_cast<TablePage *>(buffer_pool_manager_ ->FetchPage(i));
        if(page->InsertTuple(newRow,schema_,txn,lock_manager_,log_manager_)){
          oldPage->ApplyDelete(rid,txn,log_manager_);
          return true;
        }else{
          preI=i;
          i=page->GetNextPageId();
        }
      }
      //create a new page
      page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(i));
      page->Init(preI,i,log_manager_,txn);
      if(page->InsertTuple(newRow,schema_,txn,lock_manager_,log_manager_)){
          oldPage->ApplyDelete(rid,txn,log_manager_);
          return true;
      }
      return false;
    }else{
      return false;
    }
  }
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->ApplyDelete(rid,txn,log_manager_);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  int i=first_page_id_;
  while(i!=INVALID_PAGE_ID){
    auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
    i=page->GetNextPageId();
    delete page;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  return page->GetTuple(row,schema_,txn,lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator(this);
}

TableIterator TableHeap::End() {
  return TableIterator(this,nullptr);
}
