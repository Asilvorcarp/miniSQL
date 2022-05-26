#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

//own constructor
TableIterator::TableIterator(TableHeap *th,RowId *row_id){
  table_heap=th;
  page=reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(row_id->GetPageId()));
  row=new Row(*row_id);
}

TableIterator::TableIterator(TableHeap *th){
  table_heap=th;
  page=reinterpret_cast<TablePage *>(th->buffer_pool_manager_->FetchPage(th->first_page_id_));
  RowId *row_id=new RowId();
  if(page->GetFirstTupleRid(row_id)){
    row=new Row(*row_id);
  }
}

TableIterator::TableIterator() {
  
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap=other.table_heap;
  txn=other.txn;
  page=other.page;
  row=other.row;
}

TableIterator::~TableIterator() {
  
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(row==nullptr&&itr.row==nullptr){
    return true;
  }
  return (row->GetRowId().GetPageId()==itr.row->GetRowId().GetPageId())&&(row->GetRowId().GetSlotNum()==itr.row->GetRowId().GetSlotNum());
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(operator==(itr));
}

const Row &TableIterator::operator*() {
  return *row;
}

Row *TableIterator::operator->() {
  return row;
}

TableIterator &TableIterator::operator++() {
  RowId *row_id=new RowId();
  if(page->GetNextTupleRid(row->GetRowId(),row_id)){
    row=new Row(*row_id);
    return *this;
  }else{//need to find next page
    int i=page->GetNextPageId();
    if(i!=INVALID_PAGE_ID){
      page=reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(i));
      if(page->GetFirstTupleRid(row_id)){
        row=new Row(*row_id);
        table_heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return *this;
      }else{
        row=nullptr;
        table_heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return *this;
      }
    }else{
      row=nullptr;
      return *this;
    }
  }
}

TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  RowId *row_id=new RowId();
  if(page->GetNextTupleRid(row->GetRowId(),row_id)){
    row=new Row(*row_id);
    return TableIterator(tmp);
  }else{//need to find next page
    int i=page->GetNextPageId();
    if(i!=INVALID_PAGE_ID){
      page=reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(i));
      if(page->GetFirstTupleRid(row_id)){
        row=new Row(*row_id);
        table_heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return TableIterator(tmp);
      }else{
        row=nullptr;
        table_heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return TableIterator(tmp);
      }
    }else{
      row=nullptr;
      return TableIterator(tmp);
    }
  }
}
