#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "page/table_page.h"
#include "transaction/transaction.h"


class TableHeap;
class TablePage;

//End(): page=nullptr  row=last row of last page
class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(const TableIterator &other);

  //own constructor
  explicit TableIterator(TableHeap *th,RowId *row_id);

  explicit TableIterator(TableHeap *th);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  bool isNull() const{
    return row == nullptr;
  }

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  Transaction *txn;
  TableHeap *table_heap;
  TablePage *page;
  Row *row;
  // add your own private member variables here
};

#endif //MINISQL_TABLE_ITERATOR_H
