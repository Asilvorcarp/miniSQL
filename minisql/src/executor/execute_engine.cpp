#include "executor/execute_engine.h"
#include "glog/logging.h"

#define ENABLE_EXECUTE_DEBUG // todo:for debug

ExecuteEngine::ExecuteEngine() {
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

// ExecuteContext not used, output directly to stdout
  /** // todo
   * 执行完SQL语句后需要输出
    * 执行所用的时间 在项目验收中将会用于考察索引效果
    * done 对查询语句 - 共查询到多少条记录
    * 对插入/删除/更新语句 - 影响了多少条记录（参考MySQL输出）
   **/
// todo: count time spent in each function

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
  string dbName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
  LOG(INFO) << "Create DB: " << dbName << std::endl;
#endif
  if (dbs_.find(dbName) != dbs_.end()) {
    cout << "Database " << dbName << " already exists." << endl;
    return DB_FAILED;
  }
  dbs_.insert(std::make_pair(dbName, new DBStorageEngine(dbName)));

  cout << "Database " << dbName << " created." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
  string dbName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
  LOG(INFO) << "Drop DB: " << dbName << std::endl;
#endif
  if (dbs_.find(dbName) == dbs_.end()) {
    cout << "Database " << dbName << " does not exist." << endl;
    return DB_FAILED;
  }
  delete dbs_[dbName];
  cout << "Database " << dbName << " dropped." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
  LOG(INFO) << "Showing Databases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "No database exists." << endl;
    return DB_SUCCESS;
  }
  for (auto &db : dbs_) {
    cout << db.first << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
  string dbName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
  LOG(INFO) << "Use DB: " << dbName << std::endl;
#endif
  if (dbs_.find(dbName) == dbs_.end()) {
    cout << "Database " << dbName << " does not exist." << endl;
    return DB_FAILED;
  }
  current_db_ = dbName;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
  LOG(INFO) << "Showing Tables" << std::endl;
#endif
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    cout << "No table exists." << endl;
    return DB_SUCCESS;
  }
  for (auto &table : tables) {
    cout << table->GetTableName() << endl;
  }
  return DB_SUCCESS;
}

// yj: done
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  string tableName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
  LOG(INFO) << "Create Table: " << tableName << std::endl;
#endif
  pSyntaxNode columnDefList = ast->child_->next_; // kNodeColumnDefinitionList
  vector<Column *> columns;
  map<string, uint32_t> columnNameToIndex;
  uint32_t columnIndex = 0;
  pSyntaxNode columnDef;
  for (columnDef = columnDefList->child_; columnDef->type_ == kNodeColumnDefinition; columnDef = columnDef->next_) {
    bool isUnique = false;
    if (columnDef->val_) { // ensure pointer not null
      isUnique = string(columnDef->val_) == "unique";
    }
    bool isNullable = true; // "not null" can only be specified by "primary key" later
    string columnName = columnDef->child_->val_;
    string columnType = columnDef->child_->next_->val_;
    columnNameToIndex.insert(std::make_pair(columnName, columnIndex));
    if (columnType == "int") {
      columns.push_back(new Column(columnName, TypeId::kTypeInt, columnIndex, isNullable, isUnique));
    } else if (columnType == "float") {
      columns.push_back(new Column(columnName, TypeId::kTypeFloat, columnIndex, isNullable, isUnique));
    } else if (columnType == "char") {
      string lengthString = columnDef->child_->next_->child_->val_;
      // error if length not valid (float or <=0)
      if (lengthString.find('.') != string::npos) {
        cout << "Invalid length for char type." << endl;
        return DB_FAILED;
      }
      int length = stoi(lengthString);
      if (length <= 0) {
        cout << "Invalid length for char type." << endl;
        return DB_FAILED;
      }
      columns.push_back(new Column(columnName, TypeId::kTypeChar, length, columnIndex, isNullable, isUnique));
    //// } else if (columnType == "varchar") { // if only varchar is supported
    ////   int length = stoi(columnDef->child_->next_->child_->val_);
    ////   columns.push_back(new Column(columnName, TypeId::KMaxTypeId, length, columnIndex, isNullable, isUnique));
    } else {
      cout << "Invalid column type: " << columnType << endl;
      return DB_FAILED; 
    }
    columnIndex++;
  } 
  // columnDef is now after all column definition (now: NULL or columnList)
  pSyntaxNode columnList;
  vector<uint32_t> primaryKeyIndexs = {};
  for (columnList = columnDef; columnList && columnList->type_ == kNodeColumnList; columnList = columnList->next_) {
    // get primaryKeyIndexs
    if (string(columnList->val_) == "primary keys") {
      for (pSyntaxNode identifier = columnList->child_; identifier && identifier->type_ == kNodeIdentifier; identifier = identifier->next_) {
        // try to find the column in the column list
        try
        {
          uint32_t indexInColumns = columnNameToIndex.at(string(identifier->val_));
          // found: mark "unique & not null" for the primary key
          primaryKeyIndexs.push_back(indexInColumns);
        }
        catch(const std::out_of_range& e)
        { // not found
          cout << "Primary key " << string(identifier->val_) << " does not exist." << endl;
          return DB_FAILED; // DB_KEY_NOT_FOUND;
        }
      }
      if (primaryKeyIndexs.size() == 0) {
        cout << "Empty primary key list." << endl;
        return DB_FAILED; 
      }
    }else{
      // not support "foreign keys" and "check"
      LOG(ERROR) << "Unknown column list type: " << columnList->val_ << endl;
      return DB_FAILED; 
    }
  }
  TableSchema* table_schema = new TableSchema(columns); // input of CreateTable
  TableInfo* table_info = nullptr; // output of CreateTable
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->CreateTable(tableName, table_schema,
                                                  nullptr, table_info, primaryKeyIndexs);
  // todo: ensure that ret is !DB_TABLE_ALREADY_EXIST!, DB_FAILED or DB_SUCCESS
  if (ret == DB_TABLE_ALREADY_EXIST) {
    cout << "Table " << tableName << " already exists." << endl;
    return DB_FAILED;
  } else if (ret == DB_FAILED) {
    cout << "Create table failed." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  cout << "Table " << tableName << " created." << endl;
  return DB_SUCCESS;
}

//dxp
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
  string tableName = ast->child_->val_;   //drop table <表名>
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
  LOG(INFO) << "Drop Table:" << tableName << std::endl;
#endif
  if(dbs_[current_db_]->catalog_mgr_->DropTable(tableName)==DB_SUCCESS){
    cout << "Table " << tableName << " dropped." << endl;
    return DB_SUCCESS;
  }
  else{
    cout << "Don't find " << tableName << "." << endl;
    return DB_TABLE_NOT_EXIST;
  }
  return dbs_[current_db_]->catalog_mgr_->DropTable(tableName);
}

//dxp
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
  string tableName = ast->child_->val_; //找表的名字，根据语法树。//SHOW INDEX FROM <表名>
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(tableName,indexes);
  if(indexes.empty()){
    cout << "No index exists." << std::endl;
    return DB_SUCCESS;
  }
  for(vector<IndexInfo *>::iterator its= indexes.begin();its!=indexes.end();its++){
    cout <<(*its)->GetIndexName()<<std::endl;
  }
  return DB_SUCCESS;
}

//dxp
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string indexName = ast->child_->val_; //找index的名字，根据语法树。
  string tableName = ast->child_->next_->val_; //找表的名字，根据语法树。
  pSyntaxNode temp_pointer = ast->child_->next_->next_->child_;
  vector<std::string> index_keys; //找生成索引的属性。
  while(temp_pointer){
    index_keys.push_back(temp_pointer->val_);
    temp_pointer = temp_pointer->next_;
  }
  //not sure ！！参数列表中的Transaction *txn,IndexInfo *&index_info 不知道怎么传 
  IndexInfo * nuknow;
  return dbs_[current_db_]->catalog_mgr_->CreateIndex(tableName,indexName,index_keys,nullptr,nuknow);
}

//dxp not finished  //好像只能drop index 索引名； 但调用需要知道表名；
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
  string indexName = ast->child_->val_; //根据语法树找index的名字。
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //not finished
  return DB_FAILED;
}

// new: to get child value list of a node
vector<string> GetChildValues(const pSyntaxNode &columnListNode) {
  vector<string> columnList;
  pSyntaxNode temp_pointer = columnListNode->child_;
  while (temp_pointer) {
    #ifdef ENABLE_EXECUTE_DEBUG
      LOG(INFO) << "A val of child: " << string(temp_pointer->val_) << std::endl; // for test
    #endif
    columnList.push_back(string(temp_pointer->val_));
    temp_pointer = temp_pointer->next_;
  }
  return columnList;
}

// new: to get child list of a node
vector<pSyntaxNode> GetChilds(const pSyntaxNode &columnListNode) {
  vector<pSyntaxNode> columnList;
  pSyntaxNode temp_pointer = columnListNode->child_;
  while (temp_pointer) {
    columnList.push_back(temp_pointer);
    temp_pointer = temp_pointer->next_;
  }
  return columnList;
}

// new: to get the result of a node
bool GetResultOfNode(const pSyntaxNode &ast, const Row &row){
  if (ast == nullptr) {
    LOG(ERROR) << "Unexpected nullptr." << endl;
    return false;
  }
  switch (ast->type_) {
    case kNodeConditions: // where
      return GetResultOfNode(ast->child_, row);
    case kNodeConnector:
      switch (ast->val_[0]) {
        case 'a': // & and    // todo: test capital AND
          return GetResultOfNode(ast->child_, row) && GetResultOfNode(ast->child_->next_, row);
        case 'o': // | or
          return GetResultOfNode(ast->child_, row) || GetResultOfNode(ast->child_->next_, row);
        default:
          LOG(ERROR) << "Unknown connector: " << string(ast->val_) << endl;
          return false;
      }
    case kNodeCompareOperator: /** operators '=', '<>', '<=', '>=', '<', '>', is, not */

      // refer -> table_heap_test.cpp

      if (string(ast->val_) == "="){
        // ASSERT_EQ(CmpBool::kTrue, fields[i]->CompareEquals(fields[i]));
        //todo /* code */
        return true;
      }else if (string(ast->val_) == "<>"){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == "<="){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == "<>"){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == ">="){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == "<"){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == ">"){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == "is"){
        //todo /* code */
        return true;
      }else if (string(ast->val_) == "not"){
        //todo /* code */
        return true;
      }else{
        LOG(ERROR) << "Unknown kNodeCompareOperator val: " << string(ast->val_) << endl;
        return false;
      }
    default:
      LOG(ERROR) << "Unknown node type: " << ast->type_ << endl;
      return false;
  }
  return false;
}

// todo(yj): almost done
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  pSyntaxNode selectNode = ast->child_; //things after 'select', like '*', 'name, id'
  pSyntaxNode fromNode = selectNode->next_; //things after 'from', like 't1'
  pSyntaxNode whereNode = fromNode->next_; //things after 'where', like 'name = a' (may be null)

  // 1. from
  string fromTable = fromNode->val_; // from table name
  TableInfo *table_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTable(fromTable, table_info);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Table not exist." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);

  // get the columns to be selected
  vector<string> selectColumns; // select column names
  vector<uint32_t> selectColumnIndexs;
  bool if_select_all = false;
  if (selectNode->type_ == kNodeAllColumns) 
  {// select * (all columns)
    if_select_all = true;
  }else{// select columns
    assert(selectNode->type_ == kNodeColumnList);
    assert(string(selectNode->val_) == "select columns");
    selectColumns = GetChildValues(selectNode);
    for (auto &columnName: selectColumns){
      uint32_t index;
      dberr_t ret = table_info->GetSchema()->GetColumnIndex(columnName, index);
      if (ret == DB_COLUMN_NAME_NOT_EXIST){
        cout << "Select column " << columnName << " not exist." << endl;
        return DB_FAILED;
      }else{
        assert(ret == DB_SUCCESS);
        selectColumnIndexs.push_back(index);
      }
    }
  }

  // traverse the table
  TableIterator iter = table_info->GetTableHeap()->Begin(nullptr);
  // TableIterator iter_end = table_info->GetTableHeap()->End(); // todo: End函数有bug
  int select_count = 0;
  vector<vector<string>> select_result;
  for (; !iter.isNull(); iter++) {
    Row row = *iter;
    // 2. where
    if (GetResultOfNode(whereNode, row)) {
      // 3. select
      vector<string> result_line;
      std::vector<Field *> &fields = row.GetFields();
      if (if_select_all){
        for (size_t i = 0; i < fields.size(); i++) {
          result_line.push_back(fields[i]->GetData());
        }
      }else{
        for (auto &i : selectColumnIndexs){
          result_line.push_back(fields[i]->GetData());
        }
      }
      select_result.push_back(result_line);
      select_count++;
    }
  }
  cout << "Select count: " << select_count << endl;

  // todo print the result
  // print the first line (column names)
  cout << "\t";
  for (auto &columnName : selectColumns){
    cout << columnName << "\t";
  }
  cout << endl;
  // print the results
  uint32_t temp_index = 0;
  for (auto &line : select_result){
    cout << temp_index << "\t";
    for (auto &field : line){
      // todo: maybe add a operator<< to Type
      cout << field << "\t"; // todo mod
    }
    cout << endl;
    temp_index++;
  }

  return DB_SUCCESS;
}

// todo(yj): almost done
dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  // 1. get table name
  pSyntaxNode tableNode = ast->child_; // table name
  string tableName = tableNode->val_;

  // 2. get TableSchema and TableHeap
  TableInfo *table_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTable(tableName, table_info);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Table not exist." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  TableSchema *table_schema = table_info->GetSchema();
  TableHeap *table_heap = table_info->GetTableHeap();

  // 3. convert values to a Row
  pSyntaxNode valuesNode = tableNode->next_; // values
  vector<pSyntaxNode> childs = GetChilds(valuesNode); 
  // ensure the same size of values & columns
  if (childs.size() != table_schema->GetColumnCount()) {
    cout << "Column number not match." << endl;
    return DB_FAILED;
  }
  vector<Column *> columns = table_schema->GetColumns();
  vector<Field> fields;
  for (uint32_t i = 0; i < childs.size(); i++) {
    if (childs[i]->type_ == kNodeNull){
      if ( !columns[i]->IsNullable() ) {
        cout << "Null value is not allowed for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      fields.push_back(Field(columns[i]->GetType()));
    }else if (columns[i]->GetType() == kTypeInt) {
      if (childs[i]->type_ != kNodeNumber || string(childs[i]->val_).find(".") != string::npos) {
        // error if type not match / has float point "."
        cout << "Wrong type, expected int value for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      int32_t value = atoi(childs[i]->val_); 
      fields.push_back(Field(kTypeInt, value));
    }else if (columns[i]->GetType() == kTypeFloat) {
      if (childs[i]->type_ != kNodeNumber) {
        // error if type not match
        cout << "Wrong type, expected float value for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      float value = atof(childs[i]->val_);
      fields.push_back(Field(kTypeFloat, value));
    }else if(columns[i]->GetType() == kTypeChar) {
      if (childs[i]->type_ != kNodeString) {
        // error if type not match
        cout << "Wrong type, expected string value for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      if (strlen(childs[i]->val_) > columns[i]->GetLength()) {
        // error if too long
        cout << "The string is too long for " << columns[i]->GetName() << "." << endl;
        cout << "The string is " << strlen(childs[i]->val_) << " characters long, but the column's max length is " \
             << columns[i]->GetLength() << " characters." << endl;
        return DB_FAILED;
      }
      // todo: not sure about what "manage_data" means, currently set to true
      fields.push_back(Field(kTypeChar, childs[i]->val_, strlen(childs[i]->val_), true));
    }else{
      LOG(ERROR) << "Unsupported type." << endl;
      return DB_FAILED;
    }
  }
  Row row(fields);

  // 4. insert
  // todo: implement things about primary key (error if Equals) inside TableHeap
  bool ret_bool = table_heap->InsertTuple(row, nullptr);
  if (ret_bool == false){ // todo: more error type like duplicated primary key
    cout << "Insert failed." << endl;
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  // ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  // table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

// needless to implement
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
  #endif
  return DB_FAILED;
}

// needless to implement
dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
  #endif
  return DB_FAILED;
}

// needless to implement
dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
  #endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // remember to support comments("-- xxx")
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
