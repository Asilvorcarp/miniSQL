#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <fstream>
#include <algorithm>
#include <time.h>

extern "C" {
extern int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
#include "parser/parser.h"
};

#define ENABLE_EXECUTE_DEBUG // debug

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
    * 对插入/删除/更新语句 - 影响了多少条记录（参考MySQL输出）
   **/
// todo: count time spent in each function

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
  string dbName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
  LOG(INFO) << "Create DB: " << dbName << std::endl;
#endif
  long time_start = clock();
  if (dbs_.find(dbName) != dbs_.end()) {
    cout << "Error: Database " << dbName << " already exists." << endl;
    return DB_FAILED;
  }
  dbs_.insert(std::make_pair(dbName, new DBStorageEngine(dbName)));
  long time_end = clock();
  cout << "Database " << dbName << " created." << "  (" << (double)(time_end - time_start)/CLOCKS_PER_SEC  << " sec)" << endl;

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
  string dbName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
  LOG(INFO) << "Drop DB: " << dbName << std::endl;
#endif
  long time_start = clock();
  if (dbs_.find(dbName) == dbs_.end()) {
    cout << "Error: Database " << dbName << " does not exist." << endl;
    return DB_FAILED;
  }
  delete dbs_[dbName];
  long time_end = clock();
  cout << "Database " << dbName << " dropped." << "  (" << (double)(time_end - time_start)/CLOCKS_PER_SEC  << " sec)" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
  LOG(INFO) << "Showing Databases" << std::endl;
#endif
  long time_start = clock();
  if (dbs_.empty()) {
    cout << "No database exists." << endl;
    return DB_SUCCESS;
  }
  int count = 0;
  for (auto &db : dbs_) {
    cout << db.first << endl;
    count++;
  }
  long time_end = clock();
  cout << "Showed "<< count << " databases. (" << (double)(time_end - time_start)/CLOCKS_PER_SEC  << " sec)" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
  string dbName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
  LOG(INFO) << "Use DB: " << dbName << std::endl;
#endif
  if (dbs_.find(dbName) == dbs_.end()) {
    cout << "Error: Database " << dbName << " does not exist." << endl;
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
  long time_start = clock();
  int count = 0;
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    cout << "No table exists." << endl;
    return DB_SUCCESS;
  }
  for (auto &table : tables) {
    cout << table->GetTableName() << endl;
    count++;
  }
  long time_end = clock();
  cout << "Showed "<< count << " tables. (" << (double)(time_end - time_start)/CLOCKS_PER_SEC  << " sec)" << endl;
  return DB_SUCCESS;
}

// yj: done
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  string tableName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
  LOG(INFO) << "Create Table: " << tableName << std::endl;
#endif
  long time_start = clock();
  pSyntaxNode columnDefList = ast->child_->next_; // kNodeColumnDefinitionList
  vector<Column *> columns;
  map<string, uint32_t> columnNameToIndex;
  uint32_t columnIndex = 0;
  pSyntaxNode columnDef;
  vector<string> uniqueKeys = {};
  for (columnDef = columnDefList->child_; columnDef->type_ == kNodeColumnDefinition; columnDef = columnDef->next_) {
    bool isUnique = false;
    if (columnDef->val_) { // ensure pointer not null
      isUnique = string(columnDef->val_) == "unique";
    }
    bool isNullable = true; // "not null" can only be specified by "primary key" later
    string columnName = columnDef->child_->val_;
    string columnType = columnDef->child_->next_->val_;
    if (isUnique)
      uniqueKeys.push_back(columnName);
    columnNameToIndex.insert(std::make_pair(columnName, columnIndex));
    if (columnType == "int") {
      columns.push_back(new Column(columnName, TypeId::kTypeInt, columnIndex, isNullable, isUnique));
    } else if (columnType == "float") {
      columns.push_back(new Column(columnName, TypeId::kTypeFloat, columnIndex, isNullable, isUnique));
    } else if (columnType == "char") {
      string lengthString = columnDef->child_->next_->child_->val_;
      // error if length not valid (float or <=0)
      if (lengthString.find('.') != string::npos) {
        cout << "Error: Invalid length for char type." << endl;
        return DB_FAILED;
      }
      int length = stoi(lengthString);
      if (length <= 0) {
        cout << "Error: Invalid length for char type." << endl;
        return DB_FAILED;
      }
      columns.push_back(new Column(columnName, TypeId::kTypeChar, length, columnIndex, isNullable, isUnique));
    //// } else if (columnType == "varchar") { // if only varchar is supported
    ////   int length = stoi(columnDef->child_->next_->child_->val_);
    ////   columns.push_back(new Column(columnName, TypeId::KMaxTypeId, length, columnIndex, isNullable, isUnique));
    } else {
      cout << "Error: Invalid column type: " << columnType << endl;
      return DB_FAILED; 
    }
    columnIndex++;
  } 
  // columnDef is now after all column definition (now: NULL or columnList)
  pSyntaxNode columnList;
  vector<string> primaryKeys = {};
  vector<uint32_t> primaryKeyIndexs = {};
  for (columnList = columnDef; columnList && columnList->type_ == kNodeColumnList; columnList = columnList->next_) {
    // get primaryKeyIndexs
    if (string(columnList->val_) == "primary keys") {
      for (pSyntaxNode identifier = columnList->child_; identifier && identifier->type_ == kNodeIdentifier; identifier = identifier->next_) {
        // try to find the column in the column list
        try
        {
          primaryKeys.push_back(identifier->val_);
          uint32_t indexInColumns = columnNameToIndex.at(string(identifier->val_));
          // found: mark "unique & not null" for the primary key
          primaryKeyIndexs.push_back(indexInColumns);
        }
        catch(const std::out_of_range& e)
        { // not found
          cout << "Error: Primary key " << string(identifier->val_) << " does not exist." << endl;
          return DB_FAILED; // DB_KEY_NOT_FOUND;
        }
      }
      if (primaryKeyIndexs.size() == 0) {
        cout << "Error: Empty primary key list." << endl;
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
  auto cat = dbs_[current_db_]->catalog_mgr_;
  dberr_t ret = cat->CreateTable(tableName, table_schema,
                     nullptr, table_info, primaryKeyIndexs);
  if (ret == DB_TABLE_ALREADY_EXIST) {
    cout << "Error: Table " << tableName << " already exists." << endl;
    return DB_FAILED;
  } else if (ret == DB_FAILED) {
    cout << "Error: Create table failed." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  // create index for primary key
  IndexInfo *pkIndexInfo = nullptr;
  string pkIndexName = cat->AutoGenPKIndexName(tableName);
  dberr_t pk_ret = cat->CreateIndex(tableName, pkIndexName, 
                        primaryKeys, nullptr, pkIndexInfo);
  assert(pk_ret == DB_SUCCESS);
  // create index for unique key
  for (auto &uniqueKey : uniqueKeys) {
    IndexInfo *uniqueIndexInfo = nullptr;
    string uniqueIndexName = cat->AutoGenUniIndexName(tableName, uniqueKey);
    dberr_t uni_ret = cat->CreateIndex(tableName, uniqueIndexName, 
                           {uniqueKey}, nullptr, uniqueIndexInfo);
    assert(uni_ret == DB_SUCCESS);
  }
  long time_end = clock();
  cout << "Table " << tableName << " created." << "  (" << (time_end - time_start)*1.0/CLOCKS_PER_SEC  << " sec)" << endl;
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
    cout << "Error: Table " << tableName << " dropped." << endl;
    return DB_SUCCESS;
  }
  else{
    cout << "Error: Can't find " << tableName << "." << endl;
    return DB_TABLE_NOT_EXIST;
  }
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
  // show all indexs (parser dont't support "show index from <Table>")
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << endl;
#endif
  auto table_names = dbs_[current_db_]->catalog_mgr_->GetAllTableNames();
  vector<IndexInfo *> allIndexes;
  for (auto &table_name : table_names) {
    vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
    if(indexes.empty()){
      cout << "No index exists." << endl;
      return DB_SUCCESS;
    }
    allIndexes.insert(allIndexes.end(), indexes.begin(), indexes.end());
  }
  for(auto its = allIndexes.begin(); its!=allIndexes.end(); its++){
    cout << (*its)->GetIndexName() << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  long time_start = clock();
  string indexName = ast->child_->val_; //找index的名字，根据语法树。
  string tableName = ast->child_->next_->val_; //找表的名字，根据语法树。
  pSyntaxNode temp_pointer = ast->child_->next_->next_->child_;
  vector<std::string> index_keys; //找生成索引的属性。
  while(temp_pointer){
    index_keys.push_back(temp_pointer->val_);
    temp_pointer = temp_pointer->next_;
  }
  IndexInfo *index_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->CreateIndex(tableName, indexName, 
                                              index_keys, nullptr, index_info);
  if (ret == DB_TABLE_NOT_EXIST) {
    cout << "Error: Table " << tableName << " does not exist." << endl;
    return DB_FAILED;
  }else if (ret == DB_INDEX_ALREADY_EXIST) {
    cout << "Error: Index " << indexName << " already exists." << endl;
    return DB_FAILED;
  }else if (ret == DB_COLUMN_NAME_NOT_EXIST) {
    cout << "Error: Key does not exist." << endl;
    return DB_FAILED;
  }else if (ret == DB_COLUMN_NOT_UNIQUE) {
    // 只能在唯一键/主键上建立索引
    cout << "Error: Key is not unique or primary." << endl;
    return DB_FAILED;
  } else if (ret == DB_FAILED) {
    cout << "Error: Create index failed." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  long time_end = clock();
  cout << "Created " << "index " << indexName << ". (" << (time_end - time_start)*1.0/CLOCKS_PER_SEC  << " sec)" << endl;
  return DB_SUCCESS;
}

//dxp
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
  string indexName = ast->child_->val_; //根据语法树找index的名字。
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  int ret = dbs_[current_db_]->catalog_mgr_->DropIndex(indexName);
  if(ret == 0){
    cout << "Error: index not found." << endl;
    return DB_INDEX_NOT_FOUND;
  }
  else{
    cout << "Drop " << "index " << indexName << ","<< ret << " in total." <<std::endl;
    return DB_SUCCESS;
  }
}

// new: get child value list of a node
vector<string> GetChildValues(const pSyntaxNode &columnListNode) {
  vector<string> columnList;
  pSyntaxNode temp_pointer = columnListNode->child_;
  while (temp_pointer) {
    // LOG(INFO) << "A val of child: " << string(temp_pointer->val_) << std::endl; // for test
    columnList.push_back(string(temp_pointer->val_));
    temp_pointer = temp_pointer->next_;
  }
  return columnList;
}

// new: get child list of a node
vector<pSyntaxNode> GetChilds(const pSyntaxNode &columnListNode) {
  vector<pSyntaxNode> columnList;
  pSyntaxNode temp_pointer = columnListNode->child_;
  while (temp_pointer) {
    columnList.push_back(temp_pointer);
    temp_pointer = temp_pointer->next_;
  }
  return columnList;
}

// new: get the result of a CompareOperator node 
// operators '=', '<>', '<=', '>=', '<', '>', is, not
CmpBool GetCompareResult(const pSyntaxNode &ast, const Row &row, TableSchema *schema) {
  string fieldName = ast->child_->val_;
  uint32_t fieldIndex;
  schema->GetColumnIndex(fieldName, fieldIndex);
  TypeId type = schema->GetColumn(fieldIndex)->GetType();

  Field tempField(type); // null now
  if (ast->child_->next_->type_ != kNodeNull){
    string rhs = ast->child_->next_->val_;
    tempField.FromString(rhs);
  }
  
  if (string(ast->val_) == "="){
    return row.GetField(fieldIndex)->CompareEquals(tempField);
  }else if (string(ast->val_) == "<>"){
    return row.GetField(fieldIndex)->CompareNotEquals(tempField);
  }else if (string(ast->val_) == "<="){
    return row.GetField(fieldIndex)->CompareLessThanEquals(tempField);
  }else if (string(ast->val_) == ">="){
    return row.GetField(fieldIndex)->CompareGreaterThanEquals(tempField);
  }else if (string(ast->val_) == "<"){
    return row.GetField(fieldIndex)->CompareLessThan(tempField);
  }else if (string(ast->val_) == ">"){
    return row.GetField(fieldIndex)->CompareGreaterThan(tempField);
  }else if (string(ast->val_) == "is"){
    return GetCmpBool(row.GetField(fieldIndex)->IsNull() == tempField.IsNull());
  }else if (string(ast->val_) == "not"){
    return GetCmpBool(row.GetField(fieldIndex)->IsNull() != tempField.IsNull());
  }else{
    LOG(ERROR) << "Unknown kNodeCompareOperator val: " << string(ast->val_) << endl;
    return kFalse;
  }
}

// new: get the result of a node (kTrue, kFalse, kNull)
CmpBool GetResultOfNode(const pSyntaxNode &ast, const Row &row, TableSchema *schema) {
  if (ast == nullptr) {
    LOG(ERROR) << "Unexpected nullptr." << endl;
    return kFalse;
  }
  CmpBool l, r;
  switch (ast->type_) {
    case kNodeConditions: // where
      return GetResultOfNode(ast->child_, row, schema);
    case kNodeConnector: // and, or
      l = GetResultOfNode(ast->child_, row, schema);
      r = GetResultOfNode(ast->child_->next_, row, schema);
      switch (ast->val_[0]) {
        case 'a': // & and
          if (l == kTrue && r == kTrue) {
            return kTrue;
          } else if (l == kFalse || r == kFalse) {
            return kFalse;
          } else {
            return kNull;
          }
        case 'o': // | or
          if (l == kTrue || r == kTrue) {
            return kTrue;
          } else if (l == kFalse && r == kFalse) {
            return kFalse;
          } else {
            return kNull;
          }
        default:
          LOG(ERROR) << "Unknown connector: " << string(ast->val_) << endl;
          return kFalse;
      }
    case kNodeCompareOperator: /** operators '=', '<>', '<=', '>=', '<', '>', is, not */
      return GetCompareResult(ast, row, schema);
    default:
      LOG(ERROR) << "Unknown node type: " << ast->type_ << endl;
      return kFalse;
  }
  return kFalse;
}

inline bool isAnd(const pSyntaxNode &ast) {
  return ast->type_ == kNodeConnector && string(ast->val_) == "and";
}

inline bool isEqual(const pSyntaxNode &ast) {
  return ast->type_ == kNodeCompareOperator && string(ast->val_) == "=";
}

bool ExecuteEngine::canAccelerate(pSyntaxNode whereNode, TableInfo* &table_info, CatalogManager* &cat,
                                  IndexInfo* &index, Row* &key){
  if (whereNode == nullptr) {
    return false;
  }
  assert(whereNode->type_ == kNodeConditions);
  // traverse the tree
  pSyntaxNode curr = whereNode->child_;
  vector<string> colNames;
  vector<string> colValues;
  while (!isEqual(curr)) {
    if (!isAnd(curr)) {
      // not equal/and
      return false;
    }
    pSyntaxNode rhs = curr->child_->next_;
    if (isEqual(rhs)) {
      colNames.push_back(rhs->child_->val_);
      colValues.push_back(rhs->child_->next_->val_);
    }else return false;
    curr = curr->child_;
  }
  // the last equal operater
  colNames.push_back(curr->child_->val_);
  colValues.push_back(curr->child_->next_->val_);
  // get index
  vector<pair<string, uint32_t>> val_key_map;
  for (uint32_t i = 0; i < colNames.size(); i++) {
    uint32_t colIndex;
    dberr_t ret = table_info->GetSchema()->GetColumnIndex(colNames[i], colIndex);
    if (ret != DB_SUCCESS) {
      return false;
    }
    val_key_map.push_back(make_pair(colValues[i], colIndex));
  }
  // sort according to key_map
  sort(val_key_map.begin(), val_key_map.end(), 
    [](const pair<string, uint32_t> &a, const pair<string, uint32_t> &b) {
      return a.second < b.second;
    }
  );
  // extract key_map
  vector<uint32_t> key_map;
  for (uint32_t i = 0; i < val_key_map.size(); i++) {
    key_map.push_back(val_key_map[i].second);
  }
  vector<IndexInfo *> index_list;
  cat->GetIndexesForKeyMap(table_info->GetTableName(), key_map, index_list);
  if (index_list.size() == 0) {
    return false;
  }
  index = index_list[0];
  // get key
  vector<Field> fields;
  for (uint32_t i = 0; i < val_key_map.size(); i++) {
    Field field(table_info->GetSchema()->GetColumn(val_key_map[i].second)->GetType());
    field.FromString(val_key_map[i].first);
    fields.push_back(field);
  }
  key = new Row(fields);
  return true;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  pSyntaxNode selectNode = ast->child_; //things after 'select', like '*', 'name, id'
  pSyntaxNode fromNode = selectNode->next_; //things after 'from', like 't1'
  pSyntaxNode whereNode = fromNode->next_; //things after 'where', like 'name = a' (may be null)

  //记录时间
  long time_start = clock();    //计时开始

  // 1. from
  string fromTable = fromNode->val_; // from table name
  TableInfo *table_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTable(fromTable, table_info);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Error: Table not exist." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  TableSchema *table_schema = table_info->GetSchema();

  // get the columns to be selected
  vector<string> selectColumns; // select column names
  vector<uint32_t> selectColumnIndexs;
  bool if_select_all = false;
  if (selectNode->type_ == kNodeAllColumns) 
  {// select * (all columns)
    if_select_all = true;
    for (auto &column : table_schema->GetColumns()) {
      selectColumns.push_back(column->GetName());
    }
  }else{// select columns
    assert(selectNode->type_ == kNodeColumnList);
    assert(string(selectNode->val_) == "select columns");
    selectColumns = GetChildValues(selectNode);
    for (auto &columnName: selectColumns){
      uint32_t index;
      dberr_t ret = table_schema->GetColumnIndex(columnName, index);
      if (ret == DB_COLUMN_NAME_NOT_EXIST){
        cout << "Error: Select column " << columnName << " not exist." << endl;
        return DB_FAILED;
      }else{
        assert(ret == DB_SUCCESS);
        selectColumnIndexs.push_back(index);
      }
    }
  }

  int select_count = 0;
  vector<vector<string>> select_result;

  // accelerate query using index if possible
  IndexInfo* index_info = nullptr; // output of canAccelerate
  Row* key;                        // output of canAccelerate
  if (canAccelerate(whereNode, table_info, dbs_[current_db_]->catalog_mgr_, index_info, key)){
    // accelerate using index
    vector<RowId> scanRet;
    index_info->GetIndex()->ScanKey(*key, scanRet, nullptr);
    assert(scanRet.size() <= 1);
    select_count = scanRet.size();
    // select
    for (auto &rowId: scanRet){
      Row* row = new Row(rowId);
      table_info->GetTableHeap()->GetTuple(row, nullptr);
      vector<string> result_line;
      std::vector<Field *> fields = row->GetFields();
      if (if_select_all){
        for (size_t i = 0; i < fields.size(); i++) {
          result_line.push_back(fields[i]->ToString());
        }
      }else{
        for (auto &i : selectColumnIndexs){
          result_line.push_back(fields[i]->ToString());
        }
      }
      select_result.push_back(result_line);
    } 

  }else{

    // traverse the table
    TableIterator iter = table_info->GetTableHeap()->Begin(nullptr);
    // todo: not using table_info->GetTableHeap()->End() for its bug 
    for (; !iter.isNull(); iter++) {
      Row row = *iter;
      // 2. where // todo: not sure about kTrue
      if (!whereNode || GetResultOfNode(whereNode, row, table_schema) == kTrue) {
        // 3. select
        vector<string> result_line;
        std::vector<Field *> fields = row.GetFields();
        if (if_select_all){
          for (size_t i = 0; i < fields.size(); i++) {
            result_line.push_back(fields[i]->ToString());
          }
        }else{
          for (auto &i : selectColumnIndexs){
            result_line.push_back(fields[i]->ToString());
          }
        }
        select_result.push_back(result_line);
        select_count++;
      }
    }
  }

  // todo: make the result printing more pretty

  // print the first line (column names) (if result not empty)
  if (select_count){
    cout << "i\t";
    for (auto &columnName : selectColumns){
      cout << columnName << "\t";
    }
    cout << endl;
  }
  // print the results
  uint32_t temp_index = 0;
  for (auto &line : select_result){
    cout << temp_index << "\t";
    for (auto &field : line){
      cout << field << "\t";
    }
    cout << endl;
    temp_index++;
  }
  //记录时间
  long time_end = clock();    //计时结束
  cout << select_count << " rows in set (" << (double)(time_end - time_start)/CLOCKS_PER_SEC  << " sec)" << endl;
  
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  // 1. get table name
  pSyntaxNode tableNode = ast->child_; // table name
  string tableName = tableNode->val_;
  long time_start = clock();

  // 2. get TableSchema and TableHeap
  TableInfo *table_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTable(tableName, table_info);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Error: Table not exist." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  TableSchema *table_schema = table_info->GetSchema();

  // 3. convert values to a Row
  pSyntaxNode valuesNode = tableNode->next_; // values
  vector<pSyntaxNode> childs = GetChilds(valuesNode); 
  // ensure the same size of values & columns
  if (childs.size() != table_schema->GetColumnCount()) {
    cout << "Error: Column number not match." << endl;
    return DB_FAILED;
  }
  vector<Column *> columns = table_schema->GetColumns();
  vector<Field> fields;
  for (uint32_t i = 0; i < childs.size(); i++) {
    if (childs[i]->type_ == kNodeNull){
      if ( !columns[i]->IsNullable() ) {
        cout << "Error: Null value is not allowed for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      fields.push_back(Field(columns[i]->GetType()));
    }else if (columns[i]->GetType() == kTypeInt) {
      if (childs[i]->type_ != kNodeNumber || string(childs[i]->val_).find(".") != string::npos) {
        // error if type not match / has float point "."
        cout << "Error: Wrong type, expected int value for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      int32_t value = atoi(childs[i]->val_); 
      fields.push_back(Field(kTypeInt, value));
    }else if (columns[i]->GetType() == kTypeFloat) {
      if (childs[i]->type_ != kNodeNumber) {
        // error if type not match
        cout << "Error: Wrong type, expected float value for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      float value = atof(childs[i]->val_);
      fields.push_back(Field(kTypeFloat, value));
    }else if(columns[i]->GetType() == kTypeChar) {
      if (childs[i]->type_ != kNodeString) {
        // error if type not match
        cout << "Error: Wrong type, expected string value for " << columns[i]->GetName() << "." << endl;
        return DB_FAILED;
      }
      // LOG(INFO) << strlen(childs[i]->val_) <<endl; // for test
      if (strlen(childs[i]->val_) > columns[i]->GetLength()) {
        // error if too long
        cout << "Error: The string is too long for " << columns[i]->GetName() << "." << endl;
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
  auto cat = dbs_[current_db_]->catalog_mgr_;
  ret = cat->Insert(table_info, row, nullptr);
  if (ret == DB_PK_DUPLICATE){
    cout << "Error: Primary key duplicate." << endl;
    return DB_FAILED;
  }else if (ret == DB_UNI_KEY_DUPLICATE){
    cout << "Error: Unique key duplicate." << endl;
    return DB_FAILED;
  }else if (ret == DB_TUPLE_TOO_LARGE){
    cout << "Error: Tuple too large." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  long time_end = clock();
  cout << "Query OK, 1 row affected (" << (double)(time_end - time_start)/CLOCKS_PER_SEC << " sec)" << endl;
  return DB_SUCCESS;
}

//dxp   //todo：需要维护index
dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  // ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  // table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
  pSyntaxNode fromNode = ast->child_; //things after 'from', like 't1'
  pSyntaxNode whereNode = ast->child_->next_; //things after 'where', like 'name = a' (may be null)
  
  long time_start = clock();
  // 1. from
  string fromTable = fromNode->val_; // from table name
  TableInfo *table_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTable(fromTable, table_info);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Error: Table not exist." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  TableSchema *table_schema = table_info->GetSchema();

  auto cat = dbs_[current_db_]->catalog_mgr_;
  // traverse the table
  TableIterator iter = table_info->GetTableHeap()->Begin(nullptr);
  int delete_count = 0;
  for (; !iter.isNull(); iter++) { 
    Row row = *iter;
    // 2. where // todo: not sure about kTrue
    if (!whereNode || GetResultOfNode(whereNode, row, table_schema) == kTrue) {
      dberr_t ret = cat->Delete(table_info, row, nullptr);
      if (ret == DB_FAILED){
        cout << "Error: Delete failed." << endl;
        return DB_FAILED;
      }
      assert(ret == DB_SUCCESS);
      delete_count++;
    }
  }
  long time_end = clock();
  cout << "Query OK, "<<delete_count<<" row deleted (" << (double)(time_end - time_start)/CLOCKS_PER_SEC << " sec)" << endl;
  return DB_SUCCESS;
}

//dxp //todo：需要维护index 以及  lack of unique and primary key constraint 
dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  pSyntaxNode UpdateNode = ast->child_; //things after 'from', like 't1'
  pSyntaxNode SetNode = UpdateNode->next_;////things after 'set', like 'age = 18'
  pSyntaxNode whereNode = SetNode->next_; //things after 'where', like 'name = a' (may be null)

  long time_start = clock();
  // 1. from
  string Table_name = UpdateNode->val_; // from table name
  TableInfo *table_info = nullptr;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTable(Table_name, table_info);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Error: Table not exist." << endl;
    return DB_FAILED;
  }
  assert(ret == DB_SUCCESS);
  TableSchema *table_schema = table_info->GetSchema();
  TableHeap *table_heap = table_info->GetTableHeap();

  vector<pSyntaxNode> childs = GetChilds(SetNode); //the things need to modify(parent node)
  vector<Column *> columns = table_schema->GetColumns();

  // traverse the table
  TableIterator iter = table_info->GetTableHeap()->Begin(nullptr);
  int update_count = 0;

  for (; !iter.isNull(); iter++) {
    Row row = *iter;    //old row
    // 满足where条件
    if (!whereNode || GetResultOfNode(whereNode, row, table_schema) == kTrue) {
      auto row_id = row.GetRowId();
      std::vector<Field *> &fields = row.GetFields();   //old fields

      //create a new row
      vector<Field> temp_fields;    //new fields
      for (uint32_t i = 0; i < table_schema->GetColumnCount(); i++) {
        int renew = 0;
        int find_location = -1;
        //判断是否该属性需要被更新
        for(size_t j = 0;j<childs.size();j++){
          if(columns[i]->GetName()==childs[j]->child_->val_){
            find_location = j;
            renew = 1;    //在语法树中找到，需要更新
            break;
          }
        }
        //未从语法树找到，使用原有
        if(renew == 0){
          temp_fields.push_back(*fields[i]);
          continue;
        }
        //需要从语法树获取
        //int
        if (columns[i]->GetType() == kTypeInt) {
          if (childs[find_location]->child_->next_->type_ != kNodeNumber || string(childs[find_location]->child_->next_->val_).find(".") != string::npos) {
            // error if type not match / has float point "."
            cout << "Error: Wrong type, expected int value for " << columns[i]->GetName() << "." << endl;
            return DB_FAILED;
          }
          int32_t value = atoi(childs[find_location]->child_->next_->val_); 
          temp_fields.push_back(Field(kTypeInt, value));
        }
        //float
        else if (columns[i]->GetType() == kTypeFloat) {
          if (childs[find_location]->child_->next_->type_ != kNodeNumber) {
            // error if type not match
            cout << "Error: Wrong type, expected float value for " << columns[i]->GetName() << "." << endl;
            return DB_FAILED;
          }
          float value = atof(childs[find_location]->child_->next_->val_);
          temp_fields.push_back(Field(kTypeFloat, value));
        }
        //string
        else if(columns[i]->GetType() == kTypeChar) {
          if (childs[find_location]->child_->next_->type_ != kNodeString) {
            // error if type not match
            cout << "Error: Wrong type, expected string value for " << columns[i]->GetName() << "." << endl;
            return DB_FAILED;
          }
          // LOG(INFO) << strlen(childs[i]->val_) <<endl; // for test
          if (strlen(childs[find_location]->child_->next_->val_) > columns[i]->GetLength()) {
            // error if too long
            cout << "Error: The string is too long for " << columns[i]->GetName() << "." << endl;
            cout << "Error: The string is " << strlen(childs[find_location]->child_->next_->val_) << " characters long, but the column's max length is " \
              << columns[i]->GetLength() << " characters." << endl;
            return DB_FAILED;
          }
          // todo: not sure about what "manage_data" means, currently set to true
          temp_fields.push_back(Field(kTypeChar, childs[find_location]->child_->next_->val_, strlen(childs[find_location]->child_->next_->val_), true));
        }
        else{
          LOG(ERROR) << "Unsupported type." << endl;
          return DB_FAILED;
        }
      }
      Row new_row(temp_fields);
      bool ret_bool = table_heap->UpdateTuple(new_row,row_id,nullptr);
      if (ret_bool == false){
        cout << "Error: Update failed." << endl;
        return DB_FAILED;
      }
      update_count++;
    }
  }
  long time_end = clock();
  cout << "Query OK, "<<update_count<<" row updated (" << (double)(time_end - time_start)/CLOCKS_PER_SEC << " sec)" << endl;
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
  string fileName = ast->child_->val_;
  std::fstream cmdIn(fileName, std::ios::in); // get command from file
  const int buf_size = 1024;
  char cmd[buf_size];
  // repeat until EOF
  while (cmdIn.getline(cmd, buf_size)) {
    // todo: support multi-line command
    // skip empty line
    if (cmd[0] == '\0') {
      continue;
    }
    // support comments start with "--"
    if (cmd[0] == '-') {
      cout << "\n[COMMENT] " << cmd << endl;
      continue;
    }
    cout << "\n[CMD] " << cmd << endl;
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    // init parser module
    MinisqlParserInit();
    // parse
    yyparse();
    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }
    this->Execute(MinisqlGetParserRootNode(), context);
    // sleep(1); // probably not needed
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    // reset cmd
    memset(cmd, 0, buf_size);
    // quit condition
    if (context->flag_quit_) {
      break;
    }
  }
  cmdIn.close();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
