#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <set>

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

// todo: maybe output messages to ExecuteContext instead of cout
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

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  string tableName = ast->child_->val_;
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
  LOG(INFO) << "Create Table: " << tableName << std::endl;
#endif
  pSyntaxNode columnDefs = ast->child_->next_;
  vector<Column *> columns;
  set<string> columnNameSet;
  uint32_t columnIndex = 0;
  pSyntaxNode columnDef;
  for (columnDef = columnDefs; columnDef->type_ == kNodeColumnDefinition; columnDef = columnDef->next_) {
    bool isUnique = string(columnDef->val_) == "unique";
    bool isNullable = false; 
    // todo: support "nullable", test with the following query:
      // create table t1(a int, b char(-5) unique not null, c float, primary key(a, c));
      // create table t1(a int not null, b char(-5), c float, primary key(a, c));
      // create table t1(a int, b char(-5) unique, c float, primary key(a, c));
    string columnName = columnDef->child_->val_;
    string columnType = columnDef->child_->next_->val_;
    columnNameSet.insert(columnName);
    if (columnType == "int") {
      columns.push_back(new Column(columnName, TypeId::kTypeInt, columnIndex, isNullable, isUnique));
    } else if (columnType == "float") {
      columns.push_back(new Column(columnName, TypeId::kTypeInt, columnIndex, isNullable, isUnique));
    } else if (columnType == "char") {
      int length = stoi(columnDef->child_->next_->child_->val_);
      columns.push_back(new Column(columnName, TypeId::kTypeInt, length, columnIndex, isNullable, isUnique));
    // } else if (columnType == "varchar") { // todo: if varchar is supported
    //   int length = stoi(columnDef->child_->next_->child_->val_);
    //   columns.push_back(new Column(columnName, TypeId::KMaxTypeId, length, columnIndex, isNullable, isUnique));
    } else {
      cout << "Invalid column type: " << columnType << endl;
      return DB_FAILED; // todo: maybe roolback
    }
    columnIndex++;
  }
  pSyntaxNode columnList;
  for (columnList = columnDef; columnList->type_ == kNodeColumnList; columnList = columnList->next_) {
    if (string(columnList->val_) == "primary keys") {
      vector<string> primaryKeys;
      for (pSyntaxNode identifier = columnList->child_; identifier->type_ == kNodeIdentifier; identifier = identifier->next_) {
        // return failure if the key not exists (identifier->val_ is the keyName)
        if (columnNameSet.find(identifier->val_) == columnNameSet.end()) {
          cout << "Primary key " << identifier->val_ << " does not exist." << endl;
          // DB_KEY_NOT_FOUND;
          return DB_FAILED; // todo: maybe roolback
        }
        primaryKeys.push_back(identifier->val_);
      }
      if (primaryKeys.size() == 0) {
        cout << "Empty primary key list." << endl;
        return DB_FAILED; // todo: maybe roolback
      }
    }
    // todo: maybe support "foreign keys" and "check"
  }
  // ! todo: add primaryKeys to the table info or table meta or somewhere else
  dberr_t ret = DB_SUCCESS; //! todo: modify
  // TableInfo* tf = TableInfo::Create(new SimpleMemHeap()); // ! todo: mod
  // ret = dbs_[current_db_]->catalog_mgr_->CreateTable(tableName, new TableSchema(columns),
  //                                                 nullptr, /* TableInfo */ tf);
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
  // if(dbs_[current_db_]->catalog_mgr_->DropTable(tableName)==DB_SUCCESS){
  //   cout << "Table " << tableName << " dropped." << endl;
  //   return DB_SUCCESS;
  // }
  // else{
  //   cout << "Don't find " << tableName << "." << endl;
  //   return DB_TABLE_NOT_EXIST;
  // }
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
  vector<std::string> index_keys;
  while(temp_pointer){
    index_keys.push_back(temp_pointer->val_);
    temp_pointer = temp_pointer->next_;
  }
  //not sure ！！参数列表中的Transaction *txn,IndexInfo *&index_info 不知道怎么传 
  IndexInfo * nuknow;
  return dbs_[current_db_]->catalog_mgr_->CreateIndex(tableName,indexName,index_keys,nullptr,nuknow);
}

// 不知道语法，drop index 索引名；  与  drop index 索引名 on 表名；均无法生成语法树
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //not finished
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
  // needless to implement
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
  #endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
  // needless to implement
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
  #endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
  // needless to implement
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
  #endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
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
