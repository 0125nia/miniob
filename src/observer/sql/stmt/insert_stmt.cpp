/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

// 创建对应所需的 表信息 字段信息（标识所需改变的字段） 字段真实值
InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

//   struct InsertSqlNode
// {
//   std::string        relation_name;  ///< Relation to insert into
//   std::vector<Value> values;         ///< 要插入的值
// };
// 创建 insert
RC InsertStmt::create(Db *db, const InsertSqlNode &inserts, Stmt *&stmt)
{
  // 前置表及表数据字段检查
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // 查看表信息是否存在
  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 查看fields信息(table元信息)和添加的目标信息结构是否匹配
  // check the fields number

  // 此处的inserts的数据类型则为InsertSqlNode

  const Value     *values     = inserts.values.data();
  // static_cast指的是隐式转换 进到size方法并且一直进可以看到 
  // 他所返回的size_type的数据类型本质上是无符号整数
  // 此处转为int 方便对字段数进行匹配比较
  const int        value_num  = static_cast<int>(inserts.values.size());
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // everything alright
  stmt = new InsertStmt(table, values, value_num);
  return RC::SUCCESS;
}
