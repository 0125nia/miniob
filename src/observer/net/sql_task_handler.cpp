/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2024/01/10.
//

#include "net/sql_task_handler.h"
#include "net/communicator.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"

// 处理事件的主函数
RC SqlTaskHandler::handle_event(Communicator *communicator)
{
  SessionEvent *event = nullptr;
  RC rc = communicator->read_event(event);
  // 从通信器中获取事件对象 在前置处理后放回SessionEvent对象中
  if (OB_FAIL(rc)) {
    return rc;
  }

  // 如果上方没有组装对象 这里是返回空值 说明此命令出了问题直接返回
  if (nullptr == event) {
    return RC::SUCCESS;
  }

  // c++中结构体的所有成员变量都会在创建的时候 自动分配空间并初始化
  // 所以可以在这里 直接调用handle_request2方法
  session_stage_.handle_request2(event);

  // 组装颗粒度更小的执行对象(责任链中的TaskContextData)
  SQLStageEvent sql_event(event, event->query());

  // 真实处理SQL逻辑
  rc = handle_sql(&sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to handle sql. rc=%s", strrc(rc));
    event->sql_result()->set_return_code(rc);
  }

  bool need_disconnect = false;

  // 通过Communicator对象(处理连接,读取事件,发起事件为主)将执行结果回调
  rc = communicator->write_result(event, need_disconnect);
  LOG_INFO("write result return %s", strrc(rc));
  // 回收事件信息 清空会话记录
  event->session()->set_current_request(nullptr);
  Session::set_current_session(nullptr);

  delete event;

  if (need_disconnect) {
    return RC::INTERNAL;
  }
  return RC::SUCCESS;
}

// 处理sql真实逻辑
// 分多个环节 解析器 优化器 查询缓存etc
RC SqlTaskHandler::handle_sql(SQLStageEvent *sql_event)
{
  RC rc = query_cache_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do query cache. rc=%s", strrc(rc));
    return rc;
  }

  // 语句经过解析器进行解析 辨识语法是否正确 获得对应所想要执行的操作
  rc = parse_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
    return rc;
  }

  rc = resolve_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do resolve. rc=%s", strrc(rc));
    return rc;
  }

  rc = optimize_stage_.handle_request(sql_event);
  if (rc != RC::UNIMPLEMENTED && rc != RC::SUCCESS) {
    LOG_TRACE("failed to do optimize. rc=%s", strrc(rc));
    return rc;
  }

  rc = execute_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do execute. rc=%s", strrc(rc));
    return rc;
  }

  return rc;
}