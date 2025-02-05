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
// Created by Longda on 2010
//

#include "common/os/signal.h"
#include "common/log/log.h"
#include "pthread.h"
#include "common/lang/iostream.h"

namespace common {

void set_signal_handler(int sig, sighandler_t func)
{
  struct sigaction newsa, oldsa;
  sigemptyset(&newsa.sa_mask);
  newsa.sa_flags   = 0;
  newsa.sa_handler = func;

  int rc = sigaction(sig, &newsa, &oldsa);
  if (rc) {
    cerr << "Failed to set signal " << sig << SYS_OUTPUT_FILE_POS << SYS_OUTPUT_ERROR << endl;
  }
}

/*
** Set Singal handling Fucntion
*/
  /* 
  SIGQUIT：通常在用户按下 Ctrl+\ 时触发，通常用于终止进程。
  SIGINT：通常在用户按下 Ctrl+C 时触发，用于中断程序的运行。
  SIGHUP：挂起信号，通常用于重新加载配置文件或重启服务。
  SIGTERM：用于请求程序终止，通常用于优雅地关闭进程。
  */
void set_signal_handler(sighandler_t func)
{
  set_signal_handler(SIGQUIT, func);
  set_signal_handler(SIGINT, func);
  set_signal_handler(SIGHUP, func);
  set_signal_handler(SIGTERM, func);
  signal(SIGPIPE, SIG_IGN);
}

void block_default_signals(sigset_t *signal_set, sigset_t *old_set)
{
  sigemptyset(signal_set);
#ifndef DEBUG
  // SIGINT will effect our gdb debugging
  sigaddset(signal_set, SIGINT);
#endif
  sigaddset(signal_set, SIGTERM);
  sigaddset(signal_set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, signal_set, old_set);
}

void unblock_default_signals(sigset_t *signal_set, sigset_t *old_set)
{
  sigemptyset(signal_set);
#ifndef DEBUG
  sigaddset(signal_set, SIGINT);
#endif
  sigaddset(signal_set, SIGTERM);
  sigaddset(signal_set, SIGUSR1);
  pthread_sigmask(SIG_UNBLOCK, signal_set, old_set);
}

void *wait_for_signals(void *args)
{
  LOG_INFO("Start thread to wait signals.");
  sigset_t *signal_set = (sigset_t *)args;
  int       sig_number = -1;
  while (true) {
    errno   = 0;
    int ret = sigwait(signal_set, &sig_number);
    LOG_INFO("sigwait return value: %d, %d \n", ret, sig_number);
    if (ret != 0) {
      LOG_ERROR("error (%d) %s\n", errno, strerror(errno));
    }
  }
  return NULL;
}

void start_wait_for_signals(sigset_t *signal_set)
{
  pthread_t      pThread;
  pthread_attr_t pThreadAttrs;

  // create all threads as detached.  We will not try to join them.
  pthread_attr_init(&pThreadAttrs);
  pthread_attr_setdetachstate(&pThreadAttrs, PTHREAD_CREATE_DETACHED);

  pthread_create(&pThread, &pThreadAttrs, wait_for_signals, (void *)signal_set);
}
}  // namespace common
