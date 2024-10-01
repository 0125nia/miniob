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
// Created by Wangyunlai on 2023/01/11.
//

namespace common {

// 线程池中push任务简单实现
template <typename T>
int SimpleQueue<T>::push(T &&value)
{
  // RALL 模式锁
  // PS：一旦上锁无需手动解除，在当前作用域执行完后自动放开锁
  // 这个作用域在此处是当前函数 当当前函数执行完 锁会自动释放
  // 当这种锁的颗粒度大的时候 就会锁住很多资源（并且无法手动解锁） 慎用
  lock_guard<mutex> lock(mutex_);
  queue_.push(std::move(value));
  return 0;
}

template <typename T>
int SimpleQueue<T>::pop(T &value)
{
  lock_guard<mutex> lock(mutex_);
  if (queue_.empty()) {
    return -1;
  }

  value = std::move(queue_.front());
  queue_.pop();
  return 0;
}

template <typename T>
int SimpleQueue<T>::size() const
{
  return queue_.size();
}

} // namespace common