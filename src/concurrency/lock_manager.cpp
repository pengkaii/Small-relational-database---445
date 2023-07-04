//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  //隔离级别：读未提交  
  //当前锁模式 永远不允许有S，IS，SIX锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);//设置事务中断
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    //shrinking阶段不可以  在 GROWING 状态下允许加 X、IX 锁
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //读已提交  在 SHRINKING 状态下仅允许使用 IS、S 锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //可重复读 在 SHRINKING 状态下不允许使用任何锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //单纯获取一个map:table_oid --> request
  //  上表锁
  table_lock_map_latch_.lock();
  //oid 没有队列，自己建立一个
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  //找到oid 对应的请求队列
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();//先把请求队列锁了
  table_lock_map_latch_.unlock();
  
  //遍历请求队列
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
  //旧事务去循环这个队列，看队列有没有事务id跟当前请求的事务id相等
  //判断新旧事务
    if (request->txn_id_ == txn->GetTransactionId()) {
      //这个请求的granted一定为true,因为如果事务之前的请求没有被通过，事务会被阻塞在lockmanager中，不可能再去获取一把锁
      //锁模式相同，直接返回
      if (request->lock_mode_ == lock_mode) {//请求队列里面的锁模式和当前的锁模式相同 不能重复
        lock_request_queue->latch_.unlock();//直接返回，而且已经有这把锁了
        return true;
      }
      //改变原有队列有锁正在升级，升级冲突
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {//请求队列资源只能有一个在锁升级，是这个表的请
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);//当前事务中断
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      //历史请求request是意向锁T1，则当前T2的lock_mode必须能满足   
      //锁升级条件判断
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        //当前 不能满足锁升级条件
        //中断事务，抛出异常
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //锁升级满足，先把历史旧事务请求request 从队列中移除
      lock_request_queue->request_queue_.remove(request);
      //是已授权的事务request，所以要从  事务的 表锁集合  中删除，第三个参数是false表示删除
      InsertOrDeleteTableLockSet(txn, request, false);
      //创建一个新加锁  请求节点记录
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      //找到第一个 未加锁的位置，锁升级优先级当前最高
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      //将 新的请求节点记录   重新插入 第一个未分配的位置，使用条件变量模型等待  锁授权
      //将  请求记录  插入到该位置
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      //标记资源为正在 锁升级
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      

      //互斥锁与条件变量集合用
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      //条件变量等待模型  给请求记录授予锁，没有的话下一步wait()阻塞等待
      //事务处于等待锁的状态
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);//唤醒  cv_条件变量
        
        //检查事务的状态是否为终止、 （如果事务因为某些原因（如死锁、超时等）被中止，那么获取锁的尝试应该停止）
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;//终止，升级失败
          lock_request_queue->request_queue_.remove(upgrade_lock_request);//从队列中移除请求记录
          lock_request_queue->cv_.notify_all();//通知其他线程
          //唤醒所有等待该条件变量（条件变量指示锁可用）的其他事务线程，让他们知道当前事务已经被中止，可以尝试获取锁
          return false;
        }
        //检查事务是否已经被中止。如果是，那么就需要终止锁升级的过程，并通知其他事务可以尝试获取锁。
        //这样做可以避免无效事务持续等待锁，提高系统的并发性能。
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;// 升级值为无效值 意味着 当前事务记录 成功授予锁
      upgrade_lock_request->granted_ = true;//授锁标志
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);//加入表锁集合（事务管理的锁）

      if (lock_mode != LockMode::EXCLUSIVE) {//只要当前T2锁模式 不是排他锁，事情办完，就可以通知其他事务线程尝试获取锁（当前的完成了）
        lock_request_queue->cv_.notify_all();
      }
      return true;//返回成功
    }
  }
  //如果lock table 中没有重复的txn_id 直接加到请求队列末尾，等待锁授权
  //新事务
  //新的锁请求节点，等待授予锁
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);//新的锁请求 记录
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;//成功授权
  //处理表的只关注表  加入事务管理 的表锁集合
  InsertOrDeleteTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  //1、map中不存在对应的表id
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  
  //2、行锁S/X未清空，不能解表锁
  //1和2 处理的是能不能解锁
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue = table_lock_map_[oid];

  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  //3、找到请求队列queue的请求节点尝试解锁
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
  //历史请求队列的事务id 和当前的事务id
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      //从请求队列中解除
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();//通知其他的线程继续做加解锁操作
      lock_request_queue->latch_.unlock();
      //事务状态变更，根据说明文档来实现即可
      //可重复读：释放S或X锁都会进入shrinking状态
      //读提交和读未提交：在释放X锁后进入shrinking状态
      //不同的隔离级别去变更事务的状态
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
            //事务的当前状态不是提交，中止状态
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);//收缩状态
        }
      }
      //从事务的表锁集合中删除
      InsertOrDeleteTableLockSet(txn, lock_request, false);
      return true;
    }
  }
  //请求队列找不到事务id就抛出一个异常
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  //当前锁模式 只允许加S|X锁，意向锁统统不允许加
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  
  //隔离级别判断
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //加锁的时候要判断表的锁状态
  //区别点：加行锁X时，要判断表锁X存不存在
  if (lock_mode == LockMode::EXCLUSIVE) { //表X IX SIX不存在，抛出异常
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  //行锁 用rid
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  //找到请求队列
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
  //旧事务
  //到对应的请求队列加锁
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
            //升级不兼容
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //当前的锁 满足升级条件  锁升级
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);//表锁里面删除
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      
      //找第一个没加锁的位置
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      //插入行的 锁集合
      InsertOrDeleteRowLockSet(txn, upgrade_lock_request, true);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  //新事务
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  //等待模型里面 判断前面的事务兼容以后，当前记录 授予到锁
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);//加入到行锁集合里面

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  //哈希表加锁，不在哈希表里面，无效的解锁
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];

  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  
  
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      //事务状态变更
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      InsertOrDeleteRowLockSet(txn, lock_request, false);
      return true;
    }
  }
  //请求队列没有该锁，抛出事务异常
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  //加入图中的事务集合
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  //执行被依赖的对象，t1依赖t2
  //等待图
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    //找到t1 的依赖项t2的位置迭代器
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
    //是否有环
  for (auto const &start_txn_id : txn_set_) {//遍历所有集合，遍历图中的所有的顶点
    if (Dfs(start_txn_id)) {//搜到环的话
      *txn_id = *active_set_.begin();//活跃集合的事务id   
      for (auto const &active_txn_id : active_set_) {
        *txn_id = std::max(*txn_id, active_txn_id);//有环时中止最年轻的事务，也就是编号ID最大的事务，因为事务ID是递增的。
      }
      active_set_.clear();
      return true;
    }

    active_set_.clear();
  }
  //当遍历完所有加入图中的事务之后，才能断定，没有环
  return false;
}

auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
    //这是把出边删掉了，txn_id = 2
    //2 ---> 3,9
  waits_for_.erase(txn_id);//通过图删掉 出边

  for (auto a_txn_id : txn_set_) {
    //把入边删掉
    //1 --->2,3,5,9   这样把2干掉  1 --->,3,5,9 
    if (a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}
//得到边 的集合
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  //遍历一下这个图（哈希表） key-value   等待图
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);//t1 指向 t2的所有边都加入到result的集合里，最后返回的是获取所有边
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {//true 定期做一个后台检测
    std::this_thread::sleep_for(cycle_detection_interval);//定的时间
    //每次sleep苏醒之后，动态构建一个图
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      //行和表的map都要操作，因此都要加锁
      //1、动态建图
      //1.1 、针对表上的事务
      for (auto &pair : table_lock_map_) {//表的哈希表 key是表的ID,value是请求队列
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();//请求队列加锁
        //一个事务，同一时刻只可能在一个表上请求锁
        //table001 -- T100 T200 T300
        //table002 -- T110 T300 T310
        //遍历请求队列的锁请求
        for (auto const &lock_request : pair.second->request_queue_) {
        //授过锁的事务，else是正在请求的事务
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {//没有授权的，需要加锁，等待加锁
          //该变量的作用是：当等待的事务被终止时，要能找到对应的请求队列通知其他阻塞的事务
            for (auto txn_id : granted_set) {
                //集合：没有被授权的事务集合
              map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
              //没有被授权锁，它在死锁检测的过程中，判环的过程中被终止掉，需要map_txn_oid_记录一下
              //存的哪一个事务，对应哪一个资源
              //如果当前请求没有被授权，则添加对授权事务的依赖
              AddEdge(lock_request->txn_id_, txn_id);
              //当前正在等待加锁的事务跟前面已经授权的所有事务建立依赖边
            }
          }
        }
        pair.second->latch_.unlock();
      }
      //1.2 针对行上的事务
      for (auto &pair : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for (auto const &lock_request : pair.second->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
                //没授权的，用行哈希表记录事务ID，对应的资源
              map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock();
      }
     
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();
      //每次sleep苏醒之后，动态构建一个图
      txn_id_t txn_id;
      //2. 判环，注意txn_id是个入参，当有环时，返回应该终止的事务编号
      while (HasCycle(&txn_id)) {
        //如果有环，需要一直处理到没有环为止
        Transaction *txn = TransactionManager::GetTransaction(txn_id);//返回的是有环的 编号最大的事务
        txn->SetState(TransactionState::ABORTED);//事务状态标记为中止
        DeleteNode(txn_id);//把这个边从图中删掉了，并且把这个事务标记为终止了  从图中删除出边和入边
        
        //如果中止的事务ID是无锁的，则唤醒其他阻塞线程
        //授锁权过的事务中止不影响阻塞的线程
        if (map_txn_oid_.count(txn_id) > 0) {//没授锁权的事务集合  == 等待事务（通知其他线程）
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.unlock();
        }

        if (map_txn_rid_.count(txn_id) > 0) {
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.unlock();
        }
      }
      //所有环都被处理后，清理集合
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

//给新的请求记录 授锁，没有的话下一步wait()阻塞等待
//授予锁的条件
//1、前面事务都加锁  2、前面事务都兼容
auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  //lr 前面事务组
  for (auto &lr : lock_request_queue->request_queue_) {
    //看锁兼容矩阵  lr为T1持有的锁队列   看与T2兼不兼容
    //lr->granted_ 前面事务组都授予锁了
    if (lr->granted_) {
      switch (lock_request->lock_mode_) {//T2 want
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}
//根据锁类型和操作类型，找到对应的集合进行插入和删除
void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert) {
  //插入行锁 只会加入S和X锁
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        InsertRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        InsertRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

}  // namespace bustub
