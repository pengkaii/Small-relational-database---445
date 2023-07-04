//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//根据不同隔离级别，提供不同并发性能
//表加 IS  init()
//行加 S   
//不同隔离级别的锁处理方式
//读未提交：不加锁，也不解锁
//读提交：读锁在最后一次调用Next时提前释放
//可重复读：在事务提交/中止时 由Transaction Manager 统一释放
#include "execution/executors/seq_scan_executor.h"
namespace bustub {
//输入遍历一个表，需要知道它的当前遍历在哪、遍历的是哪一个表（两个重要遍历）
//执行器的上下文，计划节点（每执行一个SQL，转化为很多执行节点）
//GetCatlog 系统表，看到数据库里面有那些表，模式，索引信息
//顺序扫描执行节点（GetType类型，GetTableOid表的唯一ID）
//执行器上下文：GetBuffPoolManager缓冲池管理器，GetCatalog系统表，GetTransaction 事务
//GetLockManager锁管理器，GetLogManager日志管理器，GetTransactionManager 事务管理器
//GetCatalog系统表 查询表、索引、模式、视图信息，在执行器查询Catalog::GetTable()表和Catalog::GetIndex()索引
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}
void SeqScanExecutor::Init() {
  //表 IS  init()  排除了读未提交（不加锁），只对读已提交，可重复读进行表加锁  （表的级别加上意向共享锁）
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    if (table_iter_ == table_info_->table_->End()) {
      //读提交：读锁在最后一次调用Next时提前释放(先释放行锁，再释放表锁)
      //迭代器到末尾，做一个解锁，因为读未提交没有加锁，所以不管这个隔离级别，只考虑读提交，可重复读
      //而可重复读只在最后commit的时候才释放锁，不需要手动去控制，只剩下 读提交
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
        table_oid_t oid = table_info_->oid_;
        for (auto rid : locked_row_set) {
          exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid);
        }
        exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
      }  
      return false;
    }
    *tuple = *table_iter_;
    *rid = tuple->GetRid();
    ++table_iter_;//谓词过滤 判断当前元组是否符合过滤谓词   评估元组（tuple）与表信息中的模式（table_info_->schema_）
  } while (plan_->filter_predicate_ != nullptr && !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());
  //返回一个tuple，前面只对表加了is锁，当去操作一个行的时候，应该加行锁  S锁
  //隔离级别不是读未提交，而是读提交，可重复读 加行锁
  //读未提交 不加锁
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    //行 S   
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            table_info_->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }

  return true;
}

}  // namespace bustub
