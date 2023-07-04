//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      //从索引信息得到索引树
      //计划中索引对象的类型始终为 BPlusTreeIndexForOneIntegerColumn，安全地将其转换并存储在执行器对象中
      //索引树的迭代器
      //从索引对象tree_ 构造索引迭代器iter
      iter_{plan_->filter_predicate_ != nullptr ? BPlusTreeIndexIteratorForOneIntegerColumn(nullptr, nullptr)
                                                : tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {
  if (plan_->filter_predicate_ != nullptr) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockTable(
            exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
        if (!is_locked) {
          throw ExecutionException("IndexScan Executor Get Table Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("IndexScan Executor Get Table Lock Failed" + e.GetInfo());
      }
    }
    const auto *right_expr =
        dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Value v = right_expr->val_;
    tree_->ScanKey(Tuple{{v}, index_info_->index_->GetKeySchema()}, &rids_, exec_ctx_->GetTransaction());
    rid_iter_ = rids_.begin();
  }
}
//先获得索引的迭代器iter_ ->然后从迭代器iter_拿出rid（解引用）-> 通过rid，在表堆中拿到对应的tuple元数据
//按索引的顺序 返回tuple和rid
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->filter_predicate_ != nullptr) {
    if (rid_iter_ != rids_.end()) {
      *rid = *rid_iter_;//解引用，拿到pair<key,value> 的rid
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                                LockManager::LockMode::SHARED, table_info_->oid_, *rid);
          if (!is_locked) {
            throw ExecutionException("IndexScan Executor Get Table Lock Failed");
          }
        } catch (TransactionAbortException e) {
          throw ExecutionException("IndexScan Executor Get Row Lock Failed");
        }
      }

      auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
      rid_iter_++;
      return result;
    }
    return false;
  }
  if (iter_ == tree_->GetEndIterator()) {
    return false;
  }
  //IndexScanExecutor 迭代索引iter_以找到元组的 RID，然后，用RID 在相应的表中找它们的元组tuple。
  //最后，它逐个发出这些元组。
  *rid = (*iter_).second;//迭代器解引用，就是pair<key,value> 类型 拿到rid
  //拿到rid，去堆表里面查tuple  table_是table_info_的一个指向堆表的指针
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;
  return result;
}

}  // namespace bustub
//从索引对象 构造索引迭代器iter，扫描所有键和元组 ID，从表堆中查找元组，并按照索引键的顺序发出所有元组作为执行器的输出。
//BusTub 仅支持具有单个唯一整数列的索引。测试用例中不会有重复的键。
