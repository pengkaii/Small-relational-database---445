//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//可以使用 DELETE 语句来规划 DeletePlanNode。它只有一个子节点==要从表中删除的记录。
//您的删除执行器应该产生一个整数输出，表示从表中删除的行数，它还需要更新索引
// DeleteExecutor 始终位于其所在查询计划的根部。DeleteExecutor 不应修改其结果集。
//只需要从子执行器获取 RID 并调用 TableHeap::MarkDelete() 来有效地删除元组。所有删除操作将在事务提交时应用。
//需要更新从其中删除元组的表的所有索引。
//只删一次，但是Next()函数可以多次调用。
//目标：从表中删掉几个tuple
//tuple怎么获取？----> 子节点Next()函数
//要对哪个表？------>TableInfo里
//表相关联的索引怎么找？------> 系统表中有这些信息
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}
 //并发查询执行
 //表 IX  Init()
 //行 X   Next()
void DeleteExecutor::Init() {
  child_executor_->Init();
  //表 IX  Init()
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);//表索引数组
}
//删除在表数组的tuple元数据
//Next() 返回一个代表修改行数的 tuple，
//Next() 只会返回一个包含一个 integer value 的 tuple，表示 table 中有多少行受到了影响
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_delete_tuple{};
  RID emit_rid;
  int32_t delete_count = 0;
  //只有一个子节点，表示要从表中删除的记录。您的删除执行器应该产生一个整数输出，表示从表中删除的行数。需要更新索引
  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    //行 X   Next()
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, emit_rid);
      if (!is_locked) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }

    bool deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());

    if (deleted) {
        //删除后，更新相关联的索引     在表索引数组里面一个个遍历，更新索引的DeleteEntry（相关联的key,*rid,事务）
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_delete_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->DeleteEntry(to_delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      delete_count++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};//tuple的count删除行数从子执行器next()得到
  is_end_ = true;
  return true;
}

}  // namespace bustub
