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
//����ʹ�� DELETE ������滮 DeletePlanNode����ֻ��һ���ӽڵ�==Ҫ�ӱ���ɾ���ļ�¼��
//����ɾ��ִ����Ӧ�ò���һ�������������ʾ�ӱ���ɾ����������������Ҫ��������
// DeleteExecutor ʼ��λ�������ڲ�ѯ�ƻ��ĸ�����DeleteExecutor ��Ӧ�޸���������
//ֻ��Ҫ����ִ������ȡ RID ������ TableHeap::MarkDelete() ����Ч��ɾ��Ԫ�顣����ɾ���������������ύʱӦ�á�
//��Ҫ���´�����ɾ��Ԫ��ı������������
//ֻɾһ�Σ�����Next()�������Զ�ε��á�
//Ŀ�꣺�ӱ���ɾ������tuple
//tuple��ô��ȡ��----> �ӽڵ�Next()����
//Ҫ���ĸ���------>TableInfo��
//���������������ô�ң�------> ϵͳ��������Щ��Ϣ
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}
 //������ѯִ��
 //�� IX  Init()
 //�� X   Next()
void DeleteExecutor::Init() {
  child_executor_->Init();
  //�� IX  Init()
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);//����������
}
//ɾ���ڱ������tupleԪ����
//Next() ����һ�������޸������� tuple��
//Next() ֻ�᷵��һ������һ�� integer value �� tuple����ʾ table ���ж������ܵ���Ӱ��
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_delete_tuple{};
  RID emit_rid;
  int32_t delete_count = 0;
  //ֻ��һ���ӽڵ㣬��ʾҪ�ӱ���ɾ���ļ�¼������ɾ��ִ����Ӧ�ò���һ�������������ʾ�ӱ���ɾ������������Ҫ��������
  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    //�� X   Next()
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
        //ɾ���󣬸��������������     �ڱ�������������һ��������������������DeleteEntry���������key,*rid,����
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
  *tuple = Tuple{values, &GetOutputSchema()};//tuple��countɾ����������ִ����next()�õ�
  is_end_ = true;
  return true;
}

}  // namespace bustub
