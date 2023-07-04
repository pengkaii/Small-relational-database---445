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
//���ݲ�ͬ���뼶���ṩ��ͬ��������
//��� IS  init()
//�м� S   
//��ͬ���뼶���������ʽ
//��δ�ύ����������Ҳ������
//���ύ�����������һ�ε���Nextʱ��ǰ�ͷ�
//���ظ������������ύ/��ֹʱ ��Transaction Manager ͳһ�ͷ�
#include "execution/executors/seq_scan_executor.h"
namespace bustub {
//�������һ������Ҫ֪�����ĵ�ǰ�������ġ�����������һ����������Ҫ������
//ִ�����������ģ��ƻ��ڵ㣨ÿִ��һ��SQL��ת��Ϊ�ܶ�ִ�нڵ㣩
//GetCatlog ϵͳ���������ݿ���������Щ��ģʽ��������Ϣ
//˳��ɨ��ִ�нڵ㣨GetType���ͣ�GetTableOid���ΨһID��
//ִ���������ģ�GetBuffPoolManager����ع�������GetCatalogϵͳ��GetTransaction ����
//GetLockManager����������GetLogManager��־��������GetTransactionManager ���������
//GetCatalogϵͳ�� ��ѯ��������ģʽ����ͼ��Ϣ����ִ������ѯCatalog::GetTable()���Catalog::GetIndex()����
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}
void SeqScanExecutor::Init() {
  //�� IS  init()  �ų��˶�δ�ύ������������ֻ�Զ����ύ�����ظ������б����  ����ļ����������������
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
      //���ύ�����������һ�ε���Nextʱ��ǰ�ͷ�(���ͷ����������ͷű���)
      //��������ĩβ����һ����������Ϊ��δ�ύû�м��������Բ���������뼶��ֻ���Ƕ��ύ�����ظ���
      //�����ظ���ֻ�����commit��ʱ����ͷ���������Ҫ�ֶ�ȥ���ƣ�ֻʣ�� ���ύ
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
    ++table_iter_;//ν�ʹ��� �жϵ�ǰԪ���Ƿ���Ϲ���ν��   ����Ԫ�飨tuple�������Ϣ�е�ģʽ��table_info_->schema_��
  } while (plan_->filter_predicate_ != nullptr && !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());
  //����һ��tuple��ǰ��ֻ�Ա����is������ȥ����һ���е�ʱ��Ӧ�ü�����  S��
  //���뼶���Ƕ�δ�ύ�����Ƕ��ύ�����ظ��� ������
  //��δ�ύ ������
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    //�� S   
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
