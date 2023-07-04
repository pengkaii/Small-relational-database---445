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
      //��������Ϣ�õ�������
      //�ƻ����������������ʼ��Ϊ BPlusTreeIndexForOneIntegerColumn����ȫ�ؽ���ת�����洢��ִ����������
      //�������ĵ�����
      //����������tree_ ��������������iter
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
//�Ȼ�������ĵ�����iter_ ->Ȼ��ӵ�����iter_�ó�rid�������ã�-> ͨ��rid���ڱ�����õ���Ӧ��tupleԪ����
//��������˳�� ����tuple��rid
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->filter_predicate_ != nullptr) {
    if (rid_iter_ != rids_.end()) {
      *rid = *rid_iter_;//�����ã��õ�pair<key,value> ��rid
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
  //IndexScanExecutor ��������iter_���ҵ�Ԫ��� RID��Ȼ����RID ����Ӧ�ı��������ǵ�Ԫ��tuple��
  //��������������ЩԪ�顣
  *rid = (*iter_).second;//�����������ã�����pair<key,value> ���� �õ�rid
  //�õ�rid��ȥ�ѱ������tuple  table_��table_info_��һ��ָ��ѱ��ָ��
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;
  return result;
}

}  // namespace bustub
//���������� ��������������iter��ɨ�����м���Ԫ�� ID���ӱ���в���Ԫ�飬��������������˳�򷢳�����Ԫ����Ϊִ�����������
//BusTub ��֧�־��е���Ψһ�����е����������������в������ظ��ļ���
