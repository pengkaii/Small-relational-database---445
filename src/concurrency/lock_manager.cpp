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
  //���뼶�𣺶�δ�ύ  
  //��ǰ��ģʽ ��Զ��������S��IS��SIX��
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);//���������ж�
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    //shrinking�׶β�����  �� GROWING ״̬������� X��IX ��
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //�����ύ  �� SHRINKING ״̬�½�����ʹ�� IS��S ��
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //���ظ��� �� SHRINKING ״̬�²�����ʹ���κ���
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //������ȡһ��map:table_oid --> request
  //  �ϱ���
  table_lock_map_latch_.lock();
  //oid û�ж��У��Լ�����һ��
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  //�ҵ�oid ��Ӧ���������
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();//�Ȱ������������
  table_lock_map_latch_.unlock();
  
  //�����������
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
  //������ȥѭ��������У���������û������id����ǰ���������id���
  //�ж��¾�����
    if (request->txn_id_ == txn->GetTransactionId()) {
      //��������grantedһ��Ϊtrue,��Ϊ�������֮ǰ������û�б�ͨ��������ᱻ������lockmanager�У���������ȥ��ȡһ����
      //��ģʽ��ͬ��ֱ�ӷ���
      if (request->lock_mode_ == lock_mode) {//��������������ģʽ�͵�ǰ����ģʽ��ͬ �����ظ�
        lock_request_queue->latch_.unlock();//ֱ�ӷ��أ������Ѿ����������
        return true;
      }
      //�ı�ԭ�ж�����������������������ͻ
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {//���������Դֻ����һ����������������������
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);//��ǰ�����ж�
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      //��ʷ����request��������T1����ǰT2��lock_mode����������   
      //�����������ж�
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        //��ǰ ������������������
        //�ж������׳��쳣
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //���������㣬�Ȱ���ʷ����������request �Ӷ������Ƴ�
      lock_request_queue->request_queue_.remove(request);
      //������Ȩ������request������Ҫ��  ����� ��������  ��ɾ����������������false��ʾɾ��
      InsertOrDeleteTableLockSet(txn, request, false);
      //����һ���¼���  ����ڵ��¼
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      //�ҵ���һ�� δ������λ�ã����������ȼ���ǰ���
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      //�� �µ�����ڵ��¼   ���²��� ��һ��δ�����λ�ã�ʹ����������ģ�͵ȴ�  ����Ȩ
      //��  �����¼  ���뵽��λ��
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      //�����ԴΪ���� ������
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      

      //����������������������
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      //���������ȴ�ģ��  �������¼��������û�еĻ���һ��wait()�����ȴ�
      //�����ڵȴ�����״̬
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);//����  cv_��������
        
        //��������״̬�Ƿ�Ϊ��ֹ�� �����������ΪĳЩԭ������������ʱ�ȣ�����ֹ����ô��ȡ���ĳ���Ӧ��ֹͣ��
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;//��ֹ������ʧ��
          lock_request_queue->request_queue_.remove(upgrade_lock_request);//�Ӷ������Ƴ������¼
          lock_request_queue->cv_.notify_all();//֪ͨ�����߳�
          //�������еȴ���������������������ָʾ�����ã������������̣߳�������֪����ǰ�����Ѿ�����ֹ�����Գ��Ի�ȡ��
          return false;
        }
        //��������Ƿ��Ѿ�����ֹ������ǣ���ô����Ҫ��ֹ�������Ĺ��̣���֪ͨ����������Գ��Ի�ȡ����
        //���������Ա�����Ч��������ȴ��������ϵͳ�Ĳ������ܡ�
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;// ����ֵΪ��Чֵ ��ζ�� ��ǰ�����¼ �ɹ�������
      upgrade_lock_request->granted_ = true;//������־
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);//����������ϣ�������������

      if (lock_mode != LockMode::EXCLUSIVE) {//ֻҪ��ǰT2��ģʽ ������������������꣬�Ϳ���֪ͨ���������̳߳��Ի�ȡ������ǰ������ˣ�
        lock_request_queue->cv_.notify_all();
      }
      return true;//���سɹ�
    }
  }
  //���lock table ��û���ظ���txn_id ֱ�Ӽӵ��������ĩβ���ȴ�����Ȩ
  //������
  //�µ�������ڵ㣬�ȴ�������
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);//�µ������� ��¼
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

  lock_request->granted_ = true;//�ɹ���Ȩ
  //������ֻ��ע��  ����������� �ı�������
  InsertOrDeleteTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  //1��map�в����ڶ�Ӧ�ı�id
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  
  //2������S/Xδ��գ����ܽ����
  //1��2 ��������ܲ��ܽ���
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
  //3���ҵ��������queue������ڵ㳢�Խ���
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
  //��ʷ������е�����id �͵�ǰ������id
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      //����������н��
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();//֪ͨ�������̼߳������ӽ�������
      lock_request_queue->latch_.unlock();
      //����״̬���������˵���ĵ���ʵ�ּ���
      //���ظ������ͷ�S��X���������shrinking״̬
      //���ύ�Ͷ�δ�ύ�����ͷ�X�������shrinking״̬
      //��ͬ�ĸ��뼶��ȥ��������״̬
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
            //����ĵ�ǰ״̬�����ύ����ֹ״̬
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);//����״̬
        }
      }
      //������ı���������ɾ��
      InsertOrDeleteTableLockSet(txn, lock_request, false);
      return true;
    }
  }
  //��������Ҳ�������id���׳�һ���쳣
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  //��ǰ��ģʽ ֻ�����S|X����������ͳͳ�������
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  
  //���뼶���ж�
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
  //������ʱ��Ҫ�жϱ����״̬
  //����㣺������Xʱ��Ҫ�жϱ���X�治����
  if (lock_mode == LockMode::EXCLUSIVE) { //��X IX SIX�����ڣ��׳��쳣
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  //���� ��rid
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  //�ҵ��������
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
  //������
  //����Ӧ��������м���
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
            //����������
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //��ǰ���� ������������  ������
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);//��������ɾ��
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      
      //�ҵ�һ��û������λ��
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
      //�����е� ������
      InsertOrDeleteRowLockSet(txn, upgrade_lock_request, true);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  //������
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  //�ȴ�ģ������ �ж�ǰ�����������Ժ󣬵�ǰ��¼ ���赽��
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);//���뵽������������

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  //��ϣ����������ڹ�ϣ�����棬��Ч�Ľ���
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
      //����״̬���
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
  //�������û�и������׳������쳣
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  //����ͼ�е����񼯺�
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  //ִ�б������Ķ���t1����t2
  //�ȴ�ͼ
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    //�ҵ�t1 ��������t2��λ�õ�����
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
    //�Ƿ��л�
  for (auto const &start_txn_id : txn_set_) {//�������м��ϣ�����ͼ�е����еĶ���
    if (Dfs(start_txn_id)) {//�ѵ����Ļ�
      *txn_id = *active_set_.begin();//��Ծ���ϵ�����id   
      for (auto const &active_txn_id : active_set_) {
        *txn_id = std::max(*txn_id, active_txn_id);//�л�ʱ��ֹ�����������Ҳ���Ǳ��ID����������Ϊ����ID�ǵ����ġ�
      }
      active_set_.clear();
      return true;
    }

    active_set_.clear();
  }
  //�����������м���ͼ�е�����֮�󣬲��ܶ϶���û�л�
  return false;
}

auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
    //���ǰѳ���ɾ���ˣ�txn_id = 2
    //2 ---> 3,9
  waits_for_.erase(txn_id);//ͨ��ͼɾ�� ����

  for (auto a_txn_id : txn_set_) {
    //�����ɾ��
    //1 --->2,3,5,9   ������2�ɵ�  1 --->,3,5,9 
    if (a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}
//�õ��� �ļ���
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  //����һ�����ͼ����ϣ�� key-value   �ȴ�ͼ
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);//t1 ָ�� t2�����б߶����뵽result�ļ������󷵻ص��ǻ�ȡ���б�
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {//true ������һ����̨���
    std::this_thread::sleep_for(cycle_detection_interval);//����ʱ��
    //ÿ��sleep����֮�󣬶�̬����һ��ͼ
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      //�кͱ��map��Ҫ��������˶�Ҫ����
      //1����̬��ͼ
      //1.1 ����Ա��ϵ�����
      for (auto &pair : table_lock_map_) {//��Ĺ�ϣ�� key�Ǳ��ID,value���������
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();//������м���
        //һ������ͬһʱ��ֻ������һ������������
        //table001 -- T100 T200 T300
        //table002 -- T110 T300 T310
        //����������е�������
        for (auto const &lock_request : pair.second->request_queue_) {
        //�ڹ���������else���������������
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {//û����Ȩ�ģ���Ҫ�������ȴ�����
          //�ñ����������ǣ����ȴ���������ֹʱ��Ҫ���ҵ���Ӧ���������֪ͨ��������������
            for (auto txn_id : granted_set) {
                //���ϣ�û�б���Ȩ�����񼯺�
              map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
              //û�б���Ȩ���������������Ĺ����У��л��Ĺ����б���ֹ������Ҫmap_txn_oid_��¼һ��
              //�����һ�����񣬶�Ӧ��һ����Դ
              //�����ǰ����û�б���Ȩ������Ӷ���Ȩ���������
              AddEdge(lock_request->txn_id_, txn_id);
              //��ǰ���ڵȴ������������ǰ���Ѿ���Ȩ������������������
            }
          }
        }
        pair.second->latch_.unlock();
      }
      //1.2 ������ϵ�����
      for (auto &pair : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for (auto const &lock_request : pair.second->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
                //û��Ȩ�ģ����й�ϣ���¼����ID����Ӧ����Դ
              map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock();
      }
     
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();
      //ÿ��sleep����֮�󣬶�̬����һ��ͼ
      txn_id_t txn_id;
      //2. �л���ע��txn_id�Ǹ���Σ����л�ʱ������Ӧ����ֹ��������
      while (HasCycle(&txn_id)) {
        //����л�����Ҫһֱ����û�л�Ϊֹ
        Transaction *txn = TransactionManager::GetTransaction(txn_id);//���ص����л��� �����������
        txn->SetState(TransactionState::ABORTED);//����״̬���Ϊ��ֹ
        DeleteNode(txn_id);//������ߴ�ͼ��ɾ���ˣ����Ұ����������Ϊ��ֹ��  ��ͼ��ɾ�����ߺ����
        
        //�����ֹ������ID�������ģ��������������߳�
        //����Ȩ����������ֹ��Ӱ���������߳�
        if (map_txn_oid_.count(txn_id) > 0) {//û����Ȩ�����񼯺�  == �ȴ�����֪ͨ�����̣߳�
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
      //���л����������������
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

//���µ������¼ ������û�еĻ���һ��wait()�����ȴ�
//������������
//1��ǰ�����񶼼���  2��ǰ�����񶼼���
auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  //lr ǰ��������
  for (auto &lr : lock_request_queue->request_queue_) {
    //�������ݾ���  lrΪT1���е�������   ����T2�治����
    //lr->granted_ ǰ�������鶼��������
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
//���������ͺͲ������ͣ��ҵ���Ӧ�ļ��Ͻ��в����ɾ��
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
  //�������� ֻ�����S��X��
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
