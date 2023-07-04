//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}
 //������ѯִ��
 //�� ��IX  Init()
 //�� ��X   Next()
void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    //�� IX 
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);//����������
}
    // auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {   
    //     if(is_success){     
    //         return false;  
    //     }   
    //     int count = 0;   
    //     while(childeren_->Next(tuple, rid)) {     
    //         if(tableInfo_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {       
    //             count++;       
    //             auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(tableInfo_->name_);  
                //forѭ���������֮�󣬰���������ص�������������һ��    
                //��ԭ����һ��tupleת����������Ҫ��key������ֻ��Ҫ��key�嵽��������ȥ�������룩 index_info->index_->InsertEntry(key,rid,exec_ctx_->GetTransaction());   
    //             for(auto index_info: indexs) {         
    //                 auto key = tuple->KeyFromTuple(plan_->OutputSchema(), index_info->key_schema_, index_info->index_->GetKeyAttrs());         
    //                 index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());       
    //             }     
    //         }   
    //     }   
    //     std::vector<Value> value;   
    //     value.emplace_back(INTEGER, count);   
    //      Schema schema(plan_->OutputSchema());   
    //      *tuple = Tuple(value, &schema);   
    //      is_success = true;   
    //      return true; 
    // }


auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_insert_tuple{};
  RID emit_rid;
  int32_t insert_count = 0;  //�����˼���
  //ÿ��ȥ�������ӵ�next����������ÿ�η���һ��tuple��rid��ֻҪ������false��һֱ����
  //��whileѭ���������֮��˵������Ҫ�����ֵ����������ˣ���ʱҪ���ظ���һ��
  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
    //���Ȱ�tuple���뵽����
    //�ڲ���InsertTuple�������棬���rid��һ�����£��Ѿ���ֵ�����治����ȥ����rid�����治�ø�ֵ
    bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());
    if (inserted) {
      try {
        //�� X
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
        if (!is_locked) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
      //����tuple��Ҫ���¡������������������
      //���� table_indexes_ �����е�ÿ�� IndexInfo ���󣬽���������������в�����ص�key������InsertEntry()��
      //���²���Ԫ��tuple�ı���������� == InsertEntry()
      //��������ʱ��������Ҫȫ����tuple��Ϣ��������Ҫ����ת�����tuple��Ϣ key��tuple��5���У�ֻ��һ�����У�
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_insert_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->InsertEntry(to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      insert_count++;
    }
//to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs())
//��ͬ�������������Ҫ��key

//ʹ�� std::for_each �㷨���� table_indexes_ �����е�ÿ�� IndexInfo ���󣬲���������������в���key��������
//to_insert_tuple, rid, table_info = table_info_, exec_ctx = exec_ctx_: 
//�ֱ��ʾҪ�����Ԫ�顢��¼ID�������Ϣ��ִ�������ġ����Ƕ����������ͣ������� lambda ������ʹ��
//index->index_: ��ʾ IndexInfo �е��������ݽṹ������ͨ�� -> ���������������еĳ�Ա�����ͺ���

//to_insert_tuple.KeyFromTuple(����Ԫ������һ����Ա���������ڴ�Ԫ������ȡ�ؼ��֣�������ת��Ϊ�������ݽṹ����Ҫ��key�����͡�
//table_info->schema_ ��ʾԪ���ģʽ��index->key_schema_ ��ʾ�����ļ�ģʽ��index->index_->GetKeyAttrs() ��ʾ�������������б�
// std::for_each �㷨���� table_indexes_ �����е�ÿ�� IndexInfo ���󣬲���������������в���key��������
//�� lambda �����У�ʹ�� to_insert_tuple ��ȡ����Ĺؼ���ky��ʹ�� rid �ṩ��¼ ID��ʹ�� table_info �ṩ�����Ϣ��ʹ�� exec_ctx �ṩִ��������
  }
  //��Ҫ�����������˼�����Ϣ����Ҫ��tuple������� Tuple(std::vector<Value> values, const Schema *schema);
  //ִ����������һ������Ԫ��Tuple{values, &GetOutputSchema()}��Ϊ�����ָʾ�ڲ��������к�����˶����е�����
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());//Ԥ�ȷ��� values �����Ĵ�С
  //GetOutputSchema() �ٷ���ʱ������ʲô����һ������   �����˼�����Ϣ
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};//����tuple�ٸ�ֵ����rid���ø�ֵ����������н���
  is_end_ = true;
  //��Ϊ�ǻ�ɽģ�ͣ����ܻ�next�������úܶ�Σ�����������������ֻ����ô�ֻ࣬�ܷ���һ��tuple,rid����ô��һ����������������next������ʱ��
  //ֻҪ����������Ѿ�������ˣ��Ͳ����ٷ���return true��Ӧ�÷���false����ʶ���Ѿ���ɲ�����
  return true;
}
}  // namespace bustub
