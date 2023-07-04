//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//�ڳ���ƥ���ұ� tuple ʱ������ ����join key ȥ B+Tree Index ����в�ѯ��
//�����ѯ������������Ų鵽�� RID ȥ�ұ��ȡ tuple Ȼ��װ��ɽ�����

//join���ұ�������У�ǡ����������ֱ��������
//�������ÿ����ȡһ��tuple����ɽģ�͵�next������
//��tuple��ȡ��������key���ٴ������в��Ҷ�Ӧ�����е�����
//���η���ƥ�����tuple��Ȼ����������
#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_left_ = (plan->GetJoinType() == JoinType::LEFT);
  /*������Ϣ�ͱ���Ϣ��child������ұ��ǵ�ǰ������������*/
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  /*
   * 1. ���ȴ�childҲ�������Next��ȡһ��tupleA
   * 2. ��tupleA�л�ȡkey��Ҫ�ļ��У�ת��Ϊkey��tuple
   * 3. ȥ�����в�tupleB����û�����key
   * */
   /*ɨ�����Ȼ��ֱ�Ӳ�����*/
   //����˼·��ͨ��ɨ���������ÿ������tuple��������ȡ�ؼ��е�ֵvalue�������ұ�������в����Ƿ�����ͬ��keyֵ��
   //����ҵ��ˣ�����ұ��л�ȡ��Ӧ��tuple���������tuple���ұ�tuple�ϲ���һ���µ�tuple����
  while (child_executor_->Next(&left_tuple, &left_rid)) {//�����tuple
    /*���left_tuple��Ӧ��schema*/
    auto key_schema = index_info_->index_->GetKeySchema();// ���������key schema
    // �����tuple��schema�����key��Ӧ��ֵ
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> values;
    values.push_back(value);
    Tuple key(values, key_schema);//��key��ֵ��װ��һ��tuple
    std::vector<RID> results;
    //ʹ���������ҷ���key���ұ�tuple��rid
    index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
    //ȥ�����в���û�����key��Ӧ��tupleB����result��¼rid
    /*���ƥ��������ΪkeyB�������ظ������������ƥ�䣬�϶�ƥ��һ�ξͽ�����inner*/
    
    if (!results.empty()) {// ������ҵ����ұ�tuple
      for (auto rid_b : results) { // ����ÿ���ұ�tuple��rid
        Tuple right_tuple; 
        // ����ÿ���������в鵽��rid_b������ͨ��catalog��ö�Ӧ��tuple������ұ��tupleB����right_tuple
        if (table_info_->table_->GetTuple(rid_b, &right_tuple, exec_ctx_->GetTransaction())) {
          std::vector<Value> tuple_values;
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
            tuple_values.push_back(right_tuple.GetValue(&table_info_->schema_, i));
          }
          *tuple = {tuple_values, &plan_->OutputSchema()};//���ص���tuple�����tuple+�ұ�tuple ���ӵ�
          return true;
        }
      }
    }
    /*�ұ�û��Ԫ��ʱ��������left join������Ҫ��null
     * �����inner join��û���κ���ƥ�䣬���ùܣ�ֱ�Ӻ���
     * */
    if (is_left_) {
      std::vector<Value> tuple_values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
        tuple_values.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
      }
      *tuple = {tuple_values, &plan_->OutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub

    // #include "execution/executors/nested_index_join_executor.h"
    // #include "type/value_factory.h"

    // namespace bustub {

    // NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
    //                                              std::unique_ptr<AbstractExecutor> &&child_executor)
    //     : AbstractExecutor(exec_ctx),
    //       plan_{plan},
    //       child_(std::move(child_executor)),
    //       /*������Ϣ�ͱ���Ϣ��child������ұ��ǵ�ǰ������������*/
    //       index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
    //       table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
    //       tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())} {
    //   if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    //     // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    //     throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    //   }
    // }

    // void NestIndexJoinExecutor::Init() { child_->Init(); }

    // auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //   Tuple left_tuple{};
    //   RID emit_rid{};
    //   /*
    //    * 1. ���ȴ�childҲ�������Next��ȡһ��tupleA
    //    * 2. ��tupleA�л�ȡkey��Ҫ�ļ��У�ת��Ϊkey�������и���������Ϣ��keyA = keyB������tupleA��һ������tupleB
    //    * 3. ȥ������index_info_->index_���Ƿ������key��Ӧ��tupleB
    //    * */
    //   std::vector<Value> vals;
    //   while (child_->Next(&left_tuple, &emit_rid)) {//��ɽģ��next���һ��left_tuple
    //      /*ɨ�����Ȼ��ֱ�Ӳ�����*/
    //     Value value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_->GetOutputSchema());
    //      /*���tuple��Ӧ��schema*/
    //     std::vector<RID> rids;
    //     tree_->ScanKey(Tuple{{value}, index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    //     Tuple right_tuple{}; // ����ÿ��rid������ͨ��catalog��ö�Ӧ��tuple�����tuple����
    //      /*���ƥ��������ΪkeyB�������ظ������������ƥ�䣬�϶�ƥ��һ�ξͽ�����inner*/
    //     if (!rids.empty()) {
    //       table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
    //       for (uint32_t idx = 0; idx < child_->GetOutputSchema().GetColumnCount(); idx++) {
    //         vals.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), idx));
    //       }
    //       for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
    //         vals.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), idx));
    //       }
    //       *tuple = Tuple(vals, &GetOutputSchema());
    //       return true;
    //     }
    //     /*�ұ�û��Ԫ��ʱ��������left join������Ҫ��null
    //      * �����inner join��û���κ���ƥ�䣬���ùܣ�ֱ�Ӻ���
    //      * */
    //     if (plan_->GetJoinType() == JoinType::LEFT) {
    //       for (uint32_t idx = 0; idx < child_->GetOutputSchema().GetColumnCount(); idx++) {
    //         vals.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), idx));
    //       }
    //       for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
    //         vals.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(idx).GetType()));
    //       }
    //       *tuple = Tuple(vals, &GetOutputSchema());
    //       return true;
    //     }
    //   }
    //   return false;
    // }

    // }  // namespace bustub
