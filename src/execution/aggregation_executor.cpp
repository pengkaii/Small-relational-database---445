//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//�ۺ�ִ����ֻ��ҪΪ�����ÿ����ִ�оۺϣ���ֻ��һ���ӽڵ㡣�ۺϵ�ģʽ���ȷ����к�ۺ��С�
//ʵ�־ۺϵĳ���������ʹ�ù�ϣ�����ۺϹ�ϣ����ȫ�ʺ��ڴ档
//����Ҫ����Ϊ��ϣ�ۺ�ʵ�����׶Σ����������¹�ϣ�����ԡ����оۺϽ��������פ�����ڴ��еĹ�ϣ���У�����ϣ����Ҫ�ɻ����ҳ��֧�֣�
//SimpleAggregationHashTable������һ���ڴ��еĹ�ϣ��std::unordered_map��������ӿ�ר��������ڼ���ۺϡ�
//���໹������ SimpleAggregationHashTable::Iterator ���ͣ������ڱ�����ϣ��

//�ڲ�ѯ�ƻ����������У��ۺ��ǹܵ�breakers��Ӱ����ʵ����ʹ�� AggregationExecutor::Init() �� AggregationExecutor::Next() �����ķ�ʽ��
//�ۺϵĹ����׶���Ӧ���� AggregationExecutor::Init() ���� AggregationExecutor::Next() ��ִ�С�

//��δ���ۺϺ��������е� NULL ֵ������Ԫ�������ֵ����Ϊ NULL������������Զ����Ϊ NULL��

//��ʾ���ڿչ�ϣ�����ִ�оۺ�ʱ��CountStarAggregate Ӧ�ó�ʼ���㣬
//�����������ۺ�����Ӧ���� integer_null�������Ϊʲô GenerateInitialAggregateValue ��������ۺ�ֵ��ʼ��Ϊ NULL ��ԭ��
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),//��ϣ��
      aht_iterator_(aht_.Begin()) {}//��ϣ�������

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  //���ӽڵ��ȡtuple��rid������
  while (child_->Next(&tuple, &rid)) {
    //����tuple���ɶ�Ӧ�ľۺ�key�;ۺ�value�����ϣ����
    //�õ�һ���µ�tuple,�����tuple�е����ݣ�������ӳ���ϵ
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  //���⴦��չ�ϣ����������Ϊ�ձ�������ͳ����Ϣʱ��ֻ��countstar����0���������������Чnull*/
  if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertIntialCombine();
  }
  aht_iterator_ = aht_.Begin();//�������Ҫ����ָ�� aht_iterator_ ��Ϊ��ϣ���������
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //������ָ��ʱ��ϣ���ÿһ��
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  //ÿ�λ�ȡһ��hashmap�е�key,key_1 = [�ϵȲ֣�Ů]
  //�ȷŷ���.Key().group_bys_���ٷ�����.Val().aggregates_
  //���Ƿ����У�Ȼ���Ǿۺ�ֵ��  �Ž�tuple��values
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};//Aggregation ����� schema ��ʽΪ group-bys + aggregates
  ++aht_iterator_;

  return true;
    ///* ���⴦��ձ���������Ϊ�ձ�������ͳ����Ϣʱ��ֻ��countstar����0���������������Чnull*/  
    // �ձ�ִ�� select count(*) from t1;   
    //    if (!successful_) {     
    //     /*���������޹ص��߼���ֻ��Ҫ����һ��*/     
    //     successful_ = true;     
    //     if (plan_->group_bys_.empty()) {       
    //         std::vector<Value> value;       
    //         for (auto aggregate : plan_->agg_types_) {         
    //             switch (aggregate) {           
    //                 case AggregationType::CountStarAggregate:             
    //                 value.push_back(ValueFactory::GetIntegerValue(0));             
    //                 break;           
    //                 case AggregationType::CountAggregate:           
    //                 case AggregationType::SumAggregate:           
    //                 case AggregationType::MinAggregate:           
    //                 case AggregationType::MaxAggregate:             
    //                 value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));             
    //                 break;         
    //             }       
    //         }       
    //         *tuple = {value, &schema};       
    //         successful_ = true;       
    //         return true;     
    //     } else {       
    //         return false;    
    //     }
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

//count(column) �� count(*) �������Լ��Կ�ֵ�Ĵ���
//���⣬����Ҫ���� hashmap ���������������� hashmap ����פ�����ڴ��У�����Ҫͨ�� Buffer Pool ���� page ���洢��
