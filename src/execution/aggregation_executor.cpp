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
//聚合执行器只需要为输入的每个组执行聚合，它只有一个子节点。聚合的模式是先分组列后聚合列。
//实现聚合的常见策略是使用哈希表，即聚合哈希表完全适合内存。
//不需要担心为哈希聚合实现两阶段（分区，重新哈希）策略。所有聚合结果都可以驻留在内存中的哈希表中（即哈希表不需要由缓冲池页面支持）
//SimpleAggregationHashTable公开了一个内存中的哈希表（std::unordered_map），但其接口专门设计用于计算聚合。
//该类还公开了 SimpleAggregationHashTable::Iterator 类型，可用于遍历哈希表。

//在查询计划的上下文中，聚合是管道breakers。影响在实现中使用 AggregationExecutor::Init() 和 AggregationExecutor::Next() 函数的方式。
//聚合的构建阶段是应该在 AggregationExecutor::Init() 还是 AggregationExecutor::Next() 中执行。

//如何处理聚合函数输入中的 NULL 值（即，元组的属性值可能为 NULL）。分组列永远不会为 NULL。

//提示：在空哈希表表上执行聚合时，CountStarAggregate 应该初始化零，
//而所有其他聚合类型应返回 integer_null。这就是为什么 GenerateInitialAggregateValue 将大多数聚合值初始化为 NULL 的原因。
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),//哈希表
      aht_iterator_(aht_.Begin()) {}//哈希表迭代器

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  //从子节点获取tuple和rid的数据
  while (child_->Next(&tuple, &rid)) {
    //根据tuple生成对应的聚合key和聚合value放入哈希表中
    //拿到一个新的tuple,把这个tuple中的数据，建立起映射关系
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  //特殊处理空哈希表的情况：当为空表，且想获得统计信息时，只有countstar返回0，其他情况返回无效null*/
  if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertIntialCombine();
  }
  aht_iterator_ = aht_.Begin();//插入后需要重新指定 aht_iterator_ 因为哈希表是无序的
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //迭代器指的时哈希表的每一项
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  //每次获取一个hashmap中的key,key_1 = [上等仓，女]
  //先放分组.Key().group_bys_，再放数据.Val().aggregates_
  //先是分组列，然后是聚合值列  放进tuple的values
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};//Aggregation 输出的 schema 形式为 group-bys + aggregates
  ++aht_iterator_;

  return true;
    ///* 特殊处理空表的情况：当为空表，且想获得统计信息时，只有countstar返回0，其他情况返回无效null*/  
    // 空表执行 select count(*) from t1;   
    //    if (!successful_) {     
    //     /*跟迭代器无关的逻辑，只需要返回一次*/     
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

//count(column) 和 count(*) 的区别，以及对空值的处理。
//另外，不需要考虑 hashmap 过大的情况，即整张 hashmap 可以驻留在内存中，不需要通过 Buffer Pool 调用 page 来存储。
