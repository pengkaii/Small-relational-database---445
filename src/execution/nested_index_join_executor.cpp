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
//在尝试匹配右表 tuple 时，会拿 联接join key 去 B+Tree Index 里进行查询。
//如果查询到结果，就拿着查到的 RID 去右表获取 tuple 然后装配成结果输出

//join的右表的属性列，恰好有索引，直接走索引
//遍历左表，每个获取一个tuple（火山模型的next函数）
//从tuple中取出属性列key，再从索引中查找对应属性列的数据
//依次返回匹配的右tuple，然后整合起来
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
  /*索引信息和表信息，child是左表，右表是当前，并且有索引*/
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  /*
   * 1. 首先从child也就是左表，Next获取一个tupleA
   * 2. 从tupleA中获取key需要的几列，转换为key的tuple
   * 3. 去索引中查tupleB中有没有这个key
   * */
   /*扫描左表，然后直接查索引*/
   //基本思路是通过扫描左表，对于每个左表的tuple，从中提取关键列的值value，并在右表的索引中查找是否有相同的key值。
   //如果找到了，则从右表中获取对应的tuple，并将左表tuple和右表tuple合并成一个新的tuple返回
  while (child_executor_->Next(&left_tuple, &left_rid)) {//获得左tuple
    /*这个left_tuple对应的schema*/
    auto key_schema = index_info_->index_->GetKeySchema();// 获得索引的key schema
    // 用左表tuple和schema计算出key对应的值
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> values;
    values.push_back(value);
    Tuple key(values, key_schema);//将key的值封装成一个tuple
    std::vector<RID> results;
    //使用索引查找符合key的右表tuple的rid
    index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
    //去索引中查有没有这个key对应的tupleB，到result记录rid
    /*如果匹配上了因为keyB不存在重复，所以如果能匹配，肯定匹配一次就结束。inner*/
    
    if (!results.empty()) {// 如果查找到了右表tuple
      for (auto rid_b : results) { // 对于每个右表tuple的rid
        Tuple right_tuple; 
        // 对于每个在索引中查到的rid_b，可以通过catalog获得对应的tuple，如果右表的tupleB存在right_tuple
        if (table_info_->table_->GetTuple(rid_b, &right_tuple, exec_ctx_->GetTransaction())) {
          std::vector<Value> tuple_values;
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
            tuple_values.push_back(right_tuple.GetValue(&table_info_->schema_, i));
          }
          *tuple = {tuple_values, &plan_->OutputSchema()};//返回的是tuple是左表tuple+右表tuple 联接的
          return true;
        }
      }
    }
    /*右表没有元素时，并且是left join，则需要填null
     * 如果是inner join，没有任何行匹配，则不用管，直接忽略
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
    //       /*索引信息和表信息，child是左表，右表是当前，并且有索引*/
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
    //    * 1. 首先从child也就是左表，Next获取一个tupleA
    //    * 2. 从tupleA中获取key需要的几列，转换为key，这里有个隐含的信息是keyA = keyB，但是tupleA不一定等于tupleB
    //    * 3. 去索引中index_info_->index_查是否有这个key对应的tupleB
    //    * */
    //   std::vector<Value> vals;
    //   while (child_->Next(&left_tuple, &emit_rid)) {//火山模型next获得一个left_tuple
    //      /*扫描左表，然后直接查索引*/
    //     Value value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_->GetOutputSchema());
    //      /*这个tuple对应的schema*/
    //     std::vector<RID> rids;
    //     tree_->ScanKey(Tuple{{value}, index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    //     Tuple right_tuple{}; // 对于每个rid，可以通过catalog获得对应的tuple，如果tuple存在
    //      /*如果匹配上了因为keyB不存在重复，所以如果能匹配，肯定匹配一次就结束。inner*/
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
    //     /*右表没有元素时，并且是left join，则需要填null
    //      * 如果是inner join，没有任何行匹配，则不用管，直接忽略
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
