//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

//利用火山模型的next()
//每个tuple只能调用一次，如果直接两层循环，那么内层循环，将只会处理外层循环中的第一条数据，其他数据将不会被处理。
//因此我们可以考虑先把内层tuples全部缓存到内存数组中，只需要每次调用外层tuples获取一个tuple。

//可能存在外层的tuple没遍历完，第二个tuple就吐上来了。
//因此left左子节点每次调用Next()函数获取一个tuple之前，先把上一次tuple获取的数据遍历完。

//left join和right join的总列数都是一样的，都是把两个表的列拼起来。
//如果是leftjoin，如果右表没有任何行匹配上，则需要填NULL。
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_ineer_ = (plan_->GetJoinType() == JoinType::INNER);
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  /*先把右边的tuple缓存起来*/
  //先将右表的所有tuple缓存到内存数组中，以便后续循环连接操作中使用
  while (right_executor_->Next(&tuple, &rid)) {//获得内层循环（右边）tuples全部缓存到内存数组
    right_tuples_.push_back(tuple);
    //右表的所有元组都会被缓存在内存right_tuples_中，以便在后续的嵌套循环连接操作中使用
    //嵌套循环连接操作可以在连接过程中直接访问右表的缓存元组，
    //而不需要每次连接操作都重新访问右表执行器来获取元组数据。这提高了连接操作的效率。
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 获取左表的属性列，并将其添加到列向量中
  std::vector<Column> columns(left_schema_.GetColumns());
  /* 总的属性是两个表的属性水平拼接起来 */
  for (const auto &column : right_schema_.GetColumns()) {
    columns.push_back(column);
  }
  // 创建新的Schema对象，用于表示两个表属性的水平拼接
  Schema schema(columns);
  if (is_ineer_) {
     //如果是内连接，则调用InnerJoin函数
    return InnerJoin(schema, tuple);
  }
   // 否则，调用LeftJoin函数
  return LeftJoin(schema, tuple);
}

auto NestedLoopJoinExecutor::InnerJoin(const Schema &schema, Tuple *tuple) -> bool {
  // 如果索引已经超过了右表缓存元组的大小，表示所有的连接操作已完成，返回false
  if (index_ > right_tuples_.size()) {
    return false;
  }
  if (index_ != 0) {
    //右边缓存数组没有遍历完，则循环遍历右边缓存数组的每一个 tuple，如果匹配则构造新的 tuple 并返回 true
    // 从index开始遍历右表元组数组
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      // 使用谓词判断左表和右表的信息是否匹配
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[j], right_schema_).GetAs<bool>()) {
        // 如果匹配，构造合并后的元组并返回
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[j].GetValue(&right_schema_, i));
        }
        *tuple = {value, &schema};
        return true;
      }
    }
  }
  //如果右边缓存数组已经遍历完或者缓存数组为空，
  //则遍历左边子节点的每一个 tuple，如果和右边的任意一个 tuple 匹配，则构造新的 tuple 并返回 true
  if (index_ == 0) {
    // 对左表元组和右表缓存中的全部数据进行匹配
    //left左子节点每次调用Next()函数获取一个tuple之前，先把上一次tuple获取的数据遍历完。
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      for (const auto &right_tuple : right_tuples_) {
        // 遍历右表缓存中的每个元组
        index_ = (index_ + 1) % right_tuples_.size();
        // 使用谓词判断左表和右表的信息是否匹配
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>()) {
          // 如果匹配，构造合并后的元组并返回
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            value.push_back(right_tuple.GetValue(&right_schema_, i));
          }
          /*注意：在for循环内部返回合并后的元组信息，没有遍历完所有右表元组
          //可能for循环right_tuple有10条，但是只合并了一条，就返回了，这个时候剩下的九条，就需要在下次进入函数时，先返回*/
          *tuple = {value, &schema};
          return true;
        }
      }
    }
    //外层左表元组已经遍历完，设置一个无效索引值
    index_ = right_tuples_.size() + 1;
  }
  return false;
}

auto NestedLoopJoinExecutor::LeftJoin(const Schema &schema, Tuple *tuple) -> bool {
  if (index_ > right_tuples_.size()) {
    return false;
  }
  if (index_ != 0) {
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[j], right_schema_).GetAs<bool>()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[j].GetValue(&right_schema_, i));
        }
        is_match_ = true;
        *tuple = {value, &schema};
        return true;
      }
    }
  }
  if (index_ == 0) {
    //遍历左边子节点的每一个tuple
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {//左表next()每次获得一个tuple
      is_match_ = false;
      //循环遍历右边缓存数组的每一个tuple
      for (const auto &right_tuple : right_tuples_) {
        index_ = (index_ + 1) % right_tuples_.size();
        //if(匹配)
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>()) {
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            value.push_back(right_tuple.GetValue(&right_schema_, i));
          }
          is_match_ = true;
          *tuple = {value, &schema};
          return true;//有且只匹配一次
        }
      }
      /*右表为空和没有任何匹配的情况*/
      /*如果左边子节点的tuple跟右边没有任何一行能匹配，则需要构造一个空tuple来join*/
      if (!is_match_) {
        std::vector<Value> value;

        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        /*右边全部填空值*/
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
        }
        *tuple = {value, &schema};
        is_match_ = true;
        return true;
      }
    }
    /*左边子节点的tuple遍历完之后，不再进入后面的逻辑，直接在函数入口处返回false*/
    index_ = right_tuples_.size() + 1;
  }
  return false;
}

}  // namespace bustub

    // #include "execution/executors/nested_loop_join_executor.h"
    // #include "binder/table_ref/bound_join_ref.h"
    // #include "common/exception.h"
    // #include "type/value_factory.h"

    // namespace bustub {

    // NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
    //                                                std::unique_ptr<AbstractExecutor> &&left_executor,
    //                                                std::unique_ptr<AbstractExecutor> &&right_executor)
    //     : AbstractExecutor(exec_ctx),
    //       plan_{plan},
    //       left_executor_(std::move(left_executor)),
    //       right_executor_(std::move(right_executor)) {
    //   if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    //     // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    //     throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    //   }
    // }

    // void NestedLoopJoinExecutor::Init() {
    //   left_executor_->Init();
    //   right_executor_->Init();
    //   Tuple tuple{};
    //   RID rid{};
    //   //
    //   while (right_executor_->Next(&tuple, &rid)) {
    //     right_tuples_.push_back(tuple);
    //   }
    // }

    // auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //   RID emit_rid{};
    //   while (right_tuple_idx_ >= 0 || left_executor_->Next(&left_tuple_, &emit_rid)) {
    //     std::vector<Value> vals;
    //     for (uint32_t ridx = (right_tuple_idx_ < 0 ? 0 : right_tuple_idx_); ridx < right_tuples_.size(); ridx++) {
    //       auto &right_tuple = right_tuples_[ridx];
    //       if (Matched(&left_tuple_, &right_tuple)) {
    //         for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    //           vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
    //         }
    //         for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    //           vals.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), idx));
    //         }
    //         *tuple = Tuple(vals, &GetOutputSchema());
    //         right_tuple_idx_ = ridx + 1;
    //         return true;
    //       }
    //     }
    //     if (right_tuple_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
    //       for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    //         vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
    //       }
    //       for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    //         vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
    //       }
    //       *tuple = Tuple(vals, &GetOutputSchema());
    //       return true;
    //     }
    //     right_tuple_idx_ = -1;
    //   }
    //   return false;
    // }

    // auto NestedLoopJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
    //   auto value = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
    //                                                right_executor_->GetOutputSchema());

    //   return !value.IsNull() && value.GetAs<bool>();
    // }

    // }  // namespace bustub
