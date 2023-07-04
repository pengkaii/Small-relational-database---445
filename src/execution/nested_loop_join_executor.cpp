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

//���û�ɽģ�͵�next()
//ÿ��tupleֻ�ܵ���һ�Σ����ֱ������ѭ������ô�ڲ�ѭ������ֻ�ᴦ�����ѭ���еĵ�һ�����ݣ��������ݽ����ᱻ����
//������ǿ��Կ����Ȱ��ڲ�tuplesȫ�����浽�ڴ������У�ֻ��Ҫÿ�ε������tuples��ȡһ��tuple��

//���ܴ�������tupleû�����꣬�ڶ���tuple���������ˡ�
//���left���ӽڵ�ÿ�ε���Next()������ȡһ��tuple֮ǰ���Ȱ���һ��tuple��ȡ�����ݱ����ꡣ

//left join��right join������������һ���ģ����ǰ����������ƴ������
//�����leftjoin������ұ�û���κ���ƥ���ϣ�����Ҫ��NULL��
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
  /*�Ȱ��ұߵ�tuple��������*/
  //�Ƚ��ұ������tuple���浽�ڴ������У��Ա����ѭ�����Ӳ�����ʹ��
  while (right_executor_->Next(&tuple, &rid)) {//����ڲ�ѭ�����ұߣ�tuplesȫ�����浽�ڴ�����
    right_tuples_.push_back(tuple);
    //�ұ������Ԫ�鶼�ᱻ�������ڴ�right_tuples_�У��Ա��ں�����Ƕ��ѭ�����Ӳ�����ʹ��
    //Ƕ��ѭ�����Ӳ������������ӹ�����ֱ�ӷ����ұ�Ļ���Ԫ�飬
    //������Ҫÿ�����Ӳ��������·����ұ�ִ��������ȡԪ�����ݡ�����������Ӳ�����Ч�ʡ�
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // ��ȡ���������У���������ӵ���������
  std::vector<Column> columns(left_schema_.GetColumns());
  /* �ܵ������������������ˮƽƴ������ */
  for (const auto &column : right_schema_.GetColumns()) {
    columns.push_back(column);
  }
  // �����µ�Schema�������ڱ�ʾ���������Ե�ˮƽƴ��
  Schema schema(columns);
  if (is_ineer_) {
     //����������ӣ������InnerJoin����
    return InnerJoin(schema, tuple);
  }
   // ���򣬵���LeftJoin����
  return LeftJoin(schema, tuple);
}

auto NestedLoopJoinExecutor::InnerJoin(const Schema &schema, Tuple *tuple) -> bool {
  // ��������Ѿ��������ұ���Ԫ��Ĵ�С����ʾ���е����Ӳ�������ɣ�����false
  if (index_ > right_tuples_.size()) {
    return false;
  }
  if (index_ != 0) {
    //�ұ߻�������û�б����꣬��ѭ�������ұ߻��������ÿһ�� tuple�����ƥ�������µ� tuple ������ true
    // ��index��ʼ�����ұ�Ԫ������
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      // ʹ��ν���ж������ұ����Ϣ�Ƿ�ƥ��
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[j], right_schema_).GetAs<bool>()) {
        // ���ƥ�䣬����ϲ����Ԫ�鲢����
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
  //����ұ߻��������Ѿ���������߻�������Ϊ�գ�
  //���������ӽڵ��ÿһ�� tuple��������ұߵ�����һ�� tuple ƥ�䣬�����µ� tuple ������ true
  if (index_ == 0) {
    // �����Ԫ����ұ����е�ȫ�����ݽ���ƥ��
    //left���ӽڵ�ÿ�ε���Next()������ȡһ��tuple֮ǰ���Ȱ���һ��tuple��ȡ�����ݱ����ꡣ
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      for (const auto &right_tuple : right_tuples_) {
        // �����ұ����е�ÿ��Ԫ��
        index_ = (index_ + 1) % right_tuples_.size();
        // ʹ��ν���ж������ұ����Ϣ�Ƿ�ƥ��
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>()) {
          // ���ƥ�䣬����ϲ����Ԫ�鲢����
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            value.push_back(right_tuple.GetValue(&right_schema_, i));
          }
          /*ע�⣺��forѭ���ڲ����غϲ����Ԫ����Ϣ��û�б����������ұ�Ԫ��
          //����forѭ��right_tuple��10��������ֻ�ϲ���һ�����ͷ����ˣ����ʱ��ʣ�µľ���������Ҫ���´ν��뺯��ʱ���ȷ���*/
          *tuple = {value, &schema};
          return true;
        }
      }
    }
    //������Ԫ���Ѿ������꣬����һ����Ч����ֵ
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
    //��������ӽڵ��ÿһ��tuple
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {//���next()ÿ�λ��һ��tuple
      is_match_ = false;
      //ѭ�������ұ߻��������ÿһ��tuple
      for (const auto &right_tuple : right_tuples_) {
        index_ = (index_ + 1) % right_tuples_.size();
        //if(ƥ��)
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
          return true;//����ֻƥ��һ��
        }
      }
      /*�ұ�Ϊ�պ�û���κ�ƥ������*/
      /*�������ӽڵ��tuple���ұ�û���κ�һ����ƥ�䣬����Ҫ����һ����tuple��join*/
      if (!is_match_) {
        std::vector<Value> value;

        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        /*�ұ�ȫ�����ֵ*/
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
        }
        *tuple = {value, &schema};
        is_match_ = true;
        return true;
      }
    }
    /*����ӽڵ��tuple������֮�󣬲��ٽ��������߼���ֱ���ں�����ڴ�����false*/
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
