//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
   //������ʽ���ۼ�����
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** @return The initial aggregrate value for this aggregation executor */
  //��ʼ���ۺ�ֵvalue
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // Count start starts at zero.  Ϊ0
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // Others starts at null.   Ϊnull
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
   //�ۺϲ����ĺ���
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    //ע�����������
    //����1���Ѿ��еľۼ����� �����������ģ�20��������������25������������69��
    //����2. �����¼�������� ����: [������18]
    //�洢agg_exprs_�ۺϱ��ʽ������
    //����agg_types_�����е�ÿ��Ԫ�ص�ֵ���в�ͬ�Ĵ���
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate:
        //��result�е�i���ۺϽ������result->aggregates_[i]������1��Ҳ����������һ��
        //��������ԭ�ۺϽ����ӣ��õ��µľۺϽ�������մ洢��result�С�
          result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::CountAggregate:
        //count(*) ��count��ʲô����
        //count(*) ͳ���ж����У���ʹһ������Ҳû�У�ҲӦ�÷���0�У�������NULL����ͳ�ƽ����ʱ�򣬲��������ֵΪNULL��
        //count(����)ֻ����������һ�̣���ͳ�ƽ����ʱ�򣬻������ֵΪ��
          if (result->aggregates_[i].IsNull()) {//Ϊ�գ���0
            result->aggregates_[i] = ValueFactory::GetIntegerValue(0);
          }
          if (!input.aggregates_[i].IsNull()) {//��Ϊ�գ���һ
            result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          }
          break;
        case AggregationType::SumAggregate:
        //result û��ֵ��ʱ���ȸ���ֵ
          if (result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = input.aggregates_[i];
          } else if (!input.aggregates_[i].IsNull()) {
            //ֻ��������ʱ�����ۼ�
            result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
          }
          break;
        case AggregationType::MinAggregate:
        //Ϊ��  �����е�С
          if (result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = input.aggregates_[i];
          } else if (!input.aggregates_[i].IsNull()) {
            result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
          }
          break;
        case AggregationType::MaxAggregate:
        //Ϊ�գ������еĴ�
          if (result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = input.aggregates_[i];
          } else if (!input.aggregates_[i].IsNull()) {
            result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
          }
          break;
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    //�����Ĺ�ϣ��unordered_map<key,value> 
    //��ϣ�� �����ڣ�key,��ʼ���ձ�ۺ�ֵvalue)  
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    //�����ϣ�����Ѿ����������ˣ���ô����Ҫ����group�������з���ͳ��
    //�õ����飬��ִ�оۺ�value����
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  void InsertIntialCombine() { ht_.insert({{std::vector<Value>()}, GenerateInitialAggregateValue()}); }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

  auto Size() -> size_t { return ht_.size(); }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> &agg_exprs_;
  /** The types of aggregations that we have */
  const std::vector<AggregationType> &agg_types_;
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** Do not use or remove this function, otherwise you will get zero points. */
  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    //��һ��tupleת��Ϊ��ϣ���key��������Щ������
    std::vector<Value> keys;
    //����group by �Ѷ�Ӧ��ֵȡ�������ŵ�keys��
    //������У� plan->GetGrounpBys()
    //���簴�������ֶη�  key_1 = ���ϵȲ֣��С�  key_2 = ���ϵȣ�Ů��
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    //��Ҫ������У��ۼ����� GetAggregates()
    //value = [������18]
    //��ϣ���зŵ����ݾ��� key = [�ϵȲ֣���] ---> value = [������18]  ֻ��Ҫ�õ�������һ�е�����
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;
  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_;
  /** Simple aggregation hash table */
  SimpleAggregationHashTable aht_;
  /** Simple aggregation hash table iterator */
  SimpleAggregationHashTable::Iterator aht_iterator_;
};
}  // namespace bustub
