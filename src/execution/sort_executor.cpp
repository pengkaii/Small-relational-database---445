#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    //������ʽ���� �����ǰ���ʲô�������������һ�н�����ֵ����
    //�õ�������ͺ����ı��ʽ����ÿһ��ȥ��һ���Ƚϣ����ж�һ���ǲ���������
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      bool asc_order_by = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
      if(asc_order_by) {
        //expr���ʽ�����ÿһ��ȥ��һ��evaluate��ȥ��һ���жϣ�һ���Ƚ� ��a,b���ν���
        //���⣺a.name == b.name ���Բ�ֱ�ӷ��أ�����һ��ȥ����
        //return a < b;true/false ����
        //return a > b;true/false ����
        if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareLessThan(
                expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }
        if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareGreaterThan(
                expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }
        //��Ȳ�������һ��ȥ����
      }else{
        if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareLessThan(
                expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }
        if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareGreaterThan(
                expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return false;//������sort����false��ʾʲô����������
  });
  iterator_ = sorted_tuples_.begin();//���������������ڻ�ɽģ�ͣ�ÿ��next����һ��tuple��֪����ǰ�������ĸ�λ���ˣ�������һ��ʼҪ���ڿ�ʼλ��
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ != sorted_tuples_.end()) {
    *tuple = *iterator_;
    *rid = iterator_->GetRid();
    iterator_++;
    return true;
  }
  return false;
}

}  // namespace bustub
    // #include "execution/executors/sort_executor.h"

    // namespace bustub {

    // SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
    //                            std::unique_ptr<AbstractExecutor> &&child_executor)
    //     : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

    // void SortExecutor::Init() {
    //   child_->Init();
    //   Tuple child_tuple{};
    //   RID child_rid;
    //   while (child_->Next(&child_tuple, &child_rid)) {
    //     child_tuples_.push_back(child_tuple);
    //   }

    //   std::sort(
    //       child_tuples_.begin(), child_tuples_.end(),
    //       [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &tuple_a, const Tuple &tuple_b) {
    //         for (const auto &order_key : order_bys) {
    //           switch (order_key.first) {
    //             case OrderByType::INVALID:
    //             case OrderByType::DEFAULT:
    //             case OrderByType::ASC:
    //               if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
    //                                         .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
    //                 return true;
    //               } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
    //                                                .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
    //                 return false;
    //               }
    //               break;
    //             case OrderByType::DESC:
    //               if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
    //                                         .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
    //                 return true;
    //               } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
    //                                                .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
    //                 return false;
    //               }
    //               break;
    //           }
    //         }
    //         return false;
    //       });

    //   child_iter_ = child_tuples_.begin();
    // }

    // auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //   if (child_iter_ == child_tuples_.end()) {
    //     return false;
    //   }

    //   *tuple = *child_iter_;
    //   *rid = tuple->GetRid();
    //   ++child_iter_;

    //   return true;
    // }

    // }  // namespace bustub
