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
    //抽象表达式引用 到底是按照什么规则排序，针对哪一列进行数值计算
    //拿到这个类型和它的表达式，对每一列去做一个比较，先判断一下是不是升序还是
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      bool asc_order_by = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
      if(asc_order_by) {
        //expr表达式会根据每一列去做一个evaluate，去做一个判断，一个比较 ，a,b传参进来
        //特殊：a.name == b.name 可以不直接返回，等下一列去区分
        //return a < b;true/false 升序
        //return a > b;true/false 降序
        if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareLessThan(
                expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }
        if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareGreaterThan(
                expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }
        //相等不处理，等一列去处理
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
    return false;//调用了sort排序，false表示什么操作都不做
  });
  iterator_ = sorted_tuples_.begin();//迭代器遍历，基于火山模型，每次next返回一个tuple，知道当前遍历到哪个位置了，迭代器一开始要放在开始位置
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
