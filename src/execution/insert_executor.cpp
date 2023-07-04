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
 //并发查询执行
 //表 加IX  Init()
 //行 加X   Next()
void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    //表 IX 
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);//表索引数组
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
                //for循环遍历完成之后，把它所有相关的索引都更新了一遍    
                //将原来的一个tuple转化成索引需要的key，接着只需要把key插到索引上面去（军代码） index_info->index_->InsertEntry(key,rid,exec_ctx_->GetTransaction());   
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
  int32_t insert_count = 0;  //插入了几行
  //每次去调子算子的next（）函数，每次返回一个tuple和rid，只要不返回false，一直调用
  //当while循环调用完成之后，说明所有要插入的值都插入完成了，这时要返回给上一层
  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
    //首先把tuple插入到表中
    //在插入InsertTuple（）里面，会对rid做一个更新，已经有值，后面不用再去操作rid，后面不用赋值
    bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());
    if (inserted) {
      try {
        //行 X
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
        if (!is_locked) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
      //插入tuple后要更新《相关联》的所有索引
      //遍历 table_indexes_ 容器中的每个 IndexInfo 对象，将相关联的索引进行插入相关的key键操作InsertEntry()。
      //更新插入元组tuple的表的所有索引 == InsertEntry()
      //更新索引时，并不需要全部的tuple信息，而是需要经过转换后的tuple信息 key（tuple有5个列，只用一两个列）
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_insert_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->InsertEntry(to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      insert_count++;
    }
//to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs())
//等同于相关联索引需要的key

//使用 std::for_each 算法遍历 table_indexes_ 容器中的每个 IndexInfo 对象，并相关联的索引进行插入key键操作。
//to_insert_tuple, rid, table_info = table_info_, exec_ctx = exec_ctx_: 
//分别表示要插入的元组、记录ID、表格信息和执行上下文。它们都是引用类型，用于在 lambda 函数中使用
//index->index_: 表示 IndexInfo 中的索引数据结构，可以通过 -> 操作符来访问其中的成员变量和函数

//to_insert_tuple.KeyFromTuple(）是元组对象的一个成员函数，用于从元组中提取关键字，并将其转换为索引数据结构中需要的key键类型。
//table_info->schema_ 表示元组的模式，index->key_schema_ 表示索引的键模式，index->index_->GetKeyAttrs() 表示索引键的属性列表。
// std::for_each 算法遍历 table_indexes_ 容器中的每个 IndexInfo 对象，并相关联的索引进行插入key键操作。
//在 lambda 函数中，使用 to_insert_tuple 提取所需的关键字ky，使用 rid 提供记录 ID，使用 table_info 提供表格信息，使用 exec_ctx 提供执行上下文
  }
  //需要告诉外界插入了几行信息，需要把tuple构造出来 Tuple(std::vector<Value> values, const Schema *schema);
  //执行器将生成一个整数元组Tuple{values, &GetOutputSchema()}作为输出，指示在插入所有行后插入了多少行到表中
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());//预先分配 values 容器的大小
  //GetOutputSchema() 再返回时，它是什么样的一种类型   插入了几行信息
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};//构造tuple再赋值，而rid不用赋值，上面插入有解释
  is_end_ = true;
  //因为是火山模型，可能会next函数调用很多次，但我们这个结果数据只有这么多，只能返回一次tuple,rid，那么下一次再来调用这个外层next函数的时候
  //只要插入的数据已经插入过了，就不能再返回return true，应该返回false，标识它已经完成插入了
  return true;
}
}  // namespace bustub
