//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
//可扩展哈希表的构造函数
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}
//给定键的哈希索引  结合例子来理解
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1; //计算掩码，用于计算哈希索引
  return std::hash<K>()(key) & mask;   //对键进行哈希运算并与掩码相与，得到哈希索引
}
// 当全局深度为2时，掩码为0b11，即二进制的3。
// 因此，哈希索引就是对键进行哈希运算后的结果的二进制表示的最后两位。
// 例如，若某个键的哈希值为0b1101，则其哈希索引为0b01，即十进制的1

//函数返回全局深度的值
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);//锁定互斥锁
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}
//对应桶的本地深度
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index); //调用私有函数 获取指定索引处桶的本地深度的值
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}
//返回桶的数量
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}
//查找给定键的值，并将结果存储在value参数中
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);//键的哈希索引
  auto target_bucket = dir_[index];//对应的桶

  return target_bucket->Find(key, value); //在目标桶中查找键，并将值存储在value参数中
}
//移除定键的键值对
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  return target_bucket->Remove(key);//目标桶中移除键
}
//插入新的键值对
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {//若目标桶已满，则需要进行扩展
    auto index = IndexOf(key);//计算键的哈希索引
    auto target_bucket = dir_[index];//获取对应的桶
     // 如果目标桶的本地深度等于全局深度
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);//扩展目录大小为当前的两倍
      for (int i = 0; i < capacity; i++) {//扩容以后 新的目录更新桶指针
        dir_[i + capacity] = dir_[i];
      }
    }
    //桶的分裂
    int mask = 1 << target_bucket->GetDepth(); // 计算掩码，用于划分键到新桶或旧桶
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);// 创建新桶0，桶的深度+1
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);// 创建新桶1
    //将旧桶中的键值对重新分配到新桶
    for (const auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }
    //增加桶的数量
    num_buckets_++;
    //遍历新目录的每个i（转化为二进制），更新指向原来目标桶的桶指针
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = bucket_1;
        } else {
          dir_[i] = bucket_0;
        }
      }
    }
  }

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  for (auto &item : target_bucket->GetItems()) {//在目标桶中查找键
    if (item.first == key) { // 如果找到键，则更新对应的值
      item.second = value;
      return;
    }
  }

  target_bucket->Insert(key, value); // 否则，在目标桶中插入新的键值对
}

//===--------------------------------------------------------------------===//
// Bucket 哈希表的桶
//===--------------------------------------------------------------------===//
template <typename K, typename V>
// 桶的构造函数，初始化桶的大小和局部深度
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
//std::any_of 算法会遍历范围中的元素，并对每个元素应用谓词，直到谓词返回 true 或遍历完所有元素
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const auto &item) {
    if (item.first == key) {// 如果键匹配成功，则将对应的值存储在value参数中
      value = item.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    if (item.first == key) {// 如果键匹配成功，则移除对应的键值对
      this->list_.remove(item);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {// 如果桶已满，则插入失败
    return false;
  }
  list_.emplace_back(key, value);// 否则，在桶的末尾插入键值对
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;// 实例化可扩展哈希表模板
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
