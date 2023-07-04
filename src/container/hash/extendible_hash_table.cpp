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
//����չ��ϣ��Ĺ��캯��
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}
//�������Ĺ�ϣ����  ������������
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1; //�������룬���ڼ����ϣ����
  return std::hash<K>()(key) & mask;   //�Լ����й�ϣ���㲢���������룬�õ���ϣ����
}
// ��ȫ�����Ϊ2ʱ������Ϊ0b11���������Ƶ�3��
// ��ˣ���ϣ�������ǶԼ����й�ϣ�����Ľ���Ķ����Ʊ�ʾ�������λ��
// ���磬��ĳ�����Ĺ�ϣֵΪ0b1101�������ϣ����Ϊ0b01����ʮ���Ƶ�1

//��������ȫ����ȵ�ֵ
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);//����������
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}
//��ӦͰ�ı������
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index); //����˽�к��� ��ȡָ��������Ͱ�ı�����ȵ�ֵ
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}
//����Ͱ������
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}
//���Ҹ�������ֵ����������洢��value������
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);//���Ĺ�ϣ����
  auto target_bucket = dir_[index];//��Ӧ��Ͱ

  return target_bucket->Find(key, value); //��Ŀ��Ͱ�в��Ҽ�������ֵ�洢��value������
}
//�Ƴ������ļ�ֵ��
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  return target_bucket->Remove(key);//Ŀ��Ͱ���Ƴ���
}
//�����µļ�ֵ��
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {//��Ŀ��Ͱ����������Ҫ������չ
    auto index = IndexOf(key);//������Ĺ�ϣ����
    auto target_bucket = dir_[index];//��ȡ��Ӧ��Ͱ
     // ���Ŀ��Ͱ�ı�����ȵ���ȫ�����
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);//��չĿ¼��СΪ��ǰ������
      for (int i = 0; i < capacity; i++) {//�����Ժ� �µ�Ŀ¼����Ͱָ��
        dir_[i + capacity] = dir_[i];
      }
    }
    //Ͱ�ķ���
    int mask = 1 << target_bucket->GetDepth(); // �������룬���ڻ��ּ�����Ͱ���Ͱ
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);// ������Ͱ0��Ͱ�����+1
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);// ������Ͱ1
    //����Ͱ�еļ�ֵ�����·��䵽��Ͱ
    for (const auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }
    //����Ͱ������
    num_buckets_++;
    //������Ŀ¼��ÿ��i��ת��Ϊ�����ƣ�������ָ��ԭ��Ŀ��Ͱ��Ͱָ��
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

  for (auto &item : target_bucket->GetItems()) {//��Ŀ��Ͱ�в��Ҽ�
    if (item.first == key) { // ����ҵ���������¶�Ӧ��ֵ
      item.second = value;
      return;
    }
  }

  target_bucket->Insert(key, value); // ������Ŀ��Ͱ�в����µļ�ֵ��
}

//===--------------------------------------------------------------------===//
// Bucket ��ϣ���Ͱ
//===--------------------------------------------------------------------===//
template <typename K, typename V>
// Ͱ�Ĺ��캯������ʼ��Ͱ�Ĵ�С�;ֲ����
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
//std::any_of �㷨�������Χ�е�Ԫ�أ�����ÿ��Ԫ��Ӧ��ν�ʣ�ֱ��ν�ʷ��� true �����������Ԫ��
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const auto &item) {
    if (item.first == key) {// �����ƥ��ɹ����򽫶�Ӧ��ֵ�洢��value������
      value = item.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    if (item.first == key) {// �����ƥ��ɹ������Ƴ���Ӧ�ļ�ֵ��
      this->list_.remove(item);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {// ���Ͱ�����������ʧ��
    return false;
  }
  list_.emplace_back(key, value);// ������Ͱ��ĩβ�����ֵ��
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;// ʵ��������չ��ϣ��ģ��
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
