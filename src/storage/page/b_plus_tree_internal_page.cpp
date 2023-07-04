//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, it);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &pair, auto k) { return comparator(pair.first, k) < 0; });
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return std::prev(target)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  //该节点是内页内点，新开的父页节点，设置第一索引key失效，value为旧页的值，第二索引的键值为新节点的键值
  //设置新父节点页 键值对
  SetKeyAt(1, new_key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
  SetSize(2);//内页数组键值对 大小
}

INDEX_TEMPLATE_ARGUMENTS
//新父页的指定位置插入key （在同一个页数组里面操作）
//调用方：父节点
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  auto new_value_idx = ValueIndex(old_value) + 1;//旧页的一半的下一位（旧值兄弟页的第一个key位置)
  std::move_backward(array_ + new_value_idx, array_ + GetSize(), array_ + GetSize() + 1);//将指定范围的值最终移到array_ + GetSize() + 1处
  //为插入new_value_idx 位置腾空间
  //指定位置插入键值对
  array_[new_value_idx].first = new_key;
  array_[new_value_idx].second = new_value;

  IncreaseSize(1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  //调用方：本节点 （内部节点）
  int start_split_indx = GetMinSize();
  int original_size = GetSize();
  SetSize(start_split_indx);//原内页的大小
  //新内页
  recipient->CopyNFrom(array_ + start_split_indx, original_size - start_split_indx, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  
  //调用方： 前节点
  std::copy(items, items + size, array_ + GetSize());//从新内页的开始位置开始  复制
  //转移过来的 元素页 都要重新设置父页
  for (int i = 0; i < size; i++) {
    auto page = buffer_pool_manager->FetchPage(ValueAt(i + GetSize()));//每个转移的新值 从缓冲池拿页
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());//转化为B+树页数组
    node->SetParentPageId(GetPageId());//设置父页为前节点本身页

    buffer_pool_manager->UnpinPage(page->GetPageId(), true);//用完  释放页
  }

  IncreaseSize(size);//扩展数量
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  ValueType only_value = ValueAt(0);
  SetSize(0);
  return only_value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  //调用方：本节点
  //本节点的 第一位置的key改为 父节点的索引index指向本节点（孩子）的key
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  //调用方：右兄弟 array_
  //相邻节点在后边
  SetKeyAt(0, middle_key);//设置右兄弟的第一位置 的key为 父节点的index+1对应的key
  auto first_item = array_[0];
  recipient->CopyLastFrom(first_item, buffer_pool_manager);//转移到接收方本节点的最后位置

  std::move(array_ + 1, array_ + GetSize(), array_);//往array_首位置前移
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  //本节点
  *(array_ + GetSize()) = pair;
  IncreaseSize(1);
  
  //分到本节点的 末尾键值对 要重新设置父节点指针
  auto page = buffer_pool_manager->FetchPage(pair.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());

  buffer_pool_manager->UnpinPage(page->GetPageId(), true);//解页
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  //调用方：左兄弟 array_
  //middle_key 为父节点的index对应的key
  auto last_item = array_[GetSize() - 1];
  recipient->SetKeyAt(0, middle_key);//设置本节点的第一位置 的key为 父节点的index对应的key

  recipient->CopyFirstFrom(last_item, buffer_pool_manager);//将last_item元素 转移到到接收方本节点的第一位置

  IncreaseSize(-1);
}
  //由于接收方节点此时还没有元素，所以直接将middle_key插入到第一个位置即可。
  //这样就保证了合并后的节点仍然是有序的，并且可以正确地更新它们在父节点中的位置。
  //表示将middle_key插入到recipient节点的第一个位置，以保证合并后的节点仍然是一个有序的B+树节点。
INDEX_TEMPLATE_ARGUMENTS
//接受方：本节点
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);//移动到array_ + GetSize() + 1为底，为接收方的第一位置腾空间
  *array_ = pair;//第一下标 赋值
  IncreaseSize(1);
  
  //分到本节点的 第一位置键值对 要重新设置父节点指针
  auto page = buffer_pool_manager->FetchPage(pair.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());//本节点刚偷来的键值对 的对应页设置父页（本节点的页）
  
  //解页
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
