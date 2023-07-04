//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
//当前节点，父节点
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
//找key的第一大于等于的下标，返回两索引的差距
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) const -> int 
{
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&keyComparator](const auto &pair, auto k) {
    return keyComparator(pair.first, k) < 0;
  });
  return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator)
    -> int {
  auto distance_in_array = KeyIndex(key, keyComparator);   //大于等于key的第一位置
  if (distance_in_array == GetSize()) {                    //位置在数组末尾后一位
    *(array_ + distance_in_array) = {key, value};
    IncreaseSize(1);
    return GetSize();
  }

  if (keyComparator(array_[distance_in_array].first, key) == 0) {  //已经插入了key
    return GetSize();
  }

  std::move_backward(array_ + distance_in_array, array_ + GetSize(), array_ + GetSize() + 1);//从数组末尾后一位移动[给定范围的值]
  *(array_ + distance_in_array) = {key, value};//指定下标插入

  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
//叶子页的分裂一半
//调用方 本节点
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int start_split_indx = GetMinSize();
  SetSize(start_split_indx);//原来叶子页的大小，起分裂  本节点的大小更新
  recipient->CopyNFrom(array_ + start_split_indx, GetMaxSize() - start_split_indx);//将原叶子页的右半边分给新叶子页
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  //前节点（调用方） 的array_+GetSize()位置开始
  std::copy(items, items + size, array_ + GetSize());//GetSize()开始为0
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) const
    -> bool 
{
  int target_in_array = KeyIndex(key, keyComparator);                                              //找大于等于key的索引差距
  if (target_in_array == GetSize() || keyComparator(array_[target_in_array].first, key) != 0) {    //出界 || 指定位置不为key ，没找到key
    return false;
  }
  *value = array_[target_in_array].second;                                                         //有指定key位置，其值value
  return true;//找到了
}

INDEX_TEMPLATE_ARGUMENTS
//调用方：叶子节点
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int target_in_array = KeyIndex(key, keyComparator);//大于等于key的位置
  if (target_in_array == GetSize() || keyComparator(array_[target_in_array].first, key) != 0) {//出界||key不在当前页数组里面
    return GetSize();//没删
  }
  //从array_ + target_in_array位置开始往前移动
  std::move(array_ + target_in_array + 1, array_ + GetSize(), array_ + target_in_array);
  IncreaseSize(-1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  //调用方：本节点    的arry_全部转移到 前节点
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());//本节点的GetNextPageId()被设置为接收方的下一页（叶子）
  SetSize(0);//本节点的大小设为零
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  //调用方：右兄弟  array_
  auto first_item = GetItem(0);//得到键值 对
  std::move(array_ + 1, array_ + GetSize(), array_);//调用方的 从array_数组开始下标，下一位之后的往前移
  IncreaseSize(-1);
  recipient->CopyLastFrom(first_item);//移动到接受方（本节点）的最后一个位置
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  //调用方：左兄弟 array_
  auto last_item = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(last_item);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  *array_ = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
