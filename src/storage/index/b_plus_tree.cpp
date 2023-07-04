#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
//B+树列表初始化
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool 
{ 
    return root_page_id_ == INVALID_PAGE_ID; 
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool 
{
  root_page_id_latch_.RLock();                                       //上读锁
  auto leaf_page = FindLeaf(key, Operation::SEARCH, transaction);    //根据key找到叶子页
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());   //转化为B+树节点页

  ValueType v;
  auto existed = node->Lookup(key, &v, comparator_);                 //在叶子页数组里面 二分查找 有木有存在key
  
  leaf_page->RUnlatch();                                             //用完释放锁
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);    //释放页

  if (!existed) {  //不存在
    return false;
  }

  result->push_back(v);//存在key 对应的value
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool 
{
  root_page_id_latch_.WLock();    //上写锁
  transaction->AddIntoPageSet(nullptr);  // nullptr means root_page_id_latch_ 上一页上事务
  if (IsEmpty()) {            //空，新建树节点
    StartNewTree(key, value); //新建B+树叶子节点
    ReleaseLatchFromQueue(transaction);//当前节点是安全的，  释放事务的锁
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);//插入到叶子页
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_); //从缓冲池生一个页

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);  //b_plus_tree_page.cpp 顶部初始化叶子页

  leaf->Insert(key, value, comparator_);                       //将键值对 插入叶子页

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);    //用完该页就释放掉

  // UpdateRootPageId(1);
}

INDEX_TEMPLATE_ARGUMENTS
//插入到叶子节点
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool 
{
  auto leaf_page = FindLeaf(key, Operation::INSERT, transaction);//找到叶子页  事务上锁
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());//叶子页转化为叶子页节点

  auto size = node->GetSize();//原来叶子节点页数组 的元素大小
  auto new_size = node->Insert(key, value, comparator_);//新插入以后的 元素大小

  // duplicate key
  if (new_size == size) {   //相等，说明没插入，遇到相等的key跳出了  
  //当前节点没插入，说明是安全的
    ReleaseLatchFromQueue(transaction);//释放访问过的父节点页的锁
    leaf_page->WUnlatch();//解锁（当前叶节点）
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);//解页（缓冲池）
    return false;//插入失败
  }

  // leaf is not full   叶子页数组插入没满，插入成功，也要释放锁，页资源
  if (new_size < leaf_max_size_) {
    //插入没有满，不触发分裂，当前节点是安全的，并发控制
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // leaf is full, need to split  叶子页数组插入满了，需要分裂
  auto sibling_leaf_node = Split(node);//分裂后的兄弟叶子页数组  分裂后的兄弟节点
  //设置叶子页之间的双向链表
  sibling_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(sibling_leaf_node->GetPageId());

  //提上给父页的 key
  auto risen_key = sibling_leaf_node->KeyAt(0);//数组第一位
  InsertIntoParent(node, risen_key, sibling_leaf_node, transaction);//将该key插入到父页
  
  //释放资源
  leaf_page->WUnlatch();//本节点 解锁
  //缓冲池解页
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);//本节点解页
  buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);//兄弟节点解页
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>  //不确定是叶子节点还是内部节点
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);//从缓冲池新建页

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }

  N *new_node = reinterpret_cast<N *>(page->GetData());//B+树页节点
  new_node->SetPageType(node->GetPageType());//设置页节点类型（叶子页or内部页）

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);//叶子页转化为叶子页数组   叶子节点
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);//新叶子也数组   新叶子节点

    new_leaf->Init(page->GetPageId(), node->GetParentPageId(), leaf_max_size_);//叶子页数组初始化
    leaf->MoveHalfTo(new_leaf);//原来的叶子页数组拆分右边给新叶子页数组  本节点移动后半给新节点
  } else {//内页分裂
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);

    new_internal->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);//node->GetParentPageId()指向一样的父节点
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);//拆分不一样，值得看一下（转移的都要重置父页）
  }

  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
//插入到父页
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  //旧节点 是根节点，要创建一个新的节点R当作根节点
  if (old_node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);//缓冲池建立新页

    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }

    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());//新内页节点 根节点
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);//初始化

    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());//设置 新（父）页的数组元素键值对

    //两个分裂的原根节点，设置指向父页
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);//用完  释放页

    UpdateRootPageId(0);//只更新，不插入  因为新建了 新根节点页，更新新根节点页
    
    //当前节点完全的  释放事务组的访问锁资源
    ReleaseLatchFromQueue(transaction);
    return;
  }
  //旧节点不是根节点的情况
  //先拿旧节点的父节点
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());//缓冲池拿到父页
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  //key插入到父页 不分裂（安全） 并发控制
  if (parent_node->GetSize() < internal_max_size_) {
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    //将key插入新的parent_node父页
    
    //当前节点插入不分裂安全的，释放事务的访问锁，释放页
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  //key插入到父页 分裂
  //临时的新复制节点 的内存多了一个，插入不分裂
  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize() + 1)];
  //new了一个页  临时的  创新新节点
  auto *copy_parent_node = reinterpret_cast<InternalPage *>(mem);//普通页转内页
  //memcpy(目标起始地址，源起始地址，复制大小)
  //copy_parent_node 拿到了原来parent_node一样的资源
  std::memcpy(mem, parent_page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize()));
  
  //在临时的新复制父节点操作  先将key插入（因为内存多了一个，所以这个插入不分裂）
  copy_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  auto parent_new_sibling_node = Split(copy_parent_node);//还是要分裂，生成新的兄弟节点
  //向上提的 new_key
  KeyType new_key = parent_new_sibling_node->KeyAt(0);
  std::memcpy(parent_page->GetData(), mem,
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_parent_node->GetMinSize());
              //临时的新复制父页的左半区 复制给parent_page->GetData()
  //parent_page 与parent_node 相关联
  InsertIntoParent(parent_node, new_key, parent_new_sibling_node, transaction);//向上递归，看是否分裂
  
  //两个页释放
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_new_sibling_node->GetPageId(), true);
  delete[] mem;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();//上写锁
  transaction->AddIntoPageSet(nullptr);  // nullptr means root_page_id_latch_ 上级访问的锁写进事务

  if (IsEmpty()) {
    //空B+树，是安全的，释放事务的访问锁
    ReleaseLatchFromQueue(transaction);
    return;
  }
  //找到叶子普通页
  auto leaf_page = FindLeaf(key, Operation::DELETE, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());//转叶子页节点
  
  //node当前的叶子页数组的大小  等于 删除了key 的大小   相等于没删到
  if (node->GetSize() == node->RemoveAndDeleteRecord(key, comparator_)) {
    //没删，是安全的，并发控制
    ReleaseLatchFromQueue(transaction);//解事务
    leaf_page->WUnlatch();//解锁
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);//解页
    return;
  }
  

  //此时已经在叶子节点 删除了目标key
  //判断该叶子节点是否触发合并或者重分配
  auto node_should_delete = CoalesceOrRedistribute(node, transaction);

  //以下是释放资源的操作

  //先解锁，再解页
  //用完 解锁  （在FindLeaf（）已经自上向下加锁）
  leaf_page->WUnlatch();

  if (node_should_delete) {
    //当前节点删除，事务记录删除页的锁（哈希unordered_set记录）
    transaction->AddIntoDeletedPageSet(node->GetPageId());
  }
  
  //解页
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  
  //缓冲池的解页
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
  //对于一个事务(transaction)中的已删除页面(page_id)集合进行遍历，
  //并使用缓冲池管理器(buffer_pool_manager_)的DeletePage方法将这些页面从缓冲池中删除
  transaction->GetDeletedPageSet()->clear();//事务的set.clear()清空
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
//合并和重分配  node为叶子页 先偷再考虑合并
//已经删除后的
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  //当前节点为根节点 
  if (node->IsRootPage()) {
    auto root_should_delete = AdjustRoot(node);
    //安全（不分裂，不偷，不合并），事务解锁
    ReleaseLatchFromQueue(transaction);
    return root_should_delete;
  }
  //节点的大小 大于等于 最小分半大小，不需要合并
  //删除以后，依旧大于等于最小边界 （不合并）安全
  if (node->GetSize() >= node->GetMinSize()) {
    //安全
    ReleaseLatchFromQueue(transaction);
    return false;//没合并重分配过
  }
  //找到 父页 
  //转化为 父节点
  //目的是找到同父页的 兄弟页
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());//当前节点（已删的）的父节点
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto idx = parent_node->ValueIndex(node->GetPageId());//idx 为父节点的索引指向本节点（孩子节点）
  //parent_node父节点找node->GetPageId()孩子节点的索引idx  == idx指向node本节点
  //本节点node索引idx  （父节点的右边）
  if (idx > 0) {//本节点的 左边（合并）
    //左兄弟 合并
    auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(idx - 1));//同父节点的 左兄弟  //本节点的 左边（合并）
     //合并 也要给左兄弟节点上写锁
    sibling_page->WLatch();
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());//转化为兄弟节点  左兄弟节点

    //左兄弟节点大小 大于 分半大小，当前node本节点 可以偷左兄弟节点
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      Redistribute(sibling_node, node, parent_node, idx, true);//重分配 == 偷兄弟节点的 一个key
      //idx 为父节点的索引指向本节点（孩子节点）

      ReleaseLatchFromQueue(transaction);//释放上锁的资源
      
      //释放页的资源
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return false;//没有合并
    }

    // 上面if条件不满足，触发coalesce 合并
    //父节点的idx索引 指向 本节点（孩子） 
    auto parent_node_should_delete = Coalesce(sibling_node, node, parent_node, idx, transaction);

    if (parent_node_should_delete) {
        //执行删除操作，事务加对应锁资源
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }

    //用完 释放
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;
  }
   //父节点idx索引   指向 本节点
   //右兄弟 合并
   //本节点（index) --> 父节点（index+1)
  if (idx != parent_node->GetSize() - 1) {  //本节点的 右边（合并）
    auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(idx + 1));//同父的 右兄弟
    //合并 也要给右兄弟节点上写锁
    sibling_page->WLatch();
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());//右兄弟节点
    
    //右兄弟大小 大于 最小大小  （可偷）安全
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      Redistribute(sibling_node, node, parent_node, idx, false);//右兄弟偷一个
      
      //释放资源
      ReleaseLatchFromQueue(transaction);

      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return false;
    }
    // coalesce
    auto sibling_idx = parent_node->ValueIndex(sibling_node->GetPageId());//右兄弟索引 （父节点的右边）
    auto parent_node_should_delete = Coalesce(node, sibling_node, parent_node, sibling_idx, transaction);  // 合并
    transaction->AddIntoDeletedPageSet(sibling_node->GetPageId());
    if (parent_node_should_delete) {
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) -> bool {
  auto middle_key = parent->KeyAt(index); //父节点的索引index  指向本节点（孩子）

  if (node->IsLeafPage()) {
    //叶子节点
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *prev_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    //前节点(index-1) <-- 本节点（index)
    //本节点 的所有元素 转移到  前节点
    leaf_node->MoveAllTo(prev_leaf_node);
  } else {
    //内部节点
    //转移的元素页 重新设置父节点指针
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *prev_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    //前节点(index-1) <-- 本节点（index)
    internal_node->MoveAllTo(prev_internal_node, middle_key, buffer_pool_manager_);
  }
  
  //本节点全部转移前节点以后，还要删除父节点的指向索引
  parent->Remove(index);//删除父页的index

  return CoalesceOrRedistribute(parent, transaction);//递归父页是否会发现合并和重分配
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
//目的是从相邻节点 偷一个键值对 给当前节点
//重分配（偷）
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                                  bool from_prev) {
  //代码中的from_prev参数表示neighbor_leaf_node是leaf_node的前一个相邻叶子节点还是后一个相邻叶子节点。
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);//转化为叶子节点
    auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    
    
    if (!from_prev) {//leaf_node 后一个相邻的节点 neighbor_leaf_node  //右兄弟节点
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);//从相邻节点的第一个元素 投到 当前节点的最后位置
      parent->SetKeyAt(index + 1, neighbor_leaf_node->KeyAt(0));//父页面的index+1索引指向右兄弟节点的第一位置
    } else {//左兄弟节点
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));//更改父页面的index索引指向本节点的第一位置
    }
  } else {
    //内部节点的 重分配偷 == 需要将兄弟节点偷来的元素页重新设置父节点）
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

    if (!from_prev) {
        //右兄弟
        //相邻在后边
        //index 代表当前节点
        //本节点（index)  <- 右兄弟（index+1)
        // 将右兄弟节点的第一位置移动到当前节点的末尾
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(index + 1), buffer_pool_manager_);
        //更改父页面的index+1索引指向右兄弟节点的第一位置
      parent->SetKeyAt(index + 1, neighbor_internal_node->KeyAt(0));
    } else {
        //左兄弟
        //相邻节点在前边
        //左兄弟（index-1） ->  本节点index
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));//更改父页面的index索引指向本节点的第一位置
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  // 一个特殊情况 当根节点只剩一个孩子的时候把这个孩子变为根节点。
  //old_root_node是内部结点，且大小为1，表示内部结点其实已经没有key了，只有value。所以要把它的孩子节点但更新成新的根节点
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto *root_node = reinterpret_cast<InternalPage *>(old_root_node);//B+树的根节点内页
    auto only_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));//获得唯一孩子页
    auto *only_child_node = reinterpret_cast<BPlusTreePage *>(only_child_page->GetData());//转化为孩子节点
    only_child_node->SetParentPageId(INVALID_PAGE_ID);//孩子节点的父节点页为-1

    root_page_id_ = only_child_node->GetPageId();//设置根节点页id

    UpdateRootPageId(0);//更新在Head_Page根节点页ID

    buffer_pool_manager_->UnpinPage(only_child_page->GetPageId(), true);//用完，解孩子页
    return true;
  }
  //是叶子节点，大小为0，数组没有元素，直接删了就好（根节点也设置无效）
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
 //利用key值找到叶子结点 
 //然后获取当前叶子节点key值的index就是索引begin的位置
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();//根节点上读锁
  auto leftmost_page = FindLeaf(KeyType(), Operation::SEARCH, nullptr, true);//从根节点开始，向左依次查找直到找到最左边的叶子节点
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_page, 0);//返回一个包含该叶子节点的迭代器，其中偏移量为0
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leaf_page = FindLeaf(key, Operation::SEARCH);//最左边的叶子页
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto idx = leaf_node->KeyIndex(key, comparator_); //第一个大于等于的key
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);//返回一个包含该叶子页的迭代器
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto rightmost_page = FindLeaf(KeyType(), Operation::SEARCH, nullptr, false, true);
  auto *leaf_node = reinterpret_cast<LeafPage *>(rightmost_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_page, leaf_node->GetSize());//返回末尾的索引
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost,
                              bool rightMost) -> Page * 
{
  assert(operation == Operation::SEARCH ? !(leftMost && rightMost) : transaction != nullptr);

  assert(root_page_id_ != INVALID_PAGE_ID);
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  //并发控制  判断是否安全 事务释放页资源
  if (operation == Operation::SEARCH) {
    root_page_id_latch_.RUnlock();//根节点页解锁
    page->RLatch();//页 上读锁
  } else {
    page->WLatch();
    if (operation == Operation::DELETE && node->GetSize() > 2) {
      ReleaseLatchFromQueue(transaction);//当前节点是安全（不发生分裂，偷/合并）的，释放上面访问过的页资源
    }
    if (operation == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) {
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()) {
      ReleaseLatchFromQueue(transaction);
    }
  }

  while (!node->IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);

    //当前i_node页数组（键值） 的值（孩子索引） 在内页数组，第一key为invalid，value为page_child_id孩子页索引
    page_id_t child_node_page_id;
    if (leftMost) {//最左边的
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }
    assert(child_node_page_id > 0);

    //拿到孩子id，去缓冲池拿孩子普通页，再转化为孩子页节点
    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    

    //如果是读操作(search)，那则直接加锁，然后对上一层释放锁 
    //如果是写操作（insert/delete)，释放锁之前则要判断一下是否安全。
    //实现一个逐步加锁 + 逐步释放的新函数
    if (operation == Operation::SEARCH) {
      child_page->RLatch();//上读锁
      page->RUnlatch();//释放上一个父的锁资源  解上锁
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);//释放上一个父页的锁资源 解上页
    } else if (operation == Operation::INSERT) {
      child_page->WLatch();//孩子页上写锁
      transaction->AddIntoPageSet(page);//访问过的父页加入事务数组

      // child node is safe, release all locks on ancestors
      if (child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize() - 1) {
        ReleaseLatchFromQueue(transaction);//当前节点是安全（不发生分裂，偷/合并）的，释放上面访问过的页资源
      }
      if (!child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    } else if (operation == Operation::DELETE) {
      child_page->WLatch();//孩子页上写锁
      transaction->AddIntoPageSet(page);//访问过的父页加入事务数组

      // child node is safe, release all locks on ancestors 释放对祖先的所有锁
      if (child_node->GetSize() > child_node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    }

    page = child_page;
    node = child_node;
    //下一轮循环
  }

  return page;
}

INDEX_TEMPLATE_ARGUMENTS
//释放事务组的锁
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      this->root_page_id_latch_.WUnlock();//根节点 只解锁
    } else {
      page->WUnlatch();//解锁 unlatch
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);//解页 unpin
    }
  }
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  root_page_id_latch_.RLock();
  root_page_id_latch_.RUnlock();
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);//头页面的index_name_索引位置
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
