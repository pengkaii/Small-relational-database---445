//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  //为页缓冲池分配连续的内存空间
  pages_ = new Page[pool_size_];
  //创建可扩展哈希表用于页号到帧号的映射
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  //创建LRU-K替换器
  replacer_ = new LRUKReplacer(pool_size, replacer_k);
  // 初始状态下，每个页面都在空闲列表中
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  //检查是否有空闲页面
  bool is_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {//未被其他引用
      is_free_page = true;
      break;
    }
  }

  if (!is_free_page) {
    return nullptr;
  }
  // 分配一个新的页面标识
  //new ,fetch的区别，newpage是在缓冲池新创建一个页，磁盘没有。fetch页本身已经在缓冲池
  *page_id = AllocatePage();

  frame_id_t frame_id;
  //空闲链表不为空，拿一个帧id
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 使用替换策略LRU-K进行驱逐，获取一个被驱逐的帧
    // assert(replacer_->Evict(&frame_id));
    replacer_->Evict(&frame_id);//LRU-k算法里面的历史队列或者缓存队列
    // 获取被驱逐的帧对应的页面标识   pages_[frame_id] 对应 page_id
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    // 如果被驱逐的帧是脏页
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());// <----将脏页写入磁盘
      pages_[frame_id].is_dirty_ = false;
    }
    //重置帧对应缓存池页的内存
    pages_[frame_id].ResetMemory();

    page_table_->Remove(evicted_page_id);// 从可扩展哈希表 页面表中移除被驱逐的页面
  }

  page_table_->Insert(*page_id, frame_id);// 将新页面id和帧id插入到页面表中  可扩展哈希表的应用K，V 桶
  //缓冲池 的页的元数组
  pages_[frame_id].page_id_ = *page_id;// 设置帧的页面标识为新分配的页面标识
  pages_[frame_id].pin_count_ = 1;     // 将帧的引用计数设置为1

  replacer_->RecordAccess(frame_id);// 记录帧的访问
  replacer_->SetEvictable(frame_id, false);// 将帧设置为不可驱逐

  return &pages_[frame_id]; // 返回页的指针
}
//new ,fetch的区别，newpage是在缓冲池新创建一个页，磁盘没有。fetch页本身已经在缓冲池
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  //在page_table_可扩展哈希表中找page_id---frame_id
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;           //页访问次数加一
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];                //对应的页
  }
  //看页表是不是有空闲页
  bool is_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      is_free_page = true;
      break;
    }
  }

  if (!is_free_page) {
    return nullptr;
  }
  //空闲链表的 页有，取一个帧
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // assert(replacer_->Evict(&frame_id));
    //从替换策略LRU淘汰一个 页
    replacer_->Evict(&frame_id);
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();

    page_table_->Remove(evicted_page_id);//可扩展哈希表 去掉 淘汰页
  }

  page_table_->Insert(page_id, frame_id);//插入到可扩展哈希表里面

  pages_[frame_id].page_id_ = page_id;//页的元数据更新
  pages_[frame_id].pin_count_ = 1;
  //它将页面数据从磁盘加载到缓冲池管理器中的特定帧(frame_id)对应的页面中
  //需要访问某个页面时，但该页面尚未在缓冲池中
  //拿到一个帧号，缓冲池对应的页号，里面还没数据，从磁盘读取
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());//从磁盘读数据

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];//返回页
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  //如果不在缓冲池里面，pin为0
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }

  if (is_dirty) {
    //不能把原来脏的数据给取消
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  pages_[frame_id].pin_count_--;//对应页的访问次数-1

  if (pages_[frame_id].pin_count_ == 0) {
    //只有当被pin的数量减为0了，才能将这个frame换出（淘汰）
    //替换器淘汰页
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}
//不管其pin状态，刷新到磁盘上
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  //判断是否有效
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  //可扩展哈希表上是否存在
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  //页的页号，页的数据写到磁盘上，去除脏页
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);//<---
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {//缓冲池的每个页面
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {//find 就是可扩展哈希，在目标桶中查找键，并将值存储在value参数frame_id中
    return true;
  }
  
  //如果引用计数大于零，说明该页面仍然被其他地方引用，无法删除
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
//如果引用计数为零，则执行以下操作：  
//从替换策略中移除该页面的帧标识。
//清空页面的相关信息，包括重置内存、将页面标识置为无效、引用计数置为零、脏标志位置为false。
//从页面表中移除该页面。
//闲帧链表 free_list_ 的末尾，以便可以在需要时重新利用该帧。

  //LRU替换策略移除
  replacer_->Remove(frame_id);
  
  //页面信息清空
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);//释放页面标识为 page_id 的页面，将页面标识为未分配状态，以便在需要时可以重新分配给其他页面

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
