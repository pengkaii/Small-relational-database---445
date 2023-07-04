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
  //Ϊҳ����ط����������ڴ�ռ�
  pages_ = new Page[pool_size_];
  //��������չ��ϣ������ҳ�ŵ�֡�ŵ�ӳ��
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  //����LRU-K�滻��
  replacer_ = new LRUKReplacer(pool_size, replacer_k);
  // ��ʼ״̬�£�ÿ��ҳ�涼�ڿ����б���
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
  //����Ƿ��п���ҳ��
  bool is_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {//δ����������
      is_free_page = true;
      break;
    }
  }

  if (!is_free_page) {
    return nullptr;
  }
  // ����һ���µ�ҳ���ʶ
  //new ,fetch������newpage���ڻ�����´���һ��ҳ������û�С�fetchҳ�����Ѿ��ڻ����
  *page_id = AllocatePage();

  frame_id_t frame_id;
  //��������Ϊ�գ���һ��֡id
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // ʹ���滻����LRU-K�������𣬻�ȡһ���������֡
    // assert(replacer_->Evict(&frame_id));
    replacer_->Evict(&frame_id);//LRU-k�㷨�������ʷ���л��߻������
    // ��ȡ�������֡��Ӧ��ҳ���ʶ   pages_[frame_id] ��Ӧ page_id
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    // ����������֡����ҳ
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());// <----����ҳд�����
      pages_[frame_id].is_dirty_ = false;
    }
    //����֡��Ӧ�����ҳ���ڴ�
    pages_[frame_id].ResetMemory();

    page_table_->Remove(evicted_page_id);// �ӿ���չ��ϣ�� ҳ������Ƴ��������ҳ��
  }

  page_table_->Insert(*page_id, frame_id);// ����ҳ��id��֡id���뵽ҳ�����  ����չ��ϣ���Ӧ��K��V Ͱ
  //����� ��ҳ��Ԫ����
  pages_[frame_id].page_id_ = *page_id;// ����֡��ҳ���ʶΪ�·����ҳ���ʶ
  pages_[frame_id].pin_count_ = 1;     // ��֡�����ü�������Ϊ1

  replacer_->RecordAccess(frame_id);// ��¼֡�ķ���
  replacer_->SetEvictable(frame_id, false);// ��֡����Ϊ��������

  return &pages_[frame_id]; // ����ҳ��ָ��
}
//new ,fetch������newpage���ڻ�����´���һ��ҳ������û�С�fetchҳ�����Ѿ��ڻ����
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  //��page_table_����չ��ϣ������page_id---frame_id
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;           //ҳ���ʴ�����һ
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];                //��Ӧ��ҳ
  }
  //��ҳ���ǲ����п���ҳ
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
  //��������� ҳ�У�ȡһ��֡
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // assert(replacer_->Evict(&frame_id));
    //���滻����LRU��̭һ�� ҳ
    replacer_->Evict(&frame_id);
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();

    page_table_->Remove(evicted_page_id);//����չ��ϣ�� ȥ�� ��̭ҳ
  }

  page_table_->Insert(page_id, frame_id);//���뵽����չ��ϣ������

  pages_[frame_id].page_id_ = page_id;//ҳ��Ԫ���ݸ���
  pages_[frame_id].pin_count_ = 1;
  //����ҳ�����ݴӴ��̼��ص�����ع������е��ض�֡(frame_id)��Ӧ��ҳ����
  //��Ҫ����ĳ��ҳ��ʱ������ҳ����δ�ڻ������
  //�õ�һ��֡�ţ�����ض�Ӧ��ҳ�ţ����滹û���ݣ��Ӵ��̶�ȡ
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());//�Ӵ��̶�����

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];//����ҳ
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  //������ڻ�������棬pinΪ0
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }

  if (is_dirty) {
    //���ܰ�ԭ��������ݸ�ȡ��
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  pages_[frame_id].pin_count_--;//��Ӧҳ�ķ��ʴ���-1

  if (pages_[frame_id].pin_count_ == 0) {
    //ֻ�е���pin��������Ϊ0�ˣ����ܽ����frame��������̭��
    //�滻����̭ҳ
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}
//������pin״̬��ˢ�µ�������
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  //�ж��Ƿ���Ч
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  //����չ��ϣ�����Ƿ����
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  //ҳ��ҳ�ţ�ҳ������д�������ϣ�ȥ����ҳ
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);//<---
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {//����ص�ÿ��ҳ��
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {//find ���ǿ���չ��ϣ����Ŀ��Ͱ�в��Ҽ�������ֵ�洢��value����frame_id��
    return true;
  }
  
  //������ü��������㣬˵����ҳ����Ȼ�������ط����ã��޷�ɾ��
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
//������ü���Ϊ�㣬��ִ�����²�����  
//���滻�������Ƴ���ҳ���֡��ʶ��
//���ҳ��������Ϣ�����������ڴ桢��ҳ���ʶ��Ϊ��Ч�����ü�����Ϊ�㡢���־λ��Ϊfalse��
//��ҳ������Ƴ���ҳ�档
//��֡���� free_list_ ��ĩβ���Ա��������Ҫʱ�������ø�֡��

  //LRU�滻�����Ƴ�
  replacer_->Remove(frame_id);
  
  //ҳ����Ϣ���
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);//�ͷ�ҳ���ʶΪ page_id ��ҳ�棬��ҳ���ʶΪδ����״̬���Ա�����Ҫʱ�������·��������ҳ��

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
