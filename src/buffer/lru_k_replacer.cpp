//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {
//构造初始化
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
//驱逐
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  //1、先驱逐history_list
  if (curr_size_ == 0) {//当前大小为0
    return false;
  }
  //从历史列表的最后一个帧开始遍历
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {//可驱逐
      access_count_[frame] = 0;//访问数置零
      history_list_.erase(history_map_[frame]);
      history_map_.erase(frame);
      *frame_id = frame;//将帧标识赋值给frame_id  将被驱逐的帧的标识（frame）赋值给传入函数的参数 frame_id  //通过参数返回被驱逐的帧的标识
      curr_size_--;
      is_evictable_[frame] = false;
      return true;
    }
  }
  //缓存链表
  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      cache_list_.erase(cache_map_[frame]);
      cache_map_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      is_evictable_[frame] = false;
      return true;
    }
  }

  return false;
}
//访问一个帧，这个帧被访问以后，放到一个历史队列里面，访问频次达到阈K值就转移到缓存队列里面
//历史队列是先来先出，缓存队列是根据LRU算法去做一个淘汰
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  //帧太大超出边界
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  access_count_[frame_id]++;
  //1、访问频次达到k，转移到cache_list中去
  if (access_count_[frame_id] == k_) {
    //在历史队列中存在
    if(history_map_.count(frame_id) != 0U){
        auto it = history_map_[frame_id];     
        history_list_.erase(it);
    }
    history_map_.erase(frame_id);
    //2、添加帧到 缓存队列
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (access_count_[frame_id] > k_) {//更新到LRU的最前面，先删除，再放最前面
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}
//访问帧以后，设置为可淘汰
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  //如果帧没有出现过，不能设置为可驱逐
  if (access_count_[frame_id] == 0) {
    return;
  }

  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  //原来可被访问，现在被设置为不能访问
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;//设置为当前最新的状态
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  auto cnt = access_count_[frame_id];
  if (cnt == 0) {
    return;
  }
  //不可驱逐
  if (!is_evictable_[frame_id]) {
    throw std::exception();
  }
  if (cnt < k_) {
    history_list_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);

  } else {
    cache_list_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
  }
  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
