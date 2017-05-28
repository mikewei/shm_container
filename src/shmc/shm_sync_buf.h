/* Copyright (c) 2015-2017, Bin Wei <bin@vip.qq.com>
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * The names of its contributors may not be used to endorse or 
 * promote products derived from this software without specific prior 
 * written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef SHMC_SHM_SYNC_BUF_H_
#define SHMC_SHM_SYNC_BUF_H_

#include <sys/time.h>
#include <assert.h>
#include <string>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"

namespace shmc {

/* Meta data of sync node
 */
struct SyncMeta {
  uint64_t seq;
  timeval time;
};

/* Iterator for reading SyncBuf
 */
struct SyncIter {
  uint64_t pos;
};

/* A one-writer-multi-reader broadcast queue useful for replica data sync
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 * 
 * ShmSyncBuf is implemented as a circular queue of fixed size. One writer is
 * assumed to push new data to the tail of the queue and internally recycle 
 * free space by removing oldest data of the queue when needed. Multi-readers 
 * can read concurrently from the queue and each can get a copy of the data
 * flow. Note that reader must be fast enough to keep pace with the writer,
 * otherwise the reader will fall behind the writer, and when the difference
 * overflows the queue size reader begins to lose data.
 *
 * Read-Write concurrency safety is offered internally in a lock-free manner,
 * but Write-Write is not and needs external synchronization.
 */
template <class Alloc = SVIPC>
class ShmSyncBuf {
 public:
  ShmSyncBuf() = default;

  /* Initializer for READ & WRITE
   * @shm_key         key or name of the shm to attach or create
   * @buf_size_bytes  size of the sync buffer
   *
   * This initializer should be called by the container writer/owner and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return          true if succeed
   */
  bool InitForWrite(const std::string& shm_key,
                    size_t buf_size_bytes);

  /* Initializer for READ-ONLY
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the container reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key);

  /* Push data to the sync buffer
   * @buf  pointer to the data to be pushed
   * @len  size of @buf
   * @ts   timestamp of data
   *
   * Push data of buffer with user specified timestamp to the queue.
   *
   * @return  true if succeed
   */
  bool Push(const void* buf, size_t len, const timeval& ts);

  /* Push data to the sync buffer
   * @buf  pointer to the data to be pushed
   * @len  size of @buf
   *
   * Push data of buffer to the queue. The timestamp of data is automatically
   * set using time(2) with precision of second. This is a tradeoff as getting
   * higher precision timestamp is much slower. User can also specify the 
   * timestamp explicitly using another overloaded version of this method.
   *
   * @return  true if succeed
   */
  bool Push(const void* buf, size_t len) {
    timeval ts{time(nullptr), 0};
    return Push(buf, len, ts);
  }

  /* Push data to the sync buffer
   * @data  data to be pushed
   * @ts    timestamp of data
   *
   * Push data of string with user specified timestamp to the queue.
   *
   * @return  true if succeed
   */
  bool Push(const std::string& data, const timeval& ts) {
    return Push(data.data(), data.size(), ts);
  }

  /* Push data to the sync buffer
   * @data  data to be pushed
   *
   * Push data of buffer to the queue. The timestamp of data is automatically
   * set using time(2) with precision of second. This is a tradeoff as getting
   * higher precision timestamp is much slower. User can also specify the 
   * timestamp explicitly using another overloaded version of this method.
   *
   * @return  true if succeed
   */
  bool Push(const std::string& data) {
    return Push(data.data(), data.size());
  }

  /* Get the queue head iterator
   *
   * Note that the head iterator may be same as the tail iterator if the
   * queue is empty.
   *
   * @return  iterator of queue head
   */
  SyncIter Head() const;

  /* Get the queue tail iterator
   *
   * Note that the tail iterator is the next position to write, so it does 
   * not point to a valid data node until a new data node is pushed in.
   *
   * @return  iterator of queue tail
   */
  SyncIter Tail() const;

  /* Move the iterator to the next data position
   * @it  the iterator to move
   *
   * This method will check the integrity of current data node and update it
   * to point to the next data node.
   *
   * @return  true if suceed
   */
  bool Next(SyncIter* it) const;

  /* Find data node by sequence
   * @seq  sequence to find
   * @it   [out] iterator of found data node
   *
   * This method is very efficient with time complexity of O(1) due to an 
   * internal index of sequence.
   *
   * @return  true if suceed
   */
  bool FindBySeq(uint64_t seq, SyncIter* it) const;

  /* Find data node by timestamp
   * @ts  timestamp to find
   * @it  [out] iterator of found data node
   *
   * This method is efficient with time complexity of O(log(N)) due to the
   * binary search algorithm used.
   *
   * @return  true if suceed
   */
  bool FindByTime(const timeval& ts, SyncIter* it) const;

  /* Read data node pointed by the iterator
   * @it    iterator to read
   * @meta  [out] meta data of the node
   * @buf   [out] user buf to copy node data to
   * @size  [in|out] size of @buf as input and size of node data as output
   *
   * This method will fill @meta and copy node data to @buf. The actual copy 
   * length is MIN(node-data-size, user-buf-size), that means if 
   * node-data-size < user-buf-size all node data will be copied to the user 
   * buf and if node-data-size > user-buf-size data will be truncated to fill
   * user-buf fully. In any case the real size of node data will be returned 
   * through the out param @size.
   *
   * @return   1 if read successfully, 
   *           0 if iterator is at tail of the queue and no data to read now,
   *          -1 if error found
   */
  int Read(const SyncIter& it, SyncMeta* meta, void* buf, size_t* len) const;

  /* Read data node pointed by the iterator
   * @it    iterator to read
   * @meta  [out] meta data of the node
   * @out   [out] output string to copy node data to
   *
   * This method will fill @meta and copy all node data to the string @out.
   *
   * @return   1 if read successfully, 
   *           0 if iterator is at tail of the queue and no data to read now,
   *          -1 if error found
   */
  int Read(const SyncIter& it, SyncMeta* meta, std::string* out) const;

  /* Read data node pointed by the iterator
   * @it    iterator to read
   * @meta  [out] meta data of the node
   *
   * This method will only read meta data of current node and fill @meta.
   *
   * @return   1 if read successfully, 
   *           0 if iterator is at tail of the queue and no data to read now,
   *          -1 if error found
   */
  int Read(const SyncIter& it, SyncMeta* meta) const {
    size_t len = 0;
    return Read(it, meta, nullptr, &len);
  }

  /* getter
   *
   * @return   total size of the sync buffer
   */
  size_t sync_buf_size() const {
    assert(shm_.is_initialized());
    return shm_->sync_buf_size;
  }

  /* getter
   *
   * @return   used size of the sync buffer
   */
  size_t used_size() const {
    assert(shm_.is_initialized());
    return GetUsedSize();
  }

  /* getter
   *
   * @return   free size of the sync buffer
   */
  size_t free_size() const {
    assert(shm_.is_initialized());
    size_t size = GetFreeSize();
    return (size < kOverwriteBufferSize ? 0 : size - kOverwriteBufferSize);
  }

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmSyncBuf);

  struct ShmHead {
    volatile uint64_t magic;
    volatile uint32_t ver;
    volatile uint32_t head_size;
    volatile uint64_t next_seq;
    volatile uint64_t head_pos;
    volatile uint64_t tail_pos;
    volatile uint64_t create_time;
    volatile uint64_t seq_index_size;
    volatile uint64_t sync_buf_size;
    volatile char reserved[64];
    volatile uint32_t seq_index[0];
    // volatile char sync_buf[0]; // 8-bytes aligned
  } __attribute__((__packed__));
  static_assert(sizeof(ShmHead) == 128, "unexpected ShmHead layout");

  static constexpr uint16_t kStartTag = 0x57c0;
  static constexpr uint16_t kEndTag   = 0x57c1;
  static constexpr size_t kOverwriteBufferSize = 1024*16;

  struct SyncNodeHead {
    volatile uint16_t start_tag;
    volatile uint16_t reserved;
    volatile uint32_t len;
    volatile uint64_t seq;
    volatile uint32_t time_sec;
    volatile uint32_t time_usec;
    volatile char data[0];
  } __attribute__((__packed__));
  static_assert(sizeof(SyncNodeHead) == 24, "unexpected SyncNodeHead layout");

  struct SyncNodeTail {
    volatile uint16_t end_tag;
    volatile uint16_t reserved;
    volatile uint32_t len;
  } __attribute__((__packed__));
  static_assert(sizeof(SyncNodeTail) == 8, "unexpected SyncNodeTail layout");

  ShmHandle<ShmHead, Alloc> shm_;

 private:
  volatile char* sync_buf_ptr() {
    return (volatile char*)(&shm_->seq_index[shm_->seq_index_size]);
  }
  const volatile char* sync_buf_ptr() const {
    return (const volatile char*)(&shm_->seq_index[shm_->seq_index_size]);
  }
  static constexpr size_t min_node_size() {
    return sizeof(SyncNodeHead) + sizeof(SyncNodeTail);
  }
  static size_t GetNodeTailOffset(size_t node_data_len) {
    return sizeof(SyncNodeHead) + Utils::RoundAlign<8>(node_data_len);
  }
  static size_t GetNodeSize(size_t node_data_len) {
    return min_node_size() + Utils::RoundAlign<8>(node_data_len);
  }
  const volatile SyncNodeHead* GetSyncNodeHead(uint64_t pos) const {
    if (!IsValidNodeHeadPos(pos)) {
      Utils::Log(kDebug, "GetSyncNodeHead 1\n");
      return nullptr;
    }
    const volatile char* sync_buf = sync_buf_ptr();
    auto node_head = (const volatile SyncNodeHead*)(&sync_buf[pos]);
    if (node_head->start_tag != kStartTag) {
      Utils::Log(kDebug, "GetSyncNodeHead 2\n");
      return nullptr;
    }
    return node_head;
  }
  const volatile SyncNodeTail* GetSyncNodeTail(uint64_t pos) const {
    if (!IsValidNodeTailPos(pos)) {
      return nullptr;
    }
    const volatile char* sync_buf = sync_buf_ptr();
    auto node_tail = (const volatile SyncNodeTail*)(&sync_buf[pos]);
    if (node_tail->end_tag != kEndTag) {
      return nullptr;
    }
    return node_tail;
  }

  bool IsValidNodeHeadPos(uint64_t pos) const {
    return (IsValidPos(pos) &&
           ((pos & 0x7UL) == 0) &&
           (pos + sizeof(SyncNodeHead) <= shm_->sync_buf_size));
  }
  bool IsValidNodeTailPos(uint64_t pos) const {
    return (IsValidPos(pos) &&
           ((pos & 0x7UL) == 0) &&
           (pos + sizeof(SyncNodeTail) <= shm_->sync_buf_size));
  }
  bool IsValidPos(uint64_t pos) const {
    uint64_t size = shm_->sync_buf_size;
    uint64_t head = shm_->head_pos;
    uint64_t tail = shm_->tail_pos;
    if (head >= size || tail >= size || pos >= size) {
      Utils::Log(kDebug, "IsValidPos 1\n");
      return false;
    }
    if (PosOff(head - 1, tail) >= kOverwriteBufferSize) {
      // access [head - kOverwriteBufferSize/4, tail) is assumed safe
      head = PosSub(head, kOverwriteBufferSize / 4);
    }
    if (head <= tail) {
      if (pos < head || pos >= tail) {
        Utils::Log(kDebug, "IsValidPos 2\n");
        return false;
      }
    } else {
      if (pos < head && pos >= tail) {
        Utils::Log(kDebug, "IsValidPos 3\n");
        return false;
      }
    }
    return true;
  }
  size_t GetUsedSize() const {
    return PosOff(shm_->tail_pos, shm_->head_pos);
  }
  size_t GetFreeSize() const {
    return shm_->sync_buf_size - 1 - GetUsedSize();
  }
  uint64_t PosAdd(uint64_t pos, size_t offset) const {
    uint64_t size = shm_->sync_buf_size;
    uint64_t npos = pos + offset;
    assert(offset < size);
    if (npos >= size) npos -= size;
    return npos;
  }
  uint64_t PosSub(uint64_t pos, size_t offset) const {
    uint64_t size = shm_->sync_buf_size;
    uint64_t npos = pos + size - offset;
    assert(offset < size);
    if (npos >= size) npos -= size;
    return npos;
  }
  uint64_t PosOff(uint64_t pos_to, uint64_t pos_from) const {
    uint64_t size = shm_->sync_buf_size;
    return (pos_to >= pos_from ? pos_to - pos_from
                               : pos_to + size - pos_from);
  }
};

template <class Alloc>
bool ShmSyncBuf<Alloc>::InitForWrite(const std::string& shm_key,
                                     size_t buf_size_bytes) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmSyncBuf::InitForWrite: already initialized\n");
  }
  if (buf_size_bytes < (kOverwriteBufferSize * 2)
      && buf_size_bytes > 0x800000000UL /* 2^32 x 8B */) {
    SHMC_ERR_RET("ShmSyncBuf::InitForWrite: invalid buf_size_bytes\n");
  }
  buf_size_bytes = Utils::RoundAlign<8>(buf_size_bytes);
  size_t seq_index_size = Utils::RoundAlign<2>(buf_size_bytes / min_node_size());
  size_t shm_size = sizeof(ShmHead) + sizeof(uint32_t) * seq_index_size
                                    + buf_size_bytes;
  if (!shm_.InitForWrite(shm_key, shm_size, Utils::DefaultCreateFlags())) {
    SHMC_ERR_RET("ShmSyncBuf::InitForWrite: shm_.InitForWrite(%s, %lu) fail\n",
                                       shm_key.c_str(), shm_size);
    return false;
  }
  uint64_t now_ts = time(NULL);
  if (shm_.is_newly_created()) {
    shm_->magic = Utils::GenMagic("SyncBuf");
    shm_->ver = Utils::MakeVer(1, 0);
    shm_->head_size = sizeof(ShmHead);
    shm_->next_seq = (time(NULL) << 32UL);
    shm_->head_pos = 0;
    shm_->tail_pos = 0;
    shm_->create_time = now_ts;
    shm_->seq_index_size = seq_index_size;
    shm_->sync_buf_size = buf_size_bytes;
  } else {
    if (shm_->magic != Utils::GenMagic("SyncBuf"))
      SHMC_ERR_RET("ShmSyncBuf::InitForWrite: bad magic(0x%lx)\n", shm_->magic);
    if (Utils::MajorVer(shm_->ver) != 1)
      SHMC_ERR_RET("ShmSyncBuf::InitForWrite: bad ver(0x%x)\n", shm_->ver);
    if (shm_->head_size != sizeof(ShmHead))
      SHMC_ERR_RET("ShmSyncBuf::InitForWrite: bad head_size(%u)\n", shm_->head_size);
  }
  return true;
}

template <class Alloc>
bool ShmSyncBuf<Alloc>::InitForRead(const std::string& shm_key) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmSyncBuf::InitForRead: already initialized\n");
  }
  if (!shm_.InitForRead(shm_key, sizeof(ShmHead))) {
    SHMC_ERR_RET("ShmSyncBuf::InitForRead: shm_.InitForRead(%s, %lu) fail\n",
                                      shm_key.c_str(), sizeof(ShmHead));
  }
  if (shm_->magic != Utils::GenMagic("SyncBuf"))
    SHMC_ERR_RET("ShmSyncBuf::InitForRead: bad magic(0x%lx)\n", shm_->magic);
  if (Utils::MajorVer(shm_->ver) != 1)
    SHMC_ERR_RET("ShmSyncBuf::InitForRead: bad ver(0x%x)\n", shm_->ver);
  if (shm_->head_size != sizeof(ShmHead))
    SHMC_ERR_RET("ShmSyncBuf::InitForRead: bad head_size(%u)\n", shm_->head_size);
  size_t shm_size = sizeof(ShmHead) + sizeof(uint32_t) * shm_->seq_index_size
                                    + shm_->sync_buf_size;
  if (!shm_.CheckSize(shm_size))
    SHMC_ERR_RET("ShmSyncBuf::InitForRead: bad shm size(%u)\n", shm_.size());
  return true;
}

template <class Alloc>
bool ShmSyncBuf<Alloc>::Push(const void* buf, size_t len, const timeval& ts) {
  assert(shm_.is_initialized());
  // free old nodes
  // keep kOverwriteBufferSize buffer to avoid R-W race condition
  uint64_t sync_buf_size = shm_->sync_buf_size;
  constexpr size_t max_free_nodes = 10 + kOverwriteBufferSize / min_node_size();
  size_t new_node_size = GetNodeSize(len);
  if (new_node_size + kOverwriteBufferSize > sync_buf_size) {
    return false;  // too big
  }
  size_t free_count = 0;
  SyncIter it = Head();
  while (GetFreeSize() < new_node_size + kOverwriteBufferSize) {
    if (!Next(&it)) {
      return false;
    }
    shm_->head_pos = it.pos;
    if (++free_count > max_free_nodes) {
      return false;
    }
  }
  // write new node
  uint64_t tail_pos = shm_->tail_pos;
  if ((tail_pos & 0x7UL) != 0) {
    return false;  // shm data maybe corrupted
  }
  if (tail_pos + sizeof(SyncNodeHead) > sync_buf_size) {
    return false;  // shm data maybe corrupted
  }
  // write new node head
  auto node_head = (volatile SyncNodeHead*)(sync_buf_ptr() + tail_pos);
  node_head->start_tag = kStartTag;
  node_head->len = len;
  node_head->seq = shm_->next_seq;
  node_head->time_sec = ts.tv_sec;
  node_head->time_usec = ts.tv_usec;
  // write new node data
  size_t ntail_off = GetNodeTailOffset(len);
  if (tail_pos + ntail_off <= sync_buf_size) {
    memcpy(const_cast<char*>(node_head->data), buf, len);
  } else {
    size_t part_len = sync_buf_size - (tail_pos + sizeof(SyncNodeHead));
    memcpy(const_cast<char*>(node_head->data), buf, part_len);
    memcpy(const_cast<char*>(sync_buf_ptr()),
           static_cast<const char*>(buf) + part_len, len - part_len);
  }
  // write new node tail
  uint64_t node_tail_pos = PosAdd(tail_pos, ntail_off);
  if ((node_tail_pos & 0x7UL) != 0) {
    return false;  // shm data maybe corrupted
  }
  auto node_tail = (volatile SyncNodeTail*)(sync_buf_ptr() + node_tail_pos);
  node_tail->end_tag = kEndTag;
  node_tail->len = len;
  // commit new node
  size_t seq_idx_off = shm_->next_seq % shm_->seq_index_size;
  shm_->seq_index[seq_idx_off] = static_cast<uint32_t>(tail_pos >> 3);
  shm_->next_seq++;
  uint64_t new_tail_pos = PosAdd(tail_pos, new_node_size);
  if (new_tail_pos + sizeof(SyncNodeHead) > sync_buf_size) {
    new_tail_pos = 0;
  }
  shm_->tail_pos = new_tail_pos;
  return true;
}

template <class Alloc>
SyncIter ShmSyncBuf<Alloc>::Head() const {
  assert(shm_.is_initialized());
  return SyncIter{shm_->head_pos};
}

template <class Alloc>
SyncIter ShmSyncBuf<Alloc>::Tail() const {
  assert(shm_.is_initialized());
  return SyncIter{shm_->tail_pos};
}

template <class Alloc>
bool ShmSyncBuf<Alloc>::Next(SyncIter* it) const {
  assert(shm_.is_initialized());
  uint64_t tail_pos = shm_->tail_pos;
  if (it->pos == tail_pos) {
    return false;
  }
  const volatile SyncNodeHead* node_head = GetSyncNodeHead(it->pos);
  if (!node_head) {
    it->pos = tail_pos;
    return false;
  }
  size_t data_len = node_head->len;
  if (GetNodeSize(data_len) > PosOff(tail_pos, it->pos)) {
    it->pos = tail_pos;
    return false;
  }
  uint64_t next_pos = PosAdd(it->pos, GetNodeSize(data_len));
  if (next_pos + sizeof(SyncNodeHead) > shm_->sync_buf_size) {
    next_pos = 0;
  }
  it->pos = next_pos;
  return true;
}

template <class Alloc>
bool ShmSyncBuf<Alloc>::FindBySeq(uint64_t seq, SyncIter* it) const {
  assert(shm_.is_initialized());
  size_t seq_idx_off = seq % shm_->seq_index_size;
  uint64_t pos = static_cast<uint64_t>(shm_->seq_index[seq_idx_off]) << 3UL;
  const volatile SyncNodeHead* node_head = GetSyncNodeHead(pos);
  if (!node_head) {
    Utils::Log(kWarning, "ShmSyncBuf::FindBySeq: GetSyncNodeHead "
                        "for seq(0x%lx) fail\n", seq);
    return false;
  }
  if (node_head->seq != seq) {
    Utils::Log(kWarning, "ShmSyncBuf::FindBySeq: bad seq(0x%lx) fail\n", seq);
    return false;
  }
  it->pos = pos;
  return true;
}

template <class Alloc>
bool ShmSyncBuf<Alloc>::FindByTime(const timeval& ts, SyncIter* it) const {
  assert(shm_.is_initialized());
  const volatile SyncNodeHead* node_head = GetSyncNodeHead(Head().pos);
  if (!node_head) {
    return false;
  }
  SyncIter iter;
  SyncMeta meta;
  uint64_t seq_left = node_head->seq;
  uint64_t seq_right = shm_->next_seq - 1;
  while (seq_left < seq_right) {
    uint64_t seq_mid = (seq_left + seq_right) / 2;
    if (!FindBySeq(seq_mid, &iter)  ||
        (Read(iter, &meta) != 1)    ||
        timercmp(&meta.time, &ts, <)) {
      seq_left = seq_mid + 1;
    } else {
      seq_right = seq_mid;
    }
  }
  if (!FindBySeq(seq_right, &iter) ||
      (Read(iter, &meta) != 1)     ||
      timercmp(&meta.time, &ts, <)) {
    return false;
  }
  *it = iter;
  return true;
}

template <class Alloc>
int ShmSyncBuf<Alloc>::Read(const SyncIter& it,
                            SyncMeta* meta,
                            void* buf, size_t* len) const {
  assert(shm_.is_initialized());
  uint64_t tail_pos = shm_->tail_pos;
  if (it.pos == tail_pos) {
    meta->seq = shm_->next_seq;
    memset(&meta->time, 0, sizeof meta->time);
    return 0;
  }
  const volatile SyncNodeHead* node_head = GetSyncNodeHead(it.pos);
  if (!node_head) {
    Utils::Log(kWarning, "ShmSyncBuf::Read: GetSyncNodeHead(%lu) fail\n",
                        it.pos);
    return -1;
  }
  size_t data_len = node_head->len;
  if (GetNodeSize(data_len) > PosOff(tail_pos, it.pos)) {
    Utils::Log(kWarning, "ShmSyncBuf::Read: bad node size\n");
    return -1;
  }
  meta->seq = node_head->seq;
  meta->time.tv_sec = node_head->time_sec;
  meta->time.tv_usec = node_head->time_usec;
  size_t buf_len = *len;
  *len = data_len;
  if (buf_len == 0) {  // only read meta
    return 1;
  }
  // check node tail
  uint64_t t_pos = PosAdd(it.pos, GetNodeTailOffset(data_len));
  const volatile SyncNodeTail* node_tail = GetSyncNodeTail(t_pos);
  if (!node_tail || node_tail->len != data_len) {
    Utils::Log(kWarning, "ShmSyncBuf::Read: GetSyncNodeTail(%lu) fail\n",
                        t_pos);
    return -1;
  }
  // read data
  size_t copy_len = (data_len < buf_len ? data_len : buf_len);
  size_t sync_buf_size = shm_->sync_buf_size;
  if (it.pos + GetNodeTailOffset(data_len) <= sync_buf_size) {
    memcpy(buf, const_cast<char*>(node_head->data), copy_len);
  } else {
    size_t part_len = sync_buf_size - (it.pos + sizeof(SyncNodeHead));
    size_t first_copy_len = (copy_len < part_len ? copy_len : part_len);
    memcpy(buf, const_cast<char*>(node_head->data), first_copy_len);
    memcpy(static_cast<char*>(buf) + first_copy_len,
           const_cast<char*>(sync_buf_ptr()),
           copy_len - first_copy_len);
  }
  return 1;
}

template <class Alloc>
int ShmSyncBuf<Alloc>::Read(const SyncIter& it,
                            SyncMeta* meta,
                            std::string* out) const {
  size_t len = 0;
  int r;
  if ((r = Read(it, meta, nullptr, &len)) <= 0) {
    return r;
  }
  out->resize(len);
  if ((r = Read(it, meta, &((*out)[0]), &len)) <= 0) {
    return r;
  }
  if (out->size() != len) {
    return -1;
  }
  return 1;
}

}  // namespace shmc

#endif  // SHMC_SHM_SYNC_BUF_H_
