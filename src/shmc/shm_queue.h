/* Copyright (c) 2016-2017, Bin Wei <bin@vip.qq.com>
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
#ifndef SHMC_SHM_QUEUE_H_
#define SHMC_SHM_QUEUE_H_

#include <assert.h>
#include <type_traits>
#include <string>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"

namespace shmc {

/* Descriptor of internal buffer of ShmQueue for zero-copy
 *
 * This descriptor containing a pointer and a length points to the
 * internal buffer of the queue which can be directly read or written.
 */
struct ZeroCopyBuf {
  void* ptr;
  size_t len;
};

/* A one-writer-one-reader FIFO queue
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 *
 * ShmQueue is implemented as a FIFO queue of fixed size. One writer/producer
 * is assumed to push new data to the tail of the queue, and one 
 * reader/consumer can read concurrently from the head of the queue. If 
 * producing of the writer is much faster than consuming of the reader the 
 * queue would be full and the writer would fail to push data until more data
 * is popped.
 *
 * Read-Write concurrency safety is offered internally in a lock-free manner,
 * but Write-Write or Read-Read is not. If you have multi-readers or 
 * multi-writers you can either 1) create a group of ShmQueue or 2) using 
 * external locks.
 */
template <class Alloc = SVIPC>
class ShmQueue {
 public:
  ShmQueue() = default;

  /* Initializer for write (as producer)
   * @shm_key         key or name of the shm to attach or create
   * @buf_size_bytes  size of the sync buffer
   *
   * This initializer should be called by the container producer and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return          true if succeed
   */
  bool InitForWrite(const std::string& shm_key,
                    size_t buf_size_bytes);

  /* Initializer for read (as consumer)
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the queue consumer and
   * it will try to attach an existing shm.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key);

  /* Push data to the queue
   * @buf  pointer to the data to be pushed
   * @len  size of @buf
   *
   * Push data of buffer to the queue unless the queue is full.
   *
   * @return  true if succeed
   */
  bool Push(const void* buf, size_t len);

  /* Push data to the queue
   * @data  data to be pushed
   *
   * Push data of string to the queue unless the queue is full.
   *
   * @return  true if succeed
   */
  bool Push(const std::string& data) {
    return Push(data.data(), data.size());
  }

  /* Pop data from the queue
   * @buf  [out] pointer to the buffer for popped data
   * @len  [in|out] pointer to length of the buffer
   *
   * Pop data from the queue unless the queue is empty. As input param *@len
   * should be the allocated length of @buf, and when the function returns
   * successfully *@len records the actual length of data popped.
   *
   * @return  true if succeed
   */
  bool Pop(void* buf, size_t* len);

  /* Pop data from the queue
   * @data  [out] pointer to data popped
   *
   * Pop data from the queue unless the queue is empty.
   *
   * @return  true if succeed
   */
  bool Pop(std::string* data);

  /* Prepare step of push data in a zero-copy way
   * @len  request size of the buffer to write
   * @zcb  [out] returned descriptor of the buffer to write
   *
   * This method first checks for free space of size @len and then returned
   * the @zcb pointer to the write buffer. It does not change state of the
   * queue so it is an idempotent operation.
   *
   * @return  true if succeed
   */
  bool ZeroCopyPushPrepare(size_t len, ZeroCopyBuf* zcb);

  /* Commit step of push data in a zero-copy way
   * @zcb  buffer descriptor returned by the prepare step
   *
   * Before calling this you should call prepare step to get the buffer to
   * write and then filling the buffer in any way. This method will check
   * integrity of the descriptor and then forward tail-pointer of the queue 
   * to append the buffer into the queue. For each push this method should
   * be called only once.
   *
   * @return  true if succeed
   */
  bool ZeroCopyPushCommit(const ZeroCopyBuf& zcb);

  /* Prepare step of pop data in a zero-copy way
   * @zcb  [out] returned descriptor of the buffer to read
   *
   * This method checks for first data-node at the head of the queue and then
   * returns the @zcb pointer to the data if found. It does not change state
   * of the queue so it is an idempotent operation.
   *
   * @return  true if succeed
   */
  bool ZeroCopyPopPrepare(ZeroCopyBuf* zcb);

  /* Commit step of pop data in a zero-copy way
   * @zcb  buffer descriptor returned by the prepare step
   *
   * Before calling this you should call prepare step to get the buffer to
   * read and then reading the buffer in any way. This method will check
   * integrity of the descriptor and then forward head-pointer of the queue 
   * to remove the buffer from the queue. For each pop this method should
   * be called only once.
   *
   * @return  true if succeed
   */
  bool ZeroCopyPopCommit(const ZeroCopyBuf& zcb);

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmQueue);

  struct ShmHead {
    volatile uint64_t magic;
    volatile uint32_t ver;
    volatile uint32_t head_size;
    volatile uint32_t align_size;
    volatile uint32_t padding1;
    volatile uint64_t head_pos;
    volatile uint64_t tail_pos;
    volatile uint64_t create_time;
    volatile uint64_t queue_buf_size;
    volatile uint8_t reserved[72];
    volatile uint8_t queue_buf[0];
  } __attribute__((__packed__));
  static_assert(sizeof(ShmHead) == 128, "unexpected ShmHead layout");

  static constexpr size_t kAlignSize = 64;
  static constexpr uint16_t kStartTag = 0x9cec;

  enum NodeType {
    kDataNode = 1, kEndFlagNode = 2
  };

  struct NodeHead {
    volatile uint16_t start_tag;
    volatile uint8_t type;
    volatile uint8_t flags;
    volatile uint32_t len;
    volatile uint8_t data[0];
  } __attribute__((__packed__));

  static_assert(sizeof(ShmHead) % kAlignSize == 0, "ShmHead size must align");
  static_assert(sizeof(NodeHead) <= kAlignSize, "NodeHead > kAlignSize");
  static_assert(sizeof(ShmHead) == 128, "unexpected ShmHead size");
  static_assert(sizeof(NodeHead) == 8, "unexpected NodeHead size");

  ShmHandle<ShmHead, Alloc> shm_;

 private:
  void ZeroCopyPushCommitUnsafe(const ZeroCopyBuf& zcb);
  void ZeroCopyPopCommitUnsafe(const ZeroCopyBuf& zcb);

  static size_t GetNodeSize(size_t node_data_len) {
    return Utils::RoundAlign<kAlignSize>(sizeof(NodeHead) + node_data_len);
  }

  volatile NodeHead* GetNodeHead(uint64_t pos) {
    return reinterpret_cast<volatile NodeHead*>(shm_->queue_buf + pos);
  }
};

template <class Alloc>
bool ShmQueue<Alloc>::InitForWrite(const std::string& shm_key,
                                   size_t buf_size_bytes) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmQueue::InitForWrite: already initialized\n");
  }
  if (buf_size_bytes < 1024) {
    SHMC_ERR_RET("ShmQueue::InitForWrite: invalid buf_size_bytes\n");
  }
  buf_size_bytes = Utils::RoundAlign<kAlignSize>(buf_size_bytes);
  size_t shm_size = sizeof(ShmHead) + buf_size_bytes;
  if (!shm_.InitForWrite(shm_key, shm_size, Utils::DefaultCreateFlags())) {
    SHMC_ERR_RET("ShmQueue::InitForWrite: shm_.InitForWrite(%s, %lu) fail\n",
                                                  shm_key.c_str(), shm_size);
    return false;
  }
  uint64_t now_ts = time(NULL);
  if (shm_.is_newly_created()) {
    shm_->magic = Utils::GenMagic("Queue");
    shm_->ver = Utils::MakeVer(1, 0);
    shm_->head_size = sizeof(ShmHead);
    shm_->align_size = kAlignSize;
    shm_->head_pos = 0;
    shm_->tail_pos = 0;
    shm_->create_time = now_ts;
    shm_->queue_buf_size = buf_size_bytes;
  } else {
    if (shm_->magic != Utils::GenMagic("Queue"))
      SHMC_ERR_RET("ShmQueue::InitForWrite: bad magic(0x%lx)\n", shm_->magic);
    if (Utils::MajorVer(shm_->ver) != 1)
      SHMC_ERR_RET("ShmQueue::InitForWrite: bad ver(0x%x)\n", shm_->ver);
    if (shm_->head_size != sizeof(ShmHead))
      SHMC_ERR_RET("ShmQueue::InitForWrite: bad head_size(%u)\n",
                                                shm_->head_size);
    if (shm_->align_size != kAlignSize)
      SHMC_ERR_RET("ShmQueue::InitForWrite: bad align_size(%u)\n",
                                                shm_->align_size);
    if ((shm_->head_pos & (kAlignSize - 1)) != 0 ||
        (shm_->tail_pos & (kAlignSize - 1)) != 0 ||
        (shm_->queue_buf_size & (kAlignSize - 1)) != 0) {
      SHMC_ERR_RET("ShmQueue::InitForWrite: bad alignment(%u,%u,%lu)\n",
          shm_->head_pos, shm_->tail_pos, shm_->queue_buf_size);
    }
    if (shm_->head_pos >= shm_->queue_buf_size ||
        shm_->tail_pos >= shm_->queue_buf_size) {
      SHMC_ERR_RET("ShmQueue::InitForWrite: bad head/tail_pos(%u,%u,%lu)\n",
          shm_->head_pos, shm_->tail_pos, shm_->queue_buf_size);
    }
  }
  return true;
}

template <class Alloc>
bool ShmQueue<Alloc>::InitForRead(const std::string& shm_key) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmQueue::InitForRead: already initialized\n");
  }
  if (!shm_.InitForWrite(shm_key, sizeof(ShmHead), kNoCreate)) {
    SHMC_ERR_RET("ShmQueue::InitForRead: shm_.InitForRead(%s, %lu) fail\n",
                                         shm_key.c_str(), sizeof(ShmHead));
  }
  if (shm_->magic != Utils::GenMagic("Queue"))
    SHMC_ERR_RET("ShmQueue::InitForRead: bad magic(0x%lx)\n", shm_->magic);
  if (Utils::MajorVer(shm_->ver) != 1)
    SHMC_ERR_RET("ShmQueue::InitForRead: bad ver(0x%x)\n", shm_->ver);
  if (shm_->head_size != sizeof(ShmHead))
    SHMC_ERR_RET("ShmQueue::InitForRead: bad head_size(%u)\n", shm_->head_size);
  if (shm_->align_size != kAlignSize)
    SHMC_ERR_RET("ShmQueue::InitForWrite: bad align_size(%u)\n",
                                              shm_->align_size);
  if ((shm_->head_pos & (kAlignSize - 1)) != 0 ||
      (shm_->tail_pos & (kAlignSize - 1)) != 0 ||
      (shm_->queue_buf_size & (kAlignSize - 1)) != 0) {
    SHMC_ERR_RET("ShmQueue::InitForWrite: bad alignment(%u,%u,%lu)\n",
        shm_->head_pos, shm_->tail_pos, shm_->queue_buf_size);
  }
  if (shm_->head_pos >= shm_->queue_buf_size ||
      shm_->tail_pos >= shm_->queue_buf_size) {
    SHMC_ERR_RET("ShmQueue::InitForWrite: bad head/tail_pos(%u,%u,%lu)\n",
        shm_->head_pos, shm_->tail_pos, shm_->queue_buf_size);
  }
  size_t shm_size = sizeof(ShmHead) + shm_->queue_buf_size;
  if (!shm_.CheckSize(shm_size))
    SHMC_ERR_RET("ShmQueue::InitForRead: bad shm size(%u)\n", shm_.size());
  return true;
}

template <class Alloc>
bool ShmQueue<Alloc>::Push(const void* buf, size_t len) {
  assert(shm_.is_initialized());
  ZeroCopyBuf zcb;
  if (!ZeroCopyPushPrepare(len, &zcb)) {
    return false;
  }
  memcpy(zcb.ptr, buf, len);
  ZeroCopyPushCommitUnsafe(zcb);
  return true;
}

template <class Alloc>
bool ShmQueue<Alloc>::Pop(void* buf, size_t* len) {
  assert(shm_.is_initialized());
  ZeroCopyBuf zcb;
  if (!ZeroCopyPopPrepare(&zcb)) {
    return false;
  }
  if (len) {
    if (*len < zcb.len) {
      return false;
    }
    if (buf) {
      memcpy(buf, zcb.ptr, zcb.len);
    }
    *len = zcb.len;
  }
  ZeroCopyPopCommitUnsafe(zcb);
  return true;
}

template <class Alloc>
bool ShmQueue<Alloc>::Pop(std::string* data) {
  assert(shm_.is_initialized());
  ZeroCopyBuf zcb;
  if (!ZeroCopyPopPrepare(&zcb)) {
    return false;
  }
  if (data) {
    data->assign(static_cast<char*>(zcb.ptr), zcb.len);
  }
  ZeroCopyPopCommitUnsafe(zcb);
  return true;
}

template <class Alloc>
bool ShmQueue<Alloc>::ZeroCopyPushPrepare(size_t len, ZeroCopyBuf* zcb) {
  assert(shm_.is_initialized());
  uint64_t head_pos = shm_->head_pos;
  uint64_t tail_pos = shm_->tail_pos;
  uint64_t queue_size = shm_->queue_buf_size;
  assert((head_pos & (kAlignSize - 1)) == 0);
  assert((tail_pos & (kAlignSize - 1)) == 0);
  assert((queue_size & (kAlignSize - 1)) == 0);
  uint64_t node_len = GetNodeSize(len);
  uint64_t append_tail_pos = tail_pos + node_len;
  uint64_t write_pos;
  if (head_pos <= tail_pos) {
    // by design limit: tail_pos never get to 0 again except initial state
    // this limit makes the code simpler and cleaner
    if (append_tail_pos < queue_size) {
      // append tail
      write_pos = tail_pos;
    } else if (node_len < head_pos) {
      // wrap round
      write_pos = 0;
      // add end-flag-node
      assert(tail_pos + sizeof(NodeHead) <= queue_size);
      volatile NodeHead* node_head = GetNodeHead(tail_pos);
      node_head->start_tag = kStartTag;
      node_head->type = static_cast<uint8_t>(NodeType::kEndFlagNode);
      node_head->flags = 0;
      node_head->len = queue_size - tail_pos - sizeof(NodeHead);
    } else {
      // no space
      return false;
    }
  } else {
    if (append_tail_pos < head_pos) {
      // append tail
      write_pos = tail_pos;
    } else {
      // no space
      return false;
    }
  }
  if (zcb) {
    zcb->ptr = const_cast<uint8_t*>(GetNodeHead(write_pos)->data);
    zcb->len = len;
  }
  return true;
}

template <class Alloc>
inline void ShmQueue<Alloc>::ZeroCopyPushCommitUnsafe(const ZeroCopyBuf& zcb) {
  volatile NodeHead* node_head = reinterpret_cast<NodeHead*>(
      static_cast<uint8_t*>(zcb.ptr) - sizeof(NodeHead));
  node_head->start_tag = kStartTag;
  node_head->type = static_cast<uint8_t>(NodeType::kDataNode);
  node_head->flags = 0;
  node_head->len = zcb.len;
  // tail_pos never get to 0 except initial state because of our limit by
  // design, so need not deal with wrap round condition here
  shm_->tail_pos = Utils::RoundAlign<kAlignSize>(
                 static_cast<uint8_t*>(zcb.ptr) + zcb.len - shm_->queue_buf);
}

template <class Alloc>
bool ShmQueue<Alloc>::ZeroCopyPushCommit(const ZeroCopyBuf& zcb) {
  assert(shm_.is_initialized());
  uint64_t head_pos = shm_->head_pos;
  uint64_t tail_pos = shm_->tail_pos;
  uint64_t queue_size = shm_->queue_buf_size;

  assert((head_pos & (kAlignSize - 1)) == 0);
  assert((tail_pos & (kAlignSize - 1)) == 0);
  assert((queue_size & (kAlignSize - 1)) == 0);
  assert(head_pos < queue_size && tail_pos < queue_size);
  assert(zcb.ptr);

  uint64_t write_pos = static_cast<uint8_t*>(zcb.ptr) - sizeof(NodeHead)
                                                      - shm_->queue_buf;
  size_t write_len = GetNodeSize(zcb.len);

  if (write_pos == tail_pos) {
    if (write_pos + write_len >= (head_pos <= tail_pos ? queue_size
                                                       : head_pos)) {
      Utils::Log(kWarning, "ShmQueue::ZeroCopyPushCommit: overflow\n");
      return false;
    }
  } else if (write_pos == 0) {
    volatile NodeHead* node_head = GetNodeHead(tail_pos);
    if (node_head->start_tag != kStartTag ||
        node_head->type != NodeType::kEndFlagNode) {
      Utils::Log(kWarning, "ShmQueue::ZeroCopyPushCommit: bad end-flag-node\n");
      return false;
    }
    if (write_len >= head_pos) {
      Utils::Log(kWarning, "ShmQueue::ZeroCopyPushCommit: overflow\n");
      return false;
    }
  } else {
    // invalid pos
    Utils::Log(kWarning, "ShmQueue::ZeroCopyPushCommit: invalid pos\n");
    return false;
  }

  ZeroCopyPushCommitUnsafe(zcb);
  return true;
}

template <class Alloc>
bool ShmQueue<Alloc>::ZeroCopyPopPrepare(ZeroCopyBuf* zcb) {
  assert(shm_.is_initialized());
  uint64_t head_pos = shm_->head_pos;
  uint64_t tail_pos = shm_->tail_pos;
  uint64_t queue_size = shm_->queue_buf_size;

  assert((head_pos & (kAlignSize - 1)) == 0);
  assert((tail_pos & (kAlignSize - 1)) == 0);
  assert((queue_size & (kAlignSize - 1)) == 0);
  assert(head_pos < queue_size && tail_pos < queue_size);

  if (head_pos == tail_pos) {
    // the queue is empty
    return false;
  }
  volatile NodeHead* node_head = GetNodeHead(head_pos);
  if (node_head->start_tag != kStartTag) {
    SHMC_ERR_RET("ShmQueue::ZeroCopyPopPrepare: bad start-tag(%hu) at %lu\n",
                                           node_head->start_tag, head_pos);
  }
  // wrap round condition
  if (node_head->type == NodeType::kEndFlagNode) {
    if (tail_pos == 0) {  // never happen by design
      SHMC_ERR_RET("ShmQueue::ZeroCopyPopPrepare: tail_pos at 0 again\n");
    }
    head_pos = 0;
    node_head = GetNodeHead(head_pos);
    if (node_head->start_tag != kStartTag) {
      SHMC_ERR_RET("ShmQueue::ZeroCopyPopPrepare: bad start-tag(%hu) @%lu\n",
                                             node_head->start_tag, head_pos);
    }
  }
  // data-node expected
  if (node_head->type != NodeType::kDataNode) {
    SHMC_ERR_RET("ShmQueue::ZeroCopyPopPrepare: bad node type(%hhu) @%lu\n",
                                                 node_head->type, head_pos);
  }
  if (head_pos + GetNodeSize(node_head->len) >
      (head_pos <= tail_pos ? tail_pos : queue_size - kAlignSize)) {
    SHMC_ERR_RET("ShmQueue::ZeroCopyPopPrepare: bad node len(%u) "
        "pos(%lu,%lu,%lu)\n", node_head->len, head_pos, tail_pos, queue_size);
  }
  if (zcb) {
    zcb->ptr = const_cast<uint8_t*>(node_head->data);
    zcb->len = node_head->len;
  }
  return true;
}

template <class Alloc>
inline void ShmQueue<Alloc>::ZeroCopyPopCommitUnsafe(const ZeroCopyBuf& zcb) {
  shm_->head_pos = Utils::RoundAlign<kAlignSize>(
      static_cast<uint8_t*>(zcb.ptr) + zcb.len - shm_->queue_buf);
}

template <class Alloc>
bool ShmQueue<Alloc>::ZeroCopyPopCommit(const ZeroCopyBuf& zcb) {
  assert(shm_.is_initialized());
  uint64_t head_pos = shm_->head_pos;
  uint64_t tail_pos = shm_->tail_pos;
  uint64_t queue_size = shm_->queue_buf_size;

  assert((head_pos & (kAlignSize - 1)) == 0);
  assert((tail_pos & (kAlignSize - 1)) == 0);
  assert((queue_size & (kAlignSize - 1)) == 0);
  assert(head_pos < queue_size && tail_pos < queue_size);
  assert(zcb.ptr);

  uint64_t read_pos = static_cast<uint8_t*>(zcb.ptr) - sizeof(NodeHead)
                                                     - shm_->queue_buf;
  if (head_pos == tail_pos) {
    Utils::Log(kWarning, "ShmQueue::ZeroCopyPopCommit: overflow\n");
    return false;
  }
  if (read_pos == head_pos) {
    if (zcb.len != GetNodeHead(read_pos)->len) {
      Utils::Log(kWarning, "ShmQueue::ZeroCopyPopCommit: bad len\n");
      return false;
    }
  } else if (read_pos == 0) {  // && read_pos != head_pos
    volatile NodeHead* node_head = GetNodeHead(head_pos);
    if (node_head->start_tag != kStartTag ||
        node_head->type != NodeType::kEndFlagNode) {
      Utils::Log(kWarning, "ShmQueue::ZeroCopyPopCommit: bad end-flag-node\n");
      return false;
    }
    if (zcb.len != GetNodeHead(read_pos)->len) {
      Utils::Log(kWarning, "ShmQueue::ZeroCopyPopCommit: bad len\n");
      return false;
    }
  } else {
    // invalid pos
    Utils::Log(kWarning, "ShmQueue::ZeroCopyPopCommit: invalid pos\n");
    return false;
  }

  ZeroCopyPopCommitUnsafe(zcb);
  return true;
}

}  // namespace shmc

#endif  // SHMC_SHM_QUEUE_H_
