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
#ifndef SHMC_SHM_ARRAY_H_
#define SHMC_SHM_ARRAY_H_

#include <assert.h>
#include <string>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"

namespace shmc {

/* A fixed-size array container
 * @Node   type of array node, must be StandardLayoutType
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 *
 * This container uses a pre-allocated array of fixed-size nodes in shm.

 * Read-Write concurrency safety should be considered by the user, such as
 * race condition of reading one node and writing of the same node.
 */
template <class Node, class Alloc = SVIPC>
class ShmArray {
 public:
  ShmArray() = default;

  /* Initializer for READ & WRITE
   * @shm_key  key or name of the shm to attach or create
   * @size     size of the shm to attach or create
   *
   * This initializer should be called by the container writer/owner and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return   true if succeed
   */
  bool InitForWrite(const std::string& shm_key, size_t size);

  /* Initializer for READ-ONLY
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the container reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key);

  /* Get const node reference by index
   * @index    index of array
   *
   * @return   const node reference
   */
  const volatile Node& operator[](int index) const {
    assert(shm_.is_initialized() && index >= 0 &&
           static_cast<uint64_t>(index) < shm_->node_num);
    return shm_->nodes[index];
  }

  /* Get node reference by index
   * @index    index of array
   *
   * @return   node reference
   */
  volatile Node& operator[](int index) {
    assert(shm_.is_initialized() && index >= 0 &&
           static_cast<uint64_t>(index) < shm_->node_num);
    return shm_->nodes[index];
  }

  /* getter
   *
   * @return  total size of array
   */
  size_t size() const {
    assert(shm_.is_initialized());
    return shm_->node_num;
  }

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmArray);

  struct ShmHead {
    volatile uint64_t magic;
    volatile uint32_t ver;
    volatile uint32_t head_size;
    volatile uint32_t padding;
    volatile uint32_t node_size;
    volatile uint64_t node_num;
    volatile uint64_t create_time;
    volatile char reserved[64];
    volatile Node nodes[0];
  };
  static_assert(sizeof(ShmHead) == 104, "unexpected ShmHead layout");
  static_assert(alignof(Node) <= 8, "align requirement of Node can't be met");

  ShmHandle<ShmHead, Alloc> shm_;
};

template <class Node, class Alloc>
bool ShmArray<Node, Alloc>::InitForWrite(const std::string& shm_key,
                                         size_t size) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmArray::InitForWrite: already initialized\n");
  }
  size_t shm_size = sizeof(ShmHead) + sizeof(Node) * size;
  if (!shm_.InitForWrite(shm_key, shm_size, Utils::DefaultCreateFlags())) {
    SHMC_ERR_RET("ShmArray::InitForWrite: shm init(%s, %lu) fail\n",
                                    shm_key.c_str(), shm_size);
  }
  uint64_t now_ts = time(NULL);
  if (shm_.is_newly_created()) {
    shm_->magic = Utils::GenMagic("Array");
    shm_->ver = Utils::MakeVer(1, 0);
    shm_->head_size = sizeof(ShmHead);
    shm_->node_size = sizeof(Node);
    shm_->node_num = size;
    shm_->create_time = now_ts;
  } else {
    if (shm_->magic != Utils::GenMagic("Array"))
      SHMC_ERR_RET("ShmArray::InitForWrite: bad magic(0x%lx)\n", shm_->magic);
    if (Utils::MajorVer(shm_->ver) != 1)
      SHMC_ERR_RET("ShmArray::InitForWrite: bad ver(0x%x)\n", shm_->ver);
    if (shm_->head_size != sizeof(ShmHead))
      SHMC_ERR_RET("ShmArray::InitForWrite: bad head_size(%u)\n", shm_->head_size);
    if (shm_->node_size != sizeof(Node))
      SHMC_ERR_RET("ShmArray::InitForWrite: bad node_size(%u)\n", shm_->node_size);
    if (shm_->node_num != size)
      SHMC_ERR_RET("ShmArray::InitForWrite: bad node_num(%u)\n", shm_->node_num);
  }
  return true;
}

template <class Node, class Alloc>
bool ShmArray<Node, Alloc>::InitForRead(const std::string& shm_key) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmArray::InitForRead: already initialized\n");
  }
  if (!shm_.InitForRead(shm_key, sizeof(ShmHead))) {
    SHMC_ERR_RET("ShmArray::InitForRead: shm init(%s, %lu) fail\n",
                                    shm_key.c_str(), sizeof(ShmHead));
  }
  if (shm_->magic != Utils::GenMagic("Array"))
    SHMC_ERR_RET("ShmArray::InitForRead: bad magic(0x%lx)\n", shm_->magic);
  if (Utils::MajorVer(shm_->ver) != 1)
    SHMC_ERR_RET("ShmArray::InitForRead: bad ver(0x%x)\n", shm_->ver);
  if (shm_->head_size != sizeof(ShmHead))
    SHMC_ERR_RET("ShmArray::InitForRead: bad head_size(%u)\n", shm_->head_size);
  if (shm_->node_size != sizeof(Node))
    SHMC_ERR_RET("ShmArray::InitForRead: bad node_size(%u)\n", shm_->node_size);
  size_t shm_size = sizeof(ShmHead) + sizeof(Node) * shm_->node_num;
  if (!shm_.CheckSize(shm_size))
    SHMC_ERR_RET("ShmArray::InitForRead: bad shm size(%u)\n", shm_.size());
  return true;
}


}  // namespace shmc

#endif  // SHMC_SHM_ARRAY_H_
