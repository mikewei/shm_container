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
#ifndef SHMC_SHM_HASH_TABLE_M_H_
#define SHMC_SHM_HASH_TABLE_M_H_

#include <assert.h>
#include <string>
#include <utility>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"

namespace shmc {

/* A shm implementation of hash table (classic version, memory effecient)
 * @Key    type of search key, any type supporting =,==,std::hash
 * @Node   type of node which contains both key and value infomation
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 * 
 * This hash table implementation uses a pre-allocated array of fixed-size
 * nodes in shm. The array of nodes are treated as a ROW * COL matrix and the
 * hash colision is solved by rehashes on different rows. Compared to 
 * <ShmHashTable> this container can provides better performance of memory
 * usage (>90% if row_num > 50), but it is slower than the former one. 
 * Normally <ShmHashTable> as default hash-table container is all you need.
 *
 * Read-Write concurrency safety should be considered by the user, for example
 * race conditions such as key-readying by Node::Key() and key-writing of the
 * same node, such as value-reading and value-updating of the same node. 
 *
 * This is a low-level container, consider use <ShmHashMap> first.
 *
 * Param @Node can be any StandardLayoutType with a get-key method as below:
 *    pair<bool,Key> Node::Key() const;
 * first bool element of returned pair indicates whether the node is used,
 * and if true the second element holds the key of the node.
 */
template <class Key, class Node, class Alloc = SVIPC>
class ShmHashTableM {
 public:
  ShmHashTableM() = default;

  /* Initializer for READ & WRITE
   * @shm_key   key or name of the shm to attach or create
   * @col_num   columns of nodes, normally use capacity / row_num
   * @row_num   rows of nodes, 50 is a recommanded value
   *
   * This initializer should be called by the container writer/owner and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return    true if succeed
   */
  bool InitForWrite(const std::string& shm_key,
                    size_t col_num, size_t row_num,
                    void* user_arg = nullptr);

  /* Initializer for READ-ONLY
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the container reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key,
                    void* user_arg = nullptr);

  /* Find node by key
   * @key     key to find
   *
   * This const method returns pointer to const node (read-only) if found.
   * 
   * @return  pointer to node if found, otherwise nullptr
   */
  const volatile Node* Find(const Key& key) const;

  /* Find node by key
   * @key     key to find
   *
   * This method returns pointer to non-const node if found and user can 
   * update the content of the node. Note that Read-Write concurrency safety 
   * should be considered.
   * 
   * @return  pointer to node if found, otherwise nullptr
   */
  volatile Node* Find(const Key& key);

  /* Find node by key or allocate a new node
   * @key       key to find
   * @is_found  [out] whether the node is found by key if non-nullptr returned
   *
   * If a node with the key is found it returns pointer to the node, otherwise
   * it will find an empty node to return, if no empty node can be found it 
   * returns nullptr. If this method returns non-nullptr is_found indicates
   * whether the node returned is found by key.
   * 
   * @return  pointer to the found node or empty node or nullptr
   */
  volatile Node* FindOrAlloc(const Key& key, bool *is_found);

  /* Cursor type recording current travelling position
   */
  struct TravelPos {
    uint32_t row;
    uint32_t col;
    TravelPos(): row(0), col(0) {}
    bool at_origin() {
      return (row == 0 && col == 0);
    }
  };

  /* Travel nodes
   * @pos               [in|out] current position of travel
   * @max_travel_nodes  max nodes to travel in this method call, 0 for all
   * @f                 callback to be called when visiting each node
   *
   * This method will scan array of nodes and find non-empty nodes and
   * call the callback @f for each one. If @max_travel_nodes nodes have
   * been travelled this method will stop and save the position to @pos.
   *
   * @return            true if succeed
   */
  bool Travel(TravelPos* pos, size_t max_travel_nodes,
              std::function<void(volatile Node*)> f);

  /* Travel all nodes
   * @f                 callback to be called when visiting each node
   *
   * This method will scan array of nodes and find all non-empty nodes and
   * call the callback @f for each one.
   *
   * @return            true if succeed
   */
  bool Travel(std::function<void(volatile Node*)> f) {
    TravelPos pos;
    return Travel(&pos, 0, std::move(f));
  }

  /* getter
   *
   * @return  ideal capacity of the hash-table
   */
  size_t ideal_capacity() const {
    return capacity_;
  }

  /* getter
   *
   * The expected actual capacity is about 90% of ideal.
   *
   * @return  expected capacity of the hash-table
   */
  size_t expected_capacity() const {
    return ideal_capacity() * 90 / 100;
  }

 private:
  size_t GetIndex(size_t row, size_t col) const {
    return shm_->col_num * row + col;
  }

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmHashTableM);

  struct ShmHead {
    volatile uint64_t magic;
    volatile uint32_t ver;
    volatile uint32_t head_size;
    volatile uint32_t node_size;
    volatile uint32_t row_num;
    volatile uint32_t col_num;
    volatile uint32_t max_row_touched;
    volatile uint64_t create_time;
    volatile char reserved[64];
    volatile Node nodes[0];
  } __attribute__((__packed__));
  static_assert(sizeof(ShmHead) == 104, "unexpected ShmHead layout");
  static_assert(alignof(Node) <= 8, "align requirement of Node can't be met");

  ShmHandle<ShmHead, Alloc> shm_;

  // 50-rows is often a well performed setting
  static const size_t kMinRows = 10;
  static const size_t kMaxRows = 200;
  size_t row_mods_[kMaxRows];
  size_t capacity_  = 0;
  void* user_arg_  = nullptr;
};

template <class Key, class Node, class Alloc>
bool ShmHashTableM<Key, Node, Alloc>::InitForWrite(const std::string& shm_key,
                                                   size_t col_num,
                                                   size_t row_num,
                                                   void* user_arg) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmHashTableM::InitForWrite: already initialized\n");
  }
  if (row_num > kMaxRows || row_num < kMinRows || col_num <= row_num) {
    SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad col_num or row_num\n");
  }
  size_t node_size = sizeof(Node);
  size_t shm_size = sizeof(ShmHead) + node_size * col_num * row_num;
  user_arg_ = user_arg;
  if (!Utils::GetPrimeArray(col_num, row_num, row_mods_)) {
    SHMC_ERR_RET("ShmHashTableM::InitForWrite: get_prime_array fail\n");
  }
  capacity_ = 0;
  for (size_t i = 0; i < row_num; i++) {
    capacity_ += row_mods_[i];
  }
  if (!shm_.InitForWrite(shm_key, shm_size, Utils::DefaultCreateFlags())) {
    SHMC_ERR_RET("ShmHashTableM::InitForWrite: shm init(%s, %lu) fail\n",
                                          shm_key.c_str(), shm_size);
  }
  uint64_t now_ts = time(NULL);
  if (shm_.is_newly_created()) {
    shm_->magic = Utils::GenMagic("HashTab");
    shm_->ver = Utils::MakeVer(1, 0);
    shm_->head_size = sizeof(ShmHead);
    shm_->node_size = node_size;
    shm_->row_num = row_num;
    shm_->col_num = col_num;
    shm_->create_time = now_ts;
    shm_->max_row_touched = 0;
  } else {
    if (shm_->magic != Utils::GenMagic("HashTab"))
      SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad magic(0x%lx)\n", shm_->magic);
    if (Utils::MajorVer(shm_->ver) != 1)
      SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad ver(0x%x)\n", shm_->ver);
    if (shm_->head_size != sizeof(ShmHead))
      SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad head_size(%u)\n", shm_->head_size);
    if (shm_->node_size != node_size)
      SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad node_size(%u)\n", shm_->node_size);
    if (shm_->row_num != row_num)
      SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad row_num(%u)\n", shm_->row_num);
    if (shm_->col_num != col_num)
      SHMC_ERR_RET("ShmHashTableM::InitForWrite: bad col_num(%u)\n", shm_->col_num);
  }
  return true;
}

template <class Key, class Node, class Alloc>
bool ShmHashTableM<Key, Node, Alloc>::InitForRead(const std::string& shm_key,
                                                  void* user_arg) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmHashTableM::InitForRead: already initialized\n");
  }
  size_t node_size = sizeof(Node);
  user_arg_ = user_arg;
  if (!shm_.InitForRead(shm_key, sizeof(ShmHead))) {
    SHMC_ERR_RET("ShmHashTableM::InitForRead: shm init(%s, %lu) fail\n",
                                         shm_key.c_str(), sizeof(ShmHead));
  }
  if (shm_->magic != Utils::GenMagic("HashTab"))
    SHMC_ERR_RET("ShmHashTableM::InitForRead: bad magic(0x%lx)\n", shm_->magic);
  if (Utils::MajorVer(shm_->ver) != 1)
    SHMC_ERR_RET("ShmHashTableM::InitForRead: bad ver(0x%x)\n", shm_->ver);
  if (shm_->head_size != sizeof(ShmHead))
    SHMC_ERR_RET("ShmHashTableM::InitForRead: bad head_size(%u)\n", shm_->head_size);
  if (shm_->node_size != node_size)
    SHMC_ERR_RET("ShmHashTableM::InitForRead: bad node_size(%u)\n", shm_->node_size);
  size_t shm_size = sizeof(ShmHead) + node_size * shm_->col_num * shm_->row_num;
  if (!shm_.CheckSize(shm_size))
    SHMC_ERR_RET("ShmHashTableM::InitForRead: bad shm size(%u)\n", shm_.size());
  if (!Utils::GetPrimeArray(shm_->col_num, shm_->row_num, row_mods_)) {
    SHMC_ERR_RET("ShmHashTableM::InitForRead: GetPrimeArray fail");
  }
  capacity_ = 0;
  for (size_t i = 0; i < shm_->row_num; i++) {
    capacity_ += row_mods_[i];
  }
  return true;
}

template <class Key, class Node, class Alloc>
const volatile Node* ShmHashTableM<Key, Node, Alloc>::Find(const Key& key) const {
  assert(shm_.is_initialized());
  size_t hash_code = std::hash<Key>()(key);
  for (size_t r = 0; r < shm_->row_num; r++) {
    size_t index = GetIndex(r, hash_code % row_mods_[r]);
    const volatile Node* node = &shm_->nodes[index];
    auto key_info = node->Key();
    if (key_info.first && key == key_info.second) {
        return node;
    }
  }
  return nullptr;
}

template <class Key, class Node, class Alloc>
volatile Node* ShmHashTableM<Key, Node, Alloc>::Find(const Key& key) {
  assert(shm_.is_initialized());
  size_t hash_code = std::hash<Key>()(key);
  for (size_t r = 0; r < shm_->row_num; r++) {
    size_t index = GetIndex(r, hash_code % row_mods_[r]);
    volatile Node* node = &shm_->nodes[index];
    auto key_info = node->Key();
    if (key_info.first && key == key_info.second) {
        return node;
    }
  }
  return nullptr;
}

template <class Key, class Node, class Alloc>
volatile Node* ShmHashTableM<Key, Node, Alloc>::FindOrAlloc(const Key& key,
                                                            bool* is_found) {
  assert(shm_.is_initialized());
  volatile Node* empty_node = nullptr;
  if (is_found) *is_found = false;
  size_t hash_code = std::hash<Key>()(key);
  for (size_t r = 0; r < shm_->row_num; r++) {
    size_t index = GetIndex(r, hash_code % row_mods_[r]);
    volatile Node* node = &shm_->nodes[index];
    auto key_info = node->Key();
    if (key_info.first) {  // not empty
      if (key == key_info.second) {
        if (is_found) *is_found = true;
        return node;
      }
    } else if (!empty_node) {  // emtpy
      empty_node = node;
      if (r > shm_->max_row_touched)
        shm_->max_row_touched = r;
    }
  }
  return empty_node;
}

template <class Key, class Node, class Alloc>
bool ShmHashTableM<Key, Node, Alloc>::Travel(TravelPos* pos,
                                             size_t max_travel_nodes,
                                      std::function<void(volatile Node*)> f) {
  assert(shm_.is_initialized());
  size_t count = 0;
  for (size_t r = 0; r < shm_->row_num; r++) {
    for (size_t c = 0; c < row_mods_[r]; c++) {
      if (r == 0 && c == 0) {
        r = pos->row;
        c = pos->col;
      }
      if (max_travel_nodes > 0 && count >= max_travel_nodes) {
        pos->row = r;
        pos->col = c;
        return true;
      }
      size_t index = GetIndex(r, c);
      volatile Node* node = &shm_->nodes[index];
      if (node->Key().first) {
        f(node);
      }
      count++;
    }
  }
  pos->row = 0;
  pos->col = 0;
  return true;
}

}  // namespace shmc

#endif  // SHMC_SHM_HASH_TABLE_M_H_
