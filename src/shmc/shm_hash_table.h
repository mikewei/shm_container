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
#ifndef SHMC_SHM_HASH_TABLE_H_
#define SHMC_SHM_HASH_TABLE_H_

#include <assert.h>
#include <string>
#include <utility>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"

namespace shmc {

/* A shm implementation of hash table (faster version, default choice)
 * @Key    type of search key, any type supporting =,==,std::hash
 * @Node   type of node which contains both key and value information
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 * 
 * This hash table implementation uses a pre-allocated array of fixed-size
 * nodes in shm. The array of nodes is treated as a multi-rows hash-table and
 * the hash colision is solved by rehashes on different rows. The number of
 * rows and the size of each row is well tunned to get good time and space
 * performance.
 *
 * Read-Write concurrency safety should be considered by the user, for example
 * race conditions such as key-readying by Node::Key() and key-writing of the
 * same node, such as value-reading and value-updating of the same node. 
 *
 * This is a low-level container, consider use <ShmHashMap> first.
 *
 * Param @Node can be any StandardLayoutType with a get-key method as below:
 *    pair<bool,Key> Node::Key() const volatile;
 *                         or 
 *    pair<bool,Key> Node::Key(Arg arg) const volatile;
 * first bool element of returned pair indicates whether the node is used,
 * and if true the second element holds the key of the node.
 */
template <class Key, class Node, class Alloc = SVIPC>
class ShmHashTable {
 public:
  ShmHashTable() = default;

  /* Initializer for READ & WRITE
   * @shm_key   key or name of the shm to attach or create
   * @capacity  expected capacity of hash-table
   *
   * This initializer should be called by the container writer/owner and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return    true if succeed
   */
  bool InitForWrite(const std::string& shm_key, size_t capacity);

  /* Initializer for READ-ONLY
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the container reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key);

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

  /* Find node by key (with custom argument) 
   * @key     key to find
   * @arg     argument to pass to Node::Key(Arg)
   *
   * This const method returns pointer to const node (read-only) if found.
   * 
   * @return  pointer to node if found, otherwise nullptr
   */
  template <class Arg>
  const volatile Node* Find(const Key& key, Arg&& arg) const;

  /* Find node by key (with custom argument) 
   * @key     key to find
   * @arg     argument to pass to Node::Key(Arg)
   *
   * This method returns pointer to non-const node if found and user can 
   * update the content of the node. Note that Read-Write concurrency safety 
   * should be considered.
   * 
   * @return  pointer to node if found, otherwise nullptr
   */
  template <class Arg>
  volatile Node* Find(const Key& key, Arg&& arg);

  /* Find node by key or allocate a new node (with custom argument) 
   * @key       key to find
   * @arg       argument to pass to Node::Key(Arg)
   * @is_found  [out] whether the node is found by key if non-nullptr returned
   *
   * If a node with the key is found it returns pointer to the node, otherwise
   * it will find an empty node to return, if no empty node can be found it 
   * returns nullptr. If this method returns non-nullptr is_found indicates
   * whether the node returned is found by key.
   * 
   * @return  pointer to the found node or empty node or nullptr
   */
  template <class Arg>
  volatile Node* FindOrAlloc(const Key& key, Arg&& arg, bool *is_found);

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
    return ideal_capacity() * 85 / 100;
  }

 private:
  void CalcAllRows(size_t first_row_size,
                   size_t row_size_ratio,
                   size_t* row_num);
  size_t GetIndex(size_t row, size_t col) const {
    return row_index_[row] + col;
  }
  static size_t SquareRoot(size_t n) {
    for (size_t r = 0; r <= n; r++) {
      size_t sq = r * r;
      if (sq == n) return r;
      else if (sq > n) return r - 1;
    }
    return 0;  // never get here
  }

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmHashTable);

  struct ShmHead {
    volatile uint64_t magic;
    volatile uint32_t ver;
    volatile uint32_t head_size;
    volatile uint32_t node_size;
    volatile uint32_t node_num;
    volatile uint32_t first_row_size;
    volatile uint32_t row_size_ratio;
    volatile uint32_t row_num;
    volatile uint32_t max_row_touched;
    volatile uint64_t create_time;
    volatile char reserved[64];
    volatile Node nodes[0];
  };
  static_assert(sizeof(ShmHead) == 112, "unexpected ShmHead layout");
  static_assert(alignof(Node) <= 8, "align requirement of Node can't be met");

  ShmHandle<ShmHead, Alloc> shm_;

  static constexpr size_t kMaxRows = 100;
  static constexpr size_t kRowSizeRatio = 60;
  size_t row_mods_[kMaxRows];
  size_t row_index_[kMaxRows];
  size_t capacity_  = 0;
  size_t node_num_  = 0;
};

template <class Key, class Node, class Alloc>
void ShmHashTable<Key, Node, Alloc>::CalcAllRows(size_t first_row_size,
                                                 size_t row_size_ratio,
                                                 size_t* row_num) {
  size_t max_rows = *row_num;
  size_t row_base_index = 0;
  size_t row_mods_sum = 0;
  size_t row_count = 0;
  for (size_t row_size = first_row_size;
       row_size >= 2 && row_count < max_rows;
       row_size = row_size * row_size_ratio / 100, row_count++) {
    row_index_[row_count] = row_base_index;
    row_base_index += row_size;
    bool ret = Utils::GetPrimeArray(row_size, 1, &row_mods_[row_count]);
    assert(ret);
    row_mods_sum += row_mods_[row_count];
  }
  node_num_ = row_base_index;
  capacity_ = row_mods_sum;
  *row_num = row_count;
}

template <class Key, class Node, class Alloc>
bool ShmHashTable<Key, Node, Alloc>::InitForWrite(const std::string& shm_key,
                                                  size_t capacity) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmHashTable::InitForWrite: already initialized\n");
  }
  if (capacity < 1 || capacity > 0xffffffffUL*80/100) {
    SHMC_ERR_RET("ShmHashTable::InitForWrite: bad capacity\n");
  }
  size_t alloc_cap = (capacity < 50    ? 100            :
                     (capacity < 1000  ? capacity*2     :
                     (capacity < 10000 ? capacity*3/2   :
                                         capacity*5/4)));
  // first_row_size / (1 - kRowSizeRatio/100) = alloc_cap
  size_t first_row_size = alloc_cap * (100 - kRowSizeRatio) / 100;
  size_t row_num = kMaxRows;
  CalcAllRows(first_row_size, kRowSizeRatio, &row_num);
  // row_num = min(row_num, row_num^0.5 * 3)
  size_t row_num_calc = SquareRoot(row_num * 9);
  if (row_num_calc < row_num) {
    row_num = row_num_calc;
    CalcAllRows(first_row_size, kRowSizeRatio, &row_num);
  }
  Utils::Log(kInfo, "ShmHashTable::InitForWrite: rows=%lu nodes=%lu cap=%lu\n",
                    row_num, node_num_, capacity_);

  size_t node_size = sizeof(Node);
  size_t shm_size = sizeof(ShmHead) + node_size * node_num_;
  if (!shm_.InitForWrite(shm_key, shm_size, Utils::DefaultCreateFlags())) {
    SHMC_ERR_RET("ShmHashTable::InitForWrite: shm init(%s, %lu) fail\n",
                                         shm_key.c_str(), shm_size);
  }
  uint64_t now_ts = time(NULL);
  if (shm_.is_newly_created()) {
    shm_->magic = Utils::GenMagic("HashTab2");
    shm_->ver = Utils::MakeVer(1, 0);
    shm_->head_size = sizeof(ShmHead);
    shm_->node_size = node_size;
    shm_->node_num = node_num_;
    shm_->first_row_size = first_row_size;
    shm_->row_size_ratio = kRowSizeRatio;
    shm_->row_num = row_num;
    shm_->create_time = now_ts;
    shm_->max_row_touched = 0;
  } else {
    if (shm_->magic != Utils::GenMagic("HashTab2"))
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad magic(0x%lx)\n", shm_->magic);
    if (Utils::MajorVer(shm_->ver) != 1)
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad ver(0x%x)\n", shm_->ver);
    if (shm_->head_size != sizeof(ShmHead))
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad head_size(%u)\n", shm_->head_size);
    if (shm_->node_size != node_size)
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad node_size(%u)\n", shm_->node_size);
    if (shm_->node_num != node_num_)
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad node_num(%u)\n", shm_->node_num);
    if (shm_->first_row_size != first_row_size)
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad first_row_size(%u)\n", shm_->first_row_size);
    if (shm_->row_size_ratio != kRowSizeRatio)
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad row_size_ratio(%u)\n", shm_->row_size_ratio);
    if (shm_->row_num != row_num)
      SHMC_ERR_RET("ShmHashTable::InitForWrite: bad row_num(%u)\n", shm_->row_num);
  }
  return true;
}

template <class Key, class Node, class Alloc>
bool ShmHashTable<Key, Node, Alloc>::InitForRead(const std::string& shm_key) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmHashTable::InitForRead: already initialized\n");
  }
  size_t node_size = sizeof(Node);
  if (!shm_.InitForRead(shm_key, sizeof(ShmHead))) {
    SHMC_ERR_RET("ShmHashTable::InitForRead: shm init(%s, %lu) fail\n",
                                        shm_key.c_str(), sizeof(ShmHead));
  }
  if (shm_->magic != Utils::GenMagic("HashTab2"))
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad magic(0x%lx)\n", shm_->magic);
  if (Utils::MajorVer(shm_->ver) != 1)
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad ver(0x%x)\n", shm_->ver);
  if (shm_->head_size != sizeof(ShmHead))
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad head_size(%u)\n", shm_->head_size);
  if (shm_->node_size != node_size)
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad node_size(%u)\n", shm_->node_size);
  size_t shm_size = sizeof(ShmHead) + node_size * shm_->node_num;
  if (!shm_.CheckSize(shm_size))
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad shm size(%u)\n", shm_.size());
  size_t row_num = shm_->row_num;
  CalcAllRows(shm_->first_row_size, shm_->row_size_ratio, &row_num);
  if (shm_->row_num != row_num)
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad row_num(%u)\n", shm_->row_num);
  if (shm_->node_num != node_num_)
    SHMC_ERR_RET("ShmHashTable::InitForRead: bad node_num(%u)\n", shm_->node_num);
  return true;
}

template <class Key, class Node, class Alloc>
const volatile Node* ShmHashTable<Key, Node, Alloc>::Find(const Key& key) const {
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
volatile Node* ShmHashTable<Key, Node, Alloc>::Find(const Key& key) {
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
volatile Node* ShmHashTable<Key, Node, Alloc>::FindOrAlloc(const Key& key,
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
template <class Arg>
const volatile Node* ShmHashTable<Key, Node, Alloc>::Find(const Key& key,
                                                          Arg&& arg) const {
  assert(shm_.is_initialized());
  size_t hash_code = std::hash<Key>()(key);
  for (size_t r = 0; r < shm_->row_num; r++) {
    size_t index = GetIndex(r, hash_code % row_mods_[r]);
    const volatile Node* node = &shm_->nodes[index];
    auto key_info = node->Key(std::forward<Arg>(arg));
    if (key_info.first && key == key_info.second) {
        return node;
    }
  }
  return nullptr;
}

template <class Key, class Node, class Alloc>
template <class Arg>
volatile Node* ShmHashTable<Key, Node, Alloc>::Find(const Key& key, Arg&& arg) {
  assert(shm_.is_initialized());
  size_t hash_code = std::hash<Key>()(key);
  for (size_t r = 0; r < shm_->row_num; r++) {
    size_t index = GetIndex(r, hash_code % row_mods_[r]);
    volatile Node* node = &shm_->nodes[index];
    auto key_info = node->Key(std::forward<Arg>(arg));
    if (key_info.first && key == key_info.second) {
        return node;
    }
  }
  return nullptr;
}

template <class Key, class Node, class Alloc>
template <class Arg>
volatile Node* ShmHashTable<Key, Node, Alloc>::FindOrAlloc(const Key& key,
                                                           Arg&& arg,
                                                           bool* is_found) {
  assert(shm_.is_initialized());
  volatile Node* empty_node = nullptr;
  if (is_found) *is_found = false;
  size_t hash_code = std::hash<Key>()(key);
  for (size_t r = 0; r < shm_->row_num; r++) {
    size_t index = GetIndex(r, hash_code % row_mods_[r]);
    volatile Node* node = &shm_->nodes[index];
    auto key_info = node->Key(std::forward<Arg>(arg));
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
bool ShmHashTable<Key, Node, Alloc>::Travel(TravelPos* pos,
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

#endif  // SHMC_SHM_HASH_TABLE_H_
