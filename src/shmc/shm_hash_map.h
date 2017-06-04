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
#ifndef SHMC_SHM_HASH_MAP_H_
#define SHMC_SHM_HASH_MAP_H_

#include <assert.h>
#include <utility>
#include <string>
#include <vector>
#include <algorithm>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"
#include "shmc/shm_hash_table.h"
#include "shmc/shm_link_table.h"

#ifdef UNIT_TEST
template <class A> class ShmHashMapTest;
#endif

namespace shmc {

/* A hash-map container of key-values
 * @KeyType  type of key, need StandardLayoutType supporting =,==,std::hash
 * @Alloc    shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 * 
 * HashMap stores collection of Key-Values and supports fast queries. The key
 * is of scalar or struct type and the value is a variable-length buffer. 
 * The container internally uses <ShmHashTable> to index keys and uses 
 * <ShmLinkTable> to store values.
 *
 * Read-Write concurrency safety is offered internally in a lock-free manner,
 * but Write-Write is not and needs external synchronization.
 */
template <class KeyType, class Alloc = SVIPC>
class ShmHashMap {
 public:
  ShmHashMap() = default;

  /* Initializer for READ & WRITE
   * @shm_key        key or name of the shm to attach or create
   * @key_num        max number of keys
   * @val_node_size  size of each node used to store values
   * @val_node_num   total number of nodes for values storage
   *
   * This initializer should be called by the container writer/owner and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return          true if succeed
   */
  bool InitForWrite(const std::string& shm_key,
                    size_t key_num,
                    size_t val_node_size,
                    size_t val_node_num);

  /* Initializer for READ-ONLY
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the container reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key);

  /* Find value by key
   * @key     key to search for
   * @val     [out] found value
   *
   * This is a read-only operation and can be called concurrently with other
   * process/thread writing.
   *
   * @return  true if succeed
   */
  bool Find(const KeyType& key, std::string* val) const;

  /* Insert key-value (only if key not exist)
   * @key          key to insert
   * @val_buf      pointer to buffer of the value
   * @val_buf_len  size of the buffer
   *
   * Try to insert key-value to the container, if the key has already exist 
   * the method fail and return false.
   *
   * @return       true if succeed
   */
  bool Insert(const KeyType& key, const void* val_buf, size_t val_buf_len) {
    return DoInsert(key, val_buf, val_buf_len, false);
  }

  /* Insert key-value (only if key not exist)
   * @key      key to insert
   * @val      value to insert
   *
   * Try to insert key-value to the container, if the key has already exist 
   * the method fail and return false.
   *
   * @return   true if succeed
   */
  bool Insert(const KeyType& key, const std::string& val) {
    return Insert(key, static_cast<const void*>(val.data()), val.size());
  }

  /* Replace key-value into the container
   * @key          key to replace
   * @val_buf      pointer to buffer of the value
   * @val_buf_len  size of the buffer
   *
   * Try to replace key-value into the container, if the key does not exist 
   * the key-value will be inserted, otherwise the value will be updated.
   *
   * @return       true if succeed
   */
  bool Replace(const KeyType& key, const void* val_buf, size_t val_buf_len) {
    return DoInsert(key, val_buf, val_buf_len, true);
  }

  /* Replace key-value into the container
   * @key      key to insert
   * @val      value to insert
   *
   * Try to replace key-value into the container, if the key does not exist 
   * the key-value will be inserted, otherwise the value will be updated.
   *
   * @return   true if succeed
   */
  bool Replace(const KeyType& key, const std::string& val) {
    return Replace(key, static_cast<const void*>(val.data()), val.size());
  }

  /* Erase one key-value
   * @key      key to find and erase
   *
   * This tries to find the key-value and erase it.
   *
   * @return   true if succeed
   */
  bool Erase(const KeyType& key);

  /* Dump key-value storage details to log
   * @key      key to find and dump
   *
   * This method will try to find the key and dump the structure and content
   * details to log handler.
   *
   * @return   true if succeed
   */
  bool Dump(const KeyType& key) const;

  /* Cursor type recording current travelling position
   */
  struct TravelPos;

  /* Travel key-values
   * @pos               [in|out] current position of travel
   * @max_travel_nodes  max nodes to travel in this method call, 0 for all
   * @f                 callback to be called when visiting each key-value
   *
   * This method will scan keys from start position and find key-values and 
   * call the callback @f for each one. If @max_travel_nodes key-values have
   * been travelled this method will stop and save the position to @pos.
   *
   * @return            true if succeed
   */
  bool Travel(TravelPos* pos, size_t max_travel_nodes,
              std::function<void(const KeyType&, const std::string&)> f);

  /* Travel all key-values
   * @f                 callback to be called when visiting each key-value
   *
   * This method will scan all keys and find all present key-values and 
   * call the callback @f for each one.
   *
   * @return            true if succeed
   */
  bool Travel(std::function<void(const KeyType&, const std::string&)> f) {
    TravelPos pos;
    return Travel(&pos, 0, std::move(f));
  }

  // Health infomation about the container
  struct HealthStat;

  /* Do health check for the container.
   * @hstat     [out] the health check result.
   * @auto_fix  whether do error fix if possible.
   * 
   * This method will do a whole health check for potential data integrity
   * issues such as bad linked list, corrupted data, memory leak and so on.
   * If errors found it usually means external unexpected overflow or bug of
   * the library itself. If auto_fix is true it will try to fix the error by
   * removing bad links, freeing leaked nodes and so on.
   *
   * @return    true if finished the check successfully
   */
  bool HealthCheck(HealthStat* hstat, bool auto_fix);

  /* getter
   *
   * @return  total number of key-values
   */
  size_t size() const {
    return link_table_.link_bufs();
  }

  /* getter
   *
   * @return  percentage of free space of HashTable index
   */
  size_t hash_table_free_percentage() const {
    size_t capa = hash_table_.expected_capacity();
    size_t used = link_table_.link_bufs();
    if (capa < used) return 0;
    return (capa - used) * 100 / capa;
  }

  /* getter
   *
   * @return  percentage of free space of LinkTable storage
   */
  size_t link_table_free_percentage() const {
    size_t capa = link_table_.total_nodes();
    size_t free = link_table_.free_nodes();
    return free * 100 / capa;
  }

  /* getter
   *
   * @return  percentage of free space of the container
   */
  size_t free_percentage() const {
    return std::min(hash_table_free_percentage(),
                    link_table_free_percentage());
  }

 private:
  bool DoInsert(const KeyType& key,
                const void* val_buf,
                size_t val_buf_len,
                bool replace);
  struct HTNode;
  bool DoRead(const KeyType& key, const volatile HTNode* node,
              std::string* val) const;
  bool DoRead(const KeyType& key, const volatile HTNode* node,
              void* val_buf, size_t* val_len) const;
  static constexpr size_t kConcReadTryCount = 64;
#ifdef UNIT_TEST
  friend class ShmHashMapTest<Alloc>;
#endif

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmHashMap);

  struct HTNode {
    // fields
    volatile KeyType key;
    char padding1_[Utils::Padding<KeyType, 8>()];
    volatile link_buf_t link_buf;
    char padding2_[Utils::Padding<link_buf_t, 8>()];
    // ops
    std::pair<bool, KeyType> Key() const volatile {
      KeyType key_cp = key;  // read key before link_buf
      return std::pair<bool, KeyType>(link_buf, key_cp);
    }
  };
  static_assert(alignof(KeyType) <= 8, "align requirement of KeyType can't be met");
  static_assert(alignof(HTNode) == 8, "unexpected align requirement of HTNode");

  ShmHashTable<KeyType, HTNode, Alloc> hash_table_;
  ShmLinkTable<Alloc> link_table_;

 public:
  struct TravelPos : public ShmHashTable<KeyType, HTNode, Alloc>::TravelPos {};
  struct HealthStat {
    typename ShmLinkTable<Alloc>::HealthStat lt_stat;
    size_t total_key_values;
    size_t bad_key_values;
    size_t cleared_key_values;
    size_t leaked_values;
    size_t recycled_leaked_values;
  };
};

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::InitForWrite(const std::string& shm_key,
                                              size_t key_num,
                                              size_t val_node_size,
                                              size_t val_node_num) {
  if (!hash_table_.InitForWrite(shm_key+"0", key_num)) {
    SHMC_ERR_RET("ShmHashMap::InitForWrite: hash_table init fail\n");
  }
  if (!link_table_.InitForWrite(shm_key+"1", val_node_size, val_node_num)) {
    SHMC_ERR_RET("ShmHashMap::InitForWrite: link_table init fail\n");
  }
  return true;
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::InitForRead(const std::string& shm_key) {
  if (!hash_table_.InitForRead(shm_key+"0")) {
    SHMC_ERR_RET("ShmHashMap::InitForRead: hash_table init fail\n");
  }
  if (!link_table_.InitForRead(shm_key+"1")) {
    SHMC_ERR_RET("ShmHashMap::InitForRead: link_table init fail\n");
  }
  return true;
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::DoInsert(const KeyType& key,
                                          const void* val_buf,
                                          size_t val_buf_len,
                                          bool replace) {
  bool is_found;
  volatile HTNode* key_node = hash_table_.FindOrAlloc(key, &is_found);
  if (!key_node) {
    return false;
  }
  if (is_found && !replace) {
    return false;
  }
  link_buf_t lb = link_table_.New(val_buf, val_buf_len);
  if (!lb) {
    return false;
  }
  if (is_found) {
    link_buf_t old_lb = key_node->link_buf;
    key_node->link_buf = lb;
    link_table_.Free(old_lb);
  } else {
    // must write key before link_buf. see CHECK-KEY-POINT
    new (const_cast<KeyType*>(&key_node->key)) KeyType(key);  // contruct key object
    key_node->link_buf = lb;
  }
  return true;
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::DoRead(const KeyType& key,
                                        const volatile HTNode* node,
                                        std::string* val) const {
  // Concurrency safety description
  // HTNode state transition:
  //   INIT -> KEY set -> LB set -> LB reset -> ... -> LB cleared -> KEY cleared -> INIT
  // Atomicity requirement:
  //   LB set/reset/clear must be atomic
  //   KEY set/cleared need not be atomic
  // Optimistic lock:
  //   ~ read LB first
  //   ~   critical code
  //   ~ read LB second
  //   if two read of LB are identical and valid, the ciritical code can
  //   see consistent value of KEY and LB-CONTENT
  bool ret;
  link_buf_t lb;
  size_t try_count = 0;
  do {  // retry for RW race condition
    if (++try_count > kConcReadTryCount) {
      Utils::Log(kWarning, "ShmHashMap::DoRead fail after trying %lu times\n", try_count);
      return false;
    }
    lb = node->link_buf;  // LOAD-LB-POINT
    ret = link_table_.Read(lb, val);
    if (ret && node->key != key) {  // CHECK-KEY-POINT
      Utils::Log(kInfo, "ShmHashMap::DoRead fail as key has changed\n");
      return false;
    }
  } while (lb != node->link_buf);  // check race condition since LOAD-POINT
  return ret;
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::DoRead(const KeyType& key,
                                        const volatile HTNode* node,
                                        void* val_buf, size_t* val_len) const {
  bool ret;
  link_buf_t lb;
  size_t try_count = 0;
  do {  // retry for RW race condition
    if (++try_count > kConcReadTryCount) {
      Utils::Log(kWarning, "ShmHashMap::DoRead fail after trying %lu times\n", try_count);
      return false;
    }
    lb = node->link_buf;  // LOAD-LB-POINT
    ret = link_table_.Read(lb, val_buf, val_len);
    if (ret && node->key != key) {  // CHECK-KEY-POINT
      Utils::Log(kInfo, "ShmHashMap::DoRead fail as key has changed\n");
      return false;
    }
  } while (lb != node->link_buf);  // check race condition since LOAD-POINT
  return ret;
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::Find(const KeyType& key,
                                      std::string* val) const {
  const volatile HTNode* key_node = hash_table_.Find(key);
  if (!key_node) {
    Utils::Log(kDebug, "ShmHashMap::Find: key not found\n");
    return false;
  }
  return DoRead(key, key_node, val);
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::Erase(const KeyType& key) {
  volatile HTNode* key_node = hash_table_.Find(key);
  if (!key_node) {
    return false;
  }
  link_buf_t lb = key_node->link_buf;
  // clear index first
  key_node->link_buf.clear();  // write link_buf before key
  key_node->key.~KeyType();  // destruct key object
  // then free link_buf
  link_table_.Free(lb);
  return true;
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::Dump(const KeyType& key) const {
  Utils::Log(kInfo, "===== ShmHashMap Dump =====\n"
                   "pid:%d key:\n%s", getpid(), Utils::Hex(&key, sizeof(key)));
  const volatile HTNode* key_node = hash_table_.Find(key);
  if (!key_node) {
    Utils::Log(kError, "Dump error: not found\n");
    return false;
  }
  link_buf_t lb = const_cast<link_buf_t&>(key_node->link_buf);
  return link_table_.Dump(lb);
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::Travel(TravelPos* pos, size_t max_travel_nodes,
                    std::function<void(const KeyType&, const std::string&)> f) {
  std::string val;
  return hash_table_.Travel(pos, max_travel_nodes,
                            [this, &f, &val](volatile HTNode* node) {
    KeyType key = node->key;
    if (DoRead(key, node, &val)) {
      f(key, val);
    }
  });
}

template <class KeyType, class Alloc>
bool ShmHashMap<KeyType, Alloc>::HealthCheck(HealthStat* hstat, bool auto_fix) {
  if (!link_table_.HealthCheck(&hstat->lt_stat, auto_fix)) {
    return false;
  }
  memset(hstat, 0, sizeof(HealthStat));
  std::vector<bool> bitmap(link_table_.total_nodes() + 1);
  if (!hash_table_.Travel([this, hstat, auto_fix, &bitmap](volatile HTNode* node) {
    hstat->total_key_values++;
    KeyType key = node->key;
    link_buf_t lb = node->link_buf;
    size_t buf_len = 0;
    if (DoRead(key, node, nullptr, &buf_len)) {
      bitmap[lb.head()] = true;
    } else {
      hstat->bad_key_values++;
      if (auto_fix) {
        Erase(key);
        hstat->cleared_key_values++;
        hstat->total_key_values--;
      }
    }
  })) {
    return false;
  }
  if (!link_table_.Travel([this, hstat, auto_fix, &bitmap](link_buf_t lb) {
    if (!bitmap[lb.head()]) {
      hstat->leaked_values++;
      if (auto_fix) {
        link_table_.Free(lb);
        hstat->recycled_leaked_values++;
      }
    }
  })) {
    return false;
  }
  return true;
}

}  // namespace shmc

#endif  // SHMC_SHM_HASH_MAP_H_
