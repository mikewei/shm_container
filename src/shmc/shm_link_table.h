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
#ifndef SHMC_SHM_LINK_TABLE_H_
#define SHMC_SHM_LINK_TABLE_H_

#include <assert.h>
#include <string>
#include <vector>
#include "shmc/shm_handle.h"
#include "shmc/common_utils.h"

#ifdef UNIT_TEST
template <class A> class ShmLinkTableTest;
#endif

namespace shmc {

/* Handle type of link-buffer
 *
 * The library user need not care about the implementation and just use
 * it as a basic type which is assignable and comparable.
 */
struct link_buf_t {
  union {
    uint64_t head_and_tag;  // allow atomic access
    struct {
      uint32_t head;
      uint32_t tag;
    } m;
  };

  //- unfortunately C++ volatile type need these code
  link_buf_t() : head_and_tag(0) {}
  link_buf_t(uint32_t h, uint32_t t) {
    m.head = h;
    m.tag  = t;
  }
  link_buf_t(const volatile link_buf_t& lb) {  // NOLINT
    head_and_tag = lb.head_and_tag;
  }
  link_buf_t(const volatile link_buf_t&& lb) {  // NOLINT
    head_and_tag = lb.head_and_tag;
  }
  link_buf_t& operator=(const volatile link_buf_t& lb) {  // NOLINT
    head_and_tag = lb.head_and_tag;
    return *this;
  }
  link_buf_t& operator=(const volatile link_buf_t&& lb) {
    head_and_tag = lb.head_and_tag;
    return *this;
  }
  //- return void to avoid warning about implicit dereference of volatile
  void operator=(const volatile link_buf_t& lb) volatile {  // NOLINT
    head_and_tag = lb.head_and_tag;
  }
  void operator=(const volatile link_buf_t&& lb) volatile {
    head_and_tag = lb.head_and_tag;
  }
  bool operator==(const volatile link_buf_t& lb) const volatile {  // NOLINT
    return (head_and_tag == lb.head_and_tag);
  }
  bool operator==(const volatile link_buf_t&& lb) const volatile {
    return (head_and_tag == lb.head_and_tag);
  }
  bool operator!=(const volatile link_buf_t& lb) const volatile {  // NOLINT
    return !(*this == lb);
  }
  bool operator!=(const volatile link_buf_t&& lb) const volatile {
    return !(*this == lb);
  }
  operator bool() const {
    return (head_and_tag != 0);
  }
  operator bool() const volatile {
    return (head_and_tag != 0);
  }
  uint32_t head() const {
    return m.head;
  }
  uint32_t head() const volatile {
    return m.head;
  }
  uint32_t tag() const {
    return m.tag;
  }
  uint32_t tag() const volatile {
    return m.tag;
  }
  void clear() {
    head_and_tag = 0;
  }
  void clear() volatile {
    head_and_tag = 0;
  }
};
static_assert(sizeof(link_buf_t) == 8, "unexpected link_buf_t size");
static_assert(alignof(link_buf_t) == 8, "unexpected link_buf_t alignment");

/* A container of variable length buffers
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX, ANON, HEAP]
 *
 * Buffer of variable length can be stored as linked memory blocks in the
 * container, called link-buf here. The container has a block memory pool
 * and use a free block list to manage free space, it also works hard to
 * check for data integrity and memory leak.
 *
 * Read-Write concurrency safety is offered internally in a lock-free manner,
 * but Write-Write is not and needs external synchronization.
 *
 * This is a low-level container, consider use <ShmHashMap> first.
 */
template <class Alloc = SVIPC>
class ShmLinkTable {
 public:
  ShmLinkTable() = default;

  /* Initializer for READ & WRITE
   * @shm_key         key or name of the shm to attach or create
   * @user_node_size  size of node space used for user data storage
   * @user_node_num   total number of nodes of the container
   *
   * This initializer should be called by the container writer/owner and
   * it will try to attach an existing shm or created a new one if needed.
   *
   * @return          true if succeed
   */
  bool InitForWrite(const std::string& shm_key,
                    size_t user_node_size,
                    size_t user_node_num);

  /* Initializer for READ-ONLY
   * @shm_key  key or name of the shm to attach
   *
   * This initializer should be called by the container reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& shm_key);

  /* Allocate a new link-buffer
   * @buf   pointer of buf to copy from
   * @size  size of @buf and the new buffer
   *
   * This method allocate @size space for the new buffer and copy @buf
   * to it and finally return a handle of link_buf_t.
   *
   * @return   the newly created link_buf or empty one if failed
   */
  link_buf_t New(const void* buf, size_t size);

  /* Read link-buffer and copy to an user buf
   * @link_buf  handle of link-buf to copy from
   * @buf       [out] user buf to copy to 
   * @size      [in|out] size of @buf as input and size of link-buf as output
   *
   * This method will copy data from link-buf to user @buf. The actual copy 
   * length is MIN(link-buf-size, user-buf-size), that means if link-buf-size
   * < user-buf-size all link-buf data will be copied to the user buf and if 
   * link-buf-size > user-buf-size data will be truncated to fill user-buf
   * fully. In any case the real size of link-buf will be returned through 
   * param size.
   * If Read() is called while other process/thread is calling Free() 
   * concurrently this may fail and return false.
   *
   * @return    true if succeed
   */
  bool Read(link_buf_t link_buf, void* buf, size_t* size) const;

  /* Read link-buffer and copy to a std::string
   * @link_buf  handle of link-buf to copy from
   * @out       [out] output string to copy to
   *
   * This method will write all data of link-buf to the string @out.
   * If Read() is called while other process/thread is calling Free() 
   * concurrently this may fail and return false.
   *
   * @return    true if succeed
   */
  bool Read(link_buf_t link_buf, std::string* out) const;

  /* Free a link-buffer
   * @link_buf  handle of link-buf to free
   *
   * This method will recycle all nodes of the link-buffer.
   *
   * @return    true if succeed
   */
  bool Free(link_buf_t link_buf);

  /* Dump link-buffer info to log handler
   * @link_buf  handle of link-buf to dump
   *
   * This method will read the link-buffer and dump the structure and content
   * details to log handler.
   *
   * @return    true if succeed
   */
  bool Dump(link_buf_t link_buf) const;

  /* Travel all the link-buffers
   * @f  callback to be called when visiting each link-buffer
   *
   * This method will scan all nodes and find all present link-buffers and 
   * call the callback @f for each one.
   *
   * @return    true if succeed
   */
  bool Travel(std::function<void(link_buf_t)> f) const;

  // Health infomation about the container
  struct HealthStat {
    size_t total_free_nodes;
    size_t total_link_bufs;
    size_t total_link_buf_bytes;
    size_t leaked_nodes;
    size_t recycled_leaked_nodes;
    size_t bad_linked_link_bufs;
    size_t too_long_link_bufs;
    size_t too_short_link_bufs;
    size_t cleared_link_bufs;
  };
  /* Do health check for the container.
   * @hstat     [out] the health check result.
   * @auto_fix  whether do error fix if possible.
   * 
   * This method will do a whole health check for potential data integrity
   * issues such as bad linked list, corrupted data, memory leak and so on.
   * If errors found it usually means external unexpected overflow or bug of
   * the library self. If auto_fix is true it will try to fix the error by
   * removing bad links, freeing leaked nodes and so on.
   *
   * @return    true if finished the check successfully
   */
  bool HealthCheck(HealthStat* hstat, bool auto_fix);

  /* getter
   *
   * @return  total number of free nodes
   */
  size_t free_nodes() const {
    assert(shm_.is_initialized());
    return shm_->free_node_num;
  }
  /* getter
   *
   * @return  total number of nodes
   */
  size_t total_nodes() const {
    assert(shm_.is_initialized());
    return shm_->node_num - 1;
  }
  /* getter
   *
   * @return  total number of link-buffers
   */
  size_t link_bufs() const {
    assert(shm_.is_initialized());
    return shm_->buf_used_num;
  }
  /* getter
   *
   * @return  total bytes of link-buffers
   */
  size_t link_buf_bytes() const {
    assert(shm_.is_initialized());
    return shm_->buf_used_bytes;
  }

 private:
  uint32_t AllocNode();
  void FreeNode(uint32_t n);
  bool DumpNode(uint32_t n) const;
  uint32_t tag_seq() {
    if (++(shm_->tag_seq_next) == 0) ++(shm_->tag_seq_next);
    return shm_->tag_seq_next & 0x7fffffffU;
  }
  size_t buf_nodes(size_t buf_len) const {
    return (sizeof(BufHead) + buf_len
           + (shm_->node_size - sizeof(NodeHead)) - 1)
           / (shm_->node_size - sizeof(NodeHead));
  }
  static constexpr uint32_t kHeadFlag = 0x80000000U;
#ifdef UNIT_TEST
  friend class ShmLinkTableTest<Alloc>;
#endif

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmLinkTable);

  struct ShmHead {
    volatile uint64_t magic;
    volatile uint32_t ver;
    volatile uint32_t head_size;
    volatile uint32_t node_size;
    volatile uint32_t node_num;
    volatile uint32_t free_list;
    volatile uint32_t tag_seq_next;
    volatile uint32_t free_node_num;
    volatile uint32_t buf_used_num;
    volatile uint64_t buf_used_bytes;
    volatile uint64_t create_time;
    volatile char reserved[64];
    volatile char nodes_buf[0];  // 8-bytes aligned
  } __attribute__((__packed__));
  static_assert(sizeof(ShmHead) == 120, "unexpected ShmHead layout");

  struct NodeHead {
    volatile uint32_t next;
    volatile uint32_t tag;
    volatile char user_node[0];
  } __attribute__((__packed__));
  static_assert(sizeof(NodeHead) == 8, "unexpected NodeHead layout");

  struct BufHead {
    volatile uint32_t buf_len;
  } __attribute__((__packed__));
  static_assert(sizeof(BufHead) == 4, "unexpected BufHead layout");

  const volatile NodeHead* GetNode(size_t index) const {
    assert(index > 0 && index < shm_->node_num);
    return reinterpret_cast<const volatile NodeHead*>(
             shm_->nodes_buf + shm_->node_size * index);
  }
  volatile NodeHead* GetNode(size_t index) {
    assert(index > 0 && index < shm_->node_num);
    return reinterpret_cast<volatile NodeHead*>(
             shm_->nodes_buf + shm_->node_size * index);
  }

  ShmHandle<ShmHead, Alloc> shm_;
};

template <class Alloc>
bool ShmLinkTable<Alloc>::InitForWrite(const std::string& shm_key,
                                       size_t user_node_size,
                                       size_t user_node_num) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmLinkTable::InitForWrite: already initialized\n");
  }
  if (user_node_size < sizeof(BufHead) || user_node_num <= 0) {
    SHMC_ERR_RET("ShmLinkTable::InitForWrite: invalid user_node parma\n");
  }
  size_t node_size = user_node_size + sizeof(NodeHead);
  size_t node_num = user_node_num + 1;
  size_t shm_size = sizeof(ShmHead) + node_size * node_num;
  if (!shm_.InitForWrite(shm_key, shm_size, Utils::DefaultCreateFlags())) {
    SHMC_ERR_RET("ShmLinkTable::InitForWrite: shm init(%s, %lu) fail\n",
                                     shm_key.c_str(), shm_size);
  }
  uint64_t now_ts = time(NULL);
  if (shm_.is_newly_created()) {
    shm_->magic = Utils::GenMagic("LinkTab");
    shm_->ver = Utils::MakeVer(1, 0);
    shm_->head_size = sizeof(ShmHead);
    shm_->node_size = node_size;
    shm_->node_num = node_num;
    shm_->tag_seq_next = 1;
    shm_->create_time = now_ts;
    shm_->free_node_num = node_num - 1;
    shm_->buf_used_num = 0;
    shm_->buf_used_bytes = 0;
    // init free list
    for (uint32_t n = shm_->node_num - 1; n > 0; n--) {
      volatile NodeHead* node = GetNode(n);
      node->next = (n == shm_->node_num - 1 ? 0 : n + 1);
    }
    shm_->free_list = 1;
  } else {
    if (shm_->magic != Utils::GenMagic("LinkTab"))
      SHMC_ERR_RET("ShmLinkTable::InitForWrite: bad magic(0x%lx)\n", shm_->magic);
    if (Utils::MajorVer(shm_->ver) != 1)
      SHMC_ERR_RET("ShmLinkTable::InitForWrite: bad ver(0x%x)\n", shm_->ver);
    if (shm_->head_size != sizeof(ShmHead))
      SHMC_ERR_RET("ShmLinkTable::InitForWrite: bad head_size(%u)\n", shm_->head_size);
    if (shm_->node_size != node_size)
      SHMC_ERR_RET("ShmLinkTable::InitForWrite: bad node_size(%u)\n", shm_->node_size);
    if (shm_->node_num != node_num)
      SHMC_ERR_RET("ShmLinkTable::InitForWrite: bad node_num(%u)\n", shm_->node_num);
    uint32_t free_num = 0;
    for (uint32_t n = shm_->free_list; n; n = GetNode(n)->next, free_num++) {}
    if (shm_->free_node_num != free_num)
      SHMC_ERR_RET("ShmLinkTable::InitForWrite: bad free_node_num(%u)\n", shm_->free_node_num);
  }
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::InitForRead(const std::string& shm_key) {
  if (shm_.is_initialized()) {
    SHMC_ERR_RET("ShmLinkTable::InitForRead: already initialized\n");
  }
  if (!shm_.InitForRead(shm_key, sizeof(ShmHead))) {
    SHMC_ERR_RET("ShmLinkTable::InitForRead: shm init(%s, %lu) fail\n",
                                    shm_key.c_str(), sizeof(ShmHead));
  }
  if (shm_->magic != Utils::GenMagic("LinkTab"))
    SHMC_ERR_RET("ShmLinkTable::InitForRead: bad magic(0x%lx)\n", shm_->magic);
  if (Utils::MajorVer(shm_->ver) != 1)
    SHMC_ERR_RET("ShmLinkTable::InitForRead: bad ver(0x%x)\n", shm_->ver);
  if (shm_->head_size != sizeof(ShmHead))
    SHMC_ERR_RET("ShmLinkTable::InitForRead: bad head_size(%u)\n", shm_->head_size);
  if (shm_->node_size < sizeof(NodeHead) + sizeof(BufHead))
    SHMC_ERR_RET("ShmLinkTable::InitForRead: bad node_size(%u)\n", shm_->node_size);
  if (shm_->node_num <= 1)
    SHMC_ERR_RET("ShmLinkTable::InitForRead: bad node_num(%u)\n", shm_->node_num);
  size_t shm_size = sizeof(ShmHead) + (size_t)shm_->node_size * shm_->node_num;
  if (!shm_.CheckSize(shm_size))
    SHMC_ERR_RET("ShmLinkTable::InitForRead: bad shm size(%u)\n", shm_.size());
  return true;
}

template <class Alloc>
uint32_t ShmLinkTable<Alloc>::AllocNode() {
  if (shm_->free_list == 0)
    return 0;
  uint32_t free_node_n = shm_->free_list;
  shm_->free_list = GetNode(free_node_n)->next;
  shm_->free_node_num--;
  return free_node_n;
}

template <class Alloc>
void ShmLinkTable<Alloc>::FreeNode(uint32_t n) {
  assert(n > 0 && n < shm_->node_num);
  volatile NodeHead* node = GetNode(n);
  node->tag = 0;
  node->next = shm_->free_list;
  shm_->free_list = n;
  shm_->free_node_num++;
}

template <class Alloc>
link_buf_t ShmLinkTable<Alloc>::New(const void* buf, size_t size) {
  if (!shm_.is_initialized()) {
    return link_buf_t{0, 0};
  }
  if (size <= 0 || size > 0xffffffffU) {
    return link_buf_t{0, 0};
  }
  size_t need_nodes = buf_nodes(size);
  if (need_nodes > shm_->free_node_num) {
    return link_buf_t{0, 0};
  }
  uint32_t tag = tag_seq();
  const char* src_ptr = static_cast<const char*>(buf);
  size_t src_left = size;
  uint32_t node_head_n = 0;
  volatile uint32_t* next_ptr = &node_head_n;
  for (size_t i = 0; i < need_nodes; i++) {
    // get new node
    uint32_t n = AllocNode();
    volatile NodeHead* node = GetNode(n);
    node->next = 0;
    node->tag = tag;
    volatile char* dst_ptr = static_cast<volatile char*>(node->user_node);
    size_t dst_left = shm_->node_size - sizeof(NodeHead);
    if (i == 0) {  // first node
      node->tag |= kHeadFlag;
      volatile BufHead* buf_head = reinterpret_cast<volatile BufHead*>(dst_ptr);
      buf_head->buf_len = size;
      dst_ptr += sizeof(BufHead);
      dst_left -= sizeof(BufHead);
    }
    // copy data
    size_t copy_len = (dst_left <= src_left ? dst_left : src_left);
    memcpy(const_cast<char*>(dst_ptr), src_ptr, copy_len);
    src_ptr += copy_len;
    src_left -= copy_len;
    // link the node
    *next_ptr = n;
    next_ptr = &node->next;
  }
  shm_->buf_used_num++;
  shm_->buf_used_bytes += size;
  return link_buf_t{node_head_n, tag};
}

template <class Alloc>
bool ShmLinkTable<Alloc>::Read(link_buf_t link_buf, void* buf, size_t* size) const {
  if (!shm_.is_initialized()) {
    return false;
  }
  size_t buf_size = *size;
  if (link_buf.head() <= 0 || link_buf.head() >= shm_->node_num) {
    return false;
  }
  const volatile NodeHead* head_node = GetNode(link_buf.head());
  if (head_node->tag != (link_buf.tag() | kHeadFlag)) {
    return false;
  }
  const volatile BufHead* buf_head = reinterpret_cast<const volatile BufHead*>(
                                       head_node->user_node);
  size_t data_size = buf_head->buf_len;
  size_t left_size = data_size;
  char* dst_ptr = static_cast<char*>(buf);
  size_t dst_left = buf_size;
  // read first node
  size_t cur_node_data_size = shm_->node_size - sizeof(NodeHead)
                                              - sizeof(BufHead);
  if (cur_node_data_size > left_size)
    cur_node_data_size = left_size;
  size_t copy_len = (cur_node_data_size <= dst_left
                    ? cur_node_data_size : dst_left);
  memcpy(dst_ptr, const_cast<const char*>(head_node->user_node + sizeof(BufHead)),
         copy_len);
  left_size -= copy_len;
  dst_ptr += copy_len;
  dst_left -= copy_len;
  if (dst_left <= 0) {
    *size = data_size;
    return true;
  }
  // read rest nodes
  cur_node_data_size = shm_->node_size - sizeof(NodeHead);
  uint32_t node_n = head_node->next;
  while (left_size > 0) {
    if (node_n == 0) {  // bad list
      return false;
    }
    const volatile NodeHead* node = GetNode(node_n);
    if (node->tag != link_buf.tag()) {
      return false;
    }
    if (cur_node_data_size > left_size)
      cur_node_data_size = left_size;
    size_t copy_len = (cur_node_data_size <= dst_left
                      ? cur_node_data_size : dst_left);
    memcpy(dst_ptr, const_cast<const char*>(node->user_node), copy_len);
    left_size -= copy_len;
    dst_ptr += copy_len;
    dst_left -= copy_len;
    if (dst_left <= 0) break;
    node_n = node->next;
  }
  *size = data_size;
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::Read(link_buf_t link_buf, std::string* out) const {
  if (!shm_.is_initialized()) {
    return false;
  }
  size_t buf_len = 0;
  if (!Read(link_buf, nullptr, &buf_len)) {
    return false;
  }
  out->resize(buf_len);
  if (!Read(link_buf, static_cast<void*>(&(*out)[0]), &buf_len)) {
    return false;
  }
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::Free(link_buf_t link_buf) {
  if (!shm_.is_initialized()) {
    return false;
  }
  if (link_buf.head() <= 0 || link_buf.head() >= shm_->node_num) {
    return false;
  }
  volatile NodeHead* head_node = GetNode(link_buf.head());
  if (head_node->tag != (link_buf.tag() | kHeadFlag)) {
    return false;
  }
  volatile BufHead* buf_head = (volatile BufHead*)(head_node->user_node);
  size_t buf_len = buf_head->buf_len;
  uint32_t next_node_n = head_node->next;
  FreeNode(link_buf.head());
  uint32_t node_n = next_node_n;
  while (node_n > 0) {
    volatile NodeHead* node = GetNode(node_n);
    if (node->tag != link_buf.tag()) {
      shm_->buf_used_num--;
      shm_->buf_used_bytes -= buf_len;
      return false;
    }
    next_node_n = node->next;
    FreeNode(node_n);
    node_n = next_node_n;
  }
  shm_->buf_used_num--;
  shm_->buf_used_bytes -= buf_len;
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::Dump(link_buf_t link_buf) const {
  if (!shm_.is_initialized()) {
    Utils::Log(kError, "Dump error: shm not initialized\n");
    return false;
  }
  Utils::Log(kInfo, "link_buf_t {head:%u, tag:%u}\n",
                    link_buf.head(), link_buf.tag());
  if (link_buf.head() <= 0 || link_buf.head() >= shm_->node_num) {
    Utils::Log(kError, "Dump error: bad link_buf\n");
    return false;
  }
  const volatile NodeHead* head_node = GetNode(link_buf.head());
  DumpNode(link_buf.head());
  if (head_node->tag != (link_buf.tag() | kHeadFlag)) {
    Utils::Log(kError, "Dump error: bad tag of the head node\n");
    return false;
  }
  const volatile BufHead* buf_head = (const volatile BufHead*)(head_node->user_node);
  Utils::Log(kInfo, "link_buf_len:%u\n", buf_head->buf_len);
  uint32_t node_n = head_node->next;
  while (node_n > 0) {
    const volatile NodeHead* node = GetNode(node_n);
    DumpNode(node_n);
    if (node->tag != link_buf.tag()) {
      Utils::Log(kError, "Dump error: bad tag of node\n");
      return false;
    }
    node_n = node->next;
  }
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::DumpNode(uint32_t n) const {
  if (!(n > 0 && n < shm_->node_num)) {
    Utils::Log(kError, "Dump error: bad node index(%u)\n", n);
    return false;
  }
  const volatile NodeHead* node = GetNode(n);
  size_t user_node_size = shm_->node_size - sizeof(NodeHead);
  Utils::Log(kInfo, "--------------- NODE ---------------\n"
                   "index:%u tag:0x%x next:%u\n%s",
                   n, node->tag, node->next,
                   Utils::Hex(node->user_node, user_node_size));
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::Travel(std::function<void(link_buf_t)> f) const {
  if (!shm_.is_initialized()) {
    return false;
  }
  for (uint32_t n = 1; n < shm_->node_num; n++) {
    const volatile NodeHead* node = GetNode(n);
    if (node->tag & kHeadFlag) {
      f(link_buf_t{n, node->tag & ~kHeadFlag});
    }
  }
  return true;
}

template <class Alloc>
bool ShmLinkTable<Alloc>::HealthCheck(HealthStat* hstat, bool auto_fix) {
  if (!shm_.is_initialized()) {
    return false;
  }
  memset(hstat, 0, sizeof(HealthStat));
  std::vector<bool> bitmap(shm_->node_num);
  // mark linked nodes
  if (!Travel([this, hstat, auto_fix, &bitmap](link_buf_t lb) {
    const volatile NodeHead* head_node = GetNode(lb.head());
    assert(head_node->tag == (lb.tag() | kHeadFlag));
    const volatile BufHead* buf_head = (const volatile BufHead*)(head_node->user_node);
    hstat->total_link_bufs++;
    hstat->total_link_buf_bytes += buf_head->buf_len;
    size_t need_nodes = buf_nodes(buf_head->buf_len);
    bitmap[lb.head()] = true;
    uint32_t node_n = head_node->next;
    size_t checked_nodes = 1;
    while (node_n > 0) {
      const volatile NodeHead* node = GetNode(node_n);
      if (node->tag != lb.tag()) {
        hstat->bad_linked_link_bufs++;
        if (auto_fix) {
          // free previous nodes and this node will be freed as leaked node
          // if it is not linked in other link-bufs
          Free(lb);
          hstat->cleared_link_bufs++;
          hstat->total_link_bufs--;
          hstat->total_link_buf_bytes -= buf_head->buf_len;
        }
        return;
      }
      bitmap[node_n] = true;
      node_n = node->next;
      checked_nodes++;
    }
    if (checked_nodes > need_nodes) {
      // this still work so will not be auto-fixed
      hstat->too_long_link_bufs++;
    } else if (checked_nodes < need_nodes) {
      hstat->too_short_link_bufs++;
      if (auto_fix) {
        Free(lb);
        hstat->cleared_link_bufs++;
        hstat->total_link_bufs--;
        hstat->total_link_buf_bytes -= buf_head->buf_len;
      }
    }
  })) {
    return false;
  }
  if (shm_->buf_used_num != hstat->total_link_bufs) {
      Utils::Log(kWarning, "SHM.buf_used_num(%u) is reset to %lu\n",
                shm_->buf_used_num, hstat->total_link_bufs);
      shm_->buf_used_num = hstat->total_link_bufs;
  }
  if (shm_->buf_used_bytes != hstat->total_link_buf_bytes) {
      Utils::Log(kWarning, "SHM.buf_used_bytes(%lu) is reset to %lu\n",
                shm_->buf_used_bytes, hstat->total_link_buf_bytes);
      shm_->buf_used_bytes = hstat->total_link_buf_bytes;
  }
  // mark free nodes
  for (uint32_t n = shm_->free_list; n; n = GetNode(n)->next) {
    hstat->total_free_nodes++;
    bitmap[n] = true;
  }
  // find leaked nodes
  for (size_t i = 1; i < shm_->node_num; i++) {
    if (!bitmap[i]) {
      hstat->leaked_nodes++;
      if (auto_fix) {
        FreeNode(i);
        hstat->recycled_leaked_nodes++;
        hstat->total_free_nodes++;
      }
    }
  }
  if (shm_->free_node_num != hstat->total_free_nodes) {
    Utils::Log(kWarning, "SHM.free_node_num(%u) is reset to %u\n",
              shm_->free_node_num, hstat->total_free_nodes);
    shm_->free_node_num = hstat->total_free_nodes;
  }
  return true;
}

}  // namespace shmc

#endif  // SHMC_SHM_LINK_TABLE_H_
