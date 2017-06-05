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
#include <vector>
#include "gtestx/gtestx.h" 
#include "shmc/shm_link_table.h"

namespace {

constexpr const char* kShmKey = "0x10004";
constexpr size_t kNodeSize = 32;
constexpr size_t kNodeNum = 1000000;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB,
                                 shmc::ANON, shmc::HEAP>;

}  // namespace

template <class Alloc>
class ShmLinkTableTest : public testing::Test {
 protected:
  virtual ~ShmLinkTableTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  uint32_t InvokeAllocNode() {
    return link_tab_.AllocNode();
  }
  void InvokeFreeNode(uint32_t n) {
    return link_tab_.FreeNode(n);
  }
  volatile typename shmc::ShmLinkTable<Alloc>::NodeHead* InvokeGetNode(size_t index) {
    return link_tab_.GetNode(index);
  }

  shmc::ShmLinkTable<Alloc> link_tab_;
  Alloc alloc_;
};
TYPED_TEST_CASE(ShmLinkTableTest, TestTypes);

TYPED_TEST(ShmLinkTableTest, Alloc) {
  ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum));
  for (size_t i = 0; i < kNodeNum; i++) {
    ASSERT_TRUE(this->InvokeAllocNode());
    ASSERT_EQ(kNodeNum - i - 1, this->link_tab_.free_nodes());
  }
  ASSERT_FALSE(this->InvokeAllocNode());
  for (size_t i = 0; i < 100; i++) {
    this->InvokeFreeNode(1);
    ASSERT_EQ(1UL, this->link_tab_.free_nodes());
    ASSERT_TRUE(this->InvokeAllocNode());
  }
  for (size_t i = 1; i <= kNodeNum; i++) {
    this->InvokeFreeNode(i);
    ASSERT_EQ(i, this->link_tab_.free_nodes());
  }
}

TYPED_TEST(ShmLinkTableTest, Write) {
  ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum));
  shmc::link_buf_t lb;
  ASSERT_TRUE((lb = this->link_tab_.New("hello", 6)));
  char read_buf[64];
  size_t buf_len = sizeof(read_buf);
  ASSERT_TRUE(this->link_tab_.Read(lb, (void*)read_buf, &buf_len));
  ASSERT_EQ(read_buf, std::string("hello"));
  ASSERT_TRUE(this->link_tab_.Free(lb));
}

TYPED_TEST(ShmLinkTableTest, WriteBig) {
  ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum));
  shmc::link_buf_t lb;
  char write_buf[1000];
  for (size_t i = 0; i < sizeof(write_buf); i++) {
    write_buf[i] = (char)i;
  }
  ASSERT_TRUE((lb = this->link_tab_.New(write_buf, sizeof(write_buf))));
  char read_buf[1000];
  size_t buf_len = sizeof(read_buf);
  ASSERT_TRUE(this->link_tab_.Read(lb, (void*)read_buf, &buf_len));
  ASSERT_EQ(sizeof(read_buf), buf_len);
  for (size_t i = 0; i < sizeof(read_buf); i++) {
    ASSERT_EQ((char)i, read_buf[i]);
  }
  ASSERT_TRUE(this->link_tab_.Free(lb));
}

TYPED_TEST(ShmLinkTableTest, ReadLen) {
  ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum));
  shmc::link_buf_t lb;
  char write_buf[1000];
  for (size_t i = 0; i < sizeof(write_buf); i++) {
    write_buf[i] = (char)i;
  }
  ASSERT_TRUE((lb = this->link_tab_.New(write_buf, sizeof(write_buf))));
  // smaller read buf
  char read_buf[1000];
  size_t buf_len = sizeof(read_buf) / 2;
  ASSERT_TRUE(this->link_tab_.Read(lb, (void*)read_buf, &buf_len));
  ASSERT_EQ(sizeof(read_buf), buf_len);
  for (size_t i = 0; i < sizeof(read_buf) / 2; i++) {
    ASSERT_EQ((char)i, read_buf[i]);
  }
  // bigger read buf
  buf_len = sizeof(read_buf) * 2;
  ASSERT_TRUE(this->link_tab_.Read(lb, (void*)read_buf, &buf_len));
  ASSERT_EQ(sizeof(read_buf), buf_len);
  for (size_t i = 0; i < sizeof(read_buf); i++) {
    ASSERT_EQ((char)i, read_buf[i]);
  }
  // std::string as read buf
  std::string out;
  ASSERT_TRUE(this->link_tab_.Read(lb, &out));
  ASSERT_EQ(sizeof(write_buf), out.size());
  for (size_t i = 0; i < sizeof(write_buf); i++) {
    ASSERT_EQ((char)i, out[i]);
  }
  ASSERT_TRUE(this->link_tab_.Free(lb));
}

TYPED_TEST(ShmLinkTableTest, Travel) {
  ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum));
  ASSERT_TRUE(this->link_tab_.New("hello", 5));
  ASSERT_TRUE(this->link_tab_.New("hello", 5));
  size_t count = 0;
  ASSERT_TRUE(this->link_tab_.Travel([this, &count](shmc::link_buf_t lb) {
    std::string out;
    ASSERT_TRUE(this->link_tab_.Read(lb, &out));
    ASSERT_EQ("hello", out);
    count++;
  }));
  ASSERT_EQ(2UL, count);
}

TYPED_TEST(ShmLinkTableTest, HealthCheck) {
  ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum));
  typename decltype(this->link_tab_)::HealthStat hstat;
  ASSERT_TRUE(this->link_tab_.HealthCheck(&hstat, true));
  EXPECT_EQ(this->link_tab_.free_nodes(), hstat.total_free_nodes);
  EXPECT_EQ(0UL, hstat.total_link_bufs);
  EXPECT_EQ(0UL, hstat.total_link_buf_bytes);
  EXPECT_EQ(0UL, hstat.leaked_nodes);
  EXPECT_EQ(0UL, hstat.bad_linked_link_bufs);
  EXPECT_EQ(0UL, hstat.too_long_link_bufs);
  EXPECT_EQ(0UL, hstat.too_short_link_bufs);
  EXPECT_EQ(0UL, hstat.cleared_link_bufs);
  EXPECT_EQ(0UL, hstat.recycled_leaked_nodes);
  // bad linked
  char buf[64];
  shmc::link_buf_t lb;
  ASSERT_TRUE((lb = this->link_tab_.New(buf, 40)));
  auto node1 = this->InvokeGetNode(lb.head());
  auto node2 = this->InvokeGetNode(node1->next);
  node2->tag = 0;
  // check again
  ASSERT_TRUE(this->link_tab_.HealthCheck(&hstat, true));
  EXPECT_EQ(this->link_tab_.free_nodes(), hstat.total_free_nodes);
  EXPECT_EQ(0UL, hstat.total_link_bufs);
  EXPECT_EQ(0UL, hstat.total_link_buf_bytes);
  EXPECT_EQ(1UL, hstat.leaked_nodes);
  EXPECT_EQ(1UL, hstat.bad_linked_link_bufs);
  EXPECT_EQ(0UL, hstat.too_long_link_bufs);
  EXPECT_EQ(0UL, hstat.too_short_link_bufs);
  EXPECT_EQ(1UL, hstat.cleared_link_bufs);
  EXPECT_EQ(1UL, hstat.recycled_leaked_nodes);
  // too long link buf
  ASSERT_TRUE((lb = this->link_tab_.New(buf, 40)));
  node1 = this->InvokeGetNode(lb.head());
  *(uint32_t*)node1->user_node = 10;
  // check again
  ASSERT_TRUE(this->link_tab_.HealthCheck(&hstat, true));
  EXPECT_EQ(this->link_tab_.free_nodes(), hstat.total_free_nodes);
  EXPECT_EQ(1UL, hstat.total_link_bufs);
  EXPECT_EQ(10UL, hstat.total_link_buf_bytes);
  EXPECT_EQ(0UL, hstat.leaked_nodes);
  EXPECT_EQ(0UL, hstat.bad_linked_link_bufs);
  EXPECT_EQ(1UL, hstat.too_long_link_bufs);
  EXPECT_EQ(0UL, hstat.too_short_link_bufs);
  EXPECT_EQ(0UL, hstat.cleared_link_bufs);
  EXPECT_EQ(0UL, hstat.recycled_leaked_nodes);
  ASSERT_TRUE(this->link_tab_.Free(lb));
  // too short link buf
  ASSERT_TRUE((lb = this->link_tab_.New(buf, 40)));
  node1 = this->InvokeGetNode(lb.head());
  *(uint32_t*)node1->user_node = 80;
  // check again
  ASSERT_TRUE(this->link_tab_.HealthCheck(&hstat, true));
  EXPECT_EQ(this->link_tab_.free_nodes(), hstat.total_free_nodes);
  EXPECT_EQ(0UL, hstat.total_link_bufs);
  EXPECT_EQ(0UL, hstat.total_link_buf_bytes);
  EXPECT_EQ(0UL, hstat.leaked_nodes);
  EXPECT_EQ(0UL, hstat.bad_linked_link_bufs);
  EXPECT_EQ(0UL, hstat.too_long_link_bufs);
  EXPECT_EQ(1UL, hstat.too_short_link_bufs);
  EXPECT_EQ(1UL, hstat.cleared_link_bufs);
  EXPECT_EQ(0UL, hstat.recycled_leaked_nodes);
  // leaked node
  ASSERT_TRUE((lb = this->link_tab_.New(buf, 40)));
  node1 = this->InvokeGetNode(lb.head());
  node1->tag = 0;
  // check again
  ASSERT_TRUE(this->link_tab_.HealthCheck(&hstat, true));
  EXPECT_EQ(this->link_tab_.free_nodes(), hstat.total_free_nodes);
  EXPECT_EQ(0UL, hstat.total_link_bufs);
  EXPECT_EQ(0UL, hstat.total_link_buf_bytes);
  EXPECT_EQ(2UL, hstat.leaked_nodes);
  EXPECT_EQ(0UL, hstat.bad_linked_link_bufs);
  EXPECT_EQ(0UL, hstat.too_long_link_bufs);
  EXPECT_EQ(0UL, hstat.too_short_link_bufs);
  EXPECT_EQ(0UL, hstat.cleared_link_bufs);
  EXPECT_EQ(2UL, hstat.recycled_leaked_nodes);
  // check again
  ASSERT_TRUE(this->link_tab_.HealthCheck(&hstat, true));
  EXPECT_EQ(this->link_tab_.free_nodes(), hstat.total_free_nodes);
  EXPECT_EQ(0UL, hstat.total_link_bufs);
  EXPECT_EQ(0UL, hstat.total_link_buf_bytes);
  EXPECT_EQ(0UL, hstat.leaked_nodes);
  EXPECT_EQ(0UL, hstat.bad_linked_link_bufs);
  EXPECT_EQ(0UL, hstat.too_long_link_bufs);
  EXPECT_EQ(0UL, hstat.too_short_link_bufs);
  EXPECT_EQ(0UL, hstat.cleared_link_bufs);
  EXPECT_EQ(0UL, hstat.recycled_leaked_nodes);
}

TYPED_PERF_TEST(ShmLinkTableTest, PerfRead_100B) {
  static bool is_inited = false;
  static shmc::link_buf_t lb;
  if (!is_inited) {
    is_inited = true;
    ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum)) << PERF_ABORT;
    char write_buf[100];
    ASSERT_TRUE((lb = this->link_tab_.New(write_buf, sizeof(write_buf)))) << PERF_ABORT;
  }
  char read_buf[1000];
  size_t buf_len = sizeof(read_buf);
  ASSERT_TRUE(this->link_tab_.Read(lb, (void*)read_buf, &buf_len)) << PERF_ABORT;
}

TYPED_PERF_TEST(ShmLinkTableTest, PerfReadStr_100B) {
  static bool is_inited = false;
  static shmc::link_buf_t lb;
  if (!is_inited) {
    is_inited = true;
    ASSERT_TRUE(this->link_tab_.InitForWrite(kShmKey, kNodeSize, kNodeNum)) << PERF_ABORT;
    char write_buf[100];
    ASSERT_TRUE((lb = this->link_tab_.New(write_buf, sizeof(write_buf)))) << PERF_ABORT;
  }
  std::string out;
  ASSERT_TRUE(this->link_tab_.Read(lb, &out)) << PERF_ABORT;
}

