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
 *     * The name of of its contributors may not be used to endorse or 
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
#include "shmc/shm_array.h"

constexpr const char* kShmKey = "0x10006";
constexpr size_t kSize = 10000;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB>;

struct Node {
  uint32_t id;
  uint32_t val;
} __attribute__((packed));

template <class Alloc>
class ShmArrayTest : public testing::Test {
 protected:
  virtual ~ShmArrayTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  shmc::ShmArray<Node, Alloc> array_;
  shmc::ShmArray<Node, Alloc> array_ro_;
  Alloc alloc_;
};
TYPED_TEST_CASE(ShmArrayTest, TestTypes);

TYPED_TEST(ShmArrayTest, InitAndRW) {
  ASSERT_TRUE(this->array_.InitForWrite(kShmKey, kSize));
  ASSERT_TRUE(this->array_ro_.InitForRead(kShmKey));
  for (size_t i = 0; i < kSize; i++) {
    this->array_[i].id = i;
  }
  const decltype(this->array_ro_)& array_ro = this->array_ro_;
  for (size_t i = 0; i < kSize; i++) {
    auto id = array_ro[i].id;
    ASSERT_EQ(i, id);
  }
}

