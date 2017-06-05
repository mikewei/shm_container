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
#include "gtestx/gtestx.h"
#include "shmc/shm_handle.h"
#include "shmc/svipc_shm_alloc.h"

namespace {

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB>;

}  // namespace

template <class Alloc>
class ShmHandleTest : public testing::Test {
 protected:
  virtual ~ShmHandleTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    Alloc().Unlink("0x10002");
  }
  virtual void TearDown() {
    Alloc().Unlink("0x10002");
  }
  shmc::ShmHandle<char, Alloc> shm_handle_;
};
TYPED_TEST_CASE(ShmHandleTest, TestTypes);

TYPED_TEST(ShmHandleTest, InitForWrite) {
  ASSERT_TRUE(this->shm_handle_.InitForWrite("0x10002", 10000000));
  ASSERT_TRUE(this->shm_handle_.is_newly_created());
  ASSERT_EQ(this->shm_handle_.ptr()[10000000-1], 0);
  this->shm_handle_.ptr()[10000000-1] = 1;
  ASSERT_FALSE(this->shm_handle_.InitForWrite("0x10002", 10000000));
  this->shm_handle_.Reset();
  ASSERT_TRUE(this->shm_handle_.InitForWrite("0x10002", 10000000));
  ASSERT_FALSE(this->shm_handle_.is_newly_created());
  this->shm_handle_.ptr()[10000000-1]++;
  ASSERT_EQ(this->shm_handle_.ptr()[10000000-1], 2);
}

TYPED_TEST(ShmHandleTest, InitForRead) {
  ASSERT_FALSE(this->shm_handle_.InitForRead("0x10002", 10000000));
  ASSERT_TRUE(this->shm_handle_.InitForWrite("0x10002", 10000000));
  ASSERT_TRUE(this->shm_handle_.is_newly_created());
  ASSERT_EQ(this->shm_handle_.ptr()[10000000-1], 0);
  this->shm_handle_.ptr()[10000000-1] = 1;
  this->shm_handle_.Reset();
  ASSERT_TRUE(this->shm_handle_.InitForRead("0x10002", 10000000));
  ASSERT_EQ(this->shm_handle_.ptr()[10000000-1], 1);
  ASSERT_FALSE(this->shm_handle_.InitForRead("0x10002", 10000000));
}
