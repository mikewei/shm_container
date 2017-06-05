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
#include "shmc/shm_sync_buf.h"

namespace {

constexpr const char* kShmKey = "0x10005";
constexpr size_t kSyncBufSize = 1024*1024*100;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB,
                                 shmc::ANON, shmc::HEAP>;

}  // namespace

template <class Alloc>
class ShmSyncBufTest : public testing::Test {
 protected:
  virtual ~ShmSyncBufTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
    ASSERT_TRUE(this->sync_buf_.InitForWrite(kShmKey, kSyncBufSize));
    if (shmc::impl::AllocTraits<Alloc>::is_named) {
      this->sync_buf_ro_ = &this->sync_buf2_;
      ASSERT_TRUE(this->sync_buf_ro_->InitForRead(kShmKey));
    } else {
      this->sync_buf_ro_ = &this->sync_buf_;
    }
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  shmc::ShmSyncBuf<Alloc> sync_buf_;
  shmc::ShmSyncBuf<Alloc> sync_buf2_;
  shmc::ShmSyncBuf<Alloc>* sync_buf_ro_;
  Alloc alloc_;
};
TYPED_TEST_CASE(ShmSyncBufTest, TestTypes);

TYPED_TEST(ShmSyncBufTest, Init) {
}

TYPED_TEST(ShmSyncBufTest, PushRead) {
  ASSERT_TRUE(this->sync_buf_.Push("hello", 5));
  shmc::SyncIter it = this->sync_buf_.Head();
  char buf[6] = {0};
  size_t len = sizeof(buf);
  shmc::SyncMeta meta;
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, buf, &len));
  ASSERT_EQ(0, strncmp(buf, "hello", sizeof(buf)));
  std::string out;
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello", out);
  ASSERT_TRUE(this->sync_buf_.Next(&it));
  ASSERT_EQ(0, this->sync_buf_.Read(it, &meta, buf, &len));
  ASSERT_EQ(0, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_TRUE(this->sync_buf_.Push("world", 5));
  len = sizeof(buf);
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, buf, &len));
  ASSERT_EQ(0, strncmp(buf, "world", sizeof(buf)));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("world", out);
}

TYPED_TEST(ShmSyncBufTest, FindBySeq) {
  ASSERT_TRUE(this->sync_buf_.Push("hello 0", 7));
  ASSERT_TRUE(this->sync_buf_.Push("hello 1", 7));
  ASSERT_TRUE(this->sync_buf_.Push("hello 2", 7));
  ASSERT_TRUE(this->sync_buf_.Push("hello 3", 7));
  ASSERT_TRUE(this->sync_buf_.Push("hello 4", 7));
  ASSERT_TRUE(this->sync_buf_.Push("hello 5", 7));
  shmc::SyncMeta meta;
  std::string out;
  ASSERT_EQ(1, this->sync_buf_.Read(this->sync_buf_.Head(), &meta, &out));
  ASSERT_EQ("hello 0", out);
  shmc::SyncIter it;
  ASSERT_FALSE(this->sync_buf_.FindBySeq(meta.seq-1, &it));
  ASSERT_FALSE(this->sync_buf_.FindBySeq(meta.seq+6, &it));
  uint64_t seq = meta.seq;
  for (size_t i = 0; i <= 5; i++, seq++) {
    ASSERT_TRUE(this->sync_buf_.FindBySeq(seq, &it));
    ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
    char expected[10];
    snprintf(expected, sizeof(expected), "hello %lu", i);
    ASSERT_EQ(expected, out);
  }
}

TYPED_TEST(ShmSyncBufTest, FindByTime) {
  shmc::SyncIter it;
  shmc::SyncMeta meta;
  std::string out;
  ASSERT_FALSE(this->sync_buf_.FindByTime({0, 0}, &it));
  ASSERT_TRUE(this->sync_buf_.Push("hello 0", 7, {0, 1}));
  ASSERT_TRUE(this->sync_buf_.Push("hello 1", 7, {1, 0}));
  ASSERT_TRUE(this->sync_buf_.Push("hello 2", 7, {2, 0}));
  ASSERT_TRUE(this->sync_buf_.FindByTime({0, 0}, &it));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello 0", out);
  ASSERT_TRUE(this->sync_buf_.FindByTime({0, 1}, &it));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello 0", out);
  ASSERT_TRUE(this->sync_buf_.FindByTime({0, 2}, &it));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello 1", out);
  ASSERT_TRUE(this->sync_buf_.FindByTime({1, 0}, &it));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello 1", out);
  ASSERT_TRUE(this->sync_buf_.FindByTime({1, 1}, &it));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello 2", out);
  ASSERT_TRUE(this->sync_buf_.FindByTime({2, 0}, &it));
  ASSERT_EQ(1, this->sync_buf_.Read(it, &meta, &out));
  ASSERT_EQ("hello 2", out);
  ASSERT_FALSE(this->sync_buf_.FindByTime({2, 1}, &it));
}

// Push perf

template <class Alloc>
class ShmSyncBufPerfTest : public ShmSyncBufTest<Alloc> {
 protected:
  virtual ~ShmSyncBufPerfTest() {}
  virtual void SetUp() {
    ShmSyncBufTest<Alloc>::SetUp();
  }
  virtual void TearDown() {
    ShmSyncBufTest<Alloc>::TearDown();
  }
};
TYPED_TEST_CASE(ShmSyncBufPerfTest, TestTypes);

TYPED_PERF_TEST(ShmSyncBufPerfTest, Push_10B) {
  char buf[10];
  this->sync_buf_.Push(buf, sizeof(buf));
}

TYPED_PERF_TEST(ShmSyncBufPerfTest, Push_100B) {
  char buf[100];
  this->sync_buf_.Push(buf, sizeof(buf));
}

TYPED_PERF_TEST(ShmSyncBufPerfTest, Push_1000B) {
  char buf[1000];
  this->sync_buf_.Push(buf, sizeof(buf));
}

// Read perf

namespace {

using ReadPerfTestTypes = testing::Types<shmc::POSIX, shmc::SVIPC,
                                         shmc::SVIPC_HugeTLB, shmc::ANON>;

}  // namespace

template <class Alloc>
class ShmSyncBufReadPerfTest : public ShmSyncBufPerfTest<Alloc> {
 protected:
  virtual ~ShmSyncBufReadPerfTest() {}
  virtual void SetUp() {
    ShmSyncBufPerfTest<Alloc>::SetUp();
    if (!(wpid_ = fork())) {
      WriteProcess();
      exit(0);
    }
    it_ = this->sync_buf_ro_->Head();
  }
  virtual void TearDown() {
    kill(wpid_, SIGTERM);
    waitpid(wpid_, nullptr, 0);
    ShmSyncBufPerfTest<Alloc>::TearDown();
  }
  void WriteProcess() {
    srand(time(NULL));
    char buf[200];
    size_t count = 0;
    while (true) {
      size_t n = rand() % sizeof(buf) + 1;
      for (size_t i = 0; i < n; i++) buf[i] = (char)n;
      if (!this->sync_buf_.Push(buf, n)) {
        std::cerr << "Push fail! len=" << n << std::endl;
      }
      if (++count >= 100) {
        usleep(1000);
        count = 0;
      }
    }
  }
  pid_t wpid_;
  shmc::SyncIter it_;
};
TYPED_TEST_CASE(ShmSyncBufReadPerfTest, ReadPerfTestTypes);

TYPED_PERF_TEST(ShmSyncBufReadPerfTest, ConcReadCheck) {
  shmc::SyncMeta meta;
  char buf[1000];
  size_t len;
  int ret;
  do {
    len = sizeof(buf);
    ret = this->sync_buf_ro_->Read(this->it_, &meta, buf, &len);
  } while (ret == 0);
  ASSERT_EQ(1, ret) << PERF_ABORT;
  ASSERT_LE(len, sizeof(buf)) << PERF_ABORT;
  for (size_t i = 0; i < len; i++) {
    ASSERT_EQ((char)len, buf[i]) << PERF_ABORT;
  }
  ASSERT_TRUE(this->sync_buf_ro_->Next(&this->it_)) << PERF_ABORT;
}

TYPED_PERF_TEST(ShmSyncBufReadPerfTest, ConcReadLoop) {
  shmc::SyncMeta meta;
  char buf[1000];
  size_t len = sizeof(buf);
  int ret = this->sync_buf_ro_->Read(this->it_, &meta, buf, &len);
  ASSERT_TRUE(ret == 1 || ret == 0) << PERF_ABORT;
  if (ret == 1) {
    ASSERT_TRUE(this->sync_buf_ro_->Next(&this->it_)) << PERF_ABORT;
  } else {
    this->it_ = this->sync_buf_ro_->Head();
  }
}

TYPED_PERF_TEST(ShmSyncBufReadPerfTest, ConcReadStringLoop) {
  shmc::SyncMeta meta;
  std::string out;
  int ret = this->sync_buf_ro_->Read(this->it_, &meta, &out);
  ASSERT_TRUE(ret == 1 || ret == 0) << PERF_ABORT;
  if (ret == 1) {
    ASSERT_TRUE(this->sync_buf_ro_->Next(&this->it_)) << PERF_ABORT;
  } else {
    this->it_ = this->sync_buf_ro_->Head();
  }
}

TYPED_PERF_TEST(ShmSyncBufReadPerfTest, ConcReadMetaLoop) {
  shmc::SyncMeta meta;
  uint32_t val;
  size_t len = sizeof(val);
  int ret = this->sync_buf_ro_->Read(this->it_, &meta, &val, &len);
  ASSERT_TRUE(ret == 1 || ret == 0) << PERF_ABORT;
  if (ret == 1) {
    ASSERT_TRUE(this->sync_buf_ro_->Next(&this->it_)) << PERF_ABORT;
  } else {
    this->it_ = this->sync_buf_ro_->Head();
  }
}
