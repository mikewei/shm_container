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
#include <stdlib.h>
#include <atomic>
#include <thread>
#include "gtestx/gtestx.h" 
#include "shmc/shm_queue.h"
#include "shmc/shm_array.h"

namespace {

constexpr const char* kShmKey = "0x10007";
constexpr size_t kQueueBufSize = 1024*1024*1;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB,
                                 shmc::ANON, shmc::HEAP>;

}  // namespace

template <class Alloc>
class ShmQueueTest : public testing::Test {
 protected:
  virtual ~ShmQueueTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
    srand(time(nullptr));
    ASSERT_TRUE(this->queue_w_.InitForWrite(kShmKey, kQueueBufSize));
    ASSERT_TRUE(this->queue_r_.InitForRead(kShmKey));
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  shmc::ShmQueue<Alloc> queue_w_;
  shmc::ShmQueue<Alloc> queue_r_;
  Alloc alloc_;
};

// need partial specialization for shmc::ANON/HEAP as InitForRead does not work
template <>
class ShmQueueTest<shmc::ANON> : public testing::Test {
 protected:
  ShmQueueTest() : queue_r_(queue_w_) {}
  virtual ~ShmQueueTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
    srand(time(nullptr));
    ASSERT_TRUE(this->queue_w_.InitForWrite(kShmKey, kQueueBufSize));
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  shmc::ShmQueue<shmc::HEAP> queue_w_;
  shmc::ShmQueue<shmc::HEAP>& queue_r_;
  shmc::HEAP alloc_;
};
template <>
class ShmQueueTest<shmc::HEAP> : public testing::Test {
 protected:
  ShmQueueTest() : queue_r_(queue_w_) {}
  virtual ~ShmQueueTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
    srand(time(nullptr));
    ASSERT_TRUE(this->queue_w_.InitForWrite(kShmKey, kQueueBufSize));
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  shmc::ShmQueue<shmc::HEAP> queue_w_;
  shmc::ShmQueue<shmc::HEAP>& queue_r_;
  shmc::HEAP alloc_;
};

TYPED_TEST_CASE(ShmQueueTest, TestTypes);

TYPED_TEST(ShmQueueTest, PushOverloads) {
  char buf[] = "hello";
  std::string out;
  // Push(const void*, size_t)
  ASSERT_TRUE(this->queue_w_.Push(nullptr, 0));
  ASSERT_TRUE(this->queue_r_.Pop(&out));
  ASSERT_TRUE(out.empty());
  ASSERT_TRUE(this->queue_w_.Push(buf, sizeof(buf) - 1));
  ASSERT_TRUE(this->queue_r_.Pop(&out));
  ASSERT_EQ(buf, out);
  // Push(const std::string&)
  ASSERT_TRUE(this->queue_w_.Push(""));
  ASSERT_TRUE(this->queue_r_.Pop(&out));
  ASSERT_TRUE(out.empty());
  std::string src{buf};
  ASSERT_TRUE(this->queue_w_.Push(src));
  ASSERT_TRUE(this->queue_r_.Pop(&out));
  ASSERT_EQ(src, out);
}

TYPED_TEST(ShmQueueTest, PopOverloads) {
  std::string src{"hello"};
  // Pop(void*, size_t*)
  char buf[64] = {0};
  size_t len = sizeof(buf);
  ASSERT_TRUE(this->queue_w_.Push(src));
  ASSERT_TRUE(this->queue_r_.Pop(buf, &len));
  ASSERT_EQ(src.size(), len);
  ASSERT_EQ(src, buf);
  ASSERT_FALSE(this->queue_r_.Pop(buf, &len));
  ASSERT_TRUE(this->queue_w_.Push(src));
  len = 4;
  ASSERT_FALSE(this->queue_r_.Pop(buf, &len));
  len = 5;
  ASSERT_TRUE(this->queue_r_.Pop(buf, &len));
  ASSERT_TRUE(this->queue_w_.Push(src));
  ASSERT_TRUE(this->queue_r_.Pop(nullptr, nullptr));
  ASSERT_FALSE(this->queue_r_.Pop(nullptr, nullptr));
  // Pop(std::string*)
  std::string out;
  ASSERT_TRUE(this->queue_w_.Push(src));
  ASSERT_TRUE(this->queue_r_.Pop(&out));
  ASSERT_EQ(src, out);
  ASSERT_FALSE(this->queue_r_.Pop(&out));
  ASSERT_TRUE(this->queue_w_.Push(src));
  ASSERT_TRUE(this->queue_r_.Pop(nullptr));
  ASSERT_FALSE(this->queue_r_.Pop(nullptr));
}

TYPED_TEST(ShmQueueTest, PushThenPopLoop) {
  char buf[2048];
  std::string out;
  for (int i = 0; i < 100000; i++) {
    size_t len = rand() % sizeof(buf);
    std::string src{buf, len};
    ASSERT_TRUE(this->queue_w_.Push(src));
    ASSERT_TRUE(this->queue_r_.Pop(&out));
    ASSERT_EQ(src, out);
  }
}

TYPED_TEST(ShmQueueTest, ZeroCopyPushPop) {
  char buf[] = "hello";
  size_t len = sizeof(buf) - 1;
  typename shmc::ZeroCopyBuf zcb;
  // normal
  ASSERT_TRUE(this->queue_w_.ZeroCopyPushPrepare(len, &zcb));
  memcpy(zcb.ptr, buf, len);
  ASSERT_TRUE(this->queue_w_.ZeroCopyPushCommit(zcb));
  ASSERT_TRUE(this->queue_r_.ZeroCopyPopPrepare(&zcb));
  ASSERT_TRUE(this->queue_r_.ZeroCopyPopCommit(zcb));
  ASSERT_EQ(len, zcb.len);
  ASSERT_EQ(0, memcmp(zcb.ptr, buf, len));
  ASSERT_FALSE(this->queue_r_.ZeroCopyPopPrepare(&zcb));
  // commit less
  ASSERT_TRUE(this->queue_w_.ZeroCopyPushPrepare(len*2, &zcb));
  ASSERT_EQ(len*2, zcb.len);
  zcb.len = len;
  memcpy(zcb.ptr, buf, len);
  ASSERT_TRUE(this->queue_w_.ZeroCopyPushCommit(zcb));
  ASSERT_TRUE(this->queue_r_.ZeroCopyPopPrepare(&zcb));
  ASSERT_TRUE(this->queue_r_.ZeroCopyPopCommit(zcb));
  ASSERT_EQ(0, memcmp(zcb.ptr, buf, len));
  ASSERT_FALSE(this->queue_r_.ZeroCopyPopPrepare(&zcb));
}

namespace {

using PerfTestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB,
                                     shmc::ANON>;

}  // namespace

template <class Alloc>
class ShmQueueConcPerfTest : public ShmQueueTest<Alloc> {
 protected:
  struct SharedStatus {
    std::atomic<uint64_t> write_count;
    std::atomic<uint64_t> overflow_count;
    std::atomic<uint64_t> read_count;
    std::atomic<uint64_t> error_count;
    uint64_t total_overflow_count;
    uint64_t total_error_count;
  };
  static constexpr const char* kSharedShmKey = "0x10008";
  using SharedShmAlloc = shmc::SVIPC;

  virtual ~ShmQueueConcPerfTest() {}
  virtual void SetUp() {
    ShmQueueTest<Alloc>::SetUp();
    SharedShmAlloc().Unlink(kSharedShmKey);
    ASSERT_TRUE(status_.InitForWrite(kSharedShmKey, 1));
    timer_thread_ = std::thread([this] {
      unsigned count = 0;
      while (!stop_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (++count % 100 == 0) OnTimer();
      }
      EXPECT_EQ(0UL, status_[0].total_overflow_count);
      EXPECT_EQ(0UL, status_[0].total_error_count);
    });
    if (!(wpid_ = fork())) {
      ConsumerProcess();
      exit(0);
    }
  }
  virtual void TearDown() {
    kill(wpid_, SIGTERM);
    waitpid(wpid_, nullptr, 0);
    stop_.store(true, std::memory_order_relaxed);
    timer_thread_.join();
    SharedShmAlloc().Unlink(kSharedShmKey);
    ShmQueueTest<Alloc>::TearDown();
  }
  void ConsumerProcess() {
    union {
      char buf[1024*16];
      uint64_t seq;
    } un;
    size_t len;
    uint64_t local_read_count = 0;
    uint64_t local_error_count = 0;
    uint64_t expected_seq = 0;
    while (true) {
      len = sizeof(un.buf);
      if (this->queue_r_.Pop(un.buf, &len)) {
        local_read_count++;
        if (un.seq != expected_seq) {
          local_error_count++;
        }
        expected_seq = un.seq + 1;
      }
      if ((local_read_count & 0xff) == 0) {
        status_[0].read_count += local_read_count;
        status_[0].error_count += local_error_count;
        local_read_count = 0;
        local_error_count = 0;
      }
    }
  }
  void OnTimer() {
    std::cout << "write " << status_[0].write_count << "/s  "
              << "read " <<  status_[0].read_count << "/s  "
              << "overflow " <<  status_[0].overflow_count << "/s  "
              << "error " << status_[0].error_count << "/s" << std::endl;
    status_[0].total_overflow_count += status_[0].overflow_count;
    status_[0].total_error_count += status_[0].error_count;
    status_[0].write_count.store(0, std::memory_order_relaxed);
    status_[0].read_count.store(0, std::memory_order_relaxed);
    status_[0].overflow_count.store(0, std::memory_order_relaxed);
    status_[0].error_count.store(0, std::memory_order_relaxed);
  }

  pid_t wpid_;
  std::atomic_bool stop_{false};
  std::thread timer_thread_;
  shmc::ShmArray<SharedStatus, SharedShmAlloc> status_;
};
TYPED_TEST_CASE(ShmQueueConcPerfTest, PerfTestTypes);

TYPED_PERF_TEST_OPT(ShmQueueConcPerfTest, ConcWrite, 1000000, 1500) {
  static uint64_t local_write_count = 0;
  static uint64_t local_overflow_count = 0;
  static union {
    char buf[1024*16];
    uint64_t seq;
  } un;
  if (this->queue_w_.Push(un.buf, 100)) {
    local_write_count++;
    un.seq++;
  } else {
    local_overflow_count++;
  }
  if ((local_write_count & 0xff) == 0) {
    this->status_[0].write_count += local_write_count;
    this->status_[0].overflow_count += local_overflow_count;
    local_write_count = 0;
    local_overflow_count = 0;
  }
}

