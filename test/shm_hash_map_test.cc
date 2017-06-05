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
#include <unistd.h>
#include <vector>
#include "gtestx/gtestx.h" 
#include "shmc/shm_hash_map.h"

namespace {

constexpr const char* kShmKey = "0x10005";
constexpr size_t kKeyNum = 300000;
constexpr size_t kNodeSize = 32;
constexpr size_t kNodeNum = 2000000;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB,
                                 shmc::ANON, shmc::HEAP>;

}  // namespace

template <class Alloc>
class ShmHashMapTest : public testing::Test {
 protected:
  virtual ~ShmHashMapTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey + std::string("0"));
    this->alloc_.Unlink(kShmKey + std::string("1"));
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey + std::string("0"));
    this->alloc_.Unlink(kShmKey + std::string("1"));
  }
  shmc::ShmHashMap<size_t, Alloc> hash_map_;
  Alloc alloc_;

  decltype(hash_map_.hash_table_)* hash_table() {
    return &hash_map_.hash_table_;
  }
  decltype(hash_map_.link_table_)* link_table() {
    return &hash_map_.link_table_;
  }
};
TYPED_TEST_CASE(ShmHashMapTest, TestTypes);

TYPED_TEST(ShmHashMapTest, ReadWrite) {
  ASSERT_TRUE(this->hash_map_.InitForWrite(kShmKey, kKeyNum, kNodeSize, kNodeNum));
  ASSERT_TRUE(this->hash_map_.Insert(10000, "hello"));
  std::string out;
  ASSERT_TRUE(this->hash_map_.Find(10000, &out));
  ASSERT_EQ("hello", out);
  ASSERT_FALSE(this->hash_map_.Insert(10000, "world"));
  ASSERT_TRUE(this->hash_map_.Replace(10000, "world"));
  ASSERT_TRUE(this->hash_map_.Find(10000, &out));
  ASSERT_EQ("world", out);
  ASSERT_TRUE(this->hash_map_.Erase(10000));
  ASSERT_FALSE(this->hash_map_.Find(10000, &out));
}

TYPED_TEST(ShmHashMapTest, ReadWriteBig) {
  ASSERT_TRUE(this->hash_map_.InitForWrite(kShmKey, kKeyNum, kNodeSize, kNodeNum));
  int buf[1000];
  for (int i = 0; i < 1000; i++) buf[i] = i;
  ASSERT_TRUE(this->hash_map_.Insert(1000, buf, sizeof(buf)));
  std::string out;
  ASSERT_TRUE(this->hash_map_.Find(1000, &out));
  ASSERT_EQ(sizeof(buf), out.size());
  const int* out_buf = reinterpret_cast<const int*>(out.data());
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(i, out_buf[i]);
  }
  ASSERT_TRUE(this->hash_map_.Insert(1001, buf, sizeof(buf)));
  ASSERT_TRUE(this->hash_map_.Find(1000, &out));
  ASSERT_EQ(sizeof(buf), out.size());
  out_buf = reinterpret_cast<const int*>(out.data());
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(i, out_buf[i]);
  }
  ASSERT_TRUE(this->hash_map_.Erase(1000));
  ASSERT_TRUE(this->hash_map_.Erase(1001));
}

TYPED_TEST(ShmHashMapTest, Travel) {
  ASSERT_TRUE(this->hash_map_.InitForWrite(kShmKey, kKeyNum, kNodeSize, kNodeNum));
  for (size_t i = 0; i < 10; i++) {
    char buf[20];
    snprintf(buf, sizeof(buf), "%lu", i);
    ASSERT_TRUE(this->hash_map_.Insert(i, buf, sizeof(buf)));
  }
  // travel all
  size_t sum = 0;
  ASSERT_TRUE(this->hash_map_.Travel([&sum](const size_t& key, const std::string& val) {
    size_t val_n = strtoul(val.c_str(), NULL, 0);
    ASSERT_EQ(val_n, key);
    sum += key;
  }));
  ASSERT_EQ(45UL, sum);
  // travel with step 1
  sum = 0;
  typename decltype(this->hash_map_)::TravelPos pos;
  do {
    ASSERT_TRUE(this->hash_map_.Travel(&pos, 1, [&sum](const size_t& key, const std::string& val) {
      size_t val_n = strtoul(val.c_str(), NULL, 0);
      ASSERT_EQ(val_n, key);
      sum += key;
    }));
  } while (!pos.at_origin());
  ASSERT_EQ(45UL, sum);
}

TYPED_TEST(ShmHashMapTest, HealthCheck) {
  ASSERT_TRUE(this->hash_map_.InitForWrite(kShmKey, kKeyNum, kNodeSize, kNodeNum));
  ASSERT_EQ(100UL, this->hash_map_.free_percentage());
  typename decltype(this->hash_map_)::HealthStat hstat;
  ASSERT_TRUE(this->hash_map_.HealthCheck(&hstat, true));
  EXPECT_EQ(0UL, hstat.total_key_values);
  EXPECT_EQ(0UL, hstat.bad_key_values);
  EXPECT_EQ(0UL, hstat.cleared_key_values);
  EXPECT_EQ(0UL, hstat.leaked_values);
  EXPECT_EQ(0UL, hstat.recycled_leaked_values);
  // bad key value
  bool is_found;
  auto node = this->hash_table()->FindOrAlloc(1, &is_found);
  ASSERT_TRUE(node && !is_found);
  node->key = 1;
  node->link_buf.m.head = 1;
  ASSERT_TRUE(this->hash_map_.HealthCheck(&hstat, true));
  EXPECT_EQ(0UL, hstat.total_key_values);
  EXPECT_EQ(1UL, hstat.bad_key_values);
  EXPECT_EQ(1UL, hstat.cleared_key_values);
  EXPECT_EQ(0UL, hstat.leaked_values);
  EXPECT_EQ(0UL, hstat.recycled_leaked_values);
  // leaked value
  char buf[40];
  auto lb = this->link_table()->New(buf, sizeof(buf));
  ASSERT_TRUE(lb);
  ASSERT_TRUE(this->hash_map_.HealthCheck(&hstat, true));
  EXPECT_EQ(0UL, hstat.total_key_values);
  EXPECT_EQ(0UL, hstat.bad_key_values);
  EXPECT_EQ(0UL, hstat.cleared_key_values);
  EXPECT_EQ(1UL, hstat.leaked_values);
  EXPECT_EQ(1UL, hstat.recycled_leaked_values);
  // check again
  ASSERT_TRUE(this->hash_map_.HealthCheck(&hstat, true));
  EXPECT_EQ(0UL, hstat.total_key_values);
  EXPECT_EQ(0UL, hstat.bad_key_values);
  EXPECT_EQ(0UL, hstat.cleared_key_values);
  EXPECT_EQ(0UL, hstat.leaked_values);
  EXPECT_EQ(0UL, hstat.recycled_leaked_values);
}

template <class Alloc>
class ShmHashMapPerfTest : public ShmHashMapTest<Alloc> {
 protected:
  virtual ~ShmHashMapPerfTest() {}
  virtual void SetUp() {
    ShmHashMapTest<Alloc>::SetUp();
    this->hash_map_.InitForWrite(kShmKey, kKeyNum, kNodeSize, kNodeNum);
    char buf[200];
    for (size_t i = 0; ; i++) {
      for (size_t j = 0; j < sizeof(buf); j++) buf[j] = (char)i;
      if (!this->hash_map_.Insert(i, buf, sizeof(buf))) {
        std::cerr << "Insert done at i: " << i << std::endl;
        break;;
      }
    }
  }
  virtual void TearDown() {
    ShmHashMapTest<Alloc>::TearDown();
  }
};
TYPED_TEST_CASE(ShmHashMapPerfTest, TestTypes);

TYPED_PERF_TEST(ShmHashMapPerfTest, PerfRead_100B) {
  std::string out;
  ASSERT_TRUE(this->hash_map_.Find(200000, &out)) << PERF_ABORT;
}

namespace {

constexpr size_t kTestKeyMax = 100;

using PerfTestCTypes = testing::Types<shmc::POSIX, shmc::SVIPC,
                                      shmc::SVIPC_HugeTLB, shmc::ANON>;

}  // namespace

template <class Alloc>
class ShmHashMapPerfTestC : public ShmHashMapPerfTest<Alloc> {
 protected:
  virtual ~ShmHashMapPerfTestC() {}
  virtual void SetUp() {
    ShmHashMapPerfTest<Alloc>::SetUp();
    if (!(wpid_ = fork())) {
      WriteProcess();
      exit(0);
    }
  }
  virtual void TearDown() {
    kill(wpid_, SIGTERM);
    waitpid(wpid_, nullptr, 0);
    ShmHashMapPerfTest<Alloc>::TearDown();
  }
  void WriteProcess() {
    srand(time(NULL));
    this->hash_map_.Erase(kTestKeyMax);
    char buf[200];
    while (true) {
      size_t k = rand() % kTestKeyMax;
      size_t n = rand() % sizeof(buf) + 1;
      for (size_t i = 0; i < sizeof(buf); i++) buf[i] = (char)k;
      if (!this->hash_map_.Replace(k, (void*)buf, n)) {
        std::cerr << "Replace fail! key=" << k << " n=" << n << std::endl;
      }
    }
  }
  pid_t wpid_;
};
TYPED_TEST_CASE(ShmHashMapPerfTestC, PerfTestCTypes);

TYPED_PERF_TEST(ShmHashMapPerfTestC, PerfRead_Concurrent) {
  static size_t key = 0;
  std::string out;
  ASSERT_TRUE(this->hash_map_.Find(key, &out)) << "key=" << key << PERF_ABORT;
  for (size_t i = 0; i < out.size(); i++) {
    if ((char)key != out[i]) {
      this->hash_map_.Dump(key);
      shmc::Utils::Log(shmc::kInfo, "dump out:\n%s", shmc::Utils::Hex(out.data(), out.size()));
    }
    ASSERT_EQ((char)key, out[i]) << "i = " << i << " pid=" << getpid() << PERF_ABORT;
  }
  key = (key + 1) % kTestKeyMax;
}

