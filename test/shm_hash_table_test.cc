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
#include "shmc/shm_hash_table.h"

namespace {

constexpr const char* kShmKey = "0x10003";
constexpr size_t kCapacity = 2500000;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB,
                                 shmc::ANON, shmc::HEAP>;

struct Node {
  uint64_t key;
  uint64_t value;
  std::pair<bool,uint64_t> Key() const volatile {
    return std::make_pair((key != 0), key);
  }
  std::pair<bool,uint64_t> Key(uint64_t arg) const volatile {
    return std::make_pair((key > arg), key);
  }
};

}  // namespace

template <class Alloc>
class ShmHashTableTest : public testing::Test {
 protected:
  virtual ~ShmHashTableTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    this->alloc_.Unlink(kShmKey);
  }
  virtual void TearDown() {
    this->alloc_.Unlink(kShmKey);
  }
  shmc::ShmHashTable<uint64_t, Node, Alloc> hash_tab_;
  Alloc alloc_;
};
TYPED_TEST_CASE(ShmHashTableTest, TestTypes);

TYPED_TEST(ShmHashTableTest, Write) {
  ASSERT_TRUE(this->hash_tab_.InitForWrite(kShmKey, kCapacity));
  ASSERT_GT(this->hash_tab_.ideal_capacity(), kCapacity);
  ASSERT_GT(this->hash_tab_.expected_capacity(), kCapacity);
  srand(time(NULL));
  ASSERT_FALSE(this->hash_tab_.Find(1));
  size_t count = 0;
  while (true) {
    bool is_found;
    uint64_t k = rand();
    volatile Node* node = this->hash_tab_.FindOrAlloc(k, &is_found);
    if (!node) {
      size_t perc = count * 100 / this->hash_tab_.ideal_capacity();
      fprintf(stderr, "Almost full: count = %lu [%lu%%]\n", count, perc);
      ASSERT_GT(perc, 85UL) << "Not reach target of 85%!";
      ASSERT_GT(count, this->hash_tab_.expected_capacity()) << "Not reach expected capacity!";
      break;
    } else if (!is_found) {
      node->key = k;
      node->value = k;
      count++;
    }
  }
}

TYPED_TEST(ShmHashTableTest, ArgFind) {
  ASSERT_TRUE(this->hash_tab_.InitForWrite(kShmKey, kCapacity));
  ASSERT_FALSE(this->hash_tab_.Find(1));
  bool is_found;
  volatile Node* node = this->hash_tab_.FindOrAlloc(1, &is_found);
  ASSERT_TRUE(node && !is_found);
  node->key = 1;
  node->value = 1;
  ASSERT_TRUE(this->hash_tab_.Find(1));
  ASSERT_TRUE(this->hash_tab_.Find(1, 0UL));
  ASSERT_FALSE(this->hash_tab_.Find(1, 1UL));
  ASSERT_TRUE(this->hash_tab_.FindOrAlloc(1, 1UL, &is_found));
  ASSERT_FALSE(is_found);
}

TYPED_TEST(ShmHashTableTest, Travel) {
  ASSERT_TRUE(this->hash_tab_.InitForWrite(kShmKey, kCapacity));
  for (uint64_t k = 1; k <= 10; k++) {
    bool is_found;
    volatile Node* node = this->hash_tab_.FindOrAlloc(k, &is_found);
    ASSERT_TRUE(node && !is_found);
    node->key = k;
    node->value = k;
  }
  // travel all
  uint64_t sum = 0;
  ASSERT_TRUE(this->hash_tab_.Travel([&sum](volatile Node* node) {
    ASSERT_EQ(node->value, node->key);
    sum += node->key;
  }));
  ASSERT_EQ(55UL, sum);
  // travel with step 1
  typename decltype(this->hash_tab_)::TravelPos pos;
  sum = 0;
  do {
    ASSERT_TRUE(this->hash_tab_.Travel(&pos, 1, [&sum](volatile Node* node) {
      ASSERT_EQ(node->value, node->key);
      sum += node->key;
    }));
  } while (!pos.at_origin());
  ASSERT_EQ(55UL, sum);
}

template <class Alloc>
class ShmHashTableReadTest : public ShmHashTableTest<Alloc> {
 protected:
  virtual ~ShmHashTableReadTest() {}
  virtual void SetUp() {
    ShmHashTableTest<Alloc>::SetUp();
    shmc::ShmHashTable<uint64_t, Node, Alloc>& hash_tab = this->hash_tab_;
    hash_tab.InitForWrite(kShmKey, kCapacity);
    srand(time(NULL));
    size_t sample = hash_tab.expected_capacity() / 8192;
    size_t count = 0;
    while (true) {
      bool is_found;
      uint64_t k = rand();
      volatile Node* node = hash_tab.FindOrAlloc(k, &is_found);
      if (!node) {
        size_t perc = count * 100 / hash_tab.ideal_capacity();
        fprintf(stderr, "Almost full: count = %lu [%lu%%]\n", count, perc);
        break;
      } else if (!is_found) {
        node->key = k;
        node->value = k;
        count++;
        if (count % sample == 0) keys_.push_back(k);
      }
    }
  }
  virtual void TearDown() {
    ShmHashTableTest<Alloc>::TearDown();
  }
  std::vector<uint64_t> keys_;
};
TYPED_TEST_CASE(ShmHashTableReadTest, TestTypes);

TYPED_TEST(ShmHashTableReadTest, Read) {
  ASSERT_GT(this->hash_tab_.ideal_capacity(), kCapacity);
  ASSERT_GT(this->hash_tab_.expected_capacity(), kCapacity);
  for (uint64_t k : this->keys_) {
    volatile Node* node = this->hash_tab_.Find(k);
    ASSERT_TRUE(node);
    ASSERT_EQ(node->value, node->key);
  }
}

TYPED_PERF_TEST(ShmHashTableReadTest, PerfRead) {
  static uint64_t k = 0;
  this->hash_tab_.Find(k++);
}

TYPED_PERF_TEST(ShmHashTableReadTest, PerfReadExist) {
  static uint64_t k = 0;
  this->hash_tab_.Find(this->keys_[k++ % 8192]);
}
