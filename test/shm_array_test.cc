#include <vector>
#include "gtestx/gtestx.h" 
#include "shmc/shm_array.h"

constexpr const char* kShmKey = "0x10006";
constexpr size_t kSize = 10000;

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB>;

struct Node
{
  uint32_t id;
  uint32_t val;
} __attribute__((packed));

template <class Alloc>
class ShmArrayTest : public testing::Test
{
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

TYPED_TEST(ShmArrayTest, InitAndRW)
{
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

