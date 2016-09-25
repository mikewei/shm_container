#include "gtestx/gtestx.h" 
#include "shmc/shm_handle.h"
#include "shmc/heap_alloc.h"

template <class Alloc>
class ShmHandleForHeapTest : public testing::Test
{
protected:
  virtual ~ShmHandleForHeapTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    Alloc().Unlink("");
  }
  virtual void TearDown() {
    Alloc().Unlink("");
  }
  shmc::ShmHandle<char, Alloc> shm_handle_;
};

using TestTypes = testing::Types<shmc::HEAP>;
TYPED_TEST_CASE(ShmHandleForHeapTest, TestTypes);

TYPED_TEST(ShmHandleForHeapTest, InitForWrite)
{
  ASSERT_TRUE(this->shm_handle_.InitForWrite("", 10000000));
  ASSERT_TRUE(this->shm_handle_.is_newly_created());
  ASSERT_EQ(this->shm_handle_.ptr()[10000000-1], 0);
  this->shm_handle_.ptr()[10000000-1] = 1;
  ASSERT_FALSE(this->shm_handle_.InitForWrite("", 10000000));
  this->shm_handle_.Reset();
  ASSERT_TRUE(this->shm_handle_.InitForWrite("", 10000000));
  ASSERT_TRUE(this->shm_handle_.is_newly_created());
  this->shm_handle_.ptr()[10000000-1]++;
  ASSERT_EQ(this->shm_handle_.ptr()[10000000-1], 1);
}

