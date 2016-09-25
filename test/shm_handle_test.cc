#include "gtestx/gtestx.h" 
#include "shmc/shm_handle.h"
#include "shmc/svipc_shm_alloc.h"

template <class Alloc>
class ShmHandleTest : public testing::Test
{
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

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB>;
TYPED_TEST_CASE(ShmHandleTest, TestTypes);

TYPED_TEST(ShmHandleTest, InitForWrite)
{
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

TYPED_TEST(ShmHandleTest, InitForRead)
{
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
