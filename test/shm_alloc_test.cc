#include "gtestx/gtestx.h" 
#include "shmc/svipc_shm_alloc.h"
#include "shmc/posix_shm_alloc.h"

template <class T>
class ShmAllocTest : public testing::Test
{
protected:
  virtual ~ShmAllocTest() {}
  virtual void SetUp() {
    shmc::SetLogHandler(shmc::kDebug, [](shmc::LogLevel lv, const char* s) {
      fprintf(stderr, "[%d] %s", lv, s);
    });
    alloc_ = new T();
    alloc_->Unlink("0x10001");
  }
  virtual void TearDown() {
    alloc_->Unlink("0x10001");
    delete alloc_;
  }
  bool CheckShmSize(size_t mapped_size, size_t expected) {
    return mapped_size == shmc::Utils::RoundAlign(expected, alloc_->AlignSize());
  }
  shmc::impl::ShmAlloc* alloc_;
};

using TestTypes = testing::Types<shmc::POSIX, shmc::SVIPC, shmc::SVIPC_HugeTLB>;
TYPED_TEST_CASE(ShmAllocTest, TestTypes);

TYPED_TEST(ShmAllocTest, CreateUnlink)
{
  ASSERT_TRUE(this->alloc_->Attach_ExclCreate("0x10001", 10000000UL));
  ASSERT_FALSE(this->alloc_->Attach_ExclCreate("0x10001", 10000000UL));
  ASSERT_EQ(shmc::impl::kErrAlreadyExist, this->alloc_->last_errno());
  ASSERT_TRUE(this->alloc_->Unlink("0x10001"));
  ASSERT_FALSE(this->alloc_->Unlink("0x10001"));
  ASSERT_EQ(shmc::impl::kErrNotExist, this->alloc_->last_errno());
  ASSERT_TRUE(this->alloc_->Attach_ExclCreate("0x10001", 10000000UL));
  ASSERT_TRUE(this->alloc_->Attach_ForceCreate("0x10001", 10000000UL));
  ASSERT_TRUE(this->alloc_->Unlink("0x10001"));
}

TYPED_TEST(ShmAllocTest, AttachDetach)
{
  void* addr;
  ASSERT_FALSE((addr = this->alloc_->Attach_ReadOnly("0x10001", 10000000UL)));
  ASSERT_TRUE((addr = this->alloc_->Attach_MayCreate("0x10001", 10000000UL)));
  ASSERT_EQ(0, *reinterpret_cast<int*>(static_cast<char*>(addr) + 10000000 - 4));
  *reinterpret_cast<int*>(static_cast<char*>(addr) + 10000000 - 4) = 123456789;
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
  ASSERT_TRUE((addr = this->alloc_->Attach_ReadOnly("0x10001", 10000000UL)));
  ASSERT_EQ(123456789, *reinterpret_cast<int*>(static_cast<char*>(addr) + 10000000 - 4));
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
  ASSERT_TRUE(this->alloc_->Unlink("0x10001"));
}

TYPED_TEST(ShmAllocTest, AttachSize)
{
  void* addr;
  size_t mapped_size;
  bool created = false;
  ASSERT_TRUE((addr = this->alloc_->Attach_MayCreate("0x10001", 10000000UL, &mapped_size, &created)));
  ASSERT_TRUE(this->CheckShmSize(mapped_size, 10000000UL));
  ASSERT_TRUE(created);
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
  ASSERT_FALSE((addr = this->alloc_->Attach_MayCreate("0x10001", 15000000UL)));
  ASSERT_TRUE((addr = this->alloc_->Attach_MayCreate("0x10001", 0, &mapped_size, &created)));
  ASSERT_TRUE(this->CheckShmSize(mapped_size, 10000000UL));
  ASSERT_FALSE(created);
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
  ASSERT_FALSE((addr = this->alloc_->Attach_ReadOnly("0x10001", 15000000UL)));
  ASSERT_TRUE((addr = this->alloc_->Attach_ReadOnly("0x10001", 0, &mapped_size)));
  ASSERT_TRUE(this->CheckShmSize(mapped_size, 10000000UL));
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
}

TYPED_TEST(ShmAllocTest, AutoCreate)
{
  void* addr;
  size_t mapped_size;
  bool created = false;
  ASSERT_TRUE((addr = this->alloc_->Attach_AutoCreate("0x10001", 10000000UL, &mapped_size, &created)));
  ASSERT_TRUE(this->CheckShmSize(mapped_size, 10000000UL));
  ASSERT_TRUE(created);
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
  ASSERT_TRUE((addr = this->alloc_->Attach_AutoCreate("0x10001", 0, &mapped_size, &created)));
  ASSERT_TRUE(this->CheckShmSize(mapped_size, 10000000UL));
  ASSERT_FALSE(created);
  ASSERT_TRUE(this->alloc_->Detach(addr, 10000000UL));
  ASSERT_TRUE((addr = this->alloc_->Attach_AutoCreate("0x10001", 11000000UL, &mapped_size, &created)));
  ASSERT_TRUE(this->CheckShmSize(mapped_size, 11000000UL));
  ASSERT_TRUE(created);
  ASSERT_TRUE(this->alloc_->Detach(addr, 11000000UL));
}
