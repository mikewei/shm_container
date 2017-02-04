/* Copyright (c) 2015-2017, Bin Wei <bin@vip.qq.com>
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
#ifndef SHMC_MMAP_ALLOC_H_
#define SHMC_MMAP_ALLOC_H_

#include <sys/mman.h>
#include <string>
#include "shmc/shm_alloc.h"
#include "shmc/common_utils.h"

namespace shmc {

namespace impl {

template <bool IsShared>
class MmapAlloc : public ShmAlloc {
 public:
  virtual ~MmapAlloc() {}

  void* Attach(const std::string& key, size_t size, int flags,
                       size_t* mapped_size) override {
    if (!(flags & kCreate)) {
      // cannot attach existed one
      set_last_errno(kErrNotExist);
      return nullptr;
    }

    int mmap_prot = 0;
    if (flags & kReadOnly) {
      mmap_prot = PROT_READ;
    } else {
      mmap_prot = PROT_READ | PROT_WRITE;
    }
    int share_flag = (IsShared ? MAP_SHARED : MAP_PRIVATE);
    void* addr = mmap(nullptr, size, mmap_prot, share_flag|MAP_ANONYMOUS,
                      -1, 0);
    if (addr == reinterpret_cast<void*>(-1)) {
      set_last_errno(conv_errno());
      return nullptr;
    }
    if (mapped_size) {
      size_t page_size = shmc::Utils::GetPageSize();
      *mapped_size = shmc::Utils::RoundAlign(size, page_size);
    }
    return addr;
  }

  bool Detach(void* addr, size_t size) override {
    if (munmap(addr, size) < 0) {
      set_last_errno(conv_errno());
      return false;
    }
    return true;
  }

  bool Unlink(const std::string& key) override {
    // heap pages are freed when deatched
    return true;
  }
  size_t AlignSize() override {
    return 1;
  }

 private:
  static ShmAllocErrno conv_errno() {
    switch (errno) {
    case 0:
      return kErrOK;
    case ENOENT:
      return kErrNotExist;
    case EEXIST:
      return kErrAlreadyExist;
    case EINVAL:
      return kErrInvalid;
    case EACCES:
    case EPERM:
      return kErrPermission;
    default:
      return kErrDefault;
    }
  }
};

template <bool IsShared>
struct AllocTraits<MmapAlloc<IsShared>> {
  // anonymous memory is not named
  static constexpr bool is_named = false;
};

}  // namespace impl

using ANON = impl::MmapAlloc<true>;
using HEAP = impl::MmapAlloc<false>;

}  // namespace shmc

#endif  // SHMC_MMAP_ALLOC_H_
