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
#ifndef SHMC_SHM_ALLOC_H_
#define SHMC_SHM_ALLOC_H_

#include <string>
#include "shmc/common_utils.h"

namespace shmc {
namespace impl {

enum ShmAllocFlag {
  kCreate = 0x1,
  kCreateExcl = 0x2,
  kReadOnly = 0x4,
};

enum ShmAllocErrno {
  kErrOK = 0,
  kErrDefault = 1,
  kErrNotExist = 2,
  kErrAlreadyExist = 3,
  kErrInvalid = 4,
  kErrBadParam = 5,
  kErrPermission = 6,
  kErrBiggerSize = 7,
};

// this defines default traits, subclasses can override it by partial-spec
template <class Alloc>
struct AllocTraits {
  // named allocator can be found by InitForRead
  static constexpr bool is_named = true;
};

class ShmAlloc {
 public:
  virtual ~ShmAlloc() {}
  int last_errno() {
    return last_errno_;
  }
  void* Attach_ReadOnly(const std::string& key, size_t size,
                        size_t* mapped_size = nullptr) {
    return Attach(key, size, kReadOnly, mapped_size);
  }
  void* Attach_ExclCreate(const std::string& key, size_t size,
                          size_t* mapped_size = nullptr) {
    return Attach(key, size, kCreate|kCreateExcl, mapped_size);
  }
  void* Attach_ForceCreate(const std::string& key, size_t size,
                           size_t* mapped_size = nullptr) {
    void* addr = Attach_ExclCreate(key, size, mapped_size);
    if (!addr && last_errno() == kErrAlreadyExist && Unlink(key)) {
      addr = Attach_ExclCreate(key, size, mapped_size);
    }
    return addr;
  }
  void* Attach_MayCreate(const std::string& key, size_t size,
                         size_t* mapped_size = nullptr,
                         bool* created = nullptr) {
    if (created) *created = false;
    void* addr = Attach(key, size, 0, mapped_size);
    if (!addr && last_errno() == kErrNotExist) {
      addr = Attach_ExclCreate(key, size, mapped_size);
      if (addr && created) *created = true;
    }
    return addr;
  }
  void* Attach_AutoCreate(const std::string& key, size_t size,
                         int create_flags = kCreateIfNotExist,
                         size_t* mapped_size = nullptr,
                         bool* created = nullptr) {
    if (created) *created = false;
    void* addr = nullptr;
    if (create_flags & kCreateIfNotExist) {
      addr = Attach_MayCreate(key, size, mapped_size, created);
    } else {
      addr = Attach(key, size, 0, mapped_size);
    }
    if (!addr && last_errno() == kErrBiggerSize &&
        (create_flags & kCreateIfExtending)) {
      // A known corner issue: when SVIPC_HugeTLB is used (2M aligned)
      // and if the addional size is less than 2M it does not work because
      // kErrBiggerSize will not be triggered
      addr = Attach_ForceCreate(key, size, mapped_size);
      if (addr && created) *created = true;
    }
    return addr;
  }
  // interfaces to be implemented by sub-class
  virtual void* Attach(const std::string& key, size_t size, int flags,
                       size_t* mapped_size = nullptr) = 0;
  virtual bool Detach(void* addr, size_t size) = 0;
  virtual bool Unlink(const std::string& key) = 0;
  virtual size_t AlignSize() = 0;

 protected:
  void set_last_errno(ShmAllocErrno err) {
    last_errno_ = err;
  }

 private:
  ShmAllocErrno last_errno_ = kErrOK;
};

}  // namespace impl
}  // namespace shmc

#endif  // SHMC_SHM_ALLOC_H_
