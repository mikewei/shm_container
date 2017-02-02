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
#ifndef SHMC_SVIPC_SHM_ALLOC_H_
#define SHMC_SVIPC_SHM_ALLOC_H_

#include <sys/ipc.h>
#include <sys/shm.h>
#include <string>
#include "shmc/shm_alloc.h"
#include "shmc/common_utils.h"

namespace shmc {

namespace impl {

template <bool kEnableHugeTLB>
class SVIPCShmAlloc : public ShmAlloc {
 public:
  virtual ~SVIPCShmAlloc() {}

  void* Attach(const std::string& key, size_t size, int flags,
                                       size_t* mapped_size) override {
    key_t shm_key;
    if (!str2key(key, &shm_key)) {
      set_last_errno(kErrBadParam);
      return nullptr;
    }
    int shmget_flags = 0640;
    if (flags & kCreate)
      shmget_flags |= IPC_CREAT;
    if (flags & kCreateExcl)
      shmget_flags |= IPC_EXCL;
    if (kEnableHugeTLB) {
      shmget_flags |= SHM_HUGETLB;
      size = shmc::Utils::RoundAlign<kAlignSize_HugeTLB>(size);
    }
    int shm_id = shmget(shm_key, size, shmget_flags);
    if (shm_id < 0) {
      // if got EINVAL when attaching existing shm
      if (errno == EINVAL && !(flags & kCreate)) {
        set_last_errno(kErrBiggerSize);
      } else {
        set_last_errno(conv_errno());
      }
      return nullptr;
    }
    int shmat_flags = 0;
    if (flags & kReadOnly)
      shmat_flags |= SHM_RDONLY;
    void* addr = shmat(shm_id, nullptr, shmat_flags);
    if (addr == reinterpret_cast<void*>(-1)) {
      set_last_errno(conv_errno());
      return nullptr;
    }
    if (mapped_size) {
      struct shmid_ds shmds;
      if (shmctl(shm_id, IPC_STAT, &shmds) < 0) {
        set_last_errno(conv_errno());
        shmdt(addr);
        return nullptr;
      }
      *mapped_size = shmds.shm_segsz;
    }
    return addr;
  }

  bool Detach(void* addr, size_t size) override {
    if (shmdt(addr) < 0) {
      set_last_errno(conv_errno());
      return false;
    }
    return true;
  }

  bool Unlink(const std::string& key) override {
    key_t shm_key;
    if (!str2key(key, &shm_key)) {
      set_last_errno(kErrBadParam);
      return false;
    }
    int shm_id = shmget(shm_key, 0, 0);
    if (shm_id < 0) {
      set_last_errno(conv_errno());
      return false;
    }
    if (shmctl(shm_id, IPC_RMID, nullptr) < 0) {
      set_last_errno(conv_errno());
      return false;
    }
    return true;
  }
  size_t AlignSize() override {
    return (kEnableHugeTLB ? kAlignSize_HugeTLB : 1);
  }

 private:
  // 2M-align is a workaround for kernel bug with HugeTLB
  // which has been fixed in new kernels
  // https://bugzilla.kernel.org/show_bug.cgi?id=56881
  static constexpr size_t kAlignSize_HugeTLB = 0x200000UL;

  static bool str2key(const std::string& key, key_t* out) {
    // support decimal, octal, hexadecimal format
    errno = 0;
    int64_t shm_key = strtol(key.c_str(), NULL, 0);
    if (errno)
      return false;
    *out = static_cast<key_t>(shm_key);
    return true;
  }

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

}  // namespace impl

using SVIPC_HugeTLB = impl::SVIPCShmAlloc<true>;
using SVIPC = impl::SVIPCShmAlloc<false>;

}  // namespace shmc

#endif  // SHMC_SVIPC_SHM_ALLOC_H_
