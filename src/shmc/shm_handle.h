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
#ifndef SHMC_SHM_HANDLE_H_
#define SHMC_SHM_HANDLE_H_

#include <string>
#include "shmc/shm_alloc.h"
#include "shmc/svipc_shm_alloc.h"
#include "shmc/posix_shm_alloc.h"
#include "shmc/mmap_alloc.h"
#include "shmc/common_utils.h"

namespace shmc {

/* A shm reference holder
 * @T      type of the header of shm, must StandardLayoutType
 * @Alloc  shm allocator to use [SVIPC(default), SVIPC_HugeTLB, POSIX]
 *
 * This is a helper class for shm manipulations. It offers simple interface
 * to attach or create shm and hold a reference to the shm and handy methods
 * to access the shm data.
 */
template <class T, class Alloc>
class ShmHandle {
 public:
  ShmHandle() = default;

  /* Destructor
   */
  ~ShmHandle() {
    if (ptr_) Alloc().Detach(ptr_, size_);
  }

  /* Initializer for READ & WRITE
   * @shm_key     key or name of the shm to attach or create
   * @size        size of the shm
   * @mode        decides when to create new shm
   *
   * This initializer should be called by the shm writer/owner and it will
   * try to attach an existing shm or created a new one if needed and allowed.
   *
   * @return      true if succeed
   */
  bool InitForWrite(const std::string& key, size_t size,
                    int create_flags = kCreateIfNotExist) {
    if (is_initialized()) return false;
    ptr_ = static_cast<T*>(Alloc().Attach_AutoCreate(key, size, create_flags,
                                                &size_, &is_newly_created_));
    if (ptr_)
      key_ = key;
    else
      Utils::Log(kError, "ShmHandler::InitForWrite: %s\n", Utils::Perror());
    return ptr_;
  }

  /* Initializer for READ-ONLY
   * @shm_key     key or name of the shm to attach
   * @size        min-size of the shm to attach
   *
   * This initializer should be called by the shm reader and
   * it will try to attach an existing shm with read-only mode.
   *
   * @return   true if succeed
   */
  bool InitForRead(const std::string& key, size_t size) {
    if (is_initialized()) return false;
    ptr_ = static_cast<T*>(Alloc().Attach_ReadOnly(key, size, &size_));
    if (ptr_)
      key_ = key;
    else
      Utils::Log(kError, "ShmHandler::InitForRead: %s\n", Utils::Perror());
    return ptr_;
  }

  /* Reset the handle
   *
   * This will detach any attached shm.
   */
  void Reset() {
    key_.clear();
    if (ptr_) Alloc().Detach(ptr_, size_);
    ptr_ = nullptr;
    size_ = 0;
  }

  /* getter
   *
   * @return  typed pointer to the attached shm
   */
  T* ptr() {
    return ptr_;
  }

  /* getter
   *
   * @return  const-typed pointer to the attached shm
   */
  const T* ptr() const {
    return ptr_;
  }

  /* helper operator
   *
   * @return  typed pointer to the attached shm
   */
  T* operator->() {
    return ptr_;
  }

  /* helper operator
   *
   * @return  const-typed pointer to the attached shm
   */
  const T* operator->() const {
    return ptr_;
  }

  /* getter
   *
   * @return  key string of the attached shm
   */
  std::string key() const {
    return key_;
  }

  /* getter
   *
   * @return  actual size of the attached shm
   */
  size_t size() const {
    return size_;
  }

  /* getter
   *
   * @return  whether the attached shm is newly created
   */
  bool is_newly_created() const {
    return is_newly_created_;
  }

  /* getter
   *
   * @return  whether the handle is initialized.
   */
  bool is_initialized() const {
    return ptr_;
  }

  /* Check whether shm size_ is equal to expected_size with consideration of
   * alignment of the allocator
   *
   * @return  true if equal
   */
  bool CheckSize(size_t expected_size) const {
    return Utils::RoundAlign(expected_size, Alloc().AlignSize()) == size_;
  }

 private:
  SHMC_NOT_COPYABLE_AND_MOVABLE(ShmHandle);

  std::string key_;
  size_t size_ = 0;
  bool is_newly_created_ = false;
  T* ptr_ = nullptr;
};

}  // namespace shmc

#endif  // SHMC_SHM_HANDLE_H_
