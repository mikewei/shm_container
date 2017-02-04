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
#ifndef SHMC_COMMON_UTILS_H_
#define SHMC_COMMON_UTILS_H_

#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <utility>
#include <functional>
#include "shmc/global_options.h"

namespace shmc {

#define SHMC_ERR_RET(...) do { \
    Utils::Log(kError, __VA_ARGS__); \
    return false; \
  } while (0)

#define SHMC_NOT_COPYABLE_AND_MOVABLE(ClassName) \
  ClassName(const ClassName&) = delete; \
  void operator=(const ClassName&) = delete; \
  ClassName(ClassName&&) = delete; \
  void operator=(ClassName&&) = delete

namespace impl {

//- Use dummy template as it allows static member variables defined in header
template <class Dummy>
class Utils {
 public:
  static void Log(LogLevel lv, const char* fmt, ...);
  static void SetLogHandler(LogLevel lv,
                            std::function<void(LogLevel, const char*)> f);

  static void SetDefaultCreateFlags(int flgs);
  static int DefaultCreateFlags() {
    return create_flags;
  }

  static const char* Perror();
  static bool GetPrimeArray(size_t top, size_t num, size_t array[]);
  static uint64_t GenMagic(const char* str);
  static const char* Hex(const volatile void* buf, size_t len);
  static size_t GetPageSize();

  static size_t MajorVer(uint32_t ver) {
    return static_cast<size_t>(ver >> 16);
  }
  static size_t MinorVer(uint32_t ver) {
    return static_cast<size_t>(ver & 0x0000ffffU);
  }
  static uint32_t MakeVer(size_t major, size_t minor) {
    return static_cast<uint32_t>((major << 16) + (minor & 0x0000ffffU));
  }

  template <class T, size_t N>
  static constexpr size_t Padding() {
    return (sizeof(T) % N ? (N - sizeof(T) % N) : 0);
  }
  template <size_t N, class T>
  static T RoundAlign(T num) {
    return (num + N - 1) / N * N;
  }
  template <class T>
  static T RoundAlign(T num, size_t N) {
    return (num + N - 1) / N * N;
  }

 private:
  static int log_level;
  static std::function<void(LogLevel, const char*)> log_func;
  static int create_flags;
};

template <class Dummy>
int Utils<Dummy>::log_level = 0;

template <class Dummy>
std::function<void(LogLevel, const char*)> Utils<Dummy>::log_func;

template <class Dummy>
void Utils<Dummy>::Log(LogLevel lv, const char* fmt, ...) {
  if (lv <= log_level && log_func) {
    char buf[4096];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_func(lv, buf);
  }
}

template <class Dummy>
void Utils<Dummy>::SetLogHandler(LogLevel lv,
                                 std::function<void(LogLevel, const char*)> f) {
  log_level = lv;
  log_func  = f;
}

template <class Dummy>
int Utils<Dummy>::create_flags = kCreateIfNotExist;

template <class Dummy>
void Utils<Dummy>::SetDefaultCreateFlags(int flgs) {
  create_flags = flgs;
}

template <class Dummy>
const char* Utils<Dummy>::Perror() {
  // a tricky algorithm to support both GNU and XSI version of strerror_r
  static thread_local char buf[128];
  buf[0] = 0;
  const char* s = strerror_r(errno, buf, sizeof(buf));
  return (s == nullptr || (int64_t)s == -1L) ? buf : s;
}

static inline bool IsPrime(size_t num) {
  uint64_t i = 0;
  for (i = 2; i <= num / 2; i++) {
    if (0 == num % i) return false;
  }
  return true;
}

template <class Dummy>
bool Utils<Dummy>::GetPrimeArray(size_t top, size_t num, size_t array[]) {
  uint64_t i = 0;
  for (; top > 0 && num > 0; top--) {
    if (IsPrime(top)) {
      array[i++] = top;
      num--;
    }
  }
  return (num == 0);
}

template <class Dummy>
uint64_t Utils<Dummy>::GenMagic(const char* str) {
  uint64_t magic = 0;
  strncpy(reinterpret_cast<char*>(&magic), str, sizeof(magic));
  return magic;
}

template <class Dummy>
const char* Utils<Dummy>::Hex(const volatile void* buf, size_t len) {
  static const char hex_map[16] = {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  };
  thread_local static char out[8192];
  size_t pos = 0;
  size_t i;
  for (i = 0; i < len && pos < 8000; i++) {
    out[pos++] = hex_map[((const char*)buf)[i] >> 4];
    out[pos++] = hex_map[((const char*)buf)[i] & 0xf];
    if ((i + 1) % 16 == 0) out[pos++] = '\n';
    else if ((i + 1) % 2 == 0) out[pos++] = ' ';
  }
  if (i % 16) out[pos++] = '\n';
  out[pos] = 0;
  return out;
}

template <class Dummy>
size_t Utils<Dummy>::GetPageSize() {
  int64_t ret = sysconf(_SC_PAGE_SIZE);
  return ret < 0 ? 4096 : static_cast<size_t>(ret);
}

}  // namespace impl


using Utils = impl::Utils<void>;

/* Set the library log level and log handler
 * @log_level  - only logs with level <= @log_level will be handled
 * @log_func   - callback to output logs
 *
 * This function must be called before any container initialized.
 */
inline void SetLogHandler(LogLevel log_level,
                          std::function<void(LogLevel, const char*)> log_func) {
  Utils::SetLogHandler(log_level, std::move(log_func));
}

/* Set default create-flags used in container's InitForWrite
 * @create_flags  - bit-OR of some ShmCreateFlag
 *
 * By default kCreateIfNotExist is used when InitForWrite is running. You can
 * override it by calling this function. For example, if you are upgrading
 * the container for larger volume you can set kCreateIfExtending to allow
 * delete-then-recreate behavior automatically.
 * This function must be called before any container initialized.
 */
inline void SetDefaultCreateFlags(int create_flags) {
  Utils::SetDefaultCreateFlags(create_flags);
}

}  // namespace shmc

#endif  // SHMC_COMMON_UTILS_H_
