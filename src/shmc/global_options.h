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
#ifndef SHMC_GLOBAL_OPTIONS_H_
#define SHMC_GLOBAL_OPTIONS_H_

#include <functional>

namespace shmc {

/* Log level definition of the library
 */
enum LogLevel {
  kError = 1,
  kWarning,
  kInfo,
  kDebug,
};

/* Set the library log level and log handler
 * @log_level  - only logs with level <= @log_level will be handled
 * @log_func   - callback to output logs
 *
 * This function must be called before any container initialized.
 */
void SetLogHandler(LogLevel log_level,
                   std::function<void(LogLevel, const char*)> log_func);

/* bit-flag of deciding when to create shm
 */
enum ShmCreateFlag {
  // (all bits cleared) never create shm only attach existing one
  kNoCreate = 0x0,
  // (1st bit) create shm if not exist
  kCreateIfNotExist = 0x1,
  // (2nd bit) create new shm if bigger size requested
  kCreateIfExtending = 0x2,
};

/* Set default create-flags used in container's InitForWrite
 * @create_flags  - bit-OR of some ShmCreateFlag
 *
 * By default kCreateIfNotExist is used when InitForWrite is running. You can
 * override it by calling this function. For example, if you are upgrading
 * the container for larger volume you can set kCreateIfExtending to allow
 * delete-then-recreate behavior automatically.
 * This function must be called before any container initialized.
 */
void SetDefaultCreateFlags(int create_flags);

}  // namespace shmc

#endif  // SHMC_GLOBAL_OPTIONS_H_
