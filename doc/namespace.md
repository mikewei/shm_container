#<cldoc:shmc>
namespace of library

All containers support following allocators:

* SVIPC          - SysV IPC shared memory (shmget, shmat, ...)
* SVIP\_HugeTLB  - SysV IPC shared memory with HugeTLB enabled (2M page)
* POSIX          - POSIX shared memory (shm\_open, shm\_unlink, ...)
* ANON           - anonymous shared memory, allocated by mmap(2) with MAP\_SHARED|MAP\_ANONYMOUS
* HEAP           - process private memory, allocated by mmap(2) with MAP\_PRIVATE|MAP\_ANONYMOUS

All APIs are in this namespace. See Classes listed below.

#<cldoc:shmc::impl>
internal use
