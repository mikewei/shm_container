#<cldoc:index>

SHM-Container Documentation

# SHM-Container Documentation

Shared-memory is best known as an effective IPC method, moreover, it is a powerful design pattern in server architecture with some notable features:

* process restart without memory data lost
* achieve modularity with decoupled processes
* keep service HA even with failure in one process
* almost no performance cost for inter process communication

Many popular high-volume internet services (such as QQ server) have benefit from usage of shared-memory techniques. However it is not as easy as using in-process libraries (such as STL) when working with OS shm interface, because you have to manage memory pages directly. So what this project is valuable is that it offers a collection of shared-memory containers with familiar interface for you, and also with the following features:

* provide a unified interface for different underlying shm mechanism (SYSV, SYSV\_HugeTLB, POSIX, ANON, HEAP)
* provide useful container such as hash-map, fifo-queue, broadcast-queue(sync-buf), etc
* efficient C++11 lock-free implemenation aimed to offer extreme high performance
* designed for robust and fast, sufficient unit-tests, production ready and verified
* compact and clean code, only consist of C++ headers with no external dependency

See document of classes in namespace *shmc* for detail.

