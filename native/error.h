#ifndef ERROR_H
#define ERROR_H

typedef enum ExecutionProduct {
    Success = 0,
    Error = -1,
    MemoryAllocationFailed = -2,
    DiskWriteFailure = -3,
    DiskReadFailure = -4,
    FileOpenFailure = -5,
    IoUringQueueStartFailure = -6,
    IoUringSQEFailure = -7,
    IoUringCQEFileWriteFailure = -8,
    IoUringCQEFileReadFailure = -10,
    FsyncFailure = -11,
    OutOfDiskSpace = -12,
    IoUringSubmitFailure = -13
} ExecutionProduct;

#endif
