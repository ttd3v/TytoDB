#ifndef BTREE_H
#define BTREE_H

#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/uio.h>

// Type aliases
typedef u_int64_t u64;
typedef u_int32_t u32;
typedef int32_t i32;
typedef u_int8_t u8;
typedef size_t usize;



// Structs
typedef struct {
    u64 key;
    u64 value;
} Cell;

typedef struct {
    u64 max;
    u64 min;
    u64 len;
} meta;

typedef struct {
    i32 file;
    struct stat status;
    meta *m;
    u32 ml;
    u64 length;
    char* path;
    const unsigned char* empty;
} BTree;

typedef struct {
    usize method;
    u64 key;
    u64 value;
} Request;

typedef struct {
    u64 value;
    u64 exists;
} GetResponse;
// Constants
#define ELEMENTS_PER_VECTOR 256
#define VECTOR_SIZE (sizeof(Cell) * ELEMENTS_PER_VECTOR)
#define THRESHOLD 2.0f
#define MAX_PAGES 16384

typedef struct {
    u64 pointer;
    u64 length;
    Cell vector[ELEMENTS_PER_VECTOR];
} disk_fetch;

// Enums
enum RequestMethods {
    RQ_READ = 1,
    RQ_WRITE = 2,
    RQ_DELETE = 0,
};

enum ERROR {
    SOMETHING_WENT_WRONG = -1,
    PERMISSION_DENIED = -2,
    FILE_DOES_NOT_EXISTS = -3,
    FILE_EXISTS = -4,
    PROCESS_HAVE_TOO_MANY_OPEN_FILES = -5,
    SYSTEM_WIDE_LIMIT_ON_OPEN_FILES = -6,
    TRIED_OPENING_A_DIRECTORY = -7,
    INVALID_MEMORY = -8,
    INVALID_ARGUMENT = -9,
    NO_SPACE_LEFT = -10,
    IO_ERROR = -11,
    INTERRUPTED_BY_SIGNAL = -12,
    BROKEN_PIPE = -13,
    FAILED_TO_ALLOCATE_MEMORY = -14,
    RESOURCE_TEMPORARILY_UNAVAILABLE = -15,
    BAD_FILE_DESCRIPTOR = -16,
    ARGUMENT_LIST_TOO_LONG = -17,
    EXEC_FORMAT_ERROR = -18,
    NO_CHILD_PROCESSES = -19,
    ADDRESS_ALREADY_IN_USE = -20,
    ADDRESS_NOT_AVAILABLE = -21,
    ADDRESS_FAMILY_NOT_SUPPORTED = -22,
    ALREADY_IN_PROGRESS = -23,
    BAD_MESSAGE = -24,
    INVALID_REQUEST_DESCRIPTOR = -25,
    INVALID_EXCHANGE = -26,
    BAD_FILE_DESCRIPTOR_STATE = -27,
    TOO_MANY_SYMBOLIC_LINKS = -29,
    FILE_TOO_LARGE = -30,
    NO_SPACE_LEFT_ON_DEVICE = -31,
    INVALID_SEEK = -32,
    READ_ONLY_FILE_SYSTEM = -33,
    TOO_MANY_LINKS = -34,
    NUMERIC_RESULT_TOO_LARGE = -36,
    NO_LOCKS_AVAILABLE = -37,
    FUNCTION_NOT_IMPLEMENTED = -38,
    DIRECTORY_NOT_EMPTY = -39,
    TOO_MANY_LEVELS_OF_SYMBOLIC_LINKS = -40,
    UNKNOWN_ERROR = -41
};

// Function declarations
i32 handle_err(i32 f);
i32 load_metadata(BTree *self);
i32 extend(BTree *self, u64 growth_count);
i32 create(BTree *self, char* path);
i32 init(BTree *self, char* path);
int cmp_request_method_asc(const void *a, const void *b);
int cmp_cell_asc(const void *a, const void *b);
i32 bt_request(BTree *self, Request *req, usize req_count);
i32 normalize(BTree *self);

#endif // BTREE_H

