#include "vector.h"
#include "hashset.h"
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
typedef uint64_t u64;
typedef uint8_t u8;
typedef uint32_t u32;
typedef int32_t i32;
typedef uint8_t u8;
typedef size_t usize;

#define ELEMENTS_PER_VECTOR 256

typedef struct {
    u64 key;
    u64 value;
}Cell;

const size_t VECTOR_SIZE = sizeof(Cell)*ELEMENTS_PER_VECTOR;
typedef struct{
    u64 max;
    u64 min;
    u64 len;
} meta;

typedef struct{
    i32 file;
    struct stat status;
    meta *m;
    u32 ml;
    u64 length;
    char* path;
    unsigned char const*empty;
} BTree;

enum RequestMethods {
    RQ_READ = 1,
    RQ_WRITE = 2,
    RQ_DELETE = 0,
};

typedef struct{
    usize method;
    u64 key;
    u64 value;
} Request;
typedef struct{
    u64 value;
    u64 exists;
} GetResponse;

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

i32 handle_err(i32 f) {
    if (f >= 0) return 0;

    switch(errno) {
        case EPERM: return PERMISSION_DENIED;
        case EACCES: return PERMISSION_DENIED;
        case ENOENT: return FILE_DOES_NOT_EXISTS;
        case EEXIST: return FILE_EXISTS;
        case EMFILE: return PROCESS_HAVE_TOO_MANY_OPEN_FILES;
        case ENFILE: return SYSTEM_WIDE_LIMIT_ON_OPEN_FILES;
        case EISDIR: return TRIED_OPENING_A_DIRECTORY;
        case EFAULT: return INVALID_MEMORY;
        case EINVAL: return INVALID_ARGUMENT;
        case ENOSPC: return NO_SPACE_LEFT;
        case EIO: return IO_ERROR;
        case EINTR: return INTERRUPTED_BY_SIGNAL;
        case EPIPE: return BROKEN_PIPE;
        case ENOMEM: return FAILED_TO_ALLOCATE_MEMORY;
        case EAGAIN: return RESOURCE_TEMPORARILY_UNAVAILABLE;
        case EBADF: return BAD_FILE_DESCRIPTOR;
        case E2BIG: return ARGUMENT_LIST_TOO_LONG;
        case ENOEXEC: return EXEC_FORMAT_ERROR;
        case ECHILD: return NO_CHILD_PROCESSES;
        case EADDRINUSE: return ADDRESS_ALREADY_IN_USE;
        case EADDRNOTAVAIL: return ADDRESS_NOT_AVAILABLE;
        case EAFNOSUPPORT: return ADDRESS_FAMILY_NOT_SUPPORTED;
        case EALREADY: return ALREADY_IN_PROGRESS;
        case EBADMSG: return BAD_MESSAGE;
        case EBADR: return INVALID_REQUEST_DESCRIPTOR;
        case EBADE: return INVALID_EXCHANGE;
        case EBADFD: return BAD_FILE_DESCRIPTOR_STATE;
        case ELOOP: return TOO_MANY_SYMBOLIC_LINKS;
        case EFBIG: return FILE_TOO_LARGE;
        case ENOSYS: return FUNCTION_NOT_IMPLEMENTED;
        case ENOTEMPTY: return DIRECTORY_NOT_EMPTY;
        case EOVERFLOW: return NUMERIC_RESULT_TOO_LARGE;
        case ENOLCK: return NO_LOCKS_AVAILABLE;
        case ENOTTY: return INVALID_ARGUMENT;
        default: return UNKNOWN_ERROR;
    }
}


i32 load_metadata(BTree *self){
    u32 len = 0;
    i32 code = handle_err(stat(self->path, &self->status));
    if(code < 0){return code;};
    code = handle_err(pread(self->file, &len, sizeof(u32),self->status.st_size-sizeof(len)));
    if(code < 0){return code;}
    if (len > 0){
        unsigned char* buffer = malloc(len*sizeof(meta));
        if(!buffer){
            errno = ENOMEM;
            return handle_err(-1);
        }
        code = handle_err(pread(self->file, buffer, len*sizeof(meta), self->status.st_size-sizeof(len)-(len*sizeof(meta))));
        self->m = (meta*)buffer;
    }   
    self->ml = len;
    self->length = 0;
    for(u32 i = 0; i<self->ml;i++){
        self->length += self->m[i].len;
    }
    return 0;
}
i32 extend(BTree *self, u64 growth_count){
    i32 code = 0;
    
    meta* new_metadata = malloc(sizeof(meta)*(self->ml+growth_count));
    if(!new_metadata){
        errno = ENOMEM;
        return handle_err(-1);
    }
    memcpy(new_metadata, self->m, self->ml*sizeof(meta));
    memset(new_metadata + self->ml, 0, growth_count * sizeof(meta)); 
    free(self->m);
    self->m = new_metadata;
    {
        struct iovec iov[growth_count];
        for(u32 i = 0; i < growth_count; i++){
            iov[i].iov_base = (void*)self->empty;
            iov[i].iov_len = ELEMENTS_PER_VECTOR*sizeof(Cell);
        }
        code = handle_err(pwritev(self->file, iov, growth_count, self->ml*ELEMENTS_PER_VECTOR*sizeof(Cell)));
        if(code < 0){return code;};
    }
    u32 pl = self->ml+growth_count;
    {
        size_t buf_size = sizeof(u32) + sizeof(meta) * pl;
        unsigned char *buffer = malloc(buf_size); 
        if(!buffer){
            errno = ENOMEM;
            return handle_err(-1);
        }
        memcpy(buffer, self->m, pl*sizeof(meta));
        memcpy(buffer+ sizeof(meta)*pl,&pl, sizeof(u32));
        code = handle_err(pwrite(self->file, buffer, buf_size, pl*(ELEMENTS_PER_VECTOR*sizeof(Cell))));
        if(code < 0){free(buffer);return code;};
        self->ml+=growth_count;
        free(buffer);
    }
    return handle_err(fsync(self->file));
}

i32 create(BTree *self,char* path){
    int f = open(path, O_RDWR | O_CREAT,0644);
    if (f < 0){
        return handle_err(f);
    }
    u32 len = 0;
    i32 code = handle_err(pwrite(f, &len, sizeof(len), 0));
    if(code < 0){return code;}
    self->file = f;
    self->ml = len;
    self->m = NULL;
    return 0;
}

i32 init(BTree *self,char* path){
    self->empty = calloc(VECTOR_SIZE, sizeof(unsigned char));
    self->path = path;
    if(!self->empty){
        errno = ENOMEM;
        return handle_err(-1);
    }
    i32 f = open(path, O_RDWR);
    if(f < 0){
        f = handle_err(f);
        if(f != FILE_DOES_NOT_EXISTS){
            return f;
        }else{
            return create(self,path);
        }
    }
    self->file = f;
    load_metadata(self);
    return 0;
}

int cmp_request_method_asc(const void *a, const void *b) {
    const Request *ra = a;
    const Request *rb = b;
    return (ra->key > rb->key) - (ra->key < rb->key);
}

int cmp_cell_asc(const void *a, const void *b) {
    const Cell *ra = a;
    const Cell *rb = b;
    return (ra->key > rb->key) - (ra->key < rb->key);
}

typedef struct {
    u64 pointer;
    u64 length;
    Cell vector[ELEMENTS_PER_VECTOR];
} disk_fetch;

inline void sfree(void *ptr){
    free(ptr);
    ptr = NULL;
}

inline void u64_to_unsigned_char(unsigned char *value, u64 u){
    for(usize i = 0; i < 8; i++){
        value[i] = (u >> (i * 8)) & 0xFF;
    }
}

i32 bt_request(BTree *self,Request *req,usize req_count){ 
    vector vec;
    usize __write_ops__ = 0;
    usize increase = 0;
    usize decrease = 0;
    if(vec_new(&vec, sizeof(u64))<0){return handle_err(-1);};
    for(usize i = 0; i < self->ml; i++){
        if(self->m[i].len > 0){
           for(usize j = 0; j < req_count; j++){
               if(req[j].key >= self->m[i].min && req[j].key <= self->m[i].max){
                   if(req[j].method == RQ_WRITE){
                        __write_ops__++;
                   }
                   unsigned char in[8];
                    u64_to_unsigned_char(in, i);
                   if(vec_push(&vec, in) < 0){
                       return handle_err(-1);
                   };
                   break;
               }
           } 
        }
    }
    if(self->length+__write_ops__ > self->ml*ELEMENTS_PER_VECTOR){
        if(extend(self, __write_ops__) < 0){
            vec_destroy(&vec);
            return handle_err(-1);
        };
    }
    disk_fetch *instance = malloc(sizeof(disk_fetch)*vec.len);
    if(!instance){
        errno = ENOMEM;
        return handle_err(-1);
    }

    u64 length = vec.len;
    {
        struct io_uring ring;
        if(io_uring_queue_init(length, &ring, 0) < 0){
            return handle_err(-1);
        };
        for(usize i = 0; i < length; i++){
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            u64* k = (u64*)vec.buffer;
            instance[i].pointer = k[i];
            instance[i].length = self->m[k[i]/VECTOR_SIZE].len;
            io_uring_prep_read(sqe, self->file, instance[i].vector, VECTOR_SIZE, k[i]);
        }
        u64 code = io_uring_submit_and_wait(&ring, length);
        if (code < 0){
            return handle_err(code);
        }
    }
    vec_destroy(&vec);

    vector mutation;
    vector to_proc;
    if(vec_new(&mutation,sizeof(u64))<0){return handle_err(-1);};
    if(vec_new(&to_proc,sizeof(u64))<0){return handle_err(-1);};
    for(usize i = 0; i<req_count;i++){
        if(req[i].method != RQ_WRITE){
            unsigned char in[8];
            u64_to_unsigned_char(in, i);
            if(vec_push(&to_proc, &in)<0){
                return handle_err(-1);
            }
        };
    }
    {
        vector torem;
        if(vec_new(&torem, sizeof(u64))<0){return handle_err(-1);};
        for(usize f = 0; (f < length && to_proc.len>0); f++){
            disk_fetch *container = &instance[f];
            u8 changed = 0; 
            usize cl = container->length;
            for(usize i = 0; i<cl;i++){
                Cell *c = &container->vector[i];
                for(usize j=0;j<to_proc.len;j++){
                    u64 k;
                    memcpy(&k, &to_proc.buffer[j * 8], 8);  
                    Request rq = req[k];
                    if(rq.key == c->key){
                        if(rq.method == RQ_READ){
                            req[k].value = c->value;
                            unsigned char in[8];
                            u64_to_unsigned_char(in, i);
                            if(vec_push(&torem, in)<0){
                                sfree(mutation.buffer);
                                sfree(torem.buffer);
                                sfree(instance);
                                return handle_err(-1);
                            }
                        }
                        if(rq.method == RQ_DELETE){
                            c->key = container->vector[cl-1].key;
                            c->value = container->vector[cl-1].value;
                            cl--;
                            i--;
                            changed = 255;
                            decrease++;
                            unsigned char in[8];
                            u64_to_unsigned_char(in, i);
                            if(vec_push(&torem, in)<0){
                                sfree(mutation.buffer);
                                sfree(torem.buffer);
                                sfree(instance);
                                return handle_err(-1);
                            }
                        }
                        continue;
                    }
                    if(rq.method == RQ_WRITE && container->length < ELEMENTS_PER_VECTOR){
                        container->vector[container->length] = (Cell){rq.key,rq.value};
                        container->length++;
                        increase++;
                        changed=255;
                        unsigned char in[8];
                        u64_to_unsigned_char(in, j);
                        if(vec_push(&torem, &in)<0){
                            sfree(mutation.buffer);
                            sfree(torem.buffer);
                            sfree(instance);
                            return handle_err(-1);
                        }
                    }
                }
            }
            
            {
                u64* t = (u64*)torem.buffer;
                if (torem.len == 0) continue;
                for(usize i = torem.len-1; i >= 0; i--){
                    if(vec_remove(&to_proc, t[i]) < 0){
                        sfree(mutation.buffer);
                        sfree(torem.buffer);
                        sfree(instance);
                        return handle_err(-1);
                    };
                }
            }
            vec_clear(&torem);
            if(changed == 255){
                unsigned char in[8];
                u64_to_unsigned_char(in, f);
                if(vec_push(&mutation, in)<0){
                    sfree(mutation.buffer);
                    sfree(torem.buffer);
                    sfree(instance);
                    return handle_err(-1);
                };
            }
            container->length = cl;
        }
        sfree(torem.buffer);     
    }
    struct io_uring ring;
    if(io_uring_queue_init(mutation.len+1, &ring, 0) < 0){
        sfree(mutation.buffer);
        sfree(instance);
        return handle_err(-1);
    }
    u64* mu = (u64*)mutation.buffer;
    for(usize i = 0; i < mutation.len; i++){
        disk_fetch* k = &instance[mu[i]];
        qsort(k->vector, k->length, sizeof(Cell), cmp_cell_asc);
        if(k->length < ELEMENTS_PER_VECTOR){
            // Fill out of bonds with UINT64_MAX, the tombstone
            memset((k->vector)+k->length, 255, (ELEMENTS_PER_VECTOR-k->length)*sizeof(u64));
        }
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        self->m[k->pointer/VECTOR_SIZE].max = k->vector[k->length-1].key;
        self->m[k->pointer/VECTOR_SIZE].min = k->vector[0].key;
        io_uring_prep_write(sqe, self->file, k->vector, VECTOR_SIZE, k->pointer);
    }

    u64 mlc = self->ml*sizeof(meta);
    unsigned char *memory = malloc(sizeof(u32)+mlc);
    if(!memory){
        sfree(mutation.buffer);
        sfree(instance); 
        io_uring_queue_exit(&ring);
        errno = ENOMEM;
        return handle_err(-1);
    }
    memcpy(memory, self->m, mlc);
    memcpy(memory+mlc, &(unsigned char){self->ml}, sizeof(u32));
    struct io_uring_sqe *meta_sqe = io_uring_get_sqe(&ring);
    io_uring_prep_write(meta_sqe, self->file, memory, sizeof(u32)+mlc, VECTOR_SIZE*self->ml);
    if(io_uring_submit_and_wait(&ring,mutation.len)<0){
        sfree(mutation.buffer);
        sfree(instance);
        return handle_err(-1);
    }
    fsync(self->file); // metadata is important for initializing
    self->length += increase;
    self->length -= decrease;
    sfree(mutation.buffer);
    sfree(instance);
    return 0;
}



const float THRESHOLD = 2.0; 
const u64 MAX_PAGES = 16384;
i32 normalize(BTree *self){
    if(self->ml <= 3){
        return 0;
    }
    // gradient study
    float* gradient = calloc(self->ml/2, sizeof(float));
    if (gradient == NULL){
        errno = ENOMEM;
        return handle_err(-1);
    }
    
    hashset offsets;
    if (hashset_new_wise(&offsets, self->ml/2) < 0){
        return handle_err(-1);
    };


    {
        for(usize i = 0; i < (self->ml/2)-1;i++){
            float a = (float)self->m[i].max;
            float b = (float)self->m[i+1].max;
            float a1 = (float)self->m[i].min;
            float b1 = (float)self->m[i+1].min;
            float mid_a = 0.5f*(a + a1);
            float mid_b = 0.5f*(b + b1);
            gradient[i] = fabsf(mid_b - mid_a);
        }
    }
    float max = 0;
    float sum = 0;
    
    for(size_t i = 0; i < (self->ml/2) -1; i++){
        if(gradient[i] > max){
            max = gradient[i];
        }
        sum += gradient[i];
    }
    float mean = sum/(float)self->ml/2; 
    for(u64 i = 0; (i < (self->ml/2)-1) && offsets.length < MAX_PAGES;i++){
        float deviate = fabsf(mean-gradient[i]);
        if(deviate > THRESHOLD * mean){
            #pragma GCC unroll 5
            for (size_t j = 0; j < 5; j++){
                hashset_push(&offsets, (i+j)%self->ml);
            }

        }
    }

    free(gradient);
    if(offsets.length == 0){
        hashset_destroy(&offsets);
        return 0;
    }

    disk_fetch *fetch = malloc(sizeof(disk_fetch)*offsets.length);
    if(!fetch){
        errno = ENOMEM;
        return handle_err(-1);
    }

    struct io_uring *ring = malloc(sizeof(struct io_uring));
    if(!ring){
        free(fetch);
        hashset_destroy(&offsets);
        errno = ENOMEM;
        return 0;
    }
    if (io_uring_queue_init(offsets.length, ring, 0) < 0){
        free(ring);
        free(fetch);
        hashset_destroy(&offsets);
        return  handle_err(-1);
    }
    size_t len = 0;
    
    for(size_t i = 0; (i < offsets.capacity && len != offsets.length); i++){
        cell c = offsets.entries[i];
        if(c.exists){
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            fetch[len].length = self->m[c.value].len;
            fetch[len].pointer = c.value;
            io_uring_prep_read(sqe, self->file, fetch[len].vector, VECTOR_SIZE, VECTOR_SIZE*c.value);
            len++;
        }
    }

    if (io_uring_submit_and_wait(ring, offsets.length) < 0){
        free(fetch);
        hashset_destroy(&offsets);
        return handle_err(-1);
    }

    io_uring_queue_exit(ring);
    free(ring);

    Cell *plain_vector = malloc(sizeof(Cell)*ELEMENTS_PER_VECTOR*offsets.length);
    for(size_t i = 0; i < offsets.length; i++){
        memcpy(plain_vector+(i*VECTOR_SIZE), fetch[i].vector, VECTOR_SIZE);
    }

    qsort(plain_vector, offsets.length*ELEMENTS_PER_VECTOR, sizeof(Cell), cmp_cell_asc);
    
    for(size_t i = 0; i < offsets.length; i++){
        memcpy(fetch[i].vector, plain_vector+i*VECTOR_SIZE, VECTOR_SIZE);
    }

    free(plain_vector);
    ring = malloc(sizeof(struct io_uring));
    if(!ring){
        free(fetch);
        errno = ENOMEM;
        hashset_destroy(&offsets);
        return 0;
    }
    if (io_uring_queue_init((offsets.length*2)+1, ring, 0) < 0){
        free(ring);
        free(fetch);
        hashset_destroy(&offsets);
        return  handle_err(-1);
    }
    len = 0;
    
    for(size_t i = 0; (i < offsets.capacity && len != offsets.length); i++){
        cell c = offsets.entries[i];
        if(c.exists){
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            fetch[len].length = self->m[c.value].len;
            fetch[len].pointer = c.value;
            io_uring_prep_write(sqe, self->file, fetch[len].vector, VECTOR_SIZE, VECTOR_SIZE*c.value);
            len++;
        }
    }
    
    
    u_int8_t full = 0;
    for(size_t i = offsets.length-1; i-- >0;){
        size_t j;
        if (full == 0){
            for(j = ELEMENTS_PER_VECTOR-1; j-- > 0;){
                if(fetch[i].vector[j].key != UINT64_MAX){
                    break;
                }
            }
        }else{
            j = ELEMENTS_PER_VECTOR -1;
        }
        if(j == ELEMENTS_PER_VECTOR -1){full = 1;}
        Cell*vector= fetch[i].vector;
        u64 pointer = fetch[i].pointer;
        self->m[pointer] = (meta){vector[j].key,vector[0].key,j};
    }
    
    u64 mlc = self->ml*sizeof(meta);
    unsigned char *memory = malloc(sizeof(u32)+mlc);
    if(!memory){
        free(fetch);
        hashset_destroy(&offsets);
        io_uring_queue_exit(ring);
        errno = ENOMEM;
        return handle_err(-1);
    }
    memcpy(memory, self->m, mlc);
    memcpy(memory+mlc, &(unsigned char){self->ml}, sizeof(u32));

    struct io_uring_sqe *meta_sqe = io_uring_get_sqe(ring);
    io_uring_prep_write(meta_sqe, self->file, memory, sizeof(u32)+mlc, VECTOR_SIZE*self->ml);

    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        io_uring_prep_fsync(sqe, self->file, 0);
        sqe->flags |= IOSQE_IO_DRAIN;
    }
    if (io_uring_submit_and_wait(ring, (offsets.length*2)+1) < 0){
        free(fetch);
        hashset_destroy(&offsets);
        free(memory);
        io_uring_queue_exit(ring);
        return handle_err(-1);
    }
    free(memory);
    free(fetch);
    hashset_destroy(&offsets);
    io_uring_queue_exit(ring);
    return 0;
}
