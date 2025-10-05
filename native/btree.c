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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/uio.h>
#include "btree.h"
#define ELEMENTS_PER_VECTOR 256
#define GROWTH_OVERHEAD 10

static inline void sfree(void *ptr){
    free(ptr);
    ptr = NULL;
}

static inline void u64_to_unsigned_char(unsigned char *value, u64 u){
    for(usize i = 0; i < 8; i++){
        value[i] = (u >> (i * 8)) & 0xFF;
    }
}

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
    struct stat status = {0};
    i32 code = handle_err(stat(self->path, &status));
    if(code < 0){return code;};
    code = handle_err(pread(self->file, &len, sizeof(u32),status.st_size-sizeof(len)));
    if(code < 0){return code;}
    if (len > 0){
        unsigned char* buffer = malloc(len*sizeof(meta));
        if(!buffer){
            errno = ENOMEM;
            return handle_err(-1);
        }
        code = handle_err(pread(self->file, buffer, len*sizeof(meta), status.st_size-sizeof(len)-(len*sizeof(meta))));
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
    unsigned char empty[VECTOR_SIZE] = {0};
    ////printf("DEBUG: growth_count -> %lu\n",growth_count); 
    meta* new_metadata = calloc((self->ml+growth_count),sizeof(meta));
    if(!new_metadata){
        errno = ENOMEM;
        return handle_err(-1);
    }
    memcpy(new_metadata, self->m, self->ml*sizeof(meta));
    free(self->m);
    self->m = new_metadata;
    {
        struct iovec iov[growth_count];
        for(u32 i = 0; i < growth_count; i++){
            iov[i].iov_base = (void*)(&empty);
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
        ////printf("DEBUG: pl %u\n",pl);
        self->ml=pl;
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
    self->length = len;
    self->m = NULL;
    
    if(extend(self, GROWTH_OVERHEAD) < 0){
        return handle_err(-1);
    }
    
    return 0;
}

i32 init(BTree *self,char* path){ 
    self->path = path;
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

#define DEBUG_START //printf("HEARTBEATS == ");
#define DEBUG_PRINT //printf("💖 %lu\n",++DEBUG_STEP_COUNTER);
#define DEBUG_S //u64 DEBUG_STEP_COUNTER = 0;;

i32 bt_request(BTree *self,Request *req,usize req_count){
    DEBUG_S
    DEBUG_START
    //printf("bt_request\n");
    hashset vectors = {0};
    usize __write_ops__ = 0;
    usize increase = 0;
    usize decrease = 0;

    for(usize j = 0; j < req_count; j++){
        if(req[j].method == RQ_WRITE){
            __write_ops__+=1;
        }
    }

    ////printf("DEBUG: req_count->%lu\n",req_count);
    
    ////printf("DEBUG: __write_ops__->%lu\n",__write_ops__);
    
    
    if(self->length+__write_ops__ >= self->ml*ELEMENTS_PER_VECTOR){
        u64 growth_increase = GROWTH_OVERHEAD + ( (__write_ops__/ELEMENTS_PER_VECTOR) +1);
        if(extend(self, growth_increase) < 0){
            return handle_err(-1);
        };
        ////printf("DEBUG: EXTEND\n");
    }

    DEBUG_PRINT // 1

    if(hashset_new_wise(&vectors,self->ml)<0){return handle_err(-1);};
    
    __builtin_prefetch(self->m,0,1);
    __builtin_prefetch(req,0,2);

    usize proc_wri = 0;
    for (usize i = self->ml; i-- > 0;){
        ////printf("DEBUG: ml i %lu\n",i);
        meta me = self->m[i];
        ////printf("DEBUG: me-> %lu, %lu, %lu\n",me.len,me.min,me.max);
        if (me.len < ELEMENTS_PER_VECTOR && proc_wri < __write_ops__) {
            hashset_push(&vectors, i*VECTOR_SIZE);
            proc_wri += ELEMENTS_PER_VECTOR - me.len;
            ////printf("DEBUG: proc_wri->%lu\n",proc_wri);
            continue;
        }
        for(usize j = 0; j < req_count; j++) {
            Request r = req[j];
            if(r.key >= me.min && r.key <= me.max){
                hashset_push(&vectors, i*VECTOR_SIZE);
                break;
            }
        }
    }
    
    DEBUG_PRINT // 2
    disk_fetch *instance = malloc(sizeof(disk_fetch)*vectors.length);
    if(!instance){
        errno = ENOMEM;
        return handle_err(-1);
    }

    u64 length = vectors.length;
    {
        struct io_uring ring;
        if(io_uring_queue_init(length, &ring, 0) < 0){
            return handle_err(-1);
        };
        usize dflen = 0;
        for(usize i = 0; i < vectors.capacity; i++){
            hashset_cell cell = vectors.entries[i];
            if(!cell.exists) continue;
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            instance[dflen].pointer = cell.value;
            instance[dflen].length = self->m[cell.value/VECTOR_SIZE].len;
            io_uring_prep_read(sqe, self->file, instance[dflen].vector, VECTOR_SIZE, cell.value);
            dflen++;
        }
        int code = io_uring_submit_and_wait(&ring, length);
        if (code < 0){
            io_uring_queue_exit(&ring);
            return handle_err(code);
        }
        io_uring_queue_exit(&ring);
        ////printf("DEBUG: dflen->%lu\n",dflen);
    }
    hashset_destroy(&vectors);


    DEBUG_PRINT // 3

    vector mutation={0};
    u64* to_proc = malloc(req_count*sizeof(u64));
    u64 to_procl = req_count;

    #define DELETE_PROC to_proc[j] = to_proc[--to_procl];

    if(!to_proc){
        free(instance);
        errno = ENOMEM;
        return handle_err(-1);
    }
    if(vec_new(&mutation,sizeof(u64))<0){free(instance);free(to_proc);return handle_err(-1);};
    for(usize i = 0; i<req_count;i++){
        to_proc[i] = i;
    }
    DEBUG_PRINT // 4
    {
        for(usize f = 0; (f < length && to_procl>0); f++){
            disk_fetch *container = &instance[f];
            u8 changed = 0; 
            ////printf("DEBUG: + container.length -> %lu\n",container->length);
            u8 recheck = 0;
            for(usize i = 0; i<container->length;){
                Cell *c = &container->vector[i];
                for(usize j=0;j<to_procl;j++){
                    u64 v = to_proc[j];
                    Request rq = req[v];
                    if(rq.key == c->key){
                        if(rq.method == RQ_READ){
                            req[v].value = c->value;
                            DELETE_PROC
                            ;continue;
                        }
                        if(rq.method == RQ_DELETE){
                            c->key = container->vector[container->length-1].key;
                            c->value = container->vector[container->length-1].value;
                            container->length--;
                            changed = 255;
                            decrease++;
                            recheck = 255;
                            DELETE_PROC  
                            ;break;
                        }
                    }
                    
                }
                if (recheck == 255){recheck = 0;}else{i++;};
            }
            if (container->length < ELEMENTS_PER_VECTOR){
                for(usize j = 0; j < to_procl && container->length < ELEMENTS_PER_VECTOR; j++){
                    u64 v = to_proc[j];
                    Request rq = req[v];
                    if(rq.method == RQ_WRITE){
                        container->vector[container->length] = (Cell){rq.key,rq.value};
                        container->length++;
                        increase++;
                        changed=255;
                        DELETE_PROC 
                        ;continue;
                    }
                }
            }
            if(changed==255){
                vec_push(&mutation, &f);
                self->m[container->pointer/VECTOR_SIZE].len = container->length;
            } 
            ////printf("DEBUG: - container.length -> %lu\n",container->length);
        }
    }
    free(to_proc);
    DEBUG_PRINT
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
            memset((k->vector)+k->length, 255, (ELEMENTS_PER_VECTOR-k->length)*sizeof(Cell));
        }
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        self->m[k->pointer/VECTOR_SIZE].max = k->vector[k->length-1].key;
        self->m[k->pointer/VECTOR_SIZE].min = k->vector[0].key;
        io_uring_prep_write(sqe, self->file, k->vector, VECTOR_SIZE, k->pointer);
    }
    DEBUG_PRINT
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
    u32 ml_temp = self->ml;
    memcpy(memory+mlc, &ml_temp, sizeof(u32));
    
    struct io_uring_sqe *meta_sqe = io_uring_get_sqe(&ring);
    io_uring_prep_write(meta_sqe, self->file, memory, sizeof(u32)+mlc, VECTOR_SIZE*self->ml);
    if(io_uring_submit_and_wait(&ring,mutation.len+1)<0){
        sfree(mutation.buffer);
        sfree(instance);
        return handle_err(-1);
    }
    DEBUG_PRINT
    fsync(self->file); // metadata is important for initializing
    self->length += increase;
    self->length -= decrease;
    //printf("%lu %lu %lu\n",self->length,increase,decrease);
    sfree(mutation.buffer);
    sfree(instance);
    return 0;
}


i32 normalize(BTree *self){
    DEBUG_S
    DEBUG_START
    //printf("normalize\n");
    if(self->ml <= 3){
        return 0;
    }
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

    DEBUG_PRINT // 1
    
    for(size_t i = 0; i < (self->ml/2) -1; i++){
        if(gradient[i] > max){
            max = gradient[i];
        }
        sum += gradient[i];
    }

    DEBUG_PRINT // 2

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

    DEBUG_PRINT // 3

    free(gradient);
    if(offsets.length == 0){
        hashset_destroy(&offsets);
        return 0;
    }

    DEBUG_PRINT // 4

    disk_fetch *fetch = malloc(sizeof(disk_fetch)*offsets.length);
    if(!fetch){
        errno = ENOMEM;
        return handle_err(-1);
    }

    DEBUG_PRINT // 5

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

    DEBUG_PRINT // 6

    size_t len = 0;
    for(size_t i = 0; (i < offsets.capacity && len != offsets.length); i++){
        hashset_cell c = offsets.entries[i];
        if(c.exists){
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            fetch[len].length = self->m[c.value/VECTOR_SIZE].len;
            fetch[len].pointer = c.value;
            io_uring_prep_read(sqe, self->file, fetch[len].vector, VECTOR_SIZE, VECTOR_SIZE*c.value);
            len++;
        }
    }

    DEBUG_PRINT // 7
    ////printf("DEBUG: spot 7\n");

    if (io_uring_submit_and_wait(ring, len) < 0){
        free(fetch);
        hashset_destroy(&offsets);
        return handle_err(-1);
    }

    DEBUG_PRINT // 8
    ////printf("DEBUG: spot 7 was fine B)\n");
    io_uring_queue_exit(ring);
    free(ring);

    u64 omaga = offsets.length * ELEMENTS_PER_VECTOR * sizeof(Cell);
    Cell *plain_vector = malloc(omaga+sizeof(u64));
    if (!plain_vector){
        free(fetch);
        hashset_destroy(&offsets);
        errno = ENOMEM;
        return handle_err(-1);
    }
    
    for(size_t i = 0; i < len; i++){
        memcpy(plain_vector + (i * ELEMENTS_PER_VECTOR), fetch[i].vector, ELEMENTS_PER_VECTOR * sizeof(Cell));
    }

    DEBUG_PRINT;
    qsort(plain_vector, len * ELEMENTS_PER_VECTOR, sizeof(Cell), cmp_cell_asc);
    DEBUG_PRINT;

    // Copy sorted data back
    for(size_t i = 0; i < len; i++){
        memcpy(fetch[i].vector, plain_vector + (i * ELEMENTS_PER_VECTOR), ELEMENTS_PER_VECTOR * sizeof(Cell));
    }

    DEBUG_PRINT

    free(plain_vector);
    ring = malloc(sizeof(struct io_uring));
    if(!ring){
        free(fetch);
        errno = ENOMEM;
        hashset_destroy(&offsets);
        return 0;
    }
    if (io_uring_queue_init(offsets.length+2, ring, 0) < 0){
        free(ring);
        free(fetch);
        hashset_destroy(&offsets);
        return  handle_err(-1);
    }
    len = 0;
    DEBUG_PRINT
    for(size_t i = 0; (i < offsets.capacity && len != offsets.length); i++){
        hashset_cell c = offsets.entries[i];
        if(c.exists){
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            fetch[len].length = self->m[c.value].len;
            fetch[len].pointer = c.value;
            io_uring_prep_write(sqe, self->file, fetch[len].vector, VECTOR_SIZE, VECTOR_SIZE*c.value);
            len++;
        }
    }
    
    DEBUG_PRINT
    u8 full = 0;
    for(size_t i = 0; i < offsets.length; i++){
        size_t j = ELEMENTS_PER_VECTOR - 1;
        if (full == 0){
            while(j > 0 && fetch[i].vector[j].key == UINT64_MAX){
                j--;
            }
            if(j == ELEMENTS_PER_VECTOR - 1){
                full = 1;
            }
        }

        Cell *vector = fetch[i].vector;
        u64 pointer = fetch[i].pointer;
        self->m[pointer/VECTOR_SIZE] = (meta){vector[0].key, vector[j].key, j+1};
    } 
   
    DEBUG_PRINT
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
    u32 ml_temp = self->ml;
    memcpy(memory+mlc, &ml_temp, sizeof(u32));
    DEBUG_PRINT
    struct io_uring_sqe *meta_sqe = io_uring_get_sqe(ring);
    io_uring_prep_write(meta_sqe, self->file, memory, sizeof(u32)+mlc, VECTOR_SIZE*self->ml);
    DEBUG_PRINT
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        io_uring_prep_fsync(sqe, self->file, 0);
        sqe->flags |= IOSQE_IO_DRAIN;
    }
    if (io_uring_submit_and_wait(ring, offsets.length + 2) < 0){
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
