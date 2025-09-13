/*
 *  # DOCUMENTATION
 *  
 *  This code is an implementation of an in-disk hashmap. It works with in-disk chunks of 255 cells (each 16 bytes long), the length of every chunk
 *  being tracked by a array_len, in-memory, and also stored on disk. It searches by gathering the 5 most likely cells, being the pivot and the next 4
 *  (softened if N > bucket_size). For writes, it takes only the spot between those 5 slots that have free spaces. The "5" mentioned is the default, and can
 *  be altered by changing the "SPOTS_ARRAY_LENGTH" definition.
 *
 *  Both write functions and read functions receive keys already hashed, since this piece of code isn't meant to be a Rust FFI; The rust-end takes care of this
 *  side, leaving the logic to the C code (this file).
 *
 *  # API REFERENCE
 *
 *  ## new
 *  Creates a new HashMap if none is detected in the given path, or opens an existing one if any. The creation process set up the variables in the struct
 *  "Hashmap"; No sophisticated logic.
 *
 *  ## soft
 *  "softens" indexes array_len-related, ensuring they stay within its bounds.
 *
 *  ## find_spot
 *  depending on the spot, write on the given array where the cell might be based on the key; "spec" changes whether the code will return spots for write
 *  or reading operations
 *
 *  ## save_metadata
 *  flushes metadata into disk, no fsync within.
 *  
 *  ## fetch_slots
 *  Get in-disk slots, a helper function for get and write functions; Batch operations by merging reads, max batch size being "MAX_BATCH_SIZE" and the maximum
 *  memory usage per batch is "MAX_BATCH_SIZE*HASHMAP_VECTOR_SIZE". Doesn't use io_uring.
 *
 *  ## fast_sort
 *  Perform insertion-sort when N < 100 with a fallback to quicksort if N > 100
 *
 *  ## free_array
 *  Free all memory within an array of memory-allocated buffers.
 *
 *  ## free_mem
 *  Free all memory in buffers within an "f" object of "fmem"; Meant to turn the process of deallocating memory from various pointers more reliable, specially
 *  in cases where memory arithmetic couldn't be implemented.
 * 
 *  ## raw_get
 *  Uses fetch_slots and get existence-value-spot of entries. A helper for both write and get
 *
 *  ## hm_get
 *  Uses raw_get to get the existence-value of entries.
 *
 *  ## hm_write
 *  Uses fetch_spots to get the offsets in which it must write into, using io_uring to batch changes. Deletions are done by setting the value 18,446,744,073,709,551,615
 *
 *
 *
 *
 *  May contain a self-reference for freeing itself, the reference must always be (and currently are) at the EOA(End of array).
 *  
 *  ### OBSERVATIONS
 *  Slow operations such as fsyncs (regardless of it being faster on "fdata") will always only be run after the end of core-write operations — The ones
 *  that mutate or insert Cells.
 *  
 *  The code currently remains without any meaningful error returns (-1 only); After implementing all the intended functions a pattern for all errors will
 *  be created based on all the possible errors that can occur during execution. It has "printf's" as a way of helping me to debug the code. Meant to be
 *  poor for ease of iteration during early development.
 *
 *  The current code will always be run in a synchronous (single-threaded) routine.
 *  
 *  Everything within the Hashmap structure will primarily come from the rust-end, meaning that what is covered by the structure itself also
 *  have rust memory-safety guarantees; All function arguments come from the rust-end, they do not need validation (specifically in the C code).
 *  
 *
 *
 * */


#include <stdint.h>
#include "hashmap.h"
#include <fcntl.h>
#include <liburing/io_uring.h>
#include <sched.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <liburing.h>

#define HASHMAP_VECTOR_SIZE 4080
#define GROWTH_FACTOR 32
#define DEFAULT_VECTOR_COUNT 32
#define MAX_BATCH_SIZE 8224
#define REBUCKET_ON_PERCENTAGE 65
#define SPOTS_ARRAY_LENGTH 5

// Error codes
#define ERROR_GENERAL -1
#define ERROR_MEMALLOC -2
#define ERROR_NO_DISK_SPACE -3
#define ERROR_IO_URING_QUEUE -4
#define ERROR_FILE_OPEN -5
#define ERROR_FILE_CREATE -6
#define ERROR_FILE_READ -7
#define ERROR_FILE_WRITE -8
#define ERROR_FILE_SYNC -9
#define ERROR_VECTOR_ALLOC -10
#define ERROR_IOVEC_ALLOC -11
#define ERROR_FETCH_HASHSET_ALLOC -12
#define ERROR_FETCH_TO_READ_ALLOC -13
#define ERROR_FETCH_CELL_BUFFER_ALLOC -14
#define ERROR_FETCH_BUFFER_HASHMAP -15
#define ERROR_FETCH_VECTOR_CELLS -16
#define ERROR_FETCH_FETCH_ALLOC -17
#define ERROR_REBUCKET_BUFFER_ALLOC -18
#define ERROR_REBUCKET_CELLS_ALLOC -19
#define ERROR_WRITE_SLOT_FULL -20

#define c_s sizeof(Cell)

inline void free_mem(fmem* mem){
    for(size_t i = 0; i<mem->l; i++) {
        free(mem->f[i]);
        mem->f[i] = NULL;
    }
}


int draw_defaults(Hashmap *self, u_int64_t bucket_size){
    int fd = open(self->path, O_CREAT | O_RDWR, 0644);
        if(fd == -1){
            printf("Failed to create hashmap file\n");
            return ERROR_FILE_CREATE;
        }
        self->array_len = calloc((HASHMAP_VECTOR_SIZE/c_s)*DEFAULT_VECTOR_COUNT,sizeof(unsigned char));
        if(!self->array_len){
            close(fd);
            return ERROR_MEMALLOC;
        }
        self->len = 0;
        self->bucket_size = bucket_size;
        self->file = fd;

        if(!self->array_len){
            close(fd);
            printf("Failed to allocate array_len's memory\n");
            return ERROR_MEMALLOC;
        }
        
        u_int64_t buffer_size = (sizeof(u_int64_t)*2) + (self->bucket_size*HASHMAP_VECTOR_SIZE*c_s);
 
        unsigned char* buffer = calloc(buffer_size, sizeof(unsigned char));
        if(!buffer){
            free(self->array_len);
            close(fd);
            printf("Failed to allocate buffer memory\n");
            return ERROR_MEMALLOC;
        }

        {
        u_int64_t* u64_buf = (u_int64_t*)buffer;
        u64_buf[1] = self->bucket_size; 
        }

        int write_result = pwrite(fd, buffer, buffer_size, 0);
        if(write_result < 0){
            free(buffer);
            close(fd);
            free(self->array_len);
            printf("Failed to write, error: %i\n",write_result);
            return ERROR_FILE_WRITE;
        }
        free(buffer);
        unsigned char *vector_buffer = calloc(HASHMAP_VECTOR_SIZE, sizeof(unsigned char));
        if(!vector_buffer){
            free(self->array_len);
            close(fd);
            printf("Failed to allocate vector buffer\n");
            return ERROR_VECTOR_ALLOC;
        }
        
        struct iovec* list = (struct iovec*)calloc(DEFAULT_VECTOR_COUNT, sizeof(struct iovec));
        if(!list){
            free(self->array_len);
            close(fd);
            printf("Failed to allocate memory for list\n");
            return ERROR_IOVEC_ALLOC;
        }

        //pwritev(int fd, const struct iovec *iovec, int count, __off_t offset)
        #pragma GCC unroll 32 
        for(u_int64_t i = 0;i<DEFAULT_VECTOR_COUNT;i++){
            list[i].iov_base = vector_buffer;
            list[i].iov_len = HASHMAP_VECTOR_SIZE;
        }
        if (pwritev(fd, list, DEFAULT_VECTOR_COUNT, buffer_size) < 0){
            printf("failed to perform the vetorized write\n");
            free(vector_buffer);
            free(self->array_len);
            close(fd);
            free(list);
            return ERROR_FILE_WRITE;
        
        }
        free(list);
        free(vector_buffer);
        return 0;
}

int hm_new(Hashmap* self, char* path){
    if (access(path, F_OK) == -1){
        return draw_defaults(self, DEFAULT_VECTOR_COUNT);    
    }else{
        int fd = open(path,  O_RDWR, 0644);
        if(fd == -1){
            printf("Failed to open hashmap file\n");
            return ERROR_FILE_OPEN;
        }
        self->file = fd;
        u_int64_t integer_buffer[2];
        //pread(int fd, void *buf, size_t nbytes, __off_t offset)
        if(pread(fd, &integer_buffer, 16, 0) < 0){
            close(fd);
            printf("Failed to read hashmap metadata\n");
            return ERROR_FILE_READ;
        }   
        self->len = integer_buffer[0];
        self->bucket_size = integer_buffer[1];
        self->array_len = malloc(integer_buffer[1]);
        if (!self->array_len){
            close(fd);
            printf("Failed to allocate memory to array_len\n");
            return ERROR_MEMALLOC;
        }
        if(pread(fd,self->array_len,integer_buffer[1],16) < 0){
            close(fd);
            free(self->array_len);
            printf("Failed to read array_len\n");
            return ERROR_FILE_READ;
        }
        return 0;
    }
    return ERROR_GENERAL;
}
int save_metadata(Hashmap *self){
    unsigned char *buffer = malloc(self->bucket_size + 16);
    {
        u_int64_t *b = (u_int64_t*)buffer;
        b[0] = self->len;
        b[1] = self->bucket_size;
    }
    memcpy(buffer+16, self->array_len, self->bucket_size);
    if (pwrite(self->file, buffer, 16+self->bucket_size, 0) < 0){
        free(buffer);
        printf("Failed to write data from buffer into file\n");
        return ERROR_FILE_WRITE;
    }
    free(buffer);
    return 0;
}

inline u_int64_t soft(u_int64_t idx, u_int64_t size){
    return idx > size-1?idx-size:idx;
}

inline void find_spots(Hashmap *self, u_int64_t hk, u_int64_t *array){
    u_int64_t pivot = hk % self->bucket_size;
  
    // return possible spots 
    #pragma GCC unroll 5
    for(u_int64_t i = 0; i<SPOTS_ARRAY_LENGTH;i++){
        array[i] = soft(pivot+i, self->bucket_size);
    }
   
}


// HASH
#define XXH_PRIME64_2 0xC2B2AE3D27D4EB4FULL
#define XXH_PRIME64_3 0x165667B19E3779F9ULL
inline u_int64_t hash64(u_int64_t x) {
    x ^= x >> 33;
    x *= XXH_PRIME64_2;
    x ^= x >> 29;
    x *= XXH_PRIME64_3;
    x ^= x >> 32;
    return x;
}


struct fetch_entry {
    u_int64_t* request;
    u_int64_t length;
    cell_fetch* results;
    fmem* mem;
}; 


void fast_sort(u_int64_t *array, u_int64_t length) {
    if (length <= 1) return;
    
    if (length < 100) {
        
        for (u_int64_t i = 1; i < length; i++) {
            u_int64_t key = array[i];
            u_int64_t j = i;
            
            while (j > 0 && array[j-1] > key) {
                array[j] = array[j-1];
                j--;
            }
            array[j] = key;
        }
    } else {
        
        struct {
            u_int64_t low, high;
        } stack[64];          
        int top = 0;
        stack[top].low = 0;
        stack[top].high = length - 1;
        
        while (top >= 0) {
            u_int64_t low = stack[top].low;
            u_int64_t high = stack[top].high;
            top--;
            
            if (low < high) { 
                u_int64_t pivot = array[high];  
                u_int64_t i = low;
                
                for (u_int64_t j = low; j < high; j++) {
                    if (array[j] <= pivot) {
                        u_int64_t temp = array[i];
                        array[i] = array[j];
                        array[j] = temp;
                        i++;
                    }
                }
                
                
                u_int64_t temp = array[i];
                array[i] = array[high];
                array[high] = temp;
                
                u_int64_t pi = i;  
                
                if (pi > low + 1) {
                    top++;
                    stack[top].low = low;
                    stack[top].high = pi - 1;
                }
                
                if (pi + 1 < high) {
                    top++;
                    stack[top].low = pi + 1;
                    stack[top].high = high;
                }
            }
        }
    }
}

inline void free_array(unsigned char**buffer, u_int64_t len){
    for(u_int64_t i = 0; i < len;i++){
        if(buffer[i]!=NULL){
            free(buffer[i]);
            buffer[i]=NULL;
        };
    };
}

int fetch_slots(Hashmap *self, fetch_entry* entry){
    entry->mem->f = NULL;
    u_int64_t *to_read = NULL;
    u_int64_t trl = 0;
    {
        u_int64_t hcs = entry->length*10;
        __hc__* hashset = calloc(hcs, sizeof(__hc__));
        if(!hashset){
            printf("Failed to allocate memory for hashset in fetch slots\n");
            return ERROR_FETCH_HASHSET_ALLOC;
        }
        for(u_int64_t i = 0; i < entry->length; i++){
            u_int64_t spots[SPOTS_ARRAY_LENGTH]={0};
            find_spots(self, entry->request[i], spots);
            #pragma GCC unroll 5
            for(u_int64_t j = 0; j < SPOTS_ARRAY_LENGTH; j++){
                u_int64_t offset = 0;
                u_int64_t pivot = hash64(spots[j]) % hcs;
                u_int64_t cur_idx = soft(pivot+offset, hcs);
                __hc__ cur = hashset[cur_idx];
                while(cur.exists && cur.hk != spots[j] && offset < hcs){
                    offset++;
                    cur_idx = soft(pivot+offset, hcs);
                    cur = hashset[cur_idx];
                }
                if(cur.exists && cur.hk == spots[j]) continue;
                hashset[cur_idx] = (__hc__){1,spots[j]};   
                trl++;
            }
        }
        to_read = (u_int64_t*)malloc(trl*sizeof(u_int64_t));
        if(!to_read){
            free(hashset);
            printf("Failed to allocate memory for \"to_read\" in fetch_slots\n");
            return ERROR_FETCH_TO_READ_ALLOC;
        }
        {
            u_int64_t rel = 0;
            for(u_int64_t i =0;i<hcs;i++){
                __hc__ h = hashset[i];
                if(h.exists){
                    to_read[rel]=h.hk;rel++;
                }  
            }
        }
        free(hashset);
    }
    fast_sort(to_read, trl);
    unsigned char *cell_buffer[trl];
    {
        u_int64_t i = 0;
        for(i=0;i<trl;i++){cell_buffer[i]=NULL;};
        for(i=0;i<trl;i++){
            cell_buffer[i]=malloc(HASHMAP_VECTOR_SIZE);
            if(!cell_buffer[i]){
                free_array(cell_buffer, trl);
                free(to_read);
                printf("Failed to allocate cell_buffer\n");
                return ERROR_FETCH_CELL_BUFFER_ALLOC;
            }
        }
    }

    u_int64_t readen = 0;
    
    while(readen < trl){
        u_int64_t target = readen;
        for(u_int64_t i = readen+1; i < trl;i++){
            if(to_read[i] - to_read[readen] <= MAX_BATCH_SIZE){target = i;};
        }
        u_int64_t cur = readen;
        u_int64_t count = target - cur + 1;
        u_int64_t first_bucket = to_read[cur];
        u_int64_t last_bucket = to_read[target];
        u_int64_t buffer_size = (last_bucket - first_bucket + 1) * HASHMAP_VECTOR_SIZE;

        unsigned char *buffer = malloc(buffer_size);
        if(!buffer){
            printf("Failed to allocate reading buffer in fetch_slots\n");
            free(to_read);
            free_array(cell_buffer, trl);
            return ERROR_MEMALLOC;
        }
        if (pread(self->file, buffer, buffer_size, (self->bucket_size+16)+(first_bucket*HASHMAP_VECTOR_SIZE))<0){
            printf("Failed to read into buffer into buffer in fetch_slots\n");
            free(to_read);
            free(buffer);
            free_array(cell_buffer, trl);
            return ERROR_FILE_READ;
        }
        for(u_int64_t i = cur; i <= target; i++){
            memcpy(cell_buffer[i], buffer + (to_read[i]-first_bucket)*HASHMAP_VECTOR_SIZE, HASHMAP_VECTOR_SIZE);
        }
        readen = target + 1;
        free(buffer);
    }
    
    __hcb__ *buffer_hashmap = (__hcb__*)calloc(trl,sizeof(__hcb__));
    if(!buffer_hashmap){
        free(to_read);
        free_array(cell_buffer, trl);
        printf("Failed to allocate memory for buffer_hashmap\n");
        return ERROR_FETCH_BUFFER_HASHMAP;
    }
    for(u_int64_t i = 0; i < trl; i++){
        u_int64_t offset = 0;
        u_int64_t df = hash64(to_read[i])%trl;
        u_int64_t spot = df;
        while(buffer_hashmap[spot].exists){
            offset++;
            spot = soft(spot+offset, trl);
        }
        buffer_hashmap[spot] = (__hcb__){255,to_read[i],i};
    }
    
    cell_fetch* fetch = (cell_fetch*)malloc(sizeof(cell_fetch)*entry->length);
    if(!fetch){
        free(buffer_hashmap);
        free_array(cell_buffer, trl);
        free(to_read);
        printf("Failed to allocate memory for fetch\n");
        return ERROR_FETCH_FETCH_ALLOC;
    }
    entry->results = fetch;
    cell_vector* vector_cells = malloc(sizeof(cell_vector)*SPOTS_ARRAY_LENGTH*entry->length);
    if(!vector_cells){
        free(buffer_hashmap);
        free_array(cell_buffer, trl);
        free(fetch);
        free(to_read);
        printf("Failed to allocate memory for vector_cells\n");
        return ERROR_FETCH_VECTOR_CELLS;
    }
    for(size_t i = 0; i<entry->length; i++){
        fetch[i].hk = entry->request[i];
        u_int64_t spots[SPOTS_ARRAY_LENGTH];
        find_spots(self, fetch[i].hk, spots);
        fetch[i].value = vector_cells + (i*SPOTS_ARRAY_LENGTH);
        for(size_t j = 0; j < SPOTS_ARRAY_LENGTH; j++){
            u_int64_t offset = 0;
            u_int64_t df = hash64(spots[j])%trl;
            u_int64_t spot = df;
            while((!buffer_hashmap[spot].exists || buffer_hashmap[spot].spot != spots[j]) && offset < trl){
                offset++;
                spot = soft(spot+offset, trl);
            }
            u_int64_t bid = buffer_hashmap[spot].bid;
            fetch[i].value[j].value = (Cell*)cell_buffer[bid];
            fetch[i].value[j].length = self->array_len[spots[j]];
            fetch[i].value[j].index = spots[j];
        }
    }
    free(to_read);to_read = NULL;
    free(buffer_hashmap);
    entry->mem->f = malloc(sizeof(void*)*(3+trl));
    if(!entry->mem->f){
        printf("Failed to allocate memory for mem\n");
        free(fetch);
        free(vector_cells);
        free_array(cell_buffer, trl);
        return ERROR_MEMALLOC;
    }
    entry->mem->l = 3+trl;
    entry->mem->f[0] = (void*)fetch;
    entry->mem->f[1] = (void*)vector_cells;
    for(size_t i = 0; i < trl; i++){
        entry->mem->f[2+i] = cell_buffer[i];
    }
    entry->mem->f[2+trl] = entry->mem->f;
    return 0;
} 


int raw_get(Hashmap* self, u_int64_t *inputs, SomeRawGet *outputs, u_int64_t length){
    fetch_entry fetch_request;
    fetch_request.length = length;
    fetch_request.request = inputs;
    int fetch_result = fetch_slots(self, &fetch_request);
    if(fetch_result < 0){
        return fetch_result;
    }
    
    for(size_t i = 0; i < length; i++){
        cell_fetch* v = &fetch_request.results[i];
        SomeRawGet u = {0,0,0};
        #pragma GCC unroll 5
        for(size_t j = 0; j < SPOTS_ARRAY_LENGTH;j++){
            cell_vector *cv = &v->value[j];
            if(cv->length > 0 && u.exists == 0){
                for(size_t k = 0; k < cv->length;k++){
                    if(cv->value[k].hk == v->hk){
                        u.exists = 1;
                        u.value = cv->value[k].v;
                        u.spot = cv->index;
                    }
                }
            }else{
                break;
            }
        }
        outputs[i] = u;
    }
    free_mem(fetch_request.mem);
    
    return 0;
}

int hm_get(Hashmap *self, u_int64_t *inputs, SomeU64 *outputs, u_int64_t length){
    SomeRawGet raw_output[length];
    if (raw_get(self, inputs, raw_output, length) < 0){
        return ERROR_GENERAL;
    }
    for(size_t i = 0; i < length; i++){
        outputs[i] = (SomeU64){raw_output[i].exists,raw_output[i].value};
    }
    return 0;
}



int hm_rebucket(Hashmap *self);

int hm_write(Hashmap *self, Cell* inputs, u_int64_t length){
    u_int64_t rqst[length];

    for (size_t i = 0; i < length; i++) {
        rqst[i] = inputs[i].hk;
    }

    fetch_entry fetch;
    fetch.length = length;
    fetch.request = rqst;

    int fetch_code = fetch_slots(self,&fetch);
    if(fetch_code < 0){
        return fetch_code;
    }
    
    struct io_uring ring;
    if (io_uring_queue_init(length*5, &ring, 0) < 0){
        free_mem(fetch.mem);
        return ERROR_IO_URING_QUEUE;
    };

    
    for(size_t idx = 0; idx < length; idx++){
        cell_fetch *cf = &fetch.results[idx];
        u_int64_t offset = 16 + self->bucket_size;
        u_int8_t done = 0;
        Cell cell = inputs[idx];

        for (size_t i = 0; i < SPOTS_ARRAY_LENGTH; i++){
            cell_vector *v = &cf->value[i];
            for(size_t j =0; j < v->length; j++){
                if(v->value[j].hk == cell.hk){
                    offset += v->index*HASHMAP_VECTOR_SIZE+(j*c_s);
                    done = 1;
                    if(cell.v == UINT64_MAX){
                        cell=v->value[v->length-1];
                        self->array_len[v->index]--;
                    }
                    break;
                }
            }
            if(done == 1) break;
        }
        if (done == 1){
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            io_uring_prep_write(sqe, self->file, &cell, c_s, offset);
            continue;
        }
        for (size_t i = 0; i < SPOTS_ARRAY_LENGTH; i++){
            cell_vector *v = &cf->value[i];
            if(v->length < 255){
                done = 1;
                offset += (v->index*HASHMAP_VECTOR_SIZE)+(v->length*c_s);
                self->array_len[v->index]++;
                self->len++;
            }
            if(done == 1){
                break;
            }
        }
        if(done == 1){
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            io_uring_prep_write(sqe, self->file, &cell, c_s, offset);
            
            continue;
        }

        free_mem(fetch.mem);
        io_uring_queue_exit(&ring);
        return ERROR_WRITE_SLOT_FULL;
    }
  

    int result = io_uring_submit_and_wait(&ring, length);
    io_uring_queue_exit(&ring);
    free_mem(fetch.mem);
    if(result < 0){ 
        return ERROR_FILE_WRITE;
    }
    save_metadata(self);
    fdatasync(self->file);
    if((self->len*100 / self->bucket_size) >= REBUCKET_ON_PERCENTAGE){
        return hm_rebucket(self);
    }
    return 0;
}




int hm_rebucket(Hashmap *self){
    Cell* buffer = malloc(MAX_BATCH_SIZE*HASHMAP_VECTOR_SIZE);
    Cell* cells = malloc(MAX_BATCH_SIZE*HASHMAP_VECTOR_SIZE);
    if (!buffer){
        return ERROR_REBUCKET_BUFFER_ALLOC;
    }
    if(!cells){
        free(buffer);
        return ERROR_REBUCKET_CELLS_ALLOC;
    }
    u_int64_t header_offset = self->bucket_size + 16;
    Hashmap temp;
    
    temp.path = self->temp;
    int su = draw_defaults(&temp, self->bucket_size*GROWTH_FACTOR);
    if (su < 0){
        free(buffer);
        free(cells);
        return su;
    }
    

    for(u_int64_t i = 0; i < self->bucket_size;){
        u_int64_t to_read = i+MAX_BATCH_SIZE < self->bucket_size?MAX_BATCH_SIZE: self->bucket_size-i;
        if (pread(self->file, buffer, to_read*HASHMAP_VECTOR_SIZE, header_offset+(i*HASHMAP_VECTOR_SIZE)) < 0){
            free(buffer);
            free(cells);
            return ERROR_FILE_READ;
        };

        u_int64_t len = 0;
        for (u_int64_t j = 0; j < to_read; j ++){
            if(self->array_len[j+i]>0){
                for(u_int64_t k = 0; k < self->array_len[j+i];k++){
                    cells[len] = buffer[(j*(HASHMAP_VECTOR_SIZE/c_s))+k];
                    len++;
                }
            }
        }
        if(hm_write(&temp, cells, len) < 0){
            free(buffer);
            free(cells);
            return ERROR_FILE_WRITE;
        }
        i+=to_read;
    }
    free(buffer);
    free(cells);
    free(self->array_len);
    close(self->file);
    remove(self->path);
    rename(self->temp, self->path);
    self->file = temp.file;
    self->array_len = temp.array_len;
    return 0;
};
