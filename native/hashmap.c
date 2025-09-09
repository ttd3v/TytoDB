
/*
 *  # DOCUMENTATION
 *  
 *  This code is an implementation of an in-disk hashmap. It works with in-disk chunks of 255 cells (each 16 bytes long), the length of every chunk
 *  being tracked by a bitmap, in-memory, and also stored on disk. It searches by gathering the 5 most likely cells, being the pivot and the next 4
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
 *  "softens" indexes bitmap-related, ensuring they stay within its bounds.
 *
 *  ## find_spot
 *  depending on the spot, write on the given array where the cell might be based on the key; "spec" changes whether the code will return spots for write
 *  or reading operations
 *
 *  ## save_metadata
 *  flushes metadata into disk, no fsync within.
 *
 *  ### OBSERVATIONS
 *  Slow operations such as fsyncs (regardless of it being faster on "fdata") will always only be run after the end of core-write operations — The ones
 *  that mutate or insert Cells.
 *
 * */


#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#define HASHMAP_VECTOR_SIZE 4080
#define GROWTH_FACTOR 32
#define DEFAULT_VECTOR_COUNT 32
#define SPOTS_ARRAY_LENGTH 5

typedef struct {
    int file;
    unsigned char *bitmap;
    u_int64_t bucket_size;
    u_int64_t len;
} Hashmap;

typedef struct {
    u_int64_t hk; // hashed key
    u_int64_t v; // value
} Cell;
const u_int64_t c_s = sizeof(Cell);

int new(Hashmap* self, char* path){
    if (access(path, F_OK) == -1){
        int fd = open(path, O_CREAT | O_RDWR, 0644);
        if(fd == -1){
            printf("Failed to create hashmap file\n");
            return -1;
        }
        self->bitmap = calloc((HASHMAP_VECTOR_SIZE/c_s)*DEFAULT_VECTOR_COUNT,sizeof(unsigned char));
        self->len = 0;
        self->bucket_size = DEFAULT_VECTOR_COUNT*(HASHMAP_VECTOR_SIZE/c_s);
        self->file = fd;

        if(!self->bitmap){
            close(fd);
            printf("Failed to allocate bitmap's memory\n");
            return -1;
        }
        
        u_int64_t buffer_size = (sizeof(u_int64_t)*2) + ((HASHMAP_VECTOR_SIZE/c_s)*DEFAULT_VECTOR_COUNT);
 
        unsigned char* buffer = calloc(buffer_size, sizeof(unsigned char));
        if(!buffer){
            free(self->bitmap);
            close(fd);
            printf("Failed to allocate buffer memory\n");
            return -1;
        }

        {
        u_int64_t* u64_buf = (u_int64_t*)buffer;
        u64_buf[1] = self->bucket_size; 
        }

        int write_result = pwrite(fd, buffer, buffer_size, 0);
        if(write_result < 0){
            free(buffer);
            close(fd);
            free(self->bitmap);
            printf("Failed to write, error: %i\n",write_result);
            return -1;
        }
        free(buffer);
        unsigned char *vector_buffer = calloc(HASHMAP_VECTOR_SIZE, sizeof(unsigned char));
        if(!vector_buffer){
            free(self->bitmap);
            close(fd);
            printf("Failed to allocate vector buffer\n");
            return -1;
        }
        
        struct iovec* list = (struct iovec*)calloc(DEFAULT_VECTOR_COUNT, sizeof(struct iovec));
        if(!list){
            free(self->bitmap);
            close(fd);
            printf("Failed to allocate memory for list\n");
            return -1;
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
            free(self->bitmap);
            close(fd);
            free(list);
            return -1;
        
        }
        free(list);
        free(vector_buffer);
        return 0;
    }else{
        int fd = open(path,  O_RDWR, 0644);
        if(fd == -1){
            printf("Failed to open hashmap file\n");
            return -1;
        }
        u_int64_t integer_buffer[2];
        //pread(int fd, void *buf, size_t nbytes, __off_t offset)
        if(pread(fd, &integer_buffer, 16, 0) < 0){
            close(fd);
            printf("Failed to read hashmap metadata\n");
            return -1;
        }   
        self->len = integer_buffer[0];
        self->bucket_size = integer_buffer[1];
        self->bitmap = malloc(integer_buffer[1]);
        if (!self->bitmap){
            close(fd);
            printf("Failed to allocate memory to bitmap\n");
            return -1;
        }
        if(pread(fd,self->bitmap,integer_buffer[1],16) < 0){
            close(fd);
            free(self->bitmap);
            printf("Failed to read bitmap\n");
            return -1;
        }
        return 0;
    }
    return -1;
}
int save_metadata(Hashmap *self){
    unsigned char *buffer = malloc(self->bucket_size + 16);
    {
        u_int64_t *b = (u_int64_t*)buffer;
        b[0] = self->len;
        b[1] = self->bucket_size;
    }
    memcpy(buffer+16, self->bitmap, self->bucket_size);
    if (pwrite(self->file, buffer, 16+self->bucket_size, 0) < 0){
        free(buffer);
        printf("Failed to write data from buffer into file\n");
        return -1;
    }
    free(buffer);
    return 0;
}

u_int64_t soft(u_int64_t idx, u_int64_t size){
    return idx > size-1?idx-size:idx;
}

void find_spots(Hashmap *self, u_int64_t hk, u_int64_t *array, int specs){
    unsigned char done = 0;
    u_int64_t offset = 0;
    u_int64_t pivot = hk % self->bucket_size;
    u_int64_t temp[SPOTS_ARRAY_LENGTH] = {0};
   if (specs == 0){
       // find free spot 
       while (done == 0){
            #pragma GCC unroll SPOTS_ARRAY_LENGTH
            for(u_int64_t i = 0; i<SPOTS_ARRAY_LENGTH;i++){
                temp[i] = self->bitmap[soft(pivot+(i*offset), self->bucket_size)];
                if(temp[i] < 255){
                    array[0] = temp[i];
                    done = 1;
                }
            }
            offset++;
       }
   }else{
       // return possible spots 
        #pragma GCC unroll SPOTS_ARRAY_LENGTH
        for(u_int64_t i = 0; i<SPOTS_ARRAY_LENGTH;i++){
            array[i] = self->bitmap[soft(pivot+(i*offset), self->bucket_size)];
        }
   }
}
