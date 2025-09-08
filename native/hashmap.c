
#include <bits/types/struct_iovec.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

const u_int64_t HASHMAP_VECTOR_SIZE = 16384;
const u_int64_t GROWTH_FACTOR = 32;
const u_int64_t DEFAULT_VECTOR_COUNT = 32;


typedef struct {
    int file;
    unsigned char *bitmap;
    u_int64_t bucket_size;
    u_int64_t len;
} Hashmap;

typedef struct {
    u_int64_t key;
    u_int64_t value;
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
        if(pread(fd, &integer_buffer, 8, 0) < 0){
            close(fd);
            printf("Failed to read hashmap metadata\n");
            return -1;
        }   
        self->len |= integer_buffer[0];
        self->bucket_size |= integer_buffer[1];

    }
    return -1;
}
