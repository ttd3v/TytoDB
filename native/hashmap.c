#include "lib.h"
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

const unsigned long BUCKET_SIZE_MARGIN = 25;
const unsigned long DEFAULT_BUCKET_SIZE = 240;
const unsigned long HASHMAP_BLOCK_SIZE = 240;

struct Hashmap{
    int file;
    uint64_t bucket_size;
    uint64_t len;
};

typedef struct {
    unsigned char exists;
    uint64_t key;
    uint64_t value;
} Cell;
typedef struct{
    uint64_t bucket_size;
    uint64_t len;
} HashmapMetadata;

void serialize_cell(Cell *const value, unsigned char* buffer){ 
    buffer[0] = (value->exists & 0xFF);
    for(int i = 0; i < 8; i++){
        buffer[1+i] = (value->key >> 8*i & 0xFF);
    }
    for(int i = 0; i < 8; i++){
        buffer[9+i] = (value->value >> 8*i & 0xFF);
    } 
}
Cell deserialize_cell(unsigned char*const buffer){ 
    Cell value = {0};
    value.exists = buffer[0];
    for(int i = 0; i < 8; i++){
        value.key |= (uint64_t)buffer[1+i] << i*8;
        value.value |= (uint64_t)buffer[9+i] << i*8;
    }
    return value;
}

void serialize_hashmapmetadata(HashmapMetadata *const value, unsigned char* buffer){ 
    for(int i = 0; i < 8; i++){
        buffer[i] = (value->bucket_size >> 8*i & 0xFF);
        buffer[8+i] = (value->len >> 8*i & 0xFF);
    } 
}
HashmapMetadata deserialize_hashmapmetadata(unsigned char*const buffer){ 
    HashmapMetadata value = {0};
    for(int i = 0; i < 8; i++){
        value.bucket_size |= (uint64_t)buffer[i] << i*8;
        value.len |= (uint64_t)buffer[8+i] << i*8;
    }
    return value;
}

/*  Hashmap
 *  
 *  - File
 *  In this structure the file is a descriptor to operate with the disk space
 *  
 *  - Bucket_size
 *  The bucket_size is the size that the hashmap capacity (which is also limited by the margin)
 *  
 *  - Len
 *  Count of elements the hashmap is storing
 *
 *  - BUCKET_SIZE_MARGIN
 *  A constant that limits the len to **bucket_size - bucket_size*(BUCKET_SIZE_MARGIN/100)**. For example, if the value is 25(default) and the bucket size is
 *  1000 then 1000-1000*(25/100) = 750 
 *
 */





/// Hashmap_draw_defaults
///     Sets the file layout of a hashmap file
int hashmap_draw_defaults(FILE *file,uint64_t bucket_size){
    unsigned char *buffer = (unsigned char *)malloc(bucket_size*sizeof(Cell));
    int success = fwrite(buffer, sizeof(unsigned char), bucket_size*sizeof(Cell), file);
    if (success == -1){
        return success;
    }
    free(buffer);

    HashmapMetadata meta = (HashmapMetadata){bucket_size,0};
    unsigned char *meta_buffer = (unsigned char *)malloc(sizeof(HashmapMetadata));
    serialize_hashmapmetadata(&meta, meta_buffer);
    fseek(file, bucket_size*sizeof(Cell), 0);
    success = fwrite(meta_buffer, sizeof(unsigned char), sizeof(HashmapMetadata), file);

    if (success == -1){
        return success;
    }
    free(meta_buffer);
    fflush(file);

    int current = ftell(file);
    fseek(file, current, SEEK_SET);

    return 0;
}

typedef struct {
    int8_t some;
    int64_t value;
} OptionUINT64;

struct GetInput{
    uint32_t count;
    uint64_t *key;
};
struct GetOutput{
    int8_t success;
    uint32_t count;
    OptionUINT64 *value;
};

struct WriteInput{
    uint32_t count;
    uint64_t *key;
    uint64_t *value;
};


/// Hashmap_new
///     Creates a new file and a hashmap structure
/// Errors
///     0 -> Success
///     -1 -> failed to open file
///
int hashmap_new(char *path,struct Hashmap *hashmap){

    FILE *existence = fopen(path,"r");
    int exists = -1;
    if(existence){exists=0;fclose(existence);}else{exists=-1;}
    FILE *file = fopen(path, "r+b");
    
    if (file == NULL){
        return -1;
    }

    if(exists == -1){
        if (hashmap_draw_defaults(file,DEFAULT_BUCKET_SIZE) == -1){
            return -1;
        }
    }

    fseek(file,-sizeof(HashmapMetadata),SEEK_END);
    unsigned char *buffer = (unsigned char *)malloc(sizeof(HashmapMetadata));
    fread(buffer, sizeof(unsigned char), sizeof(HashmapMetadata), file);
    fseek(file, 0, SEEK_SET); 
    
    HashmapMetadata meta = deserialize_hashmapmetadata(buffer);
    free(buffer);
    hashmap->bucket_size = meta.bucket_size;
    hashmap->len = meta.len;
    hashmap->file = fileno(file);

    return 0;
}

typedef struct{
    uint64_t count;
    uint64_t capacity;
    uint64_t *values;
} vector;

int new_vector(vector *value){
    value->values = (uint64_t*)malloc(10*sizeof(uint64_t));
    value->capacity = 10;
    if (value->values == NULL){
        return -1;
    }
    value->capacity = 10;
    value->count = 0;
    return 0;
}
int push_vector(vector *value,uint64_t val){
    if (value->count < value->capacity){
        value->values[value->count] = val;
        value->count++;
    }else{
        void *temp = realloc(value->values, value->capacity*sizeof(uint64_t)*2);
        if (temp == NULL){
            return -1;
        }else{
            value->values=temp;
            value->capacity*=2;
            return push_vector(value, val);
        }
    }
    return 0;
} 

void quicksort(uint64_t *arr, int low, int high) {
    if (low < high) {
        int i = low - 1; 
        uint64_t pivot = arr[high];
        
        for (int j = low; j < high; j++) {
            if (arr[j] <= pivot) {
                i++;
                uint64_t temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }             
        uint64_t temp = arr[i + 1];
        arr[i + 1] = arr[high];
        arr[high] = temp;        
        int pivot_index = i + 1;
        quicksort(arr, low, pivot_index - 1);
        quicksort(arr, pivot_index + 1, high);
    }
}

void sort_vector(vector *value){
    quicksort(value->values, 0, value->count - 1);
}




typedef struct{
    uint64_t count;
    uint64_t capacity;
    Cell *values;
} vector_cell;

int new_vector_cell(vector_cell *value){
    value->values = (Cell*)malloc(2*sizeof(Cell));
    value->capacity = 2;
    if (value->values == NULL){
        return -1;
    }
    value->capacity = 2;
    value->count = 0;
    return 0;
}
int push_vector_cell(vector_cell *value,Cell val){
    if (value->count < value->capacity){
        value->values[value->count] = val;
        value->count++;
    }else{
        void *temp = realloc(value->values, value->capacity*sizeof(Cell)*2);
        if (temp == NULL){
            return -1;
        }else{
            value->values=temp;
            value->capacity*=2;
            return push_vector_cell(value, val);
        }
    }
    return 0;
} 



typedef uint64_t u64;

int hashmap_get(struct Hashmap *self, struct GetInput *entry, struct GetOutput *foreign_output){
    struct GetOutput output = (struct GetOutput){0,0,(OptionUINT64*)malloc(entry->count)};
    vector blocks;
    if (new_vector(&blocks)==-1) return -1;
    U64Hashmap hm = new_u64hashmap();
    for (uint64_t i=0;i<entry->count;i++){
        u64 a = entry->key[i]%(self->bucket_size/HASHMAP_BLOCK_SIZE);
        insert_u64hashmap(&hm, a,a);
    }

    struct io_uring ring;
    if (io_uring_queue_init(blocks.count, &ring, 0) < 0) return -1;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);

    unsigned char buffers[blocks.count][sizeof(Cell)*HASHMAP_BLOCK_SIZE];
    for (uint64_t i = 0; i < blocks.count; i++){
        io_uring_prep_read(sqe, self->file, &buffers[i], sizeof(Cell)*HASHMAP_BLOCK_SIZE, blocks.values[i]*(sizeof(Cell)*HASHMAP_BLOCK_SIZE));           
    }
    if (io_uring_submit(&ring) == -1) return -1;
    struct io_uring_cqe *cqe;
    if (io_uring_wait_cqe(&ring, &cqe) == -1) return -1;
    if (cqe->res < 0) return -1;
    free(blocks.values);

    vector_cell existing_cells;
    new_vector_cell(&existing_cells);
    for (uint64_t i = 0; i < blocks.count; i++){
        Cell vec_cel[HASHMAP_BLOCK_SIZE];
        for (uint64_t j = 0; j < HASHMAP_BLOCK_SIZE*sizeof(Cell); j++){
            Cell v = deserialize_cell(&buffers[i][j*sizeof(Cell)]);
            if(v.exists){
                push_vector_cell(&existing_cells, v);
            }
        }
    }

    for (uint64_t i = 0; i < entry->count; i++){
        OptionUINT64 a;
        a.some = -1;
        for (u64 j = 0; j < existing_cells.count; i++){
            Cell v = existing_cells.values[j];
            if(v.exists){
                a.some = 1;
                a.value = v.value;
                break;
            }
        }
        output.value[i] = a;
    }

    *foreign_output = output;
    return 0;
}
