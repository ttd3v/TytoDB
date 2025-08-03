#include "lib.h"
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include "error.h"

const unsigned long BUCKET_SIZE_MARGIN = 25;
const unsigned long DEFAULT_BUCKET_SIZE = 240;
const unsigned long HASHMAP_BLOCK_SIZE = 8;
const unsigned long HASHMAP_REBUCKET_GROWTH_FACTOR = 8;


struct Hashmap{
    int file;
    uint64_t bucket_size;
    uint64_t len;
    char *path;
    char *temp_path;
};

typedef struct {
    unsigned char exists;
    uint64_t key;
    uint64_t value;
} Cell;
typedef struct {
    unsigned char exists;
    uint64_t key;
    uint64_t value;
    uint64_t ptr;
} CellPtr;
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
ExecutionProduct hashmap_draw_defaults(FILE *file,uint64_t bucket_size){
    unsigned char *buffer = (unsigned char *)malloc(bucket_size*sizeof(Cell));
    int success = fwrite(buffer, sizeof(unsigned char), bucket_size*sizeof(Cell), file);
    if (success == -1){
        free(buffer);
        return -3;
    }
    free(buffer);

    HashmapMetadata meta = (HashmapMetadata){bucket_size,0};
    unsigned char *meta_buffer = (unsigned char *)malloc(sizeof(HashmapMetadata));
    serialize_hashmapmetadata(&meta, meta_buffer);
    fseek(file, bucket_size*sizeof(Cell), 0);
    success = fwrite(meta_buffer, sizeof(unsigned char), sizeof(HashmapMetadata), file);

    if (success == -1){
        free(meta_buffer);
        return -3;
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
    uint8_t *exists;
};


/// Hashmap_new
///     Creates a new file and a hashmap structure
/// Errors
///     0 -> Success
///     -1 -> failed to open file
///
int hashmap_new(struct Hashmap *hashmap){
    char *path = hashmap->path;
    FILE *existence = fopen(path,"r");
    int exists = -1;
    FILE *file;
    if (exists == -1) {
        file = fopen(path, "w+b");
        if (!file) return -1;
        if (hashmap_draw_defaults(file, DEFAULT_BUCKET_SIZE) < 0) {
            fclose(file);
        return -3;
        }
        hashmap->bucket_size = DEFAULT_BUCKET_SIZE;
        hashmap->len = 0;
    } else {
        fclose(existence);
        file = fopen(path, "r+b");
        if (!file) return -1;
        fseek(file, -sizeof(HashmapMetadata), SEEK_END);
        unsigned char *buffer = malloc(sizeof(HashmapMetadata));
        if (!buffer) {
            fclose(file);
            return -2;
        }
        fread(buffer, sizeof(unsigned char), sizeof(HashmapMetadata), file);
        HashmapMetadata meta = deserialize_hashmapmetadata(buffer);
        free(buffer);
        fseek(file, 0, SEEK_SET);
        hashmap->bucket_size = meta.bucket_size;
        hashmap->len = meta.len;
    }
    hashmap->file = fileno(file);
    return 0; 
}

typedef struct{
    uint64_t count;
    uint64_t capacity;
    uint64_t *values;
} vector;

ExecutionProduct new_vector(vector *value){
    value->values = (uint64_t*)malloc(10*sizeof(uint64_t));
    value->capacity = 10;
    if (value->values == NULL){
        return -2;
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
            return -2;
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

ExecutionProduct new_vector_cell(vector_cell *value){
    value->values = (Cell*)malloc(2*sizeof(Cell));
    value->capacity = 2;
    if (value->values == NULL){
        return -2;
    }
    value->capacity = 2;
    value->count = 0;
    return 0;
}
ExecutionProduct push_vector_cell(vector_cell *value,Cell val){
    if (value->count < value->capacity){
        value->values[value->count] = val;
        value->count++;
    }else{
        void *temp = realloc(value->values, value->capacity*sizeof(Cell)*2);
        if (temp == NULL){
            return -2;
        }else{
            value->values=temp;
            value->capacity*=2;
            return push_vector_cell(value, val);
        }
    }
    return 0;
}


typedef struct{
    uint64_t count;
    uint64_t capacity;
    CellPtr *values;
} vector_cellptr;

ExecutionProduct new_vector_cellptr(vector_cellptr *value){
    value->values = (CellPtr*)malloc(2*sizeof(CellPtr));
    value->capacity = 2;
    if (value->values == NULL){
        return -2;
    }
    value->capacity = 2;
    value->count = 0;
    return 0;
}
ExecutionProduct push_vector_cellptr(vector_cellptr *value,CellPtr val){
    if (value->count < value->capacity){
        value->values[value->count] = val;
        value->count++;
    }else{
        void *temp = realloc(value->values, value->capacity*sizeof(CellPtr)*2);
        if (temp == NULL){
            return -2;
        }else{
            value->values=temp;
            value->capacity*=2;
            return push_vector_cellptr(value, val);
        }
    }
    return 0;
}
ExecutionProduct remove_cell(vector_cellptr *value, uint64_t index) { 
    if (value == NULL || value->count == 0) {
        return -2;
    }
    
    if (index >= value->count) {
        return -1;
    }
    
    for (uint64_t i = index; i < value->count - 1; i++) {
        value->values[i] = value->values[i + 1];
    }
    
    value->count--;
    
    return 0;
}


typedef uint64_t u64;
typedef size_t usize;

ExecutionProduct hashmap_get(struct Hashmap *self, struct GetInput *entry, struct GetOutput *foreign_output){
    ExecutionProduct product = -1;
    int initialized_ring = -1;
    struct GetOutput output = (struct GetOutput){0,0,(OptionUINT64*)malloc(entry->count * sizeof(OptionUINT64))};
    vector blocks = {0};
    U64Hashmap hm = new_u64hashmap();
    struct io_uring ring;
    unsigned char **buffers = NULL;
    vector_cell existing_cells = {0};
    
    

    if(output.value == NULL) return -2; 
    
    clean:
        if(blocks.values != NULL){
            free(blocks.values);
        }
        if(hm.array != NULL){
            destroy_u64hashmap(&hm);
        }
        if(initialized_ring == 1){
            io_uring_queue_exit(&ring);
        }
        if(buffers != NULL){
            for (u64 j = 0; j < blocks.count; j++){
                if(buffers[j] != NULL) free(buffers[j]);
            }
            free(buffers);
        }
        if(existing_cells.values != NULL && existing_cells.values != NULL){
            free(existing_cells.values);
        }
        if(product != 0) {
            free(output.value);
        }
        return product;

    
    
    if (new_vector(&blocks)<0){ 
        product = -2;
        goto clean;
    }
    
    for (uint64_t i=0;i<entry->count;i++){
        u64 a = entry->key[i]%(self->bucket_size/HASHMAP_BLOCK_SIZE);
        insert_u64hashmap(&hm, a,a);
    }
    for (u64 i =0 ;i<hm.bucket_size;i++){
        if(hm.array[i].exist){ 
            if (push_vector(&blocks, hm.array[i].value) < 0){
                product = -2;
                goto clean;
            };
        };
    }


    buffers = malloc(blocks.count * sizeof(unsigned char*));
    if(buffers){
        for (u64 i = 0; i < blocks.count; i++) {
            buffers[i] = NULL;
        }
    }else{
        product = -2;
        goto clean;
    }


    
    if (io_uring_queue_init(blocks.count, &ring, 0) < 0){
        product = -6; 
        goto clean;
    }
    initialized_ring = 1;
    
    for (uint64_t i = 0; i < blocks.count; i++) {
        buffers[i] = malloc(sizeof(Cell) * HASHMAP_BLOCK_SIZE);
        
        if (!buffers[i]) {
            product = -2;
            goto clean;
        }
        
    }

    
    for (uint64_t i = 0; i < blocks.count; i++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, self->file, buffers[i], sizeof(Cell)*HASHMAP_BLOCK_SIZE, blocks.values[i]*(sizeof(Cell)*HASHMAP_BLOCK_SIZE));           
    }
    if (io_uring_submit(&ring) < 0){
        product = -6;
        goto clean;
    }
    for(usize i = 0; i < blocks.count; i++){
        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&ring, &cqe) < 0){
            product = -10;
            goto clean;
        };
        io_uring_cqe_seen(&ring, cqe);
        if (cqe->res < 0) {
            product = -10; 
            goto clean;
        }
    }

    
    if(new_vector_cell(&existing_cells) < 0){
        product = -2;
        goto clean;
    };
    for (uint64_t i = 0; i < blocks.count; i++){
        for (uint64_t j = 0; j < HASHMAP_BLOCK_SIZE; j++){
            Cell v = deserialize_cell(&buffers[i][j*sizeof(Cell)]);
            if(v.exists){
                if(push_vector_cell(&existing_cells, v) < 0){
                    product = -2;
                    goto clean;
                }    
            }
        }
    }

    for (uint64_t i = 0; i < entry->count; i++){
        OptionUINT64 a;
        a.some = -1;
        for (u64 j = 0; j < existing_cells.count; j++){
            Cell v = existing_cells.values[j];
            if(v.exists && v.key == entry->key[i]){
                a.some = 1;
                a.value = v.value;
                break;
            }
        }
        output.value[i] = a;
    }

    *foreign_output = output;
    product = 0;
    goto clean;
}
typedef struct {
    u64 *indices;
    u64 count;
    u64 capacity;
} customer_list;
ExecutionProduct hashmap_rebucket(struct Hashmap *self, struct WriteInput *remaining_entries);
ExecutionProduct init_customer_list(customer_list *list, u64 initial_count) {
    list->indices = malloc(sizeof(u64) * initial_count);
    if (!list->indices) return -2;
    
    list->capacity = initial_count;
    list->count = initial_count;
    
    // Initialize with all customer indices
    for (u64 i = 0; i < initial_count; i++) {
        list->indices[i] = i;
    }
    return 0;
}

void remove_customer(customer_list *list, u64 position) {
    // Shift remaining customers left
    for (u64 i = position; i < list->count - 1; i++) {
        list->indices[i] = list->indices[i + 1];
    }
    list->count--;
}

typedef struct{
    usize count;
    u64 ptr;
    vector_cell cells;
} hmchunk;

ExecutionProduct hashmap_write(struct Hashmap *self, struct WriteInput *entry){

//
//
//  This specific part of the codebase is very long and might be considered bad in coding readability and maintainability scores,
//  the objective of keeping this as/is instead of creating a helper functions is to allow me to benefit from the different data 
//  flows of both functions, since they use the readen data in different ways.
//  
//  I undestand that this is a bad practice, functions shouldn't be long; But at least on the moment in time I am writing this note,
//  that decision is reasonable.
//
//  Best regards, ttd3v.
//
//*
//*   ____                        __                          
//*  /\  _`\                     /\ \  __                     
//*  \ \ \L\ \     __     __     \_\ \/\_\    ___      __     
//*   \ \ ,  /   /'__`\ /'__`\   /'_` \/\ \ /' _ `\  /'_ `\
//*    \ \ \\ \ /\  __//\ \L\.\_/\ \L\ \ \ \/\ \/\ \/\ \L\ \
//*     \ \_\ \_\ \____\ \__/.\_\ \___,_\ \_\ \_\ \_\ \____ \
//*      \/_/\/ /\/____/\/__/\/_/\/__,_ /\/_/\/_/\/_/\/___L\ \
//*                                                    /\____/
//*                                                    \_/__/ 
//*
//
//
    vector blocks;
    if (new_vector(&blocks)<0) return -2;
    U64Hashmap hm = new_u64hashmap();
    for (uint64_t i=0;i<entry->count;i++){
        u64 a = entry->key[i]%(self->bucket_size/HASHMAP_BLOCK_SIZE);
        insert_u64hashmap(&hm, a,a);
    }
    for (u64 i =0 ;i<hm.bucket_size;i++){
        if(hm.array[i].exist){ if (push_vector(&blocks, hm.array[i].value) == -1){ free(blocks.values);destroy_u64hashmap(&hm);return -1; }; };
    }

    struct io_uring ring;
    if (io_uring_queue_init(blocks.count, &ring, 0) < 0) free(blocks.values);destroy_u64hashmap(&hm);return -1;

    unsigned char **buffers = malloc(blocks.count * sizeof(unsigned char*));
    if (!buffers) free(blocks.values);destroy_u64hashmap(&hm);io_uring_queue_exit(&ring);free(buffers) ;return -2;
    for (u64 i = 0; i < blocks.count; i++){
        buffers[i] = malloc(sizeof(Cell)*HASHMAP_BLOCK_SIZE);
        if (!buffers[i]){
            for (u64 j = 0; j < i; j++){
                if (buffers[j]) free(buffers[j]);    
            }
            free(buffers);
            free(blocks.values);
            destroy_u64hashmap(&hm);
            io_uring_queue_exit(&ring);
            free(buffers);
            return -2;
        }
    }

    for (uint64_t i = 0; i < blocks.count; i++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, self->file, &buffers[i], sizeof(Cell)*HASHMAP_BLOCK_SIZE, blocks.values[i]*(sizeof(Cell)*HASHMAP_BLOCK_SIZE));           
    }
    if (io_uring_submit(&ring) == -1) free(blocks.values);destroy_u64hashmap(&hm);free(buffers);io_uring_queue_exit(&ring); return -13;
    for (usize _i = 0; _i<blocks.count;_i++){
        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&ring, &cqe) == -1) free(blocks.values);destroy_u64hashmap(&hm);free(buffers);io_uring_queue_exit(&ring);return -6;
        io_uring_cqe_seen(&ring, cqe);
        if (cqe->res < 0) free(blocks.values);destroy_u64hashmap(&hm);free(buffers);io_uring_queue_exit(&ring);return -6;
    }


    vector_cellptr cells;
    if (new_vector_cellptr(&cells) == -1) free(blocks.values);destroy_u64hashmap(&hm);free(buffers);io_uring_queue_exit(&ring);return -2;

    for (uint64_t i = 0; i < blocks.count; i++){
        //chunks->ptr = blocks.values[i]*HASHMAP_BLOCK_SIZE*sizeof(Cell);        
        //new_vector_cell(&chunks->cells);
        for (uint64_t j = 0; j < HASHMAP_BLOCK_SIZE*sizeof(Cell); j++){
            Cell v = deserialize_cell(&buffers[i][j*sizeof(Cell)]);
            CellPtr y = {v.exists,v.key,v.value,blocks.values[i]*HASHMAP_BLOCK_SIZE*sizeof(Cell)};
            if(push_vector_cellptr(&cells, y)==-1) free(blocks.values);destroy_u64hashmap(&hm);free(buffers);io_uring_queue_exit(&ring);free(cells.values);return -2;
        }
    }
    free(blocks.values);destroy_u64hashmap(&hm);
    
//
//
//     __      __             __                            
//    /\ \  __/\ \         __/\ \__  __                     
//    \ \ \/\ \ \ \  _ __ /\_\ \ ,_\/\_\    ___      __     
//     \ \ \ \ \ \ \/\`'__\/\ \ \ \/\/\ \ /' _ `\  /'_ `\
//      \ \ \_/ \_\ \ \ \/ \ \ \ \ \_\ \ \/\ \/\ \/\ \L\ \
//       \ `\___x___/\ \_\  \ \_\ \__\\ \_\ \_\ \_\ \____ \
//       '\/__//__/  \/_/   \/_/\/__/ \/_/\/_/\/_/\/___L\ \
//                                                  /\____/
//                                                  \_/__/
//
//


    customer_list *customers = &(customer_list){
        .count = 0,
        .indices = NULL,
        .capacity = 0
    };
    if (init_customer_list(customers, entry->count)<0){
        io_uring_queue_exit(&ring);
        free(cells.values);
        return -2;
    };
    for (u64 i = 0; i < entry->count; i++){customers->indices[i] = i;};

    int rebucket = 0;
    U64Hashmap hmap = new_u64hashmap();
    destroy_u64hashmap(&hm); 

    unsigned char **writing_buffers = malloc(cells.count * sizeof(unsigned char*));
    
    if (!writing_buffers) {
        io_uring_queue_exit(&ring);
        free(cells.values);
        destroy_u64hashmap(&hmap);
        free(customers->indices);
        return -2;
    }
    for (uint64_t i = 0; i < cells.count; i++) {
        writing_buffers[i] = malloc(sizeof(Cell) * HASHMAP_BLOCK_SIZE);
        if (!writing_buffers[i]) {
            io_uring_queue_exit(&ring);
            free(cells.values);
            destroy_u64hashmap(&hmap);
            free(customers->indices);
            for (u64 j = 0; j < i; j++){
                if (writing_buffers[j]) free(writing_buffers[j]);    
            }
            free(buffers);
            return -2;
        }
    }

    for (u64 j = 0; j < cells.count; j++) {
        writing_buffers[j] = (unsigned char*)malloc(sizeof(Cell));
        if (writing_buffers[j] == NULL){
            io_uring_queue_exit(&ring);
            free(cells.values);
            destroy_u64hashmap(&hmap);
            free(customers->indices);
            return -2;
        }
    }

    for(u64 i = customers->count; i > 0; i--){
        u64 k = entry->key[0+customers->count-i];
        u64 v = entry->value[0+customers->count-i];
        uint8_t e = entry->exists[0+customers->count-i]; 
        for(u64 j = 0; j < cells.count; j++){
            if (cells.values[j].exists && cells.values[j].key == k){ 
                serialize_cell(&(Cell){entry->exists[i],k,v}, writing_buffers[j]);
                struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
                if (sqe == NULL) {
                    free(writing_buffers); 
                    io_uring_queue_exit(&ring);
                    free(cells.values);
                    destroy_u64hashmap(&hmap);
                    free(customers->indices);
                    free(writing_buffers);
                    return -2;
                }

                io_uring_prep_write(sqe, self->file, writing_buffers[j], sizeof(Cell), cells.values[j].ptr);
                remove_customer(customers, i);
                remove_cell(&cells, j);
                break;
            }
        }
    }
    for(u64 i = customers->count; i > 0; i--){
        u64 k = entry->key[0+customers->count-i];
        u64 v = entry->value[0+customers->count-i];
        uint8_t e = entry->exists[0+customers->count-i];
        u64 j =0;
        for(j = 0; j < cells.count; j++){
            if (!cells.values[j].exists){ 
                serialize_cell(&(Cell){entry->exists[i],k,v}, writing_buffers[j]);
                struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
                if (sqe == NULL){ 
                    free(writing_buffers);
                    io_uring_queue_exit(&ring);
                    free(cells.values);
                    destroy_u64hashmap(&hmap);
                    free(customers->indices);
                    return -1;
                }

                io_uring_prep_write(sqe, self->file, writing_buffers[j], sizeof(Cell), cells.values[j].ptr);
                remove_customer(customers, j); 
                self->len++;
                remove_cell(&cells, i);
                break;
            }
        }
        if (j == cells.count){
            rebucket = 1; break;
        }
    }

    struct io_uring_sqe *fsync_sqe = io_uring_get_sqe(&ring); 
    if (fsync_sqe == NULL){
        free(writing_buffers);
        io_uring_queue_exit(&ring);
        free(cells.values);
        destroy_u64hashmap(&hmap);
        free(customers->indices);
        return -1;
    }
    io_uring_prep_fsync(fsync_sqe, self->file, 0);

    if (io_uring_submit(&ring) < 0){
        free(writing_buffers);
        io_uring_queue_exit(&ring);
        free(cells.values);
        destroy_u64hashmap(&hmap);
        free(customers->indices);
        return -1;
    };

    for(u64 i = 0; i < entry->count-customers->count; i++){
        struct io_uring_cqe *cqe;
        io_uring_wait_cqe(&ring, &cqe);
        io_uring_cqe_seen(&ring, cqe);
        if (cqe->res < 0){
            free(writing_buffers);
            io_uring_queue_exit(&ring);
            free(cells.values);
            destroy_u64hashmap(&hmap);
            free(customers->indices);
            return -1;
        }
    }

    free(writing_buffers);
    if(rebucket == 1){
        struct WriteInput pr_input;
        pr_input.count = customers->count;
        for(usize i = 0; i < pr_input.count; i++){
            pr_input.value[i] = entry->value[customers->indices[i]];
            pr_input.key[i] = entry->key[customers->indices[i]];
            pr_input.exists[i] = entry->exists[customers->indices[i]];
        }
        if (hashmap_rebucket(self,&pr_input) < 0){
            io_uring_queue_exit(&ring);
            free(cells.values);
            destroy_u64hashmap(&hmap);
            free(customers->indices);
        };
    }
    io_uring_queue_exit(&ring);free(cells.values);destroy_u64hashmap(&hmap);free(customers->indices);
    return 0;
}


const u64 READ_CHUNK = 8192;

u64 max(u64 m, u64 n) {return m>n?m:n;}
u64 min(u64 m, u64 n) {return m<n?m:n;}
u64 clamp(u64 m, u64 n, u64 w){
    if(m >= n && m <= w){
        return m;
    }
    return m < n?n:w;  
}



ExecutionProduct hashmap_rebucket(struct Hashmap *self, struct WriteInput *remaining_entries) {
    FILE* temp = fopen(self->temp_path, "wb");
    if (temp == NULL) return -5;
    
    struct Hashmap temphm = {
        .bucket_size = self->bucket_size * HASHMAP_REBUCKET_GROWTH_FACTOR,
        .path = self->temp_path,
        .file = fileno(temp),
        .len = 0,
        .temp_path = "./.temp"
    };
    hashmap_draw_defaults(temp, self->bucket_size * HASHMAP_REBUCKET_GROWTH_FACTOR);
    
    unsigned char *buffer = NULL;
    FILE *f = fdopen(self->file, "r+b");
    
    if(f == NULL){
        fclose(temp);
        remove(self->temp_path);
        return -1;
    }
    
    u64 processed = 0;
    while(processed < self->bucket_size){
        u64 csize = clamp(self->bucket_size-processed, 1, READ_CHUNK/sizeof(Cell));
        buffer = malloc(csize * sizeof(Cell));
        if(buffer == NULL){
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return -2;
        }
        
        if(fread(buffer, sizeof(Cell), csize, f) != csize) {
            free(buffer);
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return -3;
        }
        
        struct WriteInput a = {
            .exists = malloc(csize),
            .count = 0,  
            .key = malloc(csize * sizeof(u64)),
            .value = malloc(csize * sizeof(u64))
        };
        
        if(!a.exists || !a.key || !a.value) {
            free(buffer);
            free(a.exists);
            free(a.key); 
            free(a.value);
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return -2;
        }
        
        
        u64 valid_count = 0;
        for(u64 i = 0; i < csize; i++){
            Cell ce = *(Cell*)(buffer + i * sizeof(Cell)); 
            if(ce.exists){
                a.key[valid_count] = ce.key;     
                a.value[valid_count] = ce.value;
                a.exists[valid_count] = ce.exists;
                valid_count++;
            }
        }
        a.count = valid_count;  

        ExecutionProduct res = hashmap_write(&temphm, &a);
        
        free(a.exists);
        free(a.key);
        free(a.value);
        free(buffer);
        buffer = NULL;
        
        if (res < 0){
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return res;
        }
        
        processed += csize;
    }
    
    fclose(f);
    fclose(temp);
    
    remove(self->path);
    rename(self->temp_path, self->path);
    
    FILE* fi = fopen(self->path, "r+b");
    if(fi == NULL) return -5;      
    self->file = fileno(fi); 
    self->bucket_size *= HASHMAP_REBUCKET_GROWTH_FACTOR;
    
    if (remaining_entries->count > 0){
        ExecutionProduct a = hashmap_write(self, remaining_entries);
        if (a < 0){return a;};
    }

    return 0;
}
