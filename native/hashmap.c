#include "lib.h"
#include "hashmap.h"
#include "burning_map.h"

#include <liburing.h>
#include <liburing/io_uring.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include "error.h"
#include <fcntl.h>
#include <sys/stat.h>  
#include <string.h>   
#include <stdbool.h>
#include <inttypes.h>

const unsigned long DEFAULT_BUCKET_SIZE = 1000;
const unsigned long HASHMAP_BLOCK_SIZE = 32;
const unsigned long HASHMAP_REBUCKET_GROWTH_FACTOR = 20;


int datasync(int fd) {
#ifdef _POSIX_SYNCHRONIZED_IO
    return fdatasync(fd); 
#else
    return fsync(fd);    
#endif
}

void serialize_cell(Cell * value, unsigned char* buffer){ 
    buffer[0] = value->exists;
    for(int i = 0; i < 8; i++){
        buffer[1+i] = (value->key >> 8*i & 0xFF);
    }
    for(int i = 0; i < 8; i++){
        buffer[9+i] = (value->value >> 8*i & 0xFF);
    } 
}
Cell deserialize_cell(unsigned char* buffer){ 
    Cell value;
    value.value = 0;
    value.exists = 0;
    value.key = 0;

    value.exists = buffer[0];
    #pragma GCC unroll 8
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

ExecutionProduct hashmap_draw_defaults(FILE *file,uint64_t bucket_size){
    int fd = fileno(file);
    off_t total_size = bucket_size * sizeof(Cell) + sizeof(HashmapMetadata);
    
    if (ftruncate(fd, total_size) != 0) {
        return -3; 
    }


    HashmapMetadata meta = (HashmapMetadata){bucket_size,0};
    unsigned char *meta_buffer = (unsigned char *)malloc(sizeof(HashmapMetadata));
    if(!meta_buffer){
        printf("Failed to allocate buffer for metadata writing");
        return -1;
    }
    serialize_hashmapmetadata(&meta, meta_buffer);
    int success = pwrite(fd,meta_buffer, sizeof(HashmapMetadata), bucket_size*sizeof(Cell));

    if (success < 0) {
        printf("Failed to write metadata into the hashmap file");
        free(meta_buffer);
        return -3;
    }

    free(meta_buffer);
    fflush(file);


    return 0;
}


int hashmap_new(struct Hashmap *hashmap,u_int64_t KiB){
    hashmap->cache = new_burningmap(KiB);
    if(!hashmap->cache){
        return -1;
    }
    char *path = hashmap->path;
    FILE *existence = fopen(path,"r");
    int exists = existence == NULL?-1:0;
    if (existence != NULL) fclose(existence);
    FILE *file;
    if (exists == -1) {
        file = fopen(path, "w+b");
        if (!file) {printf("Failed to open file(w+b)");
            return -1;
        }
        if (hashmap_draw_defaults(file, DEFAULT_BUCKET_SIZE) < 0) {
            fclose(file);
        return -3;
        }
        hashmap->bucket_size = DEFAULT_BUCKET_SIZE;
        hashmap->len = 0;
    } else {
        file = fopen(path, "r+b");
        if (!file) {
            printf("Failed to open file(r+b)");
            return -1;
        }
        unsigned char *buffer = (unsigned char*)malloc(sizeof(HashmapMetadata));
        if (!buffer) {
            printf("Failed to allocate memory for metadata");
            fclose(file);
            return -2;
        }
        int fd = fileno(file);
        struct stat file_stats;
        
       if (fstat(fd, &file_stats) == -1) {
            perror("fstat");
            fclose(file);
            free(buffer);
            return -1;
        } 
        if ((size_t)file_stats.st_size < sizeof(HashmapMetadata)){
            printf("Invalid file size");
            fclose(file);
            free(buffer);
            return -1;
        }
        if(pread(fd,buffer, sizeof(HashmapMetadata), file_stats.st_size-sizeof(HashmapMetadata)) < 0){
            printf("failed to read metadata");
            fclose(file);
            free(buffer);
            return -1;
        };
        HashmapMetadata meta = deserialize_hashmapmetadata(buffer);
        free(buffer);
        fseek(file, 0, SEEK_SET);
        hashmap->bucket_size = meta.bucket_size;
        hashmap->len = meta.len;
    }
    
    hashmap->file = fileno(file);
    return 0; 
}


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
    ExecutionProduct PRODUCT = 0;
    uint8_t FLAGS = 0;
    unsigned char *reading = NULL;
    uint64_t *offsets = NULL;
    struct io_uring alloc_ring;
    struct io_uring *ring = &alloc_ring;

    clean:
        if (FLAGS != 0){
            if((FLAGS & 64) == 64){free(reading);}
            if((FLAGS & 128) == 128){io_uring_queue_exit(ring);}
            if((FLAGS & 32) == 32){free(offsets);}
            return PRODUCT;
        }

    uint64_t cached_results[entry->count];
    size_t cached_results_length = 0;
    offsets = (uint64_t*)malloc(entry->count * sizeof(uint64_t));
    if(!offsets){
        PRODUCT = -2;
        goto clean;
    } else {
        FLAGS |= 32;
    }

    uint64_t offsets_l = 0;
    {
        uint64_t blocks[entry->count];
        size_t blocks_length = 0;
        
        for(size_t i = 0; i < entry->count; i++){
            SomeI64 p = get_burningmap(self->cache, entry->key[i]);
            if(p.Some){
                cached_results[cached_results_length] = p.Some;
                cached_results_length++;
            }

            uint64_t chunk = (self->bucket_size / HASHMAP_BLOCK_SIZE) % entry->key[i];
            uint64_t offset = chunk * sizeof(Cell) * HASHMAP_BLOCK_SIZE;
            
            
            uint8_t found = 0;
            for (size_t j = 0; j < blocks_length; j++){
                if(blocks[j] == chunk){
                    found = 1;
                    break;
                }
            }
            
            if(!found){
                blocks[blocks_length] = chunk;
                offsets[blocks_length] = offset;
                blocks_length++;
            }
        }
        offsets_l = blocks_length;
    }

    reading = malloc(offsets_l * sizeof(Cell) * HASHMAP_BLOCK_SIZE);
    if(!reading){
        PRODUCT = -2;
        goto clean;
    } else {
        FLAGS |= 64;
    }

    
    PRODUCT = io_uring_queue_init(offsets_l, ring, 0);
    if (PRODUCT < 0){
        PRODUCT = -6;
        goto clean;
    } else {
        FLAGS |= 128;
    }

    
    for (size_t i = 0; i < offsets_l; i++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            PRODUCT = -7;
            goto clean;
        }
        io_uring_prep_read(sqe, self->file, 
                          reading + (i * sizeof(Cell) * HASHMAP_BLOCK_SIZE), 
                          sizeof(Cell) * HASHMAP_BLOCK_SIZE, 
                          offsets[i]);
    }

    if (io_uring_submit_and_wait(ring, offsets_l) < 0){
        PRODUCT = -1;
        goto clean;
    }

    
    foreign_output->count = entry->count;
    foreign_output->success = 1;
    foreign_output->value = malloc(sizeof(OptionUINT64) * entry->count);
    if (!foreign_output->value){
        PRODUCT = -2;
        goto clean;
    }

    
    for(size_t j = 0; j < offsets_l; j++){
        OptionUINT64 result;
        result.value = 0;
        result.some = -1; 
        
        
        for(size_t i = 0; i < offsets_l * HASHMAP_BLOCK_SIZE; i++){
            Cell c = deserialize_cell(reading + (i * sizeof(Cell)));
            if (c.exists && c.key == entry->key[j]){
                result.value = c.value;
                result.some = 1;
                add_burningmap(self->cache, c.key, c.value);
                break;
            }
        }
        
        foreign_output->value[j] = result;
    }
    for(size_t i = 0; i<cached_results_length;i++){
        OptionUINT64 r;
        r.value = cached_results[i];
        r.some = 1;
        foreign_output->value[offsets_l+i] = r;        
    }

    PRODUCT = 0;
    goto clean;
}


ExecutionProduct hashmap_rebucket(struct Hashmap *self, struct WriteInput *remaining_entries);
ExecutionProduct init_customer_list(customer_list *list, u64 initial_count) {
    list->indices = malloc(sizeof(u64) * initial_count);
    if (!list->indices) return -2;
    
    list->capacity = initial_count;
    list->count = initial_count;
    
    for (u64 i = 0; i < initial_count; i++) {
        list->indices[i] = i;
    }
    return 0;
}

void remove_customer(customer_list *list, u64 position) {
    for (u64 i = position; i < list->count - 1; i++) {
        list->indices[i] = list->indices[i + 1];
    }
    list->count--;
}

ExecutionProduct hashmap_save_metadata(struct Hashmap *self) {
    if (!self) {
        return -1; 
    }
    

    HashmapMetadata meta = {
        .bucket_size = self->bucket_size,
        .len = self->len
    };
    
    
    unsigned char *meta_buffer = malloc(sizeof(HashmapMetadata));
    if (!meta_buffer) {
        return -2; 
    }
    
    serialize_hashmapmetadata(&meta, meta_buffer);
    
    off_t metadata_offset = self->bucket_size * sizeof(Cell);
    ssize_t bytes_written = pwrite(self->file, meta_buffer, sizeof(HashmapMetadata), metadata_offset);
    free(meta_buffer);
    if (bytes_written < 0) {
        return -3; 
    }
    
    if ((size_t)bytes_written != sizeof(HashmapMetadata)) {
        return -4;
    }
    

    if (datasync(self->file) < 0) {
        return -5; 
    }
    
    return 0; 
}


void dump(unsigned char* a, unsigned char* b, size_t size){
    for(size_t c = 0; c < size; c++){
        a[c] = b[c];
    }
}

ExecutionProduct hashmap_write(struct Hashmap *self, struct WriteInput *entry){
    printf("== hashmap_write\n");
    struct io_uring alloc_ring;
    uint8_t FLAGS = 0;
    struct io_uring *ring = &alloc_ring;
    unsigned char *reading_buffer = NULL;
    unsigned char *writing_buffer = NULL;
    uint64_t *offsets = NULL;
    uint64_t *writing_offsets = NULL;
    uint64_t writing_length = 0; 
    size_t step = 0;
    printf("i %zu\n",step++); 
    int PRODUCT = 0;
    clean:
        if(FLAGS != 0){
            if((FLAGS & 128) == 128){io_uring_queue_exit(ring);}
            if(offsets != NULL){free(offsets);}
            if(reading_buffer != NULL){free(reading_buffer);}
            if(writing_buffer != NULL && (FLAGS & 32) == 32){free(writing_buffer);}
            if((FLAGS & 16) == 16){free(writing_offsets);}
            return PRODUCT;
        }
    printf("i %zu\n",step++);
    uint64_t offsets_l = 0;

    
    {
        uint64_t blocks[entry->count];
        size_t blocks_length = 0;
        uint8_t m = 0;
        for(size_t index = 0; index < entry->count; index++){
            m = 1;
            size_t chunk = (self->bucket_size/HASHMAP_BLOCK_SIZE) % entry->key[index];
            deplete_burningmap(self->cache, entry->key[index]);
            for(size_t subidx = 0; subidx < blocks_length; subidx++){
                if(blocks[subidx] == chunk){m = 0; break;}
            }
            if(m == 1){
                blocks[blocks_length] = chunk; 
                blocks_length++;
            }
        }
        
        offsets = malloc(blocks_length * sizeof(uint64_t));
        if (!offsets){
            PRODUCT = -2;
            goto clean;
        } else {
            FLAGS |= 64;
        }
        
        for(size_t i = 0; i < blocks_length; i++){
            offsets[i] = blocks[i] * sizeof(Cell) * HASHMAP_BLOCK_SIZE;
        }
        offsets_l = blocks_length;
    }
    printf("i %zu\n",step++);
    reading_buffer = malloc(offsets_l * sizeof(Cell) * HASHMAP_BLOCK_SIZE);
    if(!reading_buffer){
        PRODUCT = -2;
        goto clean;
    } else {
        FLAGS |= 32;
    }

    writing_offsets = (uint64_t*)malloc(sizeof(uint64_t) * entry->count);
    if(!writing_offsets){
        PRODUCT = -2;
        goto clean;
    } else {
        FLAGS |= 16;
    }
    printf("i %zu\n",step++);
    
    PRODUCT = io_uring_queue_init(entry->count * 2, ring, 0);
    if (PRODUCT < 0){
        PRODUCT = -6;
        goto clean;
    } else {
        FLAGS |= 128;
    }

    
    for(size_t index = 0; index < offsets_l; index++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if(!sqe) {
            PRODUCT = -1;
            goto clean;
        }
        io_uring_prep_read(sqe, self->file, 
                          reading_buffer + (index * sizeof(Cell) * HASHMAP_BLOCK_SIZE),
                          sizeof(Cell) * HASHMAP_BLOCK_SIZE, 
                          offsets[index]);
    }
    printf("i %zu\n",step++);
    PRODUCT = io_uring_submit_and_wait(ring, offsets_l);
    if (PRODUCT < 0){
        goto clean;
    }

    
    writing_buffer = malloc(sizeof(Cell) * entry->count);
    if(!writing_buffer){
        PRODUCT = -2; 
        goto clean;
    }

    
    uint64_t entries_written = 0;
    bool *entry_processed = calloc(entry->count, sizeof(bool));
    if (!entry_processed) {
        PRODUCT = -2;
        goto clean;
    }
    printf("i %zu\n",step++);
    for (size_t i = 0; i < offsets_l * HASHMAP_BLOCK_SIZE; i++){
        Cell c = deserialize_cell(reading_buffer + (i * sizeof(Cell)));
        
        if (c.exists) {
            
            for(size_t j = 0; j < entry->count; j++){
                if (!entry_processed[j] && c.key == entry->key[j]) {
                    
                    c.value = entry->value[j];
                    c.exists = entry->exists[j];
                    
                    serialize_cell(&c, writing_buffer + writing_length * sizeof(Cell));
                    writing_offsets[writing_length] = offsets[i / HASHMAP_BLOCK_SIZE] + 
                                                     ((i % HASHMAP_BLOCK_SIZE) * sizeof(Cell));
                    writing_length++; 
                    entry_processed[j] = true;
                    break;
                }
            }
        }
    }
    printf("i %zu\n",step++);
    
    for (size_t i = 0; i < offsets_l * HASHMAP_BLOCK_SIZE; i++){
        Cell c = deserialize_cell(reading_buffer + (i * sizeof(Cell)));
        
        if (!c.exists) {
            
            for(size_t j = 0; j < entry->count; j++){
                if (!entry_processed[j]) {
                    
                    c.exists = entry->exists[j];
                    c.value = entry->value[j];
                    c.key = entry->key[j];
                    
                    serialize_cell(&c, writing_buffer + writing_length * sizeof(Cell));
                    writing_offsets[writing_length] = offsets[i / HASHMAP_BLOCK_SIZE] + 
                                                     ((i % HASHMAP_BLOCK_SIZE) * sizeof(Cell));
                    writing_length++; 
                    entry_processed[j] = true;
                    entries_written++;
                    break;
                }
            }
        }
    }
    printf("i %zu\n",step++);
    if(entry_processed != NULL)free(entry_processed);

    
    self->len += entries_written;

    
    if(self->len > (self->bucket_size * 57) / 100){
        
        struct WriteInput remaining = {0};
        remaining.count = 0;
        remaining.key = malloc(sizeof(uint64_t) * entry->count);
        remaining.value = malloc(sizeof(uint64_t) * entry->count);
        remaining.exists = malloc(sizeof(uint8_t) * entry->count);
        
        if (!remaining.key || !remaining.value || !remaining.exists) {
            if (remaining.key != NULL) free(remaining.key);
            if (remaining.value != NULL) free(remaining.value);
            if (remaining.exists != NULL) free(remaining.exists);
            PRODUCT = -2;
            goto clean;
        }
        printf("i %zu\n",step++);
        
        for (size_t j = 0; j < entry->count; j++) {
            bool found = false;
            for (size_t w = 0; w < writing_length; w++) {
                Cell written_cell;
                memcpy(&written_cell, writing_buffer + w * sizeof(Cell), sizeof(Cell));
                if (written_cell.key == entry->key[j]) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                remaining.key[remaining.count] = entry->key[j];
                remaining.value[remaining.count] = entry->value[j];
                remaining.exists[remaining.count] = entry->exists[j];
                remaining.count++;
            }
        }
        printf("iy %zu\n",step++);
        PRODUCT = hashmap_rebucket(self, &remaining);
        if(remaining.key!=NULL){free(remaining.key);};
        if(remaining.value!=NULL){free(remaining.value);};
        if(remaining.exists!=NULL){free(remaining.exists);};
        
        if(PRODUCT < 0){
            goto clean;
        }
    }

    printf("i %zu\n",step++);  
    for (size_t w = 0; w < writing_length; w++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            PRODUCT = -7;
            goto clean;
        }
        io_uring_prep_write(sqe, self->file, 
                           writing_buffer + w * sizeof(Cell), 
                           sizeof(Cell), 
                           writing_offsets[w]);
    }
    printf("i %zu\n",step++);
    if (writing_length > 0) {
        PRODUCT = io_uring_submit_and_wait(ring, writing_length);
        if(PRODUCT < 0){
            PRODUCT = -1;
            goto clean;
        }
    }

    printf("i %zu\n",step++);  
    PRODUCT = hashmap_save_metadata(self);
    if (PRODUCT < 0) {
        goto clean;
    }
    
    for(size_t i = 0; i < entry->count; i++){
        if(entry->exists[i]){
            add_burningmap(self->cache, entry->key[i], entry->value[i]);
        }
    }
    printf("i %zu\n",step++);
    PRODUCT = 0;
    goto clean;
}

const u64 READ_CHUNK = 83886080;

u64 max(u64 m, u64 n) {return m>n?m:n;}
u64 min(u64 m, u64 n) {return m<n?m:n;}
u64 clamp(u64 m, u64 n, u64 w){
    if(m >= n && m <= w){
        return m;
    }
    return m < n?n:w;  
}


ExecutionProduct hashmap_rebucket(struct Hashmap *self, struct WriteInput *remaining_entries) {
    printf("=== hashmap_rebucket\n");
    int FLAGS = 0;                                  
    unsigned char *buffer = NULL;
    FILE* file = fopen(self->temp_path, "wa");
    if (!file){
        printf("failed to open file\n");
        return -1;
    }else{
        FLAGS |= 128;
    }
    printf("step 0\n");
    int PRODUCT = 100000;
    clean:
    printf("goto clean\n");
        if(PRODUCT < 100000){
            if((FLAGS & 128) == 128){fclose(file);}
            if((FLAGS & 64) == 64){free(buffer);}
        }

    printf("step 1\n");
    struct Hashmap map = (struct Hashmap){
        fileno(file),
        self->bucket_size*HASHMAP_REBUCKET_GROWTH_FACTOR,
        0,
        self->temp_path,
        "",
        self->cache
    };
    printf("step 2\n");
    PRODUCT = hashmap_draw_defaults(file, self->bucket_size*HASHMAP_REBUCKET_GROWTH_FACTOR);
    if (PRODUCT < 0){
        goto clean;
    }
    printf("step 3\n");
    
    buffer = malloc(102400*sizeof(Cell));
    if(!buffer){
        PRODUCT = -2;
        goto clean;
    }else{
        FLAGS |= 64;
    }
    printf("step 4\n");
    for(u_int64_t i = 0; i < self->bucket_size;){

        printf("loop step 0\n");
        u_int64_t steps = clamp(self->bucket_size,1,102400);
        int s = pread(self->file, buffer, steps*sizeof(Cell), i*sizeof(Cell));
        if (s<0){
            PRODUCT = -1;
            printf("Failed to pread\n");
            goto clean;
        }
        printf("loop step 1\n");
        struct WriteInput entry;
        entry.key = (u_int64_t*)malloc(sizeof(u_int64_t)*steps);
        entry.value = (u_int64_t*)malloc(sizeof(u_int64_t)*steps);
        entry.exists = (u_int8_t*)malloc(sizeof(u_int8_t)*steps);
        entry.count = 0; 
        if(!entry.key){
            PRODUCT = -2;
            goto clean;
        }
        if(!entry.value){
            if(entry.key != NULL){free(entry.key);};
            PRODUCT = -2;
            goto clean;
        }
        if(!entry.exists){
            if(entry.key != NULL){free(entry.key);};
            if(entry.value != NULL){free(entry.value);};
            PRODUCT = -2;
            goto clean;
        }
        printf("loop step 2\n");

        for (u_int64_t k = 0; k < steps; k++){
            Cell c = deserialize_cell(buffer + k*sizeof(Cell));
            if(c.exists){
                entry.exists[k] = c.exists;
                entry.value[k] = c.value;
                entry.key[k] = c.key;
                entry.count++;
            }
        }
        printf("loop step 3\n");
        printf("rebucket_hashmap_write 0\n");
        PRODUCT = hashmap_write(&map, &entry);
        if (PRODUCT < 0){goto clean;};
        i += steps;
        if(entry.key != NULL){free(entry.key);};
        if(entry.value != NULL){free(entry.value);};
        if(entry.exists != NULL){free(entry.exists);};
    printf("loop step 4\n");
    }

    remove(self->path);
    rename(self->temp_path, self->path);
    self->file = fileno(file);
    self->bucket_size = map.bucket_size;
    printf("%lu %lu\n",map.bucket_size,map.len);
    PRODUCT = 0;
    if(remaining_entries->count > 0){
        PRODUCT = hashmap_write(self, remaining_entries);
    }
    goto clean;
}



ExecutionProduct hashmap_destroy(struct Hashmap *self){
    if(self->cache) destroy_burningmap(self->cache);
    if(self->file) {
        close(self->file);
    }
    return 0;
}

