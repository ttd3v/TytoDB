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
#include <sys/stat.h>  // for struct stat, fstat
#include <string.h>    // for string operations
#include <stdbool.h>
#include <inttypes.h>

const unsigned long BUCKET_SIZE_MARGIN = 5;
const unsigned long DEFAULT_BUCKET_SIZE = 240;
const unsigned long HASHMAP_BLOCK_SIZE = 32;
const unsigned long HASHMAP_REBUCKET_GROWTH_FACTOR = 2;

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
    u_int8_t e = 0;
    u_int8_t FLAGS = 0;
    unsigned char *reading = NULL;
    struct io_uring *ring;
    clean:
        if (e>0){
            if((FLAGS & 64) == 64){free(reading);};
            if((FLAGS & 128) == 128){io_uring_queue_exit(ring);}
            return PRODUCT;
        }
    e++;

    uint64_t offsets[entry->count];
    uint64_t offsets_l = 0;
    { 
        for(size_t i = 0; i < entry->count; i++){
            u_int64_t offset = ((self->bucket_size / HASHMAP_BLOCK_SIZE) % entry->key[i]) * sizeof(Cell) * HASHMAP_BLOCK_SIZE;
            u_int8_t m = 1;
            for (size_t j = 0; j < offsets_l; j++){
                if(offsets[j] == offset){m = 0;}
            }
            if(m == 1){offsets[offsets_l] = offset; offsets_l++;};
        }
    }
    reading = malloc(offsets_l*sizeof(Cell)*HASHMAP_BLOCK_SIZE);
    if(reading){
        FLAGS |= 64;
    }else{
        PRODUCT = -2;
        goto clean;
    }

    
    PRODUCT = io_uring_queue_init(offsets_l,ring,0);
    if (PRODUCT < 0){
        PRODUCT = -6;
        goto clean;
    }else{
        FLAGS |= 128;
    }

    for (size_t i = 0; i < offsets_l; i++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        io_uring_prep_read(sqe, self->file, reading+(i*sizeof(Cell)*HASHMAP_BLOCK_SIZE), sizeof(Cell)*HASHMAP_BLOCK_SIZE, offsets[i]);
    }
    if (io_uring_submit_and_wait(ring, offsets_l) < 0){PRODUCT = -1;goto clean;}


    foreign_output->count = entry->count;
    foreign_output->success = 1;
    foreign_output->value = malloc(sizeof(OptionUINT64)*entry->count);
    if (!foreign_output->value){
        PRODUCT = -2;
        goto clean;
    }
    for(size_t j = 0; j < entry->count; j++){
        OptionUINT64 in;
        in.value = 0;
        in.some = -1;
        for(size_t i = 0; i < offsets_l*HASHMAP_BLOCK_SIZE; i ++){
            Cell c = deserialize_cell(reading + (i*sizeof(Cell)));
            if (c.exists && c.key == entry->key[j]){
                in.value = c.value;
                break;
            }
        }
        foreign_output->value[j] = in;
    }
   
    PRODUCT=0;
    goto clean;
}

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
    uint8_t FLAGS = 0;
    struct io_uring *ring;
    unsigned char *reading_buffer = NULL; 
    unsigned char *writing_buffer = NULL;
    unsigned char *offsets = NULL; 
    int PRODUCT = 0;
    clean:
        if(FLAGS != 0){
            if((0x00 | (FLAGS >> 7)) == 1){io_uring_queue_exit(ring);}
            if(((0x00 | (FLAGS >> 6))&1) == 1){free(offsets);}
            if(((0x00 | (FLAGS >> 5))&1)  == 1){free(reading_buffer);};
            if((0x00 | ((FLAGS >> 4)&1)) == 1){free(writing_buffer);};
            return PRODUCT;
        }
    uint64_t offsets_l = 0;

    {
        u_int64_t blocks[entry->count];
        size_t blocks_length = 0;  
        uint8_t m = 0;
        for(size_t index = 0; index < entry->count; index++){
            m=1;
            size_t chunk = (self->bucket_size/HASHMAP_BLOCK_SIZE) % entry->key[index];
            for(size_t subidx = 0; subidx < blocks_length;subidx++){
                if(blocks[subidx] == chunk){m=0;break;};
            }
            if(m==1){
                blocks[index] = chunk;
                blocks_length++;
            }
        }
        offsets = malloc(blocks_length*sizeof(uint64_t));
        if (!offsets){
            PRODUCT = -2;
            goto clean;
        }else{
            FLAGS ^= (1 << 6);
        }
        for(size_t i = 0; i < blocks_length; i++){
            offsets[i] = blocks[i] * sizeof(Cell) * HASHMAP_BLOCK_SIZE;
        }
        offsets_l = blocks_length;
    }

    if(!reading_buffer){
        PRODUCT = -2;
        goto clean;
    }else{
        FLAGS ^= (1 << 5);
    }


    PRODUCT = io_uring_queue_init(offsets_l, ring, 0);
    if (PRODUCT < 0){
        goto clean; 
    }else{
        FLAGS |= 128;
    }


    for(size_t index = 0; index < offsets_l; index++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if(!sqe) {PRODUCT = -1;goto clean;};
        io_uring_prep_read(sqe, self->file, reading_buffer+(index*sizeof(Cell)*HASHMAP_BLOCK_SIZE), sizeof(Cell)*HASHMAP_BLOCK_SIZE, offsets[index]);
    }

    PRODUCT = io_uring_submit_and_wait(ring, offsets_l);
    if (PRODUCT < 0){
        goto clean;
    }

    io_uring_queue_exit(ring);
    FLAGS ^= 128;

    uint64_t writing_offsets[entry->count];
    size_t writing_length = 0;
    writing_buffer = malloc(sizeof(Cell)*writing_length);
    if(!writing_buffer){PRODUCT=-2; goto clean;}else{FLAGS |= (1 << 4);}
    
    {
        uint8_t g = 0;
        size_t ji = 0;
        for (size_t i = 0; i < offsets_l * HASHMAP_BLOCK_SIZE;i++){
            Cell c = deserialize_cell(reading_buffer+i*sizeof(Cell));
            for(size_t j = ji; j < entry->count;j++){
                g=0;
                if (c.key == entry->key[j] && c.exists){
                    c.value = entry->value[j];
                    c.exists = entry->exists[j];
                    g=1;
                }
                if(!c.exists){
                    c.exists = 1;
                    c.value = *entry->value;
                    c.key = *entry->key;
                }
                if(g){
                    serialize_cell(&c,writing_buffer+writing_length*sizeof(Cell));    
                    writing_offsets[writing_length] = offsets[i]+(i*sizeof(Cell)*HASHMAP_BLOCK_SIZE);
                    ji++;
                    break;
                }
            }
        }
        if(ji < entry->count){
            PRODUCT = hashmap_rebucket(self, entry);
            if(PRODUCT < 0){
                goto clean;
            }
        }
    } 

    PRODUCT = io_uring_queue_init(writing_length, ring, 0);
    if (PRODUCT < 0){
        goto clean;
    }else{
        FLAGS |= 128;
    }

    for (size_t w = 0; w < writing_length; w++){
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) {PRODUCT = -1;goto clean;}
        io_uring_prep_write(sqe, self->file, writing_buffer+w*sizeof(Cell), sizeof(Cell), writing_offsets[w]);
    }

    PRODUCT = io_uring_submit_and_wait(ring, writing_length);
    if(PRODUCT < 0){
        goto clean;
    }
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
    FILE* temp = fopen(self->temp_path, "wb");
    if (temp == NULL) return -5;
    
    char *temp_path;
    asprintf(&temp_path, "%s.temp",self->path);
    struct Hashmap temphm = {
        .bucket_size = self->bucket_size * HASHMAP_REBUCKET_GROWTH_FACTOR,
        .path = self->temp_path,
        .file = fileno(temp),
        .len = 0,
        .temp_path = temp_path
    };
    ExecutionProduct hm_def = hashmap_draw_defaults(temp, self->bucket_size * HASHMAP_REBUCKET_GROWTH_FACTOR);
    if (hm_def < 0){
        printf("rebucket\n");
        return hm_def;
    };
    
    unsigned char *buffer = NULL;
    FILE *f = fdopen(self->file, "r+b");
    
    if(f == NULL){
        printf("failed to open rebucket file\n");
        fclose(temp);
        remove(self->temp_path);
        return -1;
    }
    
    u64 processed = 0;
    buffer = malloc(READ_CHUNK + READ_CHUNK % sizeof(Cell));
    while(processed < self->bucket_size){
        u64 csize = clamp(self->bucket_size-processed, 1, (READ_CHUNK/sizeof(Cell))+1);
        if(buffer == NULL){
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return -2;
        }
        
        if(fread(buffer, sizeof(Cell), csize, f) != csize) {
            
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return -3;
        }
        u_int8_t exists[csize]; 
        u_int64_t key[csize];
        u_int64_t value[csize];
        struct WriteInput a = {
            .exists = exists,
            .count = 0,  
            .key = key,
            .value = value
        };
        
        
        
        
        u64 valid_count = 0;
        for(u64 i = 0; i < csize; i++){
            Cell ce = deserialize_cell(buffer + i * sizeof(Cell));
            if(ce.exists){
                a.key[valid_count] = ce.key;     
                a.value[valid_count] = ce.value;
                a.exists[valid_count] = ce.exists;
                valid_count++;
            }
        }
        a.count = valid_count;  

        ExecutionProduct res = hashmap_write(&temphm, &a);
        
        
        if (res < 0){
            fclose(f);
            fclose(temp);
            remove(self->temp_path);
            return res;
        }
        
        processed += csize;
    }
    free(buffer); 
    fclose(f);
    fclose(temp);
    
    remove(self->path);
    rename(self->temp_path, self->path);
    
    FILE* fi = fopen(self->path, "r+b");
    if(fi == NULL) {return -5;};      
    self->file = fileno(fi); 
    self->bucket_size *= HASHMAP_REBUCKET_GROWTH_FACTOR;
    
    if (remaining_entries->count > 0){
        ExecutionProduct result = hashmap_write(self, remaining_entries);
        if (result < 0) {
            return result;
        }


    }
    return 0;
}



ExecutionProduct hashmap_destroy(struct Hashmap *self){
    if(self->cache) destroy_burningmap(self->cache);
    if(self->file) {
        close(self->file);
    }
    return 0;
}

