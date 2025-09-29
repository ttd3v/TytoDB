#include <nmmintrin.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

typedef u_int64_t u64;
typedef struct {u64 exists;u64 value;} cell;
typedef struct {cell* entries; u64 length; u64 capacity;} hashset;


static inline u64 hash(u64 key) {
    return _mm_crc32_u64(0, key);
}

int hashset_new(hashset *self, u64 capacity) {
    *self = (hashset){
            calloc(capacity, sizeof(cell)),
            0,
            capacity
    };
    if(!self->entries){
        errno = ENOMEM;
        return  -1;
    }
    return 0;
}

int hashset_new_wise(hashset *self, u64 max_len_expected){
    return hashset_new(self, max_len_expected * 10);
}

// assumes its not full
inline int hashset_push(hashset *self, u64 entry){
    for (size_t i = 0; i < self->capacity; i++){
        size_t position = (hash(entry) + i) % self->capacity;
        if(!self->entries[position].exists){
            self->entries[position] = (cell){1,entry};
            self->length++;
            return 0;
        }else if (self->entries[position].value == entry){
            return 0;
        }
    }
    return 0;
}
int hashset_del(hashset *self, u64 entry){
    for (size_t i = 0; i < self->capacity; i++){
        size_t position = (hash(entry)+i) % self->capacity;
        if(self->entries[position].exists && self->entries[position].value == entry){
            self->entries[position] = (cell){0,entry};
            self->length--;
            return 0;
        }
    }
    return 0;
}
inline int hashset_exists(hashset *self, u64 entry){
    for (size_t i = 0; i < self->capacity; i++){
        size_t position = (hash(entry) + i) % self->capacity;
        if(self->entries[position].exists && self->entries[position].value == entry){
            return 1;
        }
    }
    return 0;
}
inline void hashset_destroy(hashset *self){
    if(self->entries != NULL){
        free(self->entries);
        self->entries = NULL;
    }
}
inline cell hashset_pop(hashset *self){
    for (size_t i = 0; i < self->capacity; i++){
        if(self->entries[i].exists){
            cell j = self->entries[i];
            self->entries[i].exists = 0;
            self->length--;
            return j;
        }
    }
    return (cell){0};
}
