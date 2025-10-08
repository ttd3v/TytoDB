#include <nmmintrin.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include "hashset.h"

static inline u64 hash(u64 key) {
    return _mm_crc32_u64(0, key);
}

int hashset_new(hashset *self, u64 capacity) {
    *self = (hashset){
            calloc(capacity, sizeof(hashset_cell)),
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
inline int hashset_push(hashset *self, u64 entry){
    size_t tombstone_pos = self->capacity; // Track first tombstone found
    
    for (size_t i = 0; i < self->capacity; i++){
        size_t position = (hash(entry) + i) % self->capacity;
        
        if(self->entries[position].exists == 0){
            // Empty slot - insert here or at tombstone
            if(tombstone_pos < self->capacity){
                // Use tombstone slot instead
                self->entries[tombstone_pos] = (hashset_cell){1, entry};
            } else {
                self->entries[position] = (hashset_cell){1, entry};
            }
            self->length++;
            return 0;
        } else if(self->entries[position].exists == 2){
            // Tombstone - remember it but keep searching
            if(tombstone_pos == self->capacity){
                tombstone_pos = position;
            }
        } else if(self->entries[position].value == entry){
            // Already exists
            return 0;
        }
    }
    
    // If we only found tombstones, use the first one
    if(tombstone_pos < self->capacity){
        self->entries[tombstone_pos] = (hashset_cell){1, entry};
        self->length++;
    }
    
    return 0;
}
int hashset_del(hashset *self, u64 entry){
    for (size_t i = 0; i < self->capacity; i++){
        size_t position = (hash(entry)+i) % self->capacity;
        if(self->entries[position].exists == 0){
            // Empty slot - not found
            return 0;
        }
        if(self->entries[position].exists == 1 && self->entries[position].value == entry){
            self->entries[position].exists = 2; // Mark as tombstone
            self->length--;
            return 0;
        }
        // If exists == 2 (tombstone), continue probing
    }
    return 0;
}

inline int hashset_exists(hashset *self, u64 entry){
    for (size_t i = 0; i < self->capacity; i++){
        size_t position = (hash(entry) + i) % self->capacity;
        if(self->entries[position].exists == 0){
            // Empty slot - not found
            return 0;
        }
        if(self->entries[position].exists == 1 && self->entries[position].value == entry){
            return 1;
        }
        // If exists == 2 (tombstone), continue probing
    }
    return 0;
}

inline void hashset_destroy(hashset *self){
    if(self->entries != NULL){
        free(self->entries);
        self->entries = NULL;
    }
}

inline hashset_cell hashset_pop(hashset *self){
    for (size_t i = 0; i < self->capacity; i++){
        if(self->entries[i].exists == 1){  // Only pop occupied slots
            hashset_cell j = self->entries[i];
            self->entries[i].exists = 2;  // Mark as tombstone
            self->length--;
            return j;
        }
    }
    return (hashset_cell){0, 0};
}
