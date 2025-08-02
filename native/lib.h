#ifndef LIB_H
#define LIB_H

#include <stdint.h>

typedef uint64_t u64;

extern const u64 HASHMAP_CELL_SIZE;

typedef struct {
    uint8_t exist;
    u64 key;
    u64 value;
} hash_cell;

typedef struct {
    u64 bucket_size;
    u64 len;
    hash_cell* array;
} U64Hashmap;


U64Hashmap new_u64hashmap(void);
void insert_u64hashmap(U64Hashmap *self, u64 key, u64 value);
void u64hashmap_rebucket(U64Hashmap *self);
void remove_u64hashmap(U64Hashmap *self, u64 key, u64 value);
hash_cell get_u64hashmap(U64Hashmap *self, u64 key);

#endif
