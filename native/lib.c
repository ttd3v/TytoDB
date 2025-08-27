#include <stdint.h>
#include <stdlib.h>
#include "lib.h"

typedef uint64_t u64;
const u64 HASHMAP_CELL_SIZE = 4;

U64Hashmap new_u64hashmap(){
    hash_cell *array = malloc(12 * sizeof(hash_cell));
    if (!array) {
        return (U64Hashmap){0, 0, NULL};
    }
    
    for (u64 i = 0; i < 12; i++) {
        array[i] = (hash_cell){0, 0, 0};
    }
    
    return (U64Hashmap){12, 0, array};
}

void destroy_u64hashmap(U64Hashmap *self) {
    if (self && self->array) {
        free(self->array);
        self->array = NULL;
        self->bucket_size = 0;
        self->len = 0;
    }
}

void insert_u64hashmap(U64Hashmap *self, u64 key, u64 value);

void u64hashmap_rebucket(U64Hashmap *self){
    if (!self || !self->array) return;
    
    u64 old_bucket_size = self->bucket_size;
    u64 new_bucket_size = self->bucket_size * 2;
    
    
    hash_cell *new_array = realloc(self->array, new_bucket_size * sizeof(hash_cell));
    if (!new_array) {
        return;
    }
    
    for (u64 i = old_bucket_size; i < new_bucket_size; i++) {
        new_array[i] = (hash_cell){0, 0, 0};
    }
    
    hash_cell *old_array = malloc(old_bucket_size * sizeof(hash_cell));
    if (!old_array) {
        return;
    }
    
    for (u64 i = 0; i < old_bucket_size; i++) {
        old_array[i] = new_array[i];
        new_array[i] = (hash_cell){0, 0, 0}; 
    }
    
    self->array = new_array;
    self->bucket_size = new_bucket_size;
    //u64 old_len = self->len;
    self->len = 0;
    
    
    for (u64 i = 0; i < old_bucket_size; i++){
        hash_cell c = old_array[i];
        if (c.exist) {
            insert_u64hashmap(self, c.key, c.value);
        }
    }
    
    free(old_array);
}

void insert_u64hashmap(U64Hashmap *self, u64 key, u64 value){
    if (!self || !self->array) return;
    
    u64 digestedkey = 0;
    digestedkey = digestedkey ^ key;
    digestedkey = digestedkey << (key % 32); 
    digestedkey = digestedkey << 10;
    digestedkey = digestedkey ^ (digestedkey >> ((key % 32) << 1));
    
    u64 pointer = (digestedkey % (self->bucket_size / HASHMAP_CELL_SIZE)) * HASHMAP_CELL_SIZE;
    
    //int rebucket = 0;
    for (u64 i = 0; i < HASHMAP_CELL_SIZE; i++){
        hash_cell *cell = &self->array[pointer + i];
        
        if(cell->exist){
            if(cell->key == key){
                cell->value = value;
                return;
            }
        } else {
            *cell = (hash_cell){1, key, value};
            self->len++;
            
            
            if((self->len * 10) / self->bucket_size >= 8) {
                u64hashmap_rebucket(self);
            }
            return;
        }
    }
    
        u64hashmap_rebucket(self);
    insert_u64hashmap(self, key, value); }

void remove_u64hashmap(U64Hashmap *self, u64 key){
    if (!self || !self->array) return;
    
    u64 digestedkey = 0;
    digestedkey = digestedkey ^ key;
    digestedkey = digestedkey << (key % 32);
    digestedkey = digestedkey << 10;
    digestedkey = digestedkey ^ (digestedkey >> ((key % 32) << 1));
    
    u64 pointer = (digestedkey % (self->bucket_size / HASHMAP_CELL_SIZE)) * HASHMAP_CELL_SIZE;
    
    for (u64 i = 0; i < HASHMAP_CELL_SIZE; i++){
        hash_cell *cell = &self->array[pointer + i];
        
        if(cell->exist && cell->key == key){
            *cell = (hash_cell){0, 0, 0};
            self->len--;
            return;
        }
    }
}

hash_cell get_u64hashmap(U64Hashmap *self, u64 key){
    if (!self || !self->array) {
        return (hash_cell){0, 0, 0};
    }
    
    u64 digestedkey = 0;
    digestedkey = digestedkey ^ key;
    digestedkey = digestedkey << (key % 32);
    digestedkey = digestedkey << 10;
    digestedkey = digestedkey ^ (digestedkey >> ((key % 32) << 1));
    
    u64 pointer = (digestedkey % (self->bucket_size / HASHMAP_CELL_SIZE)) * HASHMAP_CELL_SIZE;
    
    for (u64 i = 0; i < HASHMAP_CELL_SIZE; i++){
        hash_cell *cell = &self->array[pointer + i];
        
        if(cell->exist && cell->key == key){
            return *cell;
        }
    }
    
    return (hash_cell){0, 0, 0};
}
