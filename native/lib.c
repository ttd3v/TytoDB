#include <stdint.h>
#include "lib.h"

typedef uint64_t u64;

const u64 HASHMAP_CELL_SIZE = 4;

typedef struct {
    uint8_t exist;
    u64 key;
    u64 value;
} hash_cell;

typedef struct{
    u64 bucket_size;
    u64 len;
    hash_cell* array;
} U64Hashmap;

U64Hashmap new_u64hashmap(){
    hash_cell array[12];
    return (
        (U64Hashmap){12,0,array}
    );
}

void insert_u64hashmap(U64Hashmap *self,u64 key,u64 value);

void u64hashmap_rebucket(U64Hashmap *self){
    u64 bs = self->bucket_size*2;
    hash_cell array[bs];
    U64Hashmap tmp = (U64Hashmap){bs,0,array};
    
    for (u64 i = 0; i < self->len; i++){
        hash_cell c = self->array[i];
        if (c.exist) insert_u64hashmap(&tmp,c.key,c.value);
    }

    self->array = array;
    self->bucket_size *= 2;
}
void insert_u64hashmap(U64Hashmap *self,u64 key,u64 value){
    u64 digestedkey = 0;
    digestedkey = digestedkey ^ key;
    digestedkey = digestedkey << key;
    digestedkey = digestedkey << 10;
    digestedkey = digestedkey ^ (digestedkey >> key << 1);

    u64 pointer = (digestedkey % self->len/HASHMAP_CELL_SIZE);
    hash_cell cell[HASHMAP_CELL_SIZE];
    for (u64 i = 0; i<HASHMAP_CELL_SIZE; i++){
        cell[i] = self->array[pointer+i]; 
    } 
    int rebucket = 0;
    for (u64 i = 0; i < HASHMAP_CELL_SIZE; i++){
        if(cell[i].exist){
            if(cell[i].key == key){
                self->array[pointer+i] = (hash_cell){1,key,value};
                return;
            }
        }else{
            self->array[pointer+i] = (hash_cell){1,key,value};
            self->len++;
            if((i*10) / HASHMAP_CELL_SIZE >= 8) rebucket = 1;
            break;
        }
    }
}
void remove_u64hashmap(U64Hashmap *self,u64 key,u64 value){
    u64 digestedkey = 0;
    digestedkey = digestedkey ^ key;
    digestedkey = digestedkey << key;
    digestedkey = digestedkey << 10;
    digestedkey = digestedkey ^ (digestedkey >> key << 1);

    u64 pointer = (digestedkey % self->len/HASHMAP_CELL_SIZE);
    hash_cell cell[HASHMAP_CELL_SIZE];
    for (u64 i = 0; i<HASHMAP_CELL_SIZE; i++){
        cell[i] = self->array[pointer+i]; 
    } 
    int rebucket = 0;
    for (u64 i = 0; i < HASHMAP_CELL_SIZE; i++){
        if(cell[i].exist){
            if(cell[i].key == key){
                self->array[pointer+i] = (hash_cell){0,0,0};
                self->len--;
                return;
            }
        }
    }
}


hash_cell get_u64hashmap(U64Hashmap *self,u64 key){

    u64 digestedkey = 0;
    digestedkey = digestedkey ^ key;
    digestedkey = digestedkey << key;
    digestedkey = digestedkey << 10;
    digestedkey = digestedkey ^ (digestedkey >> key << 1);

    u64 pointer = (digestedkey % self->len/HASHMAP_CELL_SIZE);
    hash_cell cell[HASHMAP_CELL_SIZE];
    for (u64 i = 0; i<HASHMAP_CELL_SIZE; i++){
        cell[i] = self->array[pointer+i]; 
    } 
    int rebucket = 0;
    for (u64 i = 0; i < HASHMAP_CELL_SIZE; i++){
        if(cell[i].exist){
            if(cell[i].key == key){
                return cell[i];
            }
        }
    }
    return (hash_cell){0,0,0};
}
