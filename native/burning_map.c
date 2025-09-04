#include "burning_map.h"
#include "hashmap.h"
#include <stdlib.h>
#include <sys/types.h>

const u_int64_t MAX_KIB = 1073741824;
const u_int8_t MAX_HEALTH = 250;
BurningMap* new_burningmap(u_int64_t KiB){
   u_int64_t capacity = clamp(KiB, 1, MAX_KIB);
   Paper* paper_vector = (Paper*)malloc((capacity*1024)- (capacity*1024)%sizeof(Paper));
   if (!paper_vector) return NULL;
   BurningMap* geral_structure = (BurningMap*)malloc(sizeof(BurningMap));
   if(!geral_structure) {free(paper_vector);return NULL;};
   
   geral_structure->paper_vector = paper_vector;
   geral_structure->capacity = (capacity*1024)/sizeof(Paper);
   for(u_int64_t i = 0;i<geral_structure->capacity;i++) geral_structure->paper_vector[i] = (Paper){0,0,0};
   return geral_structure;
}
static inline uint64_t hash(uint64_t x) {
    x += 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb;
    return x ^ (x >> 31);
}

void add_burningmap(BurningMap *self, u_int64_t key, u_int64_t value){
    Paper* p = &self->paper_vector[hash(key)%self->capacity];
    if (p->key == key) {
        if(p->health < MAX_HEALTH){
            p->health++;
        };
        return;
    };
    if (p->health == 0) {
        if(p->health < MAX_HEALTH){
            p->health++;
        }
        p->key = key;
        p->pointer = value;
        return;
    };
    if(p->key != key && p->health>0){p->health--; return;}
}
void deplete_burningmap(BurningMap *self, u_int64_t key){
    Paper* p = &self->paper_vector[hash(key)%self->capacity];
    p->health = 0;
    p->pointer = 0;
    p->key = 0;
}
SomeI64 get_burningmap(BurningMap *self, u_int64_t key){
    Paper* p = &self->paper_vector[hash(key)%self->capacity];
    if(p->health>0 && p->key == key){
        if (p->health < MAX_HEALTH){
            p->health++;
        }
        return (SomeI64){1,p->pointer};
    }else if(p->health>0 && p->key != key){
        p->health--;
    }
    return (SomeI64){0};
}

void destroy_burningmap(BurningMap *self){
    if(self->paper_vector) free(self->paper_vector);
    if(self) free(self);
}
