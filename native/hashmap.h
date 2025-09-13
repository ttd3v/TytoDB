#ifndef HASHMAP_H
#define HASHMAP_H

#include <stdint.h>
#include <liburing.h>


typedef struct {
    int file;
    unsigned char *array_len;
    uint64_t bucket_size;
    uint64_t len;
    char* path;
    char* temp;
} Hashmap;

typedef struct {
    uint64_t hk;
    uint64_t v;
} Cell;

typedef struct {
    void** f;
    size_t l;
} fmem;

typedef struct {
    Cell* value;
    uint8_t length;
    uint64_t index;
} cell_vector;

typedef struct {
    cell_vector* value;
    uint64_t hk;
} cell_fetch;

typedef struct {
    uint64_t* request;
    uint64_t length;
    cell_fetch* results;
    fmem* mem;
} fetch_entry;

typedef struct {
    uint8_t exists;
    uint64_t value;
} SomeU64;

typedef struct {
    uint8_t exists;
    uint64_t value;
    uint64_t spot;
} SomeRawGet;
typedef struct {u_int8_t exists; u_int64_t hk;} __hc__; // hash cell
typedef struct {u_int8_t exists; u_int64_t spot; u_int64_t bid;} __hcb__; // hash cell -> buffer pointer

int hm_new(Hashmap* self, char* path);
int save_metadata(Hashmap* self);
uint64_t soft(uint64_t idx, uint64_t size);
void find_spots(Hashmap* self, uint64_t hk, uint64_t *array);
uint64_t hash64(uint64_t x);
void fast_sort(uint64_t *array, uint64_t length);
void free_array(unsigned char** buffer, uint64_t len);
int fetch_slots(Hashmap* self, fetch_entry* entry);
int raw_get(Hashmap* self, uint64_t *inputs, SomeRawGet *outputs, uint64_t length);
int hm_get(Hashmap* self, uint64_t *inputs, SomeU64 *outputs, uint64_t length);
int hm_write(Hashmap* self, Cell* inputs, uint64_t length);
int hm_rebucket(Hashmap* self);
void free_mem(fmem* mem);

#endif
