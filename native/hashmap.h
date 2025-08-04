#ifndef HASHMAP_H
#define HASHMAP_H

#include "lib.h"
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include "error.h"

// Constants
extern const unsigned long BUCKET_SIZE_MARGIN;
extern const unsigned long DEFAULT_BUCKET_SIZE;
extern const unsigned long HASHMAP_BLOCK_SIZE;
extern const unsigned long HASHMAP_REBUCKET_GROWTH_FACTOR;

// Type Definitions
typedef uint64_t u64;
typedef size_t usize;

// Structures
struct Hashmap {
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

typedef struct {
    uint64_t bucket_size;
    uint64_t len;
} HashmapMetadata;

typedef struct {
    int8_t some;
    uint64_t value;
} OptionUINT64;

struct GetInput {
    uint32_t count;
    uint64_t *key;
};

struct GetOutput {
    int8_t success;
    uint32_t count;
    OptionUINT64 *value;
};

struct WriteInput {
    uint32_t count;
    uint64_t *key;
    uint64_t *value;
    uint8_t *exists;
};

typedef struct {
    uint64_t count;
    uint64_t capacity;
    uint64_t *values;
} vector;

typedef struct {
    uint64_t count;
    uint64_t capacity;
    Cell *values;
} vector_cell;

typedef struct {
    uint64_t count;
    uint64_t capacity;
    CellPtr *values;
} vector_cellptr;

typedef struct {
    u64 *indices;
    u64 count;
    u64 capacity;
} customer_list;

typedef struct {
    usize count;
    u64 ptr;
    vector_cell cells;
} hmchunk;

// Function Prototypes

// Serialization/Deserialization
void serialize_cell(Cell *const value, unsigned char *buffer);
Cell deserialize_cell(unsigned char *const buffer);
void serialize_hashmapmetadata(HashmapMetadata *const value, unsigned char *buffer);
HashmapMetadata deserialize_hashmapmetadata(unsigned char *const buffer);

// Hashmap Management
ExecutionProduct hashmap_draw_defaults(FILE *file, uint64_t bucket_size);
int hashmap_new(struct Hashmap *hashmap);
ExecutionProduct hashmap_get(struct Hashmap *self, struct GetInput *entry, struct GetOutput *foreign_output);
ExecutionProduct hashmap_write(struct Hashmap *self, struct WriteInput *entry);
ExecutionProduct hashmap_rebucket(struct Hashmap *self, struct WriteInput *remaining_entries);

// Vector Management
ExecutionProduct new_vector(vector *value);
int push_vector(vector *value, uint64_t val);
void sort_vector(vector *value);

ExecutionProduct new_vector_cell(vector_cell *value);
ExecutionProduct push_vector_cell(vector_cell *value, Cell val);

ExecutionProduct new_vector_cellptr(vector_cellptr *value);
ExecutionProduct push_vector_cellptr(vector_cellptr *value, CellPtr val);
ExecutionProduct remove_cell(vector_cellptr *value, uint64_t index);

// Customer List Management
ExecutionProduct init_customer_list(customer_list *list, u64 initial_count);
void remove_customer(customer_list *list, u64 position);

// Utility
void quicksort(uint64_t *arr, int low, int high);
u64 max(u64 m, u64 n);
u64 min(u64 m, u64 n);
u64 clamp(u64 m, u64 n, u64 w);

#endif
