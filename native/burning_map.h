#include <sys/types.h>
#ifndef BURNING_MAP_HEADERS
#define BURNING_MAP_HEADERS

typedef struct {
    u_int8_t health;
    u_int64_t key;
    u_int64_t pointer;
} Paper;

typedef struct {
    u_int64_t capacity;
    Paper *paper_vector;
} BurningMap;

typedef struct {
    unsigned char Some;
    u_int64_t Value;
} SomeI64;

BurningMap* new_burningmap(u_int64_t KiB);
void add_burningmap(BurningMap* self, u_int64_t key, u_int64_t value);
void deplete_burningmap(BurningMap* self, u_int64_t key);

SomeI64 get_burningmap(BurningMap* self, u_int64_t key);
void destroy_burningmap(BurningMap* self);
#endif
