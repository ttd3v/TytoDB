#include <nmmintrin.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

typedef u_int64_t u64;
typedef struct {u64 exists;u64 value;} hashset_cell;
typedef struct {hashset_cell* entries; u64 length; u64 capacity;} hashset;

int hashset_new(hashset *self, u64 capacity);
int hashset_new_wise(hashset *self, u64 max_len_expected);
int hashset_push(hashset *self, u64 entry);
int hashset_del(hashset *self, u64 entry);
int hashset_exists(hashset *self, u64 entry);
void hashset_destroy(hashset *self);
hashset_cell hashset_pop(hashset *self);
