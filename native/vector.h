#ifndef VECTOR
#define VECTOR
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
typedef struct{
    unsigned char *buffer;
    size_t element_size;
    size_t capacity;
    size_t len;
} vector;

int vec_new(vector *self,size_t element_size);
int vec_wc(vector *self,size_t element_size, size_t capacity);
int vec_push(vector *self, void*input);
void vec_destroy(vector *self);
int vec_remove(vector *self, size_t index);
void vec_clear(vector *self);
#endif
