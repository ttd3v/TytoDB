
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include "vector.h"

inline int vec_new(vector *self,size_t element_size){
    self->buffer = calloc(10, element_size);
    if(!self->buffer){
        errno = ENOMEM;
        return -1;
    }
    self->len = 0;
    self->element_size = element_size;
    self->capacity = 10;
    return 0;
}
inline int vec_push(vector *self, void *input){
    if(self->capacity >= self->len+1){
        memcpy(self->buffer+(self->len*self->element_size), input, self->element_size);
        self->len++;
        return 0;
    } 
    unsigned char *nb = malloc((self->capacity*self->element_size)*2);
    if(!nb){
        errno = ENOMEM;
        return -1;
    }
    memcpy(nb, self->buffer, self->len*self->element_size);
    free(self->buffer);
    self->buffer = nb;
    self->capacity *= 2;
    return vec_push(self, input);
}
inline void vec_destroy(vector *self){
    free(self->buffer);
    self->capacity = 0;
    self->len = 0;
    self->element_size = 0;
}
inline int vec_remove(vector *self, size_t index){
    if(index >= self->len){
        errno = EINVAL;
        return -1;
    }
    if(index == self->len - 1){
        self->len--;
        return 0;
    }
    unsigned char *remove_ptr = self->buffer + (index * self->element_size);
    unsigned char *last_ptr = self->buffer + ((self->len - 1) * self->element_size);
    memcpy(remove_ptr, last_ptr, self->element_size);
    self->len--;
    
    return 0;
}
inline void vec_clear(vector *self){
    self->len = 0;
}

inline int vec_contain(vector *self, unsigned char* cmp){
    for (size_t i = 0; i < self->len; i++){
        size_t pointer = i * self->element_size; 
        if (memcmp(self->buffer + i * self->element_size, cmp, self->element_size) == 0){
            return 1;
        }
 
    }
    return 0;
}
