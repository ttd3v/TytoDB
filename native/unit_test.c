#include "hashset.h"
#include "vector.h"
#include "btree.h"
#include <asm-generic/errno-base.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#define count 5000

typedef u64 u_int64_t;

int test_hashset(){
    hashset value;
    int product = 0;
    value.entries = NULL;

    clean:
        if(value.entries != NULL) {
            hashset_destroy(&value);
            return product;
        }
    
    if (hashset_new_wise(&value, count) < 0){
        printf("ERROR: Failed to create hashset\n");
        return -1;
    };
    
    for (u64 v = 0; v < count; v++) hashset_push(&value, v);

    if (value.length < count){
        printf("ERROR: Failed to push into hashset\n");
        product = -2;
        goto clean;
    }

    for(u64 v = 0; v < count/2; v++){
        if(hashset_exists(&value, v) != 1){
            printf("ERROR: Failed to check the existence of values\n");
            product = -3;
            goto clean;
        };
    }

    for (u64 v = 0; v < count/2; v++) {
        hashset_del(&value,v);
    }

    u64 expected_remaining = count - count/2;
    if(value.length != expected_remaining){
        printf("ERROR: Failed to delete values (length=%lu, expected=%lu)\n", 
               value.length, expected_remaining);
        product = -4;
        goto clean;
    }

    for (u64 v = 0; v < count/2; v++) {
        hashset_cell hashset_value = hashset_pop(&value);
        if(!hashset_value.exists){
            printf("ERROR: Failed to pop a value from the hashset\n");
            product = -5;
            goto clean;
        }
    }

    product = 0;
    goto clean;
}

int test_vector(){
    vector vec;
    vec.buffer = NULL;
    int product = 0;
    
    clean:
        if(vec.buffer != NULL){
            vec_destroy(&vec);
            return product;
        }

    if (vec_wc(&vec, 8,count) < 0){
        product = -1;
        printf("ERROR: Failed to create a \"vec\" structure\n");
        goto clean;
    }

   for(u64 i = 0; i < count; i++){
        unsigned char val[8];
        for (u64 y = 0; y < 8; y++) val[y] = i*(i*y);
        if (vec_push(&vec, val) < 0){
            product = -2;
            printf("ERROR: Failed to push values into \"vec\"\n");
            goto clean;
        };
   }

   if (vec.len != count){
        product = -3;
        printf("ERROR: Odd \"vec\" length, there are some values missing.\n");
        goto clean;
   }

   if (vec_remove(&vec, count / 2) < 0){
       product = -4;
       printf("ERROR: Failed to remove value from \"vec\"\n");
       goto clean;
   }

   if (vec.len != count -1){
       product = -5;
       printf("ERROR: Odd \"vec\" length, the length isn\'t the expected one\n");
       goto clean;
   }

   vec_clear(&vec);
   if (vec.len != 0){
        product = -7;
        printf("ERROR: Odd \"vec\" length, should be zero\n");
        goto clean;
   }

    product = 0;
    goto clean;
}

// Simple hash function (FNV-1a)
static inline u64 hash_key(u64 key) {
    u64 hash = 14695981039346656037ULL;
    unsigned char *bytes = (unsigned char *)&key;
    for (int i = 0; i < 8; i++) {
        hash ^= bytes[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

int test_btree() {
    int product = 0;
    char template[64];
    
    // Create unique temp file name
    srand(time(NULL) ^ getpid());
    snprintf(template, sizeof(template), "/tmp/btree_test_%d_%u", 
             getpid(), (unsigned)rand());
    
    BTree inst = {0};
    if (init(&inst, template) < 0) {
        printf("ERROR: Failed to init BTree\n");
        unlink(template);
        return -2;
    }
    Request req[count] = {0};
    
    // Write with hashed keys
    for (u64 i = 0; i < count; i++) {
        u64 hashed_key = hash_key(i);
        req[i] = (Request){RQ_WRITE, hashed_key, i * 2};
    }
    int a = bt_request(&inst, req, count);
    if (a < 0) {
        printf("ERROR: Failed to write to BTree %i\n",a);
        product = -3;
        goto clean;
    }
    if (inst.length != count) {
        printf("ERROR: BTree length mismatch after write\n");
        product = -4;
        goto clean;
    }
    
    // Read with hashed keys
    for (u64 i = 0; i < count; i++) {
        u64 hashed_key = hash_key(i);
        req[i] = (Request){RQ_READ, hashed_key, UINT64_MAX};
    }
    if (bt_request(&inst, req, count) < 0) {
        printf("ERROR: Failed to read from BTree\n");
        product = -5;
        goto clean;
    }
    for (u64 i = 0; i < count / 2; i++) {
        if (req[i].value != i * 2) {
            printf("ERROR: BTree read value mismatch\n");
            product = -6;
            goto clean;
        }
    }
    
    // Delete with hashed keys
    for (u64 i = 0; i < count / 2; i++) {
        u64 hashed_key = hash_key(i);
        req[i] = (Request){RQ_DELETE, hashed_key, 0};
    }
    if (bt_request(&inst, req, count / 2) < 0) {
        printf("ERROR: Failed to delete from BTree\n");
        product = -7;
        goto clean;
    }
    
    u64 expected_remaining = count - count/2;
    if (inst.length != expected_remaining) {
        printf("ERROR: BTree length mismatch after delete (got %lu, expected %lu)\n",
               inst.length, expected_remaining);
        product = -8;
        goto clean;
    }
    
    if (normalize(&inst) < 0) {
        printf("ERROR: Failed to normalize BTree\n");
        product = -9;
        goto clean;
    }
clean:
    close(inst.file);
    unlink(template);
    return product;
}

#define RED   "\x1b[31m"
#define GREEN "\x1b[32m"
#define RESET "\x1b[0m"
#define PROGRESS printf(".");fflush(stdout);;

int main() {
    printf("😎 Count: %i",count);
    printf(RED "\n");
    int _l_xx0 = test_hashset(); PROGRESS
    int _l_xx1 = test_vector(); PROGRESS
    int _l_xx2 = test_btree(); PROGRESS
    printf(RESET "\n");

    char* SUCCESS;
    char* ERRORS;

    if(_l_xx0 >= 0 && _l_xx1 >= 0 && _l_xx2 >= 0){
        SUCCESS = "3";
        ERRORS  = "0";
    } else if((_l_xx0 >= 0 && _l_xx1 >= 0) || (_l_xx0 >= 0 && _l_xx2 >= 0) || (_l_xx1 >= 0 && _l_xx2 >= 0)){
        SUCCESS = "2";
        ERRORS  = "1";
    } else if(_l_xx0 >= 0 || _l_xx1 >= 0 || _l_xx2 >= 0){
        SUCCESS = "1";
        ERRORS  = "2";
    } else {
        SUCCESS = "0";
        ERRORS  = "3";
    }

    printf("\n=== TESTING ROUTINE ===\n"
       "HASHSET: %s%-5s%s\n"
       "VECTOR:  %s%-5s%s\n"
       "BTREE:   %s%-5s%s\n"
       "------------------------\n"
       "OVERALL: %s%-5s%s\n"
       "ERRORS:  %s\n"
       "SUCCESS: %s\n\n",
    (_l_xx0>=0?GREEN:RED), (_l_xx0>=0?"OK":"ERROR"), RESET,
    (_l_xx1>=0?GREEN:RED), (_l_xx1>=0?"OK":"ERROR"), RESET,
    (_l_xx2>=0?GREEN:RED), (_l_xx2>=0?"OK":"ERROR"), RESET,
    (_l_xx0>=0 && _l_xx1>=0 && _l_xx2>=0?GREEN:RED), 
    (_l_xx0>=0 && _l_xx1>=0 && _l_xx2>=0?"OK":"ERROR"), RESET,
    ERRORS,
    SUCCESS); 
    return 0;
}
