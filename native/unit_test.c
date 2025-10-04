#include "hashset.h"
#include "vector.h"
#include "btree.h"
#include <asm-generic/errno-base.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
u64 count = 4096;

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

    if(value.length > count/2){
        printf("ERROR: Failed to delete values\n");
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

int test_btree() {
    int product = 0;
    char template[] = "/tmp/btree_testXXXXXX";
    int fd = mkstemp(template);
    if (fd < 0) {
        printf("ERROR: Failed to create temp file\n");
        return -1;
    }
    close(fd);
    BTree inst;
    if (init(&inst, template) < 0) {
        printf("ERROR: Failed to init BTree\n");
        unlink(template);
        return -2;
    }
    Request req[count];
    
    for (u64 i = 0; i < count; i++) {
        req[i] = (Request){RQ_WRITE,i, i * 2};
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
    for (u64 i = 0; i < count; i++) {
        req[i] = (Request){RQ_READ,i,UINT64_MAX};
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
    for (u64 i = 0; i < count / 2; i++) {
        req[i] = (Request){RQ_DELETE,i,0};
    }
    if (bt_request(&inst, req, count / 2) < 0) {
        printf("ERROR: Failed to delete from BTree\n");
        product = -7;
        goto clean;
    }
    if (inst.length != count / 2) {
        printf("ERROR: BTree length mismatch after delete\n");
        product = -8;
        goto clean;
    }
    if (normalize(&inst) < 0) {
        printf("ERROR: Failed to normalize BTree\n");
        product = -9;
        goto clean;
    }
clean:
    free((void*)inst.empty);
    close(inst.file);
    unlink(template);
    return product;
}

#define RED   "\x1b[31m"
#define GREEN "\x1b[32m"
#define RESET "\x1b[0m"
#define PROGRESS printf(".");fflush(stdout);;

int main() {
    printf("😎 Count: %lu",count);
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
    
    const u64 steps = 10;
    const u64 growth = 1024;
    const u64 start_count = 512;
    count = start_count;
    printf("\n\n\n==== STRESS TEST ====\n");
    printf("``` VECTOR ```\n");
    for(u64 i = 0; i < steps; i++){
        u64 start = clock();
        count *= growth;
        test_vector();
        u64 end = clock();
        printf("$%lu\t%lu\n",count,end-start);
    }
    count = start_count;
    
    printf("``` HASHSET ```\n");
    for(u64 i = 0; i < steps; i++){
        u64 start = clock();
        count *= growth;
        test_hashset();
        u64 end = clock();
        printf("$%lu\t%lu\n",count,end-start);
    }
    count = start_count;

    printf("``` BTREE ```\n");
    for(u64 i = 0; i < steps; i++){
        u64 start = clock();
        count *= growth;
        test_btree();
        u64 end = clock();
        printf("$%lu\t%lu\n",count,end-start);
    }
    count = start_count;
    return 0;
}
