// unit_test.c
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include "hashmap.c"  // For simplicity, include impl

uint64_t rand_u64(void) {
    return ((uint64_t)rand() << 32) | rand();
}

int main(void) {
    
    const char *filename = "test_hashmap.dat";
    if (hashmap_init(filename) < 0) {
        perror("init");
        return 1;
    }

    // Correctness: insert and get 1000 entries
    srand(time(NULL));
    uint64_t sum_insert = 0;
    for (int i = 0; i < 1000; ++i) {
        uint64_t k = rand_u64(), v = rand_u64();
        if (hashmap_insert(k, v) < 0) {
            fprintf(stderr, "insert failed\n");
            hashmap_close();
            unlink(filename);
            return 0;
        }
        uint64_t got;
        if (hashmap_get(k, &got) != 1 || got != v) {
            fprintf(stderr, "get mismatch\n");
            hashmap_close();
            unlink(filename);
            return 0;
        }
        sum_insert += v;
    }
    printf("Correctness: PASS (sum_insert=%lu)\n", sum_insert);

    // Performance: read 10k entries
    const int NUM_TEST = 10000;
    uint64_t keys[NUM_TEST], values[NUM_TEST];
    for (int i = 0; i < NUM_TEST; ++i) {
        keys[i] = rand_u64();
        values[i] = rand_u64();
        hashmap_insert(keys[i], values[i]);
    }
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    uint64_t sum_read = 0;
    for (int i = 0; i < NUM_TEST; ++i) {
        uint64_t got;
        if (hashmap_get(keys[i], &got) != 1) {
            fprintf(stderr, "read failed\n");
            hashmap_close();
            unlink(filename);
            return 0;
        }
        sum_read += got;
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    double ms = (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_nsec - start.tv_nsec) / 1000000.0;
    printf("Performance: PASS (sum_read=%lu, time=%.2f ms)\n", sum_read, ms);

    if (ms > 25.0) {
        printf("Warning: Exceeds 25ms threshold\n");
    }

}
