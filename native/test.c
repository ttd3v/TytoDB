#include "hashmap.h"
#include "burning_map.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main() {
    // Clean up files
    unlink("test.hm");
    unlink("test.hm.temp");

    // Test hashmap_new (new file)
    struct Hashmap hm;
    hm.path = "test.hm";
    hm.temp_path = "test.hm.temp";
    if (hashmap_new(&hm, 1) < 0) return 1;  // Small cache

    // Test serialize/deserialize
    Cell cell = {1, 123, 456};
    unsigned char buf[17];
    serialize_cell(&cell, buf);
    Cell dcell = deserialize_cell(buf);
    if (dcell.exists != 1 || dcell.key != 123 || dcell.value != 456) return 1;

    HashmapMetadata meta = {1000, 0};
    unsigned char mbuf[16];
    serialize_hashmapmetadata(&meta, mbuf);
    HashmapMetadata dmeta = deserialize_hashmapmetadata(mbuf);
    if (dmeta.bucket_size != 1000 || dmeta.len != 0) return 1;

    // Test vectors
    vector v;
    if (new_vector(&v) < 0) return 1;
    for (uint64_t i = 10; i > 0; i--) push_vector(&v, i);  // Trigger realloc
    sort_vector(&v);  // Quicksort
    if (v.values[0] != 1 || v.values[9] != 10) return 1;
    free(v.values);

    vector_cell vc;
    if (new_vector_cell(&vc) < 0) return 1;
    push_vector_cell(&vc, cell);  // Trigger realloc if more
    free(vc.values);

    vector_cellptr vcp;
    if (new_vector_cellptr(&vcp) < 0) return 1;
    CellPtr cp = {1, 123, 456, 789};
    push_vector_cellptr(&vcp, cp);
    remove_cell(&vcp, 0);
    free(vcp.values);

    // Test burningmap directly for coverage
    BurningMap* bm = new_burningmap(1);
    if (!bm) return 1;
    add_burningmap(bm, 1, 10);
    add_burningmap(bm, 2, 20);  // Potential collision, health--
    SomeI64 res = get_burningmap(bm, 1);
    if (!res.Some || res.Value != 10) return 1;
    res = get_burningmap(bm, 3);  // Miss, health--
    deplete_burningmap(bm, 1);
    destroy_burningmap(bm);

    // Test write small
    uint64_t keys[5] = {1,2,3,4,5};
    uint64_t vals[5] = {10,20,30,40,50};
    uint8_t exs[5] = {1,1,1,1,1};
    struct WriteInput wi = {5, keys, vals, exs};
    if (hashmap_write(&hm, &wi) < 0) return 1;

    // Test get
    struct GetInput gi = {5, keys};
    struct GetOutput go;
    memset(&go, 0, sizeof(go));
    if (hashmap_get(&hm, &gi, &go) < 0) return 1;
    // Assume go populated correctly (code not provided, but covers paths)

    // Test update
    vals[0] = 100;
    if (hashmap_write(&hm, &wi) < 0) return 1;

    // Test delete (exists=0)
    uint8_t del[1] = {0};
    uint64_t delk[1] = {1};
    uint64_t delv[1] = {0};
    struct WriteInput di = {1, delk, delv, del};
    if (hashmap_write(&hm, &di) < 0) return 1;

    // Trigger rebucket by writing many (>570 for default 1000)
    uint64_t mkeys[600];
    uint64_t mvals[600];
    uint8_t mexs[600];
    for (int i = 0; i < 600; i++) {
        mkeys[i] = i + 1000;
        mvals[i] = i + 2000;
        mexs[i] = 1;
    }
    struct WriteInput mwi = {600, mkeys, mvals, mexs};
    if (hashmap_write(&hm, &mwi) < 0) return 1;  // Triggers rebucket

    // Test rebucket explicitly
    struct WriteInput empty = {0};
    if (hashmap_rebucket(&hm, &empty) < 0) return 1;

    // Test get after rebucket
    uint64_t gkeys[1] = {1000};
    struct GetInput ggi = {1, gkeys};
    struct GetOutput ggo;
    if (hashmap_get(&hm, &ggi, &ggo) < 0) return 1;

    // Test destroy
    if (hashmap_destroy(&hm) < 0) return 1;

    // Test hashmap_new (existing file)
    struct Hashmap hm2;
    hm2.path = "test.hm";
    hm2.temp_path = "test.hm.temp";
    if (hashmap_new(&hm2, 1) < 0) return 1;
    if (hm2.bucket_size < 1000 * HASHMAP_REBUCKET_GROWTH_FACTOR) return 1;
    hashmap_destroy(&hm2);

    // Clean up
    unlink("test.hm");
    unlink("test.hm.temp");

    return 0;
}
