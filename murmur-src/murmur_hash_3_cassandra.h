#include <stdint.h>
#include <stddef.h>

void MurmurHash3_x64_128_cassandra(
        const void *key, const size_t len, const int64_t seed, void *out);
