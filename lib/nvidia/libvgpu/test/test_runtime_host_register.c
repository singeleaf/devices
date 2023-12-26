#include <stdio.h>
#include <cuda.h>
#include <cuda_runtime.h>

#include "test_utils.h"


size_t usage = 0;


int test(size_t bytes) {
    void* hptr = malloc(bytes);
    CHECK_RUNTIME_API(cudaHostRegister(hptr, bytes, cudaHostRegisterMapped));
    CHECK_NVML_API(get_current_memory_usage(&usage));
    CHECK_RUNTIME_API(cudaHostUnregister(hptr));
    CHECK_NVML_API(get_current_memory_usage(&usage));
    printf("\n");
    return 0;
}


int main() {
    CHECK_RUNTIME_API(cudaSetDevice(TEST_DEVICE_ID));
    CHECK_NVML_API(get_current_memory_usage(&usage));

    size_t arr[84] = {};
    int k = 0;
    for (k = 0; k < 28; ++k) {
        // power of 2
        arr[3 * k] = 2 << k;

        // power of 2 plus 1
        arr[3 * k + 1] = (2 << k) + 1;

        // power of 2 minus 1
        arr[3 * k + 2] = (2 << k) - 1;
    }

    for (k = 0; k < 84; ++k) {
        if (0 != test(arr[k])) {
            fprintf(stderr, "Test alloc %lu bytes failed\n", arr[k]);
            return -1;
        }
    }

    CHECK_NVML_API(nvmlShutdown());
    return 0;
}
