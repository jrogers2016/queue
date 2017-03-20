#include <omp.h>
#include <stdio.h>
#include <math.h>
int main() {
    omp_set_dynamic(0);
    omp_set_num_threads(1);
    #pragma omp parallel
    {
        int j;
        for (int i=0; i<1000000; i++) {
            for (int k=0; k<1000; k++) {
                j=log(i+1);
            }
        }
        printf("Hello from thread %d, nthreads %d\n", omp_get_thread_num(), omp_get_num_threads());
    }
}
