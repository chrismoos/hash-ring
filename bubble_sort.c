#include <stdio.h>
#include "bubble_sort.h"

void bubble_sort_array(void **array, int numItems, sort_func fn) {
    // an array of less than or equal to 1 is already sorted
    if(numItems <= 1) return;
    
    void *temp;
    int x, result;
    while(1) {
        int numSorts = 0;
        for(x = 0; x < numItems; x++) {
            if(x < (numItems - 1)) {
                result = fn(array[x], array[x + 1]);
                if(result > 0) {
                    temp = array[x + 1];
                    array[x + 1] = array[x];
                    array[x] = temp;
                    numSorts++;
                }
                else if(result < 0) {
                    // no-op
                }
            }
        }
        if(numSorts == 0) break;
    }
}