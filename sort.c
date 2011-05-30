/**
 * Copyright 2011 LiveProfile
 * 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include "sort.h"

void bubble_sort_array(void **array, int numItems, compare_func fn) {
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

void quicksort(void **array, int numItems, compare_func fn) {
    if(numItems == 1) return;
    int STACK_MAX = 1024;
    
    int starts[STACK_MAX], ends[STACK_MAX];
    
    int i = 0;
    
    starts[i] = 0;
    ends[i] = numItems - 1;

    while(i >= 0) {                
       int l = starts[i] + 1, r = ends[i];
       
       if((l - 1) >= r) {
           i--;
           continue;
       }
       
       int pivot = starts[i];

       void *tmp;

       // move through the array
       while(l < r) {
           if(fn(array[l], array[pivot]) > 0) {
               tmp = array[r];
               array[r] = array[l];
               array[l] = tmp;
               r--;
           }
           else {
               l++;
           }
       }

       // if the element at l is less than the pivot, swap it
       if(fn(array[l], array[pivot]) < 0) {
           tmp = array[l];
           array[l] = array[pivot];
           array[pivot] = tmp;
       }
       l--;
       
       if(r == ends[i]) {
           ends[i] = l;
       }
       else {
           starts[i+1] = r;
           ends[i+1] = ends[i];
           ends[i++] = l;
       }
    }
}