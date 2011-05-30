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

#ifndef BUBBLE_SORT_H
#define BUBBLE_SORT_H

/**
 * Comparator function
 * This function should return:
 * -1 if a is less than b
 *  0 if a is equal to b
 *  1 if a is greater than b
 */
typedef int (*compare_func)(void *a, void *b);

void bubble_sort_array(void **array, int numItems, compare_func fn);
void quicksort(void **array, int numItems, compare_func fn);

#endif