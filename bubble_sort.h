#ifndef BUBBLE_SORT_H
#define BUBBLE_SORT_H

typedef int (*sort_func)(void *a, void *b);

void bubble_sort_array(void **array, int numItems, sort_func fn);

#endif