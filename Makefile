CC = gcc
CFLAGS = -O2 -Wall
OBJECTS = hash_ring_test.o hash_ring.o sha1.o bubble_sort.o

hash_ring_bench : $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) -o bin/hash_ring_bench
	
%.o : %.c
	$(CC) $(CFLAGS) -c $<
	
clean:
	rm -rf $(OBJECTS)