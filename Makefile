CC = gcc
CFLAGS = -O3 -Wall -fPIC
LDFLAGS = 
OBJECTS = build/hash_ring.o build/sha1.o build/sort.o
TEST_OBJECTS = build/hash_ring_test.o
SHARED_LIB = build/libhashring.so

lib: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) -o $(SHARED_LIB) -shared

test : lib $(TEST_OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(TEST_OBJECTS) -lhashring -L./build -o bin/hash_ring_test 
	bin/hash_ring_test

erl:
	cd lib/erl && make
	
build/%.o : %.c
	$(CC) $(CFLAGS) -c $< -o $@
	
clean:
	rm -rf $(OBJECTS) $(TEST_OBJECTS) $(SHARED_LIB)