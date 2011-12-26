CC = gcc
override CFLAGS += -O3 -Wall -fPIC
LDFLAGS =
OBJECTS = build/hash_ring.o build/sha1.o build/sort.o build/md5.o
TEST_OBJECTS = build/hash_ring_test.o

ifeq ($(OS), Darwin)
	SHARED_LIB = build/libhashring.so
else
	SHARED_LIB = build/libhashring.dylib
endif

lib: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) -o $(SHARED_LIB) -shared

test : lib $(TEST_OBJECTS)
	mkdir -p bin
	$(CC) $(CFLAGS) $(LDFLAGS) $(TEST_OBJECTS) -lhashring -L./build -o bin/hash_ring_test
	bin/hash_ring_test

bindings: erl java

erl:
	./rebar compile
	
java:
	cd lib/java && gradle jar

python:
	cd lib/python && ./setup.py install

build/%.o : %.c
	$(CC) $(CFLAGS) -c $< -o $@
	
clean:
	rm -rf $(OBJECTS) $(TEST_OBJECTS) $(SHARED_LIB)
	
install: lib
	cp -f $(SHARED_LIB) /usr/local/lib/
	cp hash_ring.h /usr/local/include/
