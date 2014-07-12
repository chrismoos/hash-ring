CC = gcc
override CFLAGS += -O3 -Wall -fPIC
LDFLAGS =
OBJECTS = build/hash_ring.o build/sha1.o build/sort.o build/md5.o
TEST_OBJECTS = build/hash_ring_test.o
ifdef PREFIX
	prefix=$(PREFIX)
else
	prefix=/usr/local
endif

UNAME=$(shell uname)
TOP:=$(abspath $(dir $(lastword $(MAKEFILE_LIST))))

ERLANG_RELEASE=$(shell erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().'  -noshell)
ERLANG_RELEASE_VER=$(shell echo $(ERLANG_RELEASE) | sed -E "s/R([0-9]+).*/\1/")
ERLANG_R14_OR_HIGHER=$(shell expr $(ERLANG_RELEASE_VER) \>= 14)

ifeq ($(ERLANG_R14_OR_HIGHER), 1)
	REBAR="./rebar"
else
	REBAR="./rebar.r13"
endif

ifeq ($(UNAME), Darwin)
	SHARED_LIB = build/libhashring.dylib
else
	SHARED_LIB = build/libhashring.so
endif

lib: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) -o $(SHARED_LIB) -shared

test : lib bindings $(TEST_OBJECTS)
	mkdir -p bin
	LD_LIBRARY_PATH="$(TOP)/build" $(REBAR) compile eunit
	cd lib/java && LD_LIBRARY_PATH="$(TOP)/build" gradle test
	cd lib/python && LD_LIBRARY_PATH="$(TOP)/build" python tests.py
	$(CC) $(CFLAGS) $(LDFLAGS) $(TEST_OBJECTS) -lhashring -L./build -o bin/hash_ring_test
	bin/hash_ring_test

bindings: erl java python

erl:
	$(REBAR) compile
	
java:
	cd lib/java && gradle jar

python:
	cd lib/python && ./setup.py bdist

build/%.o : %.c
	$(CC) $(CFLAGS) -c $< -o $@
	
clean:
	rm -rf $(OBJECTS) $(TEST_OBJECTS) $(SHARED_LIB)
	
install: lib
	mkdir -p $(prefix)/lib
	mkdir -p $(prefix)/include
	cp -f $(SHARED_LIB) $(prefix)/lib/
	cp hash_ring.h $(prefix)/include/
