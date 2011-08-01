OS := $(shell uname)

CFLAGS := -O3 -Wall -fPIC

ifeq ($(OS), Darwin)
	LD_FLAGS := -shared -undefined suppress -flat_namespace 
  ERLWS:=$(shell erl -noshell -eval "io:format(\"~p\",[erlang:system_info(wordsize)])" -s erlang halt)

  ifeq ($(ERLWS), 4)
	  LD_FLAGS := $(LD_FLAGS) -m32
  else
  	LD_FLAGS := $(LD_FLAGS) -m64
  endif
else
	LD_FLAGS := -shared -fPIC
endif

all: ebin clean driver
	erlc -o ebin src/hash_ring.erl

ebin:
	mkdir -p ebin

driver:
	gcc $(CFLAGS) -o ebin/hash_ring_drv.so src/hash_ring_drv.c ../../hash_ring.c ../../sha1.c ../../sort.c ../../md5.c -I../../ -I/usr/local/lib/erlang/usr/include $(LD_FLAGS)
	
test: driver
	erlc  -DTEST -o ebin src/hash_ring.erl
	erl -pa ebin -noshell -noinput -eval 'eunit:test(hash_ring, [verbose]).' -s init stop
	
clean:
	rm -rf ebin/*
