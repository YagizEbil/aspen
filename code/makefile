ifeq (, $(shell which jemalloc-config))
JEMALLOC =
else
JEMALLOCLD = $(shell jemalloc-config --libdir)
JEMALLOC = -L$(JEMALLOCLD) -ljemalloc
endif

OPT = -O3 -g

CONCEPTS = -fconcepts -DCONCEPTS
CFLAGS = -DEDGELONG -mcx16 $(OPT) -ldl -std=c++17 -march=native -Wall -Wno-subobject-linkage -DUSEMALLOC -DNDEBUG

OMPFLAGS = -DOPENMP -fopenmp
CILKFLAGS = -DCILK -fcilkplus
HGFLAGS = -DHOMEGROWN -pthread

ifdef CLANG
CC = clang++
PFLAGS = $(CILKFLAGS)
else ifdef CILK
CC = g++
PFLAGS = $(CILKFLAGS)
else ifdef OPENMP
CC = g++
PFLAGS = $(OMPFLAGS)
else ifdef HOMEGROWN
CC = g++
PFLAGS = $(HGFLAGS)
else ifdef SERIAL
CC = g++
PFLAGS =
else # default is homegrown
CC = g++
PFLAGS = $(HGFLAGS)
endif

# memory_footprint:  tools/memory_footprint.cpp
# 	$(CC) $(CFLAGS) $(PFLAGS) tools/memory_footprint.cpp -o memory_footprint
#
# run_static_algorithm:  tools/run_static_algorithm.cpp
# 	$(CC) $(CFLAGS) $(PFLAGS) tools/run_static_algorithm.cpp -o run_static_algorithm
#
# run_batch_updates:  tools/run_batch_updates.cpp
# 	$(CC) $(CFLAGS) $(PFLAGS) tools/run_batch_updates.cpp -o run_batch_updates
#
# run_simultaneous_updates_queries:  tools/run_simultaneous_updates_queries.cpp
# 	$(CC) $(CFLAGS) $(PFLAGS) tools/run_simultaneous_updates_queries.cpp -o run_simultaneous_updates_queries


ALL= memory_footprint run_static_algorithm run_batch_updates run_simultaneous_updates_queries
all: $(ALL)

PAPI_CFLAGS ?= $(shell pkg-config --cflags papi 2>/dev/null)
PAPI_LIBS ?= $(shell pkg-config --libs papi 2>/dev/null)
ifeq ($(strip $(PAPI_LIBS)),)
PAPI_LIBS = -lpapi
endif

run_batch_updates: tools/run_batch_updates.cpp
	$(CC) $(CFLAGS) $(PFLAGS) $(PAPI_CFLAGS) -o $@ $< $(JEMALLOC) $(PAPI_LIBS)

% : tools/%.cpp
	$(CC) $(CFLAGS) $(PFLAGS) -o $@ $< $(JEMALLOC)


.PHONY : clean

clean:
	rm -f *.o $(ALL)
