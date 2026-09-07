CXX ?= clang++
CXXFLAGS ?= -std=c++20 -O0 -g -fsanitize=address,undefined -Wall -Wextra
SRC := src/tettix_dump.cpp
BIN := build/tettix
EXAMPLE := tests/parquet/example.parquet

.PHONY: all dump test clean
all: $(BIN)

$(BIN): $(SRC) src/compact.hpp
	mkdir -p build
	$(CXX) $(CXXFLAGS) -I src -o $@ $(SRC)

dump: $(BIN)
	./$(BIN) dump $(EXAMPLE)

test: $(BIN)
	bash tests/parquet/check.sh

clean:
	rm -rf build
