# Build BPF object for edge XDP (requires clang, libbpf headers).
CLANG ?= clang
ARCH := $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')
BPF_CFLAGS := -O2 -g -Wall -Werror -target bpf -D__TARGET_ARCH_$(ARCH)
BPF_INCLUDES := -I/usr/include -I/usr/local/include

OUT_DIR := bpf
OBJ := $(OUT_DIR)/edge_filter.o

.PHONY: all clean

all: $(OBJ)

$(OBJ): bpf/edge_filter.c
	$(CLANG) $(BPF_CFLAGS) $(BPF_INCLUDES) -c $< -o $@

clean:
	rm -f $(OBJ)
