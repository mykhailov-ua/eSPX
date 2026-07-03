package bpf

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -no-strip -cc clang -cflags "-O2 -g -Wall -Werror" -target bpfel,bpfeb Edge ../../../deploy/edge/xdp/bpf/edge_filter.c -- -I/usr/include/$(uname -m)-linux-gnu
