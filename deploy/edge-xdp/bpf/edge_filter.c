// SPDX-License-Identifier: GPL-2.0
// XDP edge filter for eSPX tracker ingress (:8180).
// PASS non-target ports; DROP blocklisted sources, SYN/PPS floods, and global SYN cap.

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <linux/tcp.h>

#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

#ifndef TRACKER_INGRESS_PORT
#define TRACKER_INGRESS_PORT 8180
#endif

// Per-source SYN allowance per one-second window (tunable via future map).
#define SYN_LIMIT_PER_SEC 64
#define SYN_WINDOW_NS 1000000000ULL

// Global SYN cap across all sources (distributed flood). Per-CPU slice; see check_global_syn.
#define GLOBAL_SYN_LIMIT_PER_SEC 50000
#define GLOBAL_SYN_ASSUMED_CPUS 8
#define GLOBAL_SYN_PER_CPU (GLOBAL_SYN_LIMIT_PER_SEC / GLOBAL_SYN_ASSUMED_CPUS)

// Per-source TCP PPS token bucket to :8180 (established-connection floods).
#define PPS_RATE 2000
#define PPS_BURST PPS_RATE
#define NS_PER_SEC 1000000000ULL

struct ipv4_lpm_key {
	__u32 prefixlen;
	__u32 addr;
};

struct syn_state {
	__u64 window_start_ns;
	__u32 count;
};

struct pps_bucket {
	__u64 last_ns;
	__u32 tokens;
};

enum xdp_stats {
	XDP_STAT_PASS = 0,
	XDP_STAT_PASS_ALLOWLIST,
	XDP_STAT_DROP_BLOCKLIST,
	XDP_STAT_DROP_SYN,
	XDP_STAT_DROP_GLOBAL_SYN,
	XDP_STAT_DROP_PPS,
	XDP_STAT_MAX,
};

struct {
	__uint(type, BPF_MAP_TYPE_LPM_TRIE);
	__uint(max_entries, 524288);
	__type(key, struct ipv4_lpm_key);
	__type(value, __u8);
	__uint(map_flags, BPF_F_NO_PREALLOC);
} blocklist_v4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LPM_TRIE);
	__uint(max_entries, 65536);
	__type(key, struct ipv4_lpm_key);
	__type(value, __u8);
	__uint(map_flags, BPF_F_NO_PREALLOC);
} allow_v4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 524288);
	__type(key, __u32);
	__type(value, struct syn_state);
} syn_ratelimit_v4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 1048576);
	__type(key, __u32);
	__type(value, struct pps_bucket);
} ratelimit_v4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct syn_state);
} global_syn SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, XDP_STAT_MAX);
	__type(key, __u32);
	__type(value, __u64);
} stats SEC(".maps");

static __always_inline void stat_inc(__u32 idx)
{
	__u64 *val = bpf_map_lookup_elem(&stats, &idx);
	if (val)
		(*val)++;
}

static __always_inline int check_syn_limit(__u32 src_ip, __u64 now)
{
	struct syn_state *st = bpf_map_lookup_elem(&syn_ratelimit_v4, &src_ip);
	struct syn_state new_st = {};

	if (st) {
		if (now - st->window_start_ns < SYN_WINDOW_NS) {
			if (st->count >= SYN_LIMIT_PER_SEC)
				return XDP_DROP;
			new_st.window_start_ns = st->window_start_ns;
			new_st.count = st->count + 1;
		} else {
			new_st.window_start_ns = now;
			new_st.count = 1;
		}
	} else {
		new_st.window_start_ns = now;
		new_st.count = 1;
	}

	bpf_map_update_elem(&syn_ratelimit_v4, &src_ip, &new_st, BPF_ANY);
	return XDP_PASS;
}

static __always_inline int check_global_syn(__u64 now)
{
	__u32 key = 0;
	struct syn_state *st = bpf_map_lookup_elem(&global_syn, &key);
	struct syn_state new_st = {};

	if (!st)
		return XDP_PASS;

	if (now - st->window_start_ns < SYN_WINDOW_NS) {
		if (st->count >= GLOBAL_SYN_PER_CPU)
			return XDP_DROP;
		new_st.window_start_ns = st->window_start_ns;
		new_st.count = st->count + 1;
	} else {
		new_st.window_start_ns = now;
		new_st.count = 1;
	}

	bpf_map_update_elem(&global_syn, &key, &new_st, BPF_ANY);
	return XDP_PASS;
}

static __always_inline int check_pps_limit(__u32 src_ip, __u64 now)
{
	struct pps_bucket *st = bpf_map_lookup_elem(&ratelimit_v4, &src_ip);
	struct pps_bucket new_st = {};
	__u32 tokens = PPS_BURST;

	if (st) {
		tokens = st->tokens;
		__u64 elapsed = now - st->last_ns;
		if (elapsed > NS_PER_SEC)
			elapsed = NS_PER_SEC;
		if (elapsed > 0) {
			__u64 added = (elapsed * PPS_RATE) / NS_PER_SEC;
			if (added > 0) {
				tokens += (__u32)added;
				if (tokens > PPS_BURST)
					tokens = PPS_BURST;
			}
		}
	}

	if (tokens == 0)
		return XDP_DROP;

	new_st.last_ns = now;
	new_st.tokens = tokens - 1;
	bpf_map_update_elem(&ratelimit_v4, &src_ip, &new_st, BPF_ANY);
	return XDP_PASS;
}

SEC("xdp")
int xdp_edge_filter(struct xdp_md *ctx)
{
	void *data = (void *)(long)ctx->data;
	void *data_end = (void *)(long)ctx->data_end;

	struct ethhdr *eth = data;
	if ((void *)(eth + 1) > data_end)
		return XDP_PASS;

	if (eth->h_proto != bpf_htons(ETH_P_IP))
		return XDP_PASS;

	struct iphdr *iph = (void *)(eth + 1);
	if ((void *)(iph + 1) > data_end)
		return XDP_PASS;

	if (iph->protocol != IPPROTO_TCP)
		return XDP_PASS;

	__u32 ihl_len = iph->ihl * 4;
	if (ihl_len < sizeof(*iph))
		return XDP_PASS;

	struct tcphdr *tcph = (void *)iph + ihl_len;
	if ((void *)(tcph + 1) > data_end)
		return XDP_PASS;

	if (bpf_ntohs(tcph->dest) != TRACKER_INGRESS_PORT)
		return XDP_PASS;

	struct ipv4_lpm_key al_key = {
		.prefixlen = 32,
		.addr = iph->saddr,
	};
	if (bpf_map_lookup_elem(&allow_v4, &al_key)) {
		stat_inc(XDP_STAT_PASS_ALLOWLIST);
		return XDP_PASS;
	}

	struct ipv4_lpm_key bl_key = {
		.prefixlen = 32,
		.addr = iph->saddr,
	};
	if (bpf_map_lookup_elem(&blocklist_v4, &bl_key)) {
		stat_inc(XDP_STAT_DROP_BLOCKLIST);
		return XDP_DROP;
	}

	if (tcph->syn && !tcph->ack) {
		__u64 now = bpf_ktime_get_ns();
		if (check_global_syn(now) == XDP_DROP) {
			stat_inc(XDP_STAT_DROP_GLOBAL_SYN);
			return XDP_DROP;
		}
		if (check_syn_limit(iph->saddr, now) == XDP_DROP) {
			stat_inc(XDP_STAT_DROP_SYN);
			return XDP_DROP;
		}
	}

	{
		__u64 now = bpf_ktime_get_ns();
		if (check_pps_limit(iph->saddr, now) == XDP_DROP) {
			stat_inc(XDP_STAT_DROP_PPS);
			return XDP_DROP;
		}
	}

	stat_inc(XDP_STAT_PASS);
	return XDP_PASS;
}

char LICENSE[] SEC("license") = "GPL";
