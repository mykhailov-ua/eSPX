// SPDX-License-Identifier: GPL-2.0
// XDP edge filter for eSPX tracker ingress (:8180).
// PASS non-target ports; DROP blocklisted sources, protocol anomalies, SYN/PPS/RST floods.

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/udp.h>

#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

#ifndef BPF_FUNC_tcp_gen_syncookie_ipv4
#define BPF_FUNC_tcp_gen_syncookie_ipv4 163
#endif

static long (*const bpf_tcp_gen_syncookie_ipv4)(void *iph, __u16 iph_len, void *tcph,
						__u16 tcph_len, __u64 *cookie) =
	(void *)(long)BPF_FUNC_tcp_gen_syncookie_ipv4;

#ifndef TRACKER_INGRESS_PORT
#define TRACKER_INGRESS_PORT 8180
#endif

#define SYN_WINDOW_NS 1000000000ULL
#define NS_PER_SEC 1000000000ULL

#define DEFAULT_SYN_LIMIT 64
#define DEFAULT_PPS_RATE 2000
#define DEFAULT_GLOBAL_SYN_LIMIT 50000
#define DEFAULT_ASSUMED_CPUS 8
#define DEFAULT_SYN_SUBNET_LIMIT 256
#define DEFAULT_RST_RATE 64
#define DEFAULT_RST_BURST 64

#define PROG_IDX_SYN_COOKIE 0

#define CFG_FLAG_FINGERPRINT 0x01
#define CFG_FLAG_SYN_COOKIE  0x02

#define TCP_FLAG_FIN 0x01
#define TCP_FLAG_SYN 0x02
#define TCP_FLAG_RST 0x04
#define TCP_FLAG_PSH 0x08
#define TCP_FLAG_ACK 0x10
#define TCP_FLAG_URG 0x20

#define VIOLATION_SYN 1
#define VIOLATION_GLOBAL_SYN 2
#define VIOLATION_PPS 3
#define VIOLATION_SYN_SUBNET 4

struct fingerprint_event {
	__u64 ts_ns;
	__u32 src_ip;
	__u32 tcp_hash;
	__u16 window;
	__u8 ttl;
	__u8 mss;
};

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

struct sctphdr {
	__be16 source;
	__be16 dest;
};

struct edge_config {
	__u32 syn_limit;
	__u32 pps_rate;
	__u32 global_syn_limit;
	__u32 assumed_cpus;
	__u32 syn_subnet_limit;
	__u32 syn_cookie_enabled;
	__u32 fingerprint_enabled;
};

struct violation_event {
	__u64 ts_ns;
	__u32 src_ip;
	__u8 reason;
	__u8 _pad[3];
};

enum xdp_stats {
	XDP_STAT_PASS = 0,
	XDP_STAT_PASS_ALLOWLIST,
	XDP_STAT_DROP_BLOCKLIST,
	XDP_STAT_DROP_SYN,
	XDP_STAT_DROP_GLOBAL_SYN,
	XDP_STAT_DROP_PPS,
	XDP_STAT_DROP_ANOMALY,
	XDP_STAT_DROP_INVALID,
	XDP_STAT_DROP_NON_TCP,
	XDP_STAT_DROP_RST,
	XDP_STAT_DROP_SYN_SUBNET,
	XDP_STAT_SYN_COOKIE,
	XDP_STAT_FINGERPRINT,
	XDP_STAT_MAX,
};

#define STAT_NONE XDP_STAT_MAX

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
	__uint(max_entries, 65536);
	__type(key, __u32);
	__type(value, struct syn_state);
} syn_subnet_ratelimit_v4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 1048576);
	__type(key, __u32);
	__type(value, struct pps_bucket);
} ratelimit_v4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 1048576);
	__type(key, __u32);
	__type(value, struct pps_bucket);
} rst_ratelimit_v4 SEC(".maps");

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

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct edge_config);
} config SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 256 * 1024);
} violations SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 256 * 1024);
} fingerprints SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PROG_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u32);
} prog_array SEC(".maps");

static __always_inline void stat_inc(__u32 idx)
{
	__u64 *val = bpf_map_lookup_elem(&stats, &idx);
	if (val)
		(*val)++;
}

static __always_inline void stat_inc_if(__u32 idx)
{
	if (idx >= XDP_STAT_MAX)
		return;
	stat_inc(idx);
}

static __always_inline void emit_violation(__u32 src_ip, __u8 reason)
{
	struct violation_event *evt;

	evt = bpf_ringbuf_reserve(&violations, sizeof(*evt), 0);
	if (!evt)
		return;
	evt->ts_ns = bpf_ktime_get_ns();
	evt->src_ip = src_ip;
	evt->reason = reason;
	bpf_ringbuf_submit(evt, 0);
}

static __always_inline __u8 read_tcp_mss(struct tcphdr *tcph, void *data_end)
{
	__u32 doff = tcph->doff * 4;

	if (doff <= sizeof(*tcph))
		return 0;
	if ((__u8 *)tcph + doff > (__u8 *)data_end)
		return 0;

	__u8 *p = (__u8 *)tcph + sizeof(*tcph);
	if (p + 4 > (__u8 *)data_end)
		return 0;
	if (p[0] != 2 || p[1] < 4)
		return 0;
	return (__u8)(bpf_ntohs(*(__be16 *)(p + 2)) >> 8);
}

static __always_inline __u32 hash_tcp_syn_fields(__u8 ttl, __u16 window, __u8 mss, __u8 doff)
{
	__u32 h = ttl;

	h = (h << 5) ^ window;
	h = (h << 5) ^ mss;
	h = (h << 3) ^ doff;
	return h;
}

static __always_inline void emit_fingerprint(__u64 now, __u32 src_ip, __u32 tcp_hash,
					     __u16 window, __u8 ttl, __u8 mss)
{
	struct fingerprint_event *evt;

	evt = bpf_ringbuf_reserve(&fingerprints, sizeof(*evt), 0);
	if (!evt)
		return;
	evt->ts_ns = now;
	evt->src_ip = src_ip;
	evt->window = window;
	evt->ttl = ttl;
	evt->mss = mss;
	evt->tcp_hash = tcp_hash;
	bpf_ringbuf_submit(evt, 0);
}

static __always_inline __u32 ipv4_subnet24(__u32 addr)
{
	return addr & bpf_htonl(0xFFFFFF00);
}

/* Scalar config load — avoids struct edge_config stack spills (u32→u64). */
static __always_inline __u8 load_config_scalars(__u32 *syn_limit, __u32 *pps_rate,
						__u32 *global_syn_limit, __u32 *assumed_cpus,
						__u32 *syn_subnet_limit)
{
	__u32 key = 0;
	struct edge_config *map_cfg = bpf_map_lookup_elem(&config, &key);
	__u8 flags = CFG_FLAG_FINGERPRINT;

	*syn_limit = DEFAULT_SYN_LIMIT;
	*pps_rate = DEFAULT_PPS_RATE;
	*global_syn_limit = DEFAULT_GLOBAL_SYN_LIMIT;
	*assumed_cpus = DEFAULT_ASSUMED_CPUS;
	*syn_subnet_limit = DEFAULT_SYN_SUBNET_LIMIT;

	if (!map_cfg || map_cfg->assumed_cpus == 0)
		return flags;

	if (map_cfg->syn_limit)
		*syn_limit = map_cfg->syn_limit;
	if (map_cfg->pps_rate)
		*pps_rate = map_cfg->pps_rate;
	if (map_cfg->global_syn_limit)
		*global_syn_limit = map_cfg->global_syn_limit;
	*assumed_cpus = map_cfg->assumed_cpus;
	if (map_cfg->syn_subnet_limit)
		*syn_subnet_limit = map_cfg->syn_subnet_limit;
	if (map_cfg->syn_cookie_enabled)
		flags |= CFG_FLAG_SYN_COOKIE;
	if (!map_cfg->fingerprint_enabled)
		flags &= ~CFG_FLAG_FINGERPRINT;
	return flags;
}

static __always_inline void swap_eth_addrs(__u8 *a, __u8 *b)
{
	__u8 tmp[6];

	__builtin_memcpy(tmp, a, 6);
	__builtin_memcpy(a, b, 6);
	__builtin_memcpy(b, tmp, 6);
}

static __always_inline __u16 csum_fold(__u32 csum)
{
	csum = (csum & 0xffff) + (csum >> 16);
	csum = (csum & 0xffff) + (csum >> 16);
	return (__u16)~csum;
}

static __always_inline __u16 csum_tcpudp_magic(__be32 saddr, __be32 daddr,
					       __u32 len, __u8 proto,
					       __u32 csum)
{
	__u64 s = csum;

	s += (__u32)saddr;
	s += (__u32)daddr;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	s += (proto + len) << 8;
#else
	s += proto + len;
#endif
	s = (s & 0xffffffff) + (s >> 32);
	s = (s & 0xffffffff) + (s >> 32);
	return (__u16)s;
}

static __always_inline __u32 gen_syncookie_ipv4(struct iphdr *iph, __u32 ihl_len,
						struct tcphdr *tcph, __u32 tcph_len,
						__u64 *cookie_out)
{
	__s64 raw;

	raw = bpf_tcp_raw_gen_syncookie_ipv4(iph, tcph, tcph_len);
	if (raw >= 0)
		return (__u32)raw;

	if (bpf_tcp_gen_syncookie_ipv4(iph, ihl_len, tcph, tcph_len, cookie_out) >= 0)
		return (__u32)*cookie_out;

	return 0;
}

static __always_inline int emit_ipv4_synack(struct xdp_md *ctx,
					    struct ethhdr *eth,
					    struct iphdr *iph,
					    struct tcphdr *tcph,
					    __u32 ihl_len, __u32 tcph_len,
					    __u32 cookie)
{
	__s64 csum_val;
	__u32 old_len;
	__u32 new_len;
	__be32 tmp_ip;
	__be16 tmp_port;

	swap_eth_addrs(eth->h_source, eth->h_dest);

	tmp_ip = iph->saddr;
	iph->saddr = iph->daddr;
	iph->daddr = tmp_ip;
	iph->tot_len = bpf_htons(ihl_len + tcph_len);
	iph->check = 0;

	tmp_port = tcph->source;
	tcph->source = tcph->dest;
	tcph->dest = tmp_port;
	tcph->ack_seq = bpf_htonl(bpf_ntohl(tcph->seq) + 1);
	tcph->seq = bpf_htonl(cookie);
	*(__u8 *)((__u8 *)tcph + 13) = TCP_FLAG_SYN | TCP_FLAG_ACK;
	tcph->doff = 5;
	tcph->window = 0;
	tcph->urg_ptr = 0;
	tcph->check = 0;

	csum_val = bpf_csum_diff(NULL, 0, (__be32 *)tcph, tcph_len, 0);
	if (csum_val < 0)
		return XDP_DROP;
	tcph->check = csum_tcpudp_magic(iph->saddr, iph->daddr, tcph_len,
					IPPROTO_TCP, (__u32)csum_val);

	csum_val = bpf_csum_diff(NULL, 0, (__be32 *)iph, ihl_len, 0);
	if (csum_val < 0)
		return XDP_DROP;
	iph->check = csum_fold((__u32)csum_val);

	old_len = (__u8 *)(tcph + 1) - (__u8 *)eth;
	new_len = sizeof(*eth) + ihl_len + tcph_len;
	if (new_len != old_len) {
		if (bpf_xdp_adjust_tail(ctx, (__s32)new_len - (__s32)old_len))
			return XDP_DROP;
	}

	return XDP_TX;
}

static __always_inline int try_syn_cookie(struct xdp_md *ctx)
{
	__u32 idx = PROG_IDX_SYN_COOKIE;

	bpf_tail_call(ctx, &prog_array, idx);
	return XDP_DROP;
}

static __always_inline int check_syn_limit(__u32 src_ip, __u64 now, __u32 syn_limit)
{
	struct syn_state *st = bpf_map_lookup_elem(&syn_ratelimit_v4, &src_ip);
	struct syn_state new_st = {};

	if (st) {
		if (now - st->window_start_ns < SYN_WINDOW_NS) {
			if (st->count >= syn_limit)
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

static __always_inline int check_syn_subnet_limit(__u32 src_ip, __u64 now, __u32 subnet_limit)
{
	__u32 subnet = ipv4_subnet24(src_ip);
	struct syn_state *st = bpf_map_lookup_elem(&syn_subnet_ratelimit_v4, &subnet);
	struct syn_state new_st = {};

	if (st) {
		if (now - st->window_start_ns < SYN_WINDOW_NS) {
			if (st->count >= subnet_limit)
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

	bpf_map_update_elem(&syn_subnet_ratelimit_v4, &subnet, &new_st, BPF_ANY);
	return XDP_PASS;
}

static __always_inline int check_global_syn(__u64 now, __u32 global_limit, __u32 assumed_cpus)
{
	__u32 key = 0;
	__u32 per_cpu = global_limit / assumed_cpus;
	struct syn_state *st = bpf_map_lookup_elem(&global_syn, &key);
	struct syn_state new_st = {};

	if (!st || per_cpu == 0)
		return XDP_PASS;

	if (now - st->window_start_ns < SYN_WINDOW_NS) {
		if (st->count >= per_cpu)
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

static __always_inline int check_pps_limit(__u32 src_ip, __u64 now, __u32 pps_rate)
{
	struct pps_bucket *st = bpf_map_lookup_elem(&ratelimit_v4, &src_ip);
	struct pps_bucket new_st = {};
	__u32 burst = pps_rate;
	__u32 tokens = burst;

	if (st) {
		tokens = st->tokens;
		__u64 elapsed = now - st->last_ns;
		if (elapsed > NS_PER_SEC)
			elapsed = NS_PER_SEC;
		if (elapsed > 0) {
			__u64 added = (elapsed * pps_rate) / NS_PER_SEC;
			if (added > 0) {
				tokens += (__u32)added;
				if (tokens > burst)
					tokens = burst;
			}
		}
	}

	if (!tokens)
		return XDP_DROP;

	new_st.last_ns = now;
	new_st.tokens = tokens - 1;
	bpf_map_update_elem(&ratelimit_v4, &src_ip, &new_st, BPF_ANY);
	return XDP_PASS;
}

static __always_inline int check_rst_limit(__u32 src_ip, __u64 now)
{
	struct pps_bucket *st = bpf_map_lookup_elem(&rst_ratelimit_v4, &src_ip);
	struct pps_bucket new_st = {};
	__u32 tokens = DEFAULT_RST_BURST;

	if (st) {
		tokens = st->tokens;
		__u64 elapsed = now - st->last_ns;
		if (elapsed > NS_PER_SEC)
			elapsed = NS_PER_SEC;
		if (elapsed > 0) {
			__u64 added = (elapsed * DEFAULT_RST_RATE) / NS_PER_SEC;
			if (added > 0) {
				tokens += (__u32)added;
				if (tokens > DEFAULT_RST_BURST)
					tokens = DEFAULT_RST_BURST;
			}
		}
	}

	if (!tokens)
		return XDP_DROP;

	new_st.last_ns = now;
	new_st.tokens = tokens - 1;
	bpf_map_update_elem(&rst_ratelimit_v4, &src_ip, &new_st, BPF_ANY);
	return XDP_PASS;
}

static __always_inline int is_tcp_anomaly_flags(__u8 fl)
{
	if ((fl & (TCP_FLAG_SYN | TCP_FLAG_FIN)) == (TCP_FLAG_SYN | TCP_FLAG_FIN))
		return 1;
	if ((fl & (TCP_FLAG_SYN | TCP_FLAG_RST)) == (TCP_FLAG_SYN | TCP_FLAG_RST))
		return 1;
	if (fl == 0)
		return 1;
	if (fl == TCP_FLAG_FIN)
		return 1;
	if ((fl & (TCP_FLAG_FIN | TCP_FLAG_PSH | TCP_FLAG_URG)) ==
	    (TCP_FLAG_FIN | TCP_FLAG_PSH | TCP_FLAG_URG))
		return 1;
	return 0;
}

static __always_inline int is_invalid_tcp(struct tcphdr *tcph)
{
	if (tcph->doff < 5)
		return 1;
	if (tcph->source == 0)
		return 1;
	return 0;
}

static __always_inline int drop_non_tcp_tracker(__u8 proto, void *l4, void *data_end)
{
	if (proto == IPPROTO_UDP) {
		struct udphdr *udph = l4;
		if ((void *)(udph + 1) > data_end)
			return XDP_PASS;
		if (bpf_ntohs(udph->dest) == TRACKER_INGRESS_PORT)
			return XDP_DROP;
		return XDP_PASS;
	}

	if (proto == IPPROTO_SCTP) {
		struct sctphdr *sctph = l4;
		if ((void *)(sctph + 1) > data_end)
			return XDP_PASS;
		if (bpf_ntohs(sctph->dest) == TRACKER_INGRESS_PORT)
			return XDP_DROP;
		return XDP_PASS;
	}

	if (proto == IPPROTO_ICMP)
		return XDP_DROP;

	return XDP_PASS;
}

SEC("xdp")
int xdp_syn_cookie(struct xdp_md *ctx)
{
	void *data = (void *)(long)ctx->data;
	void *data_end = (void *)(long)ctx->data_end;
	struct ethhdr *eth = data;
	struct iphdr *iph;
	struct tcphdr *tcph;
	__u32 ihl_len;
	__u32 tcph_len;
	__u64 cookie_out = 0;
	__u32 cookie;
	int action;

	if ((void *)(eth + 1) > data_end)
		return XDP_DROP;

	iph = (void *)(eth + 1);
	if ((void *)(iph + 1) > data_end)
		return XDP_DROP;

	ihl_len = iph->ihl * 4;
	if (ihl_len < sizeof(*iph))
		return XDP_DROP;

	tcph = (void *)iph + ihl_len;
	if ((void *)(tcph + 1) > data_end)
		return XDP_DROP;

	tcph_len = tcph->doff * 4;
	if (tcph_len < sizeof(*tcph))
		return XDP_DROP;

	cookie = gen_syncookie_ipv4(iph, ihl_len, tcph, tcph_len, &cookie_out);
	if (!cookie)
		return XDP_DROP;

	action = emit_ipv4_synack(ctx, eth, iph, tcph, ihl_len, tcph_len, cookie);
	if (action != XDP_TX)
		return XDP_DROP;

	stat_inc(XDP_STAT_SYN_COOKIE);
	return XDP_TX;
}

SEC("xdp")
int xdp_edge_filter(struct xdp_md *ctx)
{
	void *data = (void *)(long)ctx->data;
	void *data_end = (void *)(long)ctx->data_end;
	__u32 action = XDP_PASS;
	__u32 stat_idx = STAT_NONE;
	__u8 fp_stat = 0;

	struct ethhdr *eth = data;
	if ((void *)(eth + 1) > data_end)
		return XDP_PASS;

	if (eth->h_proto != bpf_htons(ETH_P_IP))
		return XDP_PASS;

	struct iphdr *iph = (void *)(eth + 1);
	if ((void *)(iph + 1) > data_end)
		return XDP_PASS;

	__u32 ihl_len = iph->ihl * 4;
	if (ihl_len < sizeof(*iph))
		return XDP_PASS;

	void *l4 = (void *)iph + ihl_len;

	if (iph->protocol != IPPROTO_TCP) {
		action = drop_non_tcp_tracker(iph->protocol, l4, data_end);
		if (action == XDP_DROP) {
			stat_idx = XDP_STAT_DROP_NON_TCP;
			goto out;
		}
		return XDP_PASS;
	}

	struct tcphdr *tcph = l4;
	if ((void *)(tcph + 1) > data_end)
		return XDP_PASS;

	if (bpf_ntohs(tcph->dest) != TRACKER_INGRESS_PORT)
		return XDP_PASS;

	__u32 src_ip = iph->saddr;

	struct ipv4_lpm_key al_key = {
		.prefixlen = 32,
		.addr = src_ip,
	};
	if (bpf_map_lookup_elem(&allow_v4, &al_key)) {
		stat_idx = XDP_STAT_PASS_ALLOWLIST;
		goto out;
	}

	struct ipv4_lpm_key bl_key = {
		.prefixlen = 32,
		.addr = src_ip,
	};
	if (bpf_map_lookup_elem(&blocklist_v4, &bl_key)) {
		action = XDP_DROP;
		stat_idx = XDP_STAT_DROP_BLOCKLIST;
		goto out;
	}

	__u8 tcp_fl = *(__u8 *)((__u8 *)tcph + 13);

	if (is_tcp_anomaly_flags(tcp_fl)) {
		action = XDP_DROP;
		stat_idx = XDP_STAT_DROP_ANOMALY;
		goto out;
	}

	if (is_invalid_tcp(tcph)) {
		action = XDP_DROP;
		stat_idx = XDP_STAT_DROP_INVALID;
		goto out;
	}

	__u32 syn_limit, pps_rate, global_syn_limit, assumed_cpus, syn_subnet_limit;
	__u8 cfg_flags = load_config_scalars(&syn_limit, &pps_rate, &global_syn_limit,
					     &assumed_cpus, &syn_subnet_limit);

	__u64 now = bpf_ktime_get_ns();

	if (tcp_fl & TCP_FLAG_RST) {
		if (check_rst_limit(src_ip, now) == XDP_DROP) {
			action = XDP_DROP;
			stat_idx = XDP_STAT_DROP_RST;
			goto out;
		}
	}

	if ((tcp_fl & (TCP_FLAG_SYN | TCP_FLAG_ACK)) == TCP_FLAG_SYN) {
		if (cfg_flags & CFG_FLAG_FINGERPRINT) {
			__u16 win = bpf_ntohs(tcph->window);
			__u8 mss = 0;

			if (tcph->doff > 5)
				mss = read_tcp_mss(tcph, data_end);
			__u32 hash = hash_tcp_syn_fields(iph->ttl, win, mss, tcph->doff);

			emit_fingerprint(now, src_ip, hash, win, iph->ttl, mss);
			fp_stat = 1;
		}
		if (check_global_syn(now, global_syn_limit, assumed_cpus) == XDP_DROP) {
			if (cfg_flags & CFG_FLAG_SYN_COOKIE)
				return try_syn_cookie(ctx);
			emit_violation(src_ip, VIOLATION_GLOBAL_SYN);
			action = XDP_DROP;
			stat_idx = XDP_STAT_DROP_GLOBAL_SYN;
			goto out;
		}
		if (check_syn_subnet_limit(src_ip, now, syn_subnet_limit) == XDP_DROP) {
			if (cfg_flags & CFG_FLAG_SYN_COOKIE)
				return try_syn_cookie(ctx);
			emit_violation(src_ip, VIOLATION_SYN_SUBNET);
			action = XDP_DROP;
			stat_idx = XDP_STAT_DROP_SYN_SUBNET;
			goto out;
		}
		if (check_syn_limit(src_ip, now, syn_limit) == XDP_DROP) {
			if (cfg_flags & CFG_FLAG_SYN_COOKIE)
				return try_syn_cookie(ctx);
			emit_violation(src_ip, VIOLATION_SYN);
			action = XDP_DROP;
			stat_idx = XDP_STAT_DROP_SYN;
			goto out;
		}
	}

	if (check_pps_limit(src_ip, now, pps_rate) == XDP_DROP) {
		emit_violation(src_ip, VIOLATION_PPS);
		action = XDP_DROP;
		stat_idx = XDP_STAT_DROP_PPS;
		goto out;
	}

	stat_idx = XDP_STAT_PASS;

out:
	stat_inc_if(stat_idx);
	if (fp_stat)
		stat_inc(XDP_STAT_FINGERPRINT);
	return action;
}

char LICENSE[] SEC("license") = "GPL";
