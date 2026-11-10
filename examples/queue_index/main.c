/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2026 Queue Index authors
 *
 * Queue Index traffic verification.
 *
 * Runs four roles on four lcores:
 *   control  - drives the scenario and reports metrics
 *   producer - enqueues at a fixed rate, records the first enqueue failure
 *   consumer - dequeues at a configurable rate, can be stalled or slowed
 *   monitor  - queue_index sampling lcore
 *
 * Scenario:
 *   A. producer at rate, consumer slightly faster, no warning expected
 *   B. consumer stalls, measure warning lead time against the first drop
 *   C. consumer drains the backlog, measure recovery latency
 *   D. consumer runs slightly slower, measure watermark warning lead time
 *
 * With --nic the program takes over a physical port instead: it creates one
 * rte_ring per RX queue, receives into it, and periodically prints the real
 * ring occupancy next to the queue_index marks of the same ring.
 */

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <rte_atomic.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_launch.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_memory.h>
#include <rte_mempool.h>
#include <rte_pause.h>
#include <rte_ring.h>
#include <rte_queue_index.h>

#define RING_NAME "qi_example_ring"
#define BURST 32
#define DEFAULT_RING_SIZE 4096
#define DEFAULT_RATE_MPPS 30
#define WARMUP_EXTRA_MPPS 2
#define SLOW_RATE_MPPS 29
#define DRAIN_RATE_MPPS 60

#define WARMUP_NS (1000ULL * 1000 * 1000)
#define STALL_NS (300ULL * 1000 * 1000)
#define SETTLE_NS (200ULL * 1000 * 1000)
#define DRAIN_HOLD_NS (50ULL * 1000 * 1000)
#define RECOVERY_TIMEOUT_NS (20ULL * 1000 * 1000)
#define SLOW_TIMEOUT_NS (1000ULL * 1000 * 1000)
#define POLL_NS 5000ULL

/* Warning must arrive at least this early to be considered useful. */
#define MIN_LEAD_US 50
#define MAX_ETA_ERROR_PCT 50
#define MAX_RECOVERY_US 5000
#define MIN_LEVEL_LEAD_US 1000

struct ctx {
	struct rte_ring *ring;
	uint64_t hz;
	uint64_t prod_pps;
	RTE_ATOMIC(uint32_t) stop;
	RTE_ATOMIC(uint64_t) cons_pps;
	RTE_ATOMIC(uint64_t) first_fail_ns;
	uint64_t enqueued;
	uint64_t dequeued;
	uint64_t enq_fail;
};

struct report {
	uint64_t warmup_alarms;
	uint64_t stall_start_ns;
	uint64_t warn_ns;
	uint64_t full_ns;
	uint64_t eta_us;
	uint64_t eta_actual_us;
	uint32_t warn_reasons;
	bool critical_seen;
	uint64_t recovery_us;
	uint64_t slow_start_ns;
	uint64_t slow_warn_ns;
	uint32_t slow_reasons;
};

static struct ctx qi_ctx;
static uint64_t g_hz;

static inline uint64_t
now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint64_t
ns_to_tsc(uint64_t ns)
{
	return ns * g_hz / 1000000000ULL;
}

static inline void
spin_ns(uint64_t ns)
{
	uint64_t end = rte_rdtsc() + ns_to_tsc(ns);

	while (rte_rdtsc() < end)
		rte_pause();
}

static inline bool
get_state(struct rte_qi_ring_state *st)
{
	return rte_qi_state_get(RING_NAME, st) == 0;
}

static int
producer(void *arg)
{
	struct ctx *c = arg;
	void *objs[BURST];
	uint64_t interval = (uint64_t)BURST * c->hz / c->prod_pps;
	uint64_t next = rte_rdtsc();
	uint32_t i;

	for (i = 0; i < BURST; i++)
		objs[i] = (void *)(uintptr_t)(i + 1);

	while (rte_atomic_load_explicit(&c->stop,
					rte_memory_order_acquire) == 0) {
		uint64_t now = rte_rdtsc();
		unsigned int n;

		if (now < next) {
			rte_pause();
			continue;
		}

		n = rte_ring_enqueue_bulk(c->ring, objs, BURST, NULL);
		c->enqueued += n;

		if (n < BURST) {
			c->enq_fail++;
			if (rte_atomic_load_explicit(&c->first_fail_ns,
					rte_memory_order_relaxed) == 0)
				rte_atomic_store_explicit(&c->first_fail_ns,
					now_ns(), rte_memory_order_relaxed);
		}

		next += interval;
		if (next < now)
			next = now + interval;
	}

	return 0;
}

static int
consumer(void *arg)
{
	struct ctx *c = arg;
	void *objs[BURST];
	uint64_t pps = rte_atomic_load_explicit(&c->cons_pps,
						rte_memory_order_relaxed);
	uint64_t next = rte_rdtsc();

	while (rte_atomic_load_explicit(&c->stop,
					rte_memory_order_acquire) == 0) {
		uint64_t now = rte_rdtsc();
		uint64_t cur = rte_atomic_load_explicit(&c->cons_pps,
							rte_memory_order_relaxed);
		uint64_t interval;
		unsigned int n;

		if (cur != pps) {
			pps = cur;
			next = now;
		}

		if (pps == 0) {
			rte_pause();
			continue;
		}

		if (now < next) {
			rte_pause();
			continue;
		}

		n = rte_ring_dequeue_bulk(c->ring, objs, BURST, NULL);
		c->dequeued += n;

		interval = (uint64_t)BURST * c->hz / pps;
		next += interval;
		if (next < now)
			next = now + interval;
	}

	return 0;
}

static uint64_t
run_warmup(uint64_t dur_ns)
{
	uint64_t end = rte_rdtsc() + ns_to_tsc(dur_ns);
	uint64_t alarms = 0;
	uint32_t last = RTE_QI_UNKNOWN;
	struct rte_qi_ring_state st;

	while (rte_rdtsc() < end) {
		if (get_state(&st)) {
			if (st.state >= RTE_QI_WARN && last < RTE_QI_WARN)
				alarms++;
			last = st.state;
		}
		spin_ns(POLL_NS);
	}

	return alarms;
}

static void
run_stall(struct report *rep, uint64_t dur_ns)
{
	uint64_t end = rte_rdtsc() + ns_to_tsc(dur_ns);
	struct rte_qi_ring_state st;

	rte_atomic_store_explicit(&qi_ctx.first_fail_ns, 0,
				  rte_memory_order_relaxed);
	rep->stall_start_ns = now_ns();
	rte_atomic_store_explicit(&qi_ctx.cons_pps, 0,
				  rte_memory_order_release);

	while (rte_rdtsc() < end) {
		if (get_state(&st)) {
			/*
			 * Use the local CLOCK_MONOTONIC observation time for
			 * all comparisons: the library timestamps are derived
			 * from its own TSC base and may drift by tens of
			 * microseconds from this clock.
			 */
			if (rep->warn_ns == 0 && st.state >= RTE_QI_WARN) {
				rep->warn_ns = now_ns();
				rep->eta_us = st.eta_us;
				rep->warn_reasons = st.reasons;
			}
			if (st.state == RTE_QI_CRITICAL)
				rep->critical_seen = true;
		}

		if (rep->full_ns == 0) {
			uint64_t full = rte_atomic_load_explicit(
					&qi_ctx.first_fail_ns,
					rte_memory_order_acquire);

			if (full != 0)
				rep->full_ns = full;
		}

		spin_ns(POLL_NS);
	}

	if (rep->full_ns > rep->warn_ns && rep->warn_ns != 0)
		rep->eta_actual_us = (rep->full_ns - rep->warn_ns) / 1000;
}

static uint64_t
run_recovery(uint64_t dur_ns)
{
	uint64_t start_ns = now_ns();
	uint64_t end = rte_rdtsc() + ns_to_tsc(dur_ns);
	struct rte_qi_ring_state st;

	rte_atomic_store_explicit(&qi_ctx.cons_pps,
				  (uint64_t)DRAIN_RATE_MPPS * 1000000ULL,
				  rte_memory_order_release);

	while (rte_rdtsc() < end) {
		if (get_state(&st) && st.state == RTE_QI_OK)
			return now_ns() - start_ns;
		spin_ns(POLL_NS);
	}

	return 0;
}

static void
run_slow(struct report *rep, uint64_t dur_ns)
{
	uint64_t end = rte_rdtsc() + ns_to_tsc(dur_ns);
	struct rte_qi_ring_state st;

	rep->slow_start_ns = now_ns();
	rte_atomic_store_explicit(&qi_ctx.cons_pps,
				  (uint64_t)SLOW_RATE_MPPS * 1000000ULL,
				  rte_memory_order_release);

	while (rte_rdtsc() < end) {
		if (get_state(&st) && st.state >= RTE_QI_WARN) {
			rep->slow_warn_ns = now_ns();
			rep->slow_reasons = st.reasons;
			break;
		}
		spin_ns(POLL_NS);
	}

	rte_atomic_store_explicit(&qi_ctx.cons_pps, qi_ctx.prod_pps,
				  rte_memory_order_release);
}

static void
reasons_str(uint32_t reasons, char *buf, size_t len)
{
	snprintf(buf, len, "%s%s%s",
		 (reasons & RTE_QI_REASON_LEVEL) ? "LEVEL " : "",
		 (reasons & RTE_QI_REASON_TREND) ? "TREND " : "",
		 (reasons & RTE_QI_REASON_FULL) ? "FULL" : "");
	if (buf[0] == '\0')
		snprintf(buf, len, "none");
}

static void
usage(const char *prog)
{
	printf("Usage: %s [EAL options] -- [options]\n", prog);
	printf("  --nic           receive from a NIC instead of the synthetic "
	       "scenario\n");
	printf("  --port=N        NIC port id, default 0\n");
	printf("  --rxq=N         number of RX queues, default 1\n");
	printf("  --duration=S    NIC mode run time in seconds, default 30\n");
	printf("  --print-ms=N    status print interval, default 1000\n");
	printf("  --drain-mpps=N  drain rate in Mpps across all queues, "
	       "0 = unlimited\n");
	printf("  --ring-size=N   ring size, power of two, default %u\n",
	       (unsigned int)DEFAULT_RING_SIZE);
	printf("  --rate=N        synthetic producer rate in Mpps, default %u\n",
	       (unsigned int)DEFAULT_RATE_MPPS);
	printf("  --predictor=peak|ewma|linreg, default peak\n");
}

/* ------------------------------------------------------------------ */
/* NIC mode: one rte_ring per RX queue, RX lcore enqueues, one drain  */
/* lcore frees mbufs, control lcore prints real and queue_index state. */
/* ------------------------------------------------------------------ */

#define MAX_NIC_RXQ 16
#define NIC_MBUF_COUNT 32768
#define NIC_MBUF_CACHE 256
#define NIC_RX_DESC 1024

struct nic_ctx {
	uint16_t port_id;
	uint16_t nb_rxq;
	struct rte_mempool *mp;
	struct rte_ring *rings[MAX_NIC_RXQ];
	char names[MAX_NIC_RXQ][RTE_RING_NAMESIZE];
	uint64_t hz;
	uint64_t drain_pps;
	RTE_ATOMIC(uint32_t) stop;
	RTE_ATOMIC(uint64_t) drained;
};

struct nic_rx_arg {
	struct nic_ctx *ctx;
	uint16_t qid;
	RTE_ATOMIC(uint64_t) rx;
	RTE_ATOMIC(uint64_t) drop;
};

static struct nic_ctx nic;
static struct nic_rx_arg nic_args[MAX_NIC_RXQ];
static volatile sig_atomic_t nic_sigint;

static void
nic_signal_handler(int signo)
{
	(void)signo;
	nic_sigint = 1;
}

static const char *
qi_state_name(enum rte_qi_state state)
{
	switch (state) {
	case RTE_QI_UNKNOWN:
		return "UNKNOWN";
	case RTE_QI_OK:
		return "OK";
	case RTE_QI_WARN:
		return "WARN";
	case RTE_QI_CRITICAL:
		return "CRITICAL";
	default:
		return "?";
	}
}

static int
nic_rx_lcore(void *arg)
{
	struct nic_rx_arg *rx = arg;
	struct nic_ctx *c = rx->ctx;
	struct rte_mbuf *mbufs[BURST];
	uint64_t total = 0, dropped = 0;

	while (rte_atomic_load_explicit(&c->stop,
					rte_memory_order_acquire) == 0) {
		unsigned int n, sent, i;

		n = rte_eth_rx_burst(c->port_id, rx->qid, mbufs, BURST);

		if (n == 0)
			continue;

		sent = rte_ring_enqueue_burst(c->rings[rx->qid],
					      (void **)mbufs, n, NULL);
		total += sent;
		if (sent < n) {
			for (i = sent; i < n; i++)
				rte_pktmbuf_free(mbufs[i]);
			dropped += n - sent;
		}

		rte_atomic_store_explicit(&rx->rx, total,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&rx->drop, dropped,
					  rte_memory_order_relaxed);
	}

	return 0;
}

static int
nic_drain_lcore(void *arg)
{
	struct nic_ctx *c = arg;
	struct rte_mbuf *mbufs[BURST];
	uint64_t drained = 0;
	uint64_t next = rte_rdtsc();
	uint32_t q = 0;

	while (rte_atomic_load_explicit(&c->stop,
					rte_memory_order_acquire) == 0) {
		unsigned int n, i;

		n = rte_ring_dequeue_burst(c->rings[q], (void **)mbufs, BURST,
					   NULL);

		for (i = 0; i < n; i++)
			rte_pktmbuf_free(mbufs[i]);
		drained += n;
		rte_atomic_store_explicit(&c->drained, drained,
					  rte_memory_order_relaxed);

		if (++q == (uint32_t)c->nb_rxq)
			q = 0;

		if (c->drain_pps != 0) {
			uint64_t interval = (uint64_t)BURST * c->hz /
					    c->drain_pps;

			next += interval;
			if (next < rte_rdtsc())
				next = rte_rdtsc();
			while (rte_rdtsc() < next)
				rte_pause();
		}
	}

	return 0;
}

static void
nic_print(uint64_t start_tsc, uint32_t nb_rxq)
{
	struct rte_eth_stats stats;
	double elapsed = (double)(rte_rdtsc() - start_tsc) / (double)g_hz;
	uint64_t rx_sum = 0, drop_sum = 0;
	uint32_t q;
	char reasons[64];

	memset(&stats, 0, sizeof(stats));
	rte_eth_stats_get(nic.port_id, &stats);

	for (q = 0; q < nb_rxq; q++) {
		rx_sum += rte_atomic_load_explicit(&nic_args[q].rx,
						   rte_memory_order_relaxed);
		drop_sum += rte_atomic_load_explicit(&nic_args[q].drop,
						     rte_memory_order_relaxed);
	}

	printf("\n[%6.1fs] port %u rx=%" PRIu64 " drop=%" PRIu64
	       " drained=%" PRIu64 " ipackets=%" PRIu64 " imissed=%" PRIu64
	       " ierrors=%" PRIu64 "\n",
	       elapsed, (unsigned int)nic.port_id, rx_sum, drop_sum,
	       rte_atomic_load_explicit(&nic.drained, rte_memory_order_relaxed),
	       stats.ipackets, stats.imissed, stats.ierrors);

	for (q = 0; q < nb_rxq; q++) {
		struct rte_qi_ring_state st;
		char eta[24];
		uint32_t real, capacity;

		real = rte_ring_count(nic.rings[q]);
		capacity = rte_ring_get_capacity(nic.rings[q]);

		if (rte_qi_state_get(nic.names[q], &st) != 0)
			continue;

		if (st.eta_us == RTE_QI_ETA_INFINITE)
			snprintf(eta, sizeof(eta), "inf");
		else
			snprintf(eta, sizeof(eta), "%" PRIu64 "us",
				 st.eta_us);

		reasons_str(st.reasons, reasons, sizeof(reasons));

		printf("  rxq%-2u ring=%-14s real=%5u/%5u (%5.1f%%) | "
		       "qi=%-8s reasons=%-16s count=%5u net=%" PRIu64
		       "pps eta=%s\n",
		       (unsigned int)q, nic.names[q], real, capacity,
		       capacity != 0 ? (double)real * 100.0 / capacity : 0.0,
		       qi_state_name(st.state), reasons, st.count_hi,
		       st.net_pps, eta);
	}
}

static void
nic_setup(uint16_t port_id, uint16_t nb_rxq, uint32_t ring_size,
	  uint32_t nb_mbufs, uint32_t drain_mpps, unsigned int mon_lcore,
	  const char *predictor)
{
	struct rte_eth_dev_info dev_info;
	struct rte_eth_conf port_conf;
	uint16_t nb_rx_desc = NIC_RX_DESC;
	int socket_id, ret;
	uint16_t q;
	char kvargs[512];

	memset(&nic, 0, sizeof(nic));
	nic.port_id = port_id;
	nic.nb_rxq = nb_rxq;
	nic.hz = g_hz;
	nic.drain_pps = (uint64_t)drain_mpps * 1000000ULL;

	socket_id = rte_eth_dev_socket_id(port_id);
	if (socket_id < 0)
		socket_id = (int)rte_socket_id();

	nic.mp = rte_pktmbuf_pool_create("qi_nic_pool", nb_mbufs,
					 NIC_MBUF_CACHE, 0,
					 RTE_MBUF_DEFAULT_BUF_SIZE, socket_id);
	if (nic.mp == NULL)
		rte_exit(EXIT_FAILURE, "cannot create mbuf pool\n");

	ret = rte_eth_dev_info_get(port_id, &dev_info);
	if (ret != 0)
		rte_exit(EXIT_FAILURE,
			 "rte_eth_dev_info_get(%u) failed: %d\n",
			 (unsigned int)port_id, ret);
	if (nb_rxq > dev_info.max_rx_queues)
		rte_exit(EXIT_FAILURE,
			 "port %u supports at most %u RX queues\n",
			 (unsigned int)port_id,
			 (unsigned int)dev_info.max_rx_queues);

	memset(&port_conf, 0, sizeof(port_conf));

	ret = rte_eth_dev_configure(port_id, nb_rxq, 0, &port_conf);
	if (ret < 0)
		rte_exit(EXIT_FAILURE,
			 "rte_eth_dev_configure(%u) failed: %d\n",
			 (unsigned int)port_id, ret);

	ret = rte_eth_dev_adjust_nb_rx_tx_desc(port_id, &nb_rx_desc, NULL);
	if (ret < 0)
		rte_exit(EXIT_FAILURE,
			 "rte_eth_dev_adjust_nb_rx_tx_desc(%u) failed: %d\n",
			 (unsigned int)port_id, ret);

	for (q = 0; q < nb_rxq; q++) {
		ret = rte_eth_rx_queue_setup(port_id, q, nb_rx_desc,
					     (unsigned int)socket_id, NULL,
					     nic.mp);
		if (ret < 0)
			rte_exit(EXIT_FAILURE,
				 "rte_eth_rx_queue_setup(%u, %u) failed: %d\n",
				 (unsigned int)port_id, (unsigned int)q, ret);
	}

	ret = rte_eth_dev_start(port_id);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "rte_eth_dev_start(%u) failed: %d\n",
			 (unsigned int)port_id, ret);

	rte_eth_promiscuous_enable(port_id);

	for (q = 0; q < nb_rxq; q++) {
		snprintf(nic.names[q], RTE_RING_NAMESIZE, "qi_rxq_%u",
			 (unsigned int)q);
		nic.rings[q] = rte_ring_create(nic.names[q], ring_size,
					       socket_id,
					       RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (nic.rings[q] == NULL)
			rte_exit(EXIT_FAILURE, "cannot create ring %s\n",
				 nic.names[q]);
	}

	snprintf(kvargs, sizeof(kvargs),
		 "lcore=%u,sched=spin,interval_us=10,high_wm=50,low_wm=25,"
		 "warn_horizon_us=150,clear_horizon_us=300,"
		 "confirm_samples=2,min_net_pps=100000,predictor=%s,log=0",
		 mon_lcore, predictor);

	if (rte_qi_init(kvargs) != 0)
		rte_exit(EXIT_FAILURE, "queue_index init failed\n");

	for (q = 0; q < nb_rxq; q++) {
		if (rte_qi_ring_register(nic.names[q], NULL) != 0)
			rte_exit(EXIT_FAILURE, "cannot register ring %s\n",
				 nic.names[q]);
	}

	if (rte_qi_start() != 0)
		rte_exit(EXIT_FAILURE, "queue_index start failed\n");
}

static void
nic_teardown(void)
{
	uint16_t q;

	rte_eth_dev_stop(nic.port_id);
	rte_eth_dev_close(nic.port_id);

	rte_qi_stop();
	for (q = 0; q < nic.nb_rxq; q++)
		rte_qi_ring_unregister(nic.names[q]);
	rte_qi_fini();

	for (q = 0; q < nic.nb_rxq; q++)
		if (nic.rings[q] != NULL)
			rte_ring_free(nic.rings[q]);

	rte_mempool_free(nic.mp);
}

static void
nic_run(uint16_t port_id, uint16_t nb_rxq, uint32_t ring_size,
	uint32_t duration_s, uint32_t print_ms, uint32_t drain_mpps,
	const char *predictor)
{
	unsigned int rx_lcores[MAX_NIC_RXQ];
	unsigned int main_lcore, drain_lcore, mon_lcore, lcore;
	uint64_t start, end, next;
	uint32_t nb_mbufs = NIC_MBUF_COUNT;
	struct rte_eth_link link;
	uint16_t q;

	if (rte_lcore_count() < (unsigned int)nb_rxq + 3)
		rte_exit(EXIT_FAILURE,
			 "need at least %u lcores (main + %u RX + drain + "
			 "monitor), use -l 0-%u\n",
			 (unsigned int)nb_rxq + 3, (unsigned int)nb_rxq,
			 (unsigned int)nb_rxq + 2);

	if (port_id >= rte_eth_dev_count_avail())
		rte_exit(EXIT_FAILURE,
			 "port %u not available (%u ports found)\n",
			 (unsigned int)port_id,
			 (unsigned int)rte_eth_dev_count_avail());

	main_lcore = rte_get_main_lcore();
	lcore = main_lcore;
	for (q = 0; q < nb_rxq; q++) {
		lcore = rte_get_next_lcore(lcore, 1, 0);
		if (lcore >= RTE_MAX_LCORE)
			rte_exit(EXIT_FAILURE, "not enough RX lcores\n");
		rx_lcores[q] = lcore;
	}
	drain_lcore = rte_get_next_lcore(lcore, 1, 0);
	mon_lcore = rte_get_next_lcore(drain_lcore, 1, 0);
	if (mon_lcore >= RTE_MAX_LCORE)
		rte_exit(EXIT_FAILURE, "not enough worker lcores\n");

	if ((uint64_t)nb_rxq * ring_size * 2 > nb_mbufs)
		nb_mbufs = (uint32_t)((uint64_t)nb_rxq * ring_size * 2);

	nic_setup(port_id, nb_rxq, ring_size, nb_mbufs, drain_mpps,
		  mon_lcore, predictor);

	if (rte_eth_link_get_nowait(port_id, &link) == 0)
		printf("port %u link %s, %u Mbps\n", (unsigned int)port_id,
		       link.link_status == RTE_ETH_LINK_UP ? "up" : "down",
		       link.link_speed);

	for (q = 0; q < nb_rxq; q++) {
		memset(&nic_args[q], 0, sizeof(nic_args[q]));
		nic_args[q].ctx = &nic;
		nic_args[q].qid = q;
		rte_atomic_store_explicit(&nic_args[q].rx, 0,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&nic_args[q].drop, 0,
					  rte_memory_order_relaxed);
	}

	signal(SIGINT, nic_signal_handler);

	for (q = 0; q < nb_rxq; q++) {
		if (rte_eal_remote_launch(nic_rx_lcore, &nic_args[q],
					  rx_lcores[q]) != 0)
			rte_exit(EXIT_FAILURE,
				 "cannot start RX lcore %u\n", rx_lcores[q]);
	}
	if (rte_eal_remote_launch(nic_drain_lcore, &nic, drain_lcore) != 0)
		rte_exit(EXIT_FAILURE, "cannot start drain lcore\n");

	printf("NIC mode: port %u, %u RX queues, monitor lcore %u, "
	       "drain %u Mpps%s\n", (unsigned int)port_id,
	       (unsigned int)nb_rxq, mon_lcore, drain_mpps,
	       drain_mpps == 0 ? " (unlimited)" : "");

	start = rte_rdtsc();
	end = start + ns_to_tsc((uint64_t)duration_s * 1000000000ULL);
	next = start;

	while (rte_rdtsc() < end && nic_sigint == 0) {
		nic_print(start, nb_rxq);

		next += ns_to_tsc((uint64_t)print_ms * 1000000ULL);
		while (rte_rdtsc() < next && nic_sigint == 0 &&
		       rte_rdtsc() < end)
			rte_pause();
	}

	rte_atomic_store_explicit(&nic.stop, 1, rte_memory_order_release);
	for (q = 0; q < nb_rxq; q++)
		rte_eal_wait_lcore(rx_lcores[q]);
	rte_eal_wait_lcore(drain_lcore);

	nic_teardown();
}

int
main(int argc, char **argv)
{
	struct rte_qi_monitor_status ms;
	struct report rep;
	char kvargs[512];
	char reasons[64];
	uint32_t ring_size = DEFAULT_RING_SIZE;
	uint32_t rate_mpps = DEFAULT_RATE_MPPS;
	uint32_t capacity;
	uint32_t duration_s = 30;
	uint32_t print_ms = 1000;
	uint32_t drain_mpps = 0;
	uint16_t port_id = 0;
	uint16_t nb_rxq = 1;
	bool nic_mode = false;
	const char *predictor = "peak";
	unsigned int main_lcore, prod_lcore, cons_lcore, mon_lcore;
	uint64_t lead_us, slow_lead_us, eta_err_pct;
	int ret, i;
	bool pass;

	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "EAL initialization failed\n");

	g_hz = rte_get_tsc_hz();
	if (g_hz == 0)
		rte_exit(EXIT_FAILURE, "invalid TSC frequency\n");

	argc -= ret;
	argv += ret;

	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0)
			continue;
		if (strcmp(argv[i], "--nic") == 0) {
			nic_mode = true;
		} else if (strncmp(argv[i], "--port=", 7) == 0) {
			port_id = (uint16_t)strtoul(argv[i] + 7, NULL, 10);
		} else if (strncmp(argv[i], "--rxq=", 6) == 0) {
			nb_rxq = (uint16_t)strtoul(argv[i] + 6, NULL, 10);
		} else if (strncmp(argv[i], "--duration=", 11) == 0) {
			duration_s = (uint32_t)strtoul(argv[i] + 11, NULL, 10);
		} else if (strncmp(argv[i], "--print-ms=", 11) == 0) {
			print_ms = (uint32_t)strtoul(argv[i] + 11, NULL, 10);
		} else if (strncmp(argv[i], "--drain-mpps=", 13) == 0) {
			drain_mpps = (uint32_t)strtoul(argv[i] + 13, NULL, 10);
		} else if (strncmp(argv[i], "--ring-size=", 12) == 0) {
			ring_size = (uint32_t)strtoul(argv[i] + 12, NULL, 10);
		} else if (strncmp(argv[i], "--rate=", 7) == 0) {
			rate_mpps = (uint32_t)strtoul(argv[i] + 7, NULL, 10);
		} else if (strncmp(argv[i], "--predictor=", 12) == 0) {
			predictor = argv[i] + 12;
		} else {
			usage(argv[0]);
			rte_exit(EXIT_FAILURE, "unknown argument: %s\n",
				 argv[i]);
		}
	}

	if (strcmp(predictor, "peak") != 0 &&
	    strcmp(predictor, "ewma") != 0 &&
	    strcmp(predictor, "linreg") != 0)
		rte_exit(EXIT_FAILURE, "invalid predictor: %s\n", predictor);

	if (ring_size < 64 || ring_size > (1u << 24))
		rte_exit(EXIT_FAILURE, "ring size must be in [64, 16777216]\n");

	if ((ring_size & (ring_size - 1)) != 0) {
		uint32_t rounded = 64;

		while (rounded < ring_size)
			rounded <<= 1;
		printf("ring size %u rounded up to %u\n", ring_size, rounded);
		ring_size = rounded;
	}

	if (nic_mode) {
		if (nb_rxq < 1 || nb_rxq > MAX_NIC_RXQ)
			rte_exit(EXIT_FAILURE, "rxq must be in [1, %d]\n",
				 MAX_NIC_RXQ);
		if (duration_s == 0)
			rte_exit(EXIT_FAILURE, "duration must be positive\n");
		if (print_ms < 10)
			rte_exit(EXIT_FAILURE, "print-ms must be at least 10\n");

		nic_run(port_id, nb_rxq, ring_size, duration_s, print_ms,
			drain_mpps, predictor);
		return 0;
	}

	if (rate_mpps < 1 || rate_mpps > 100)
		rte_exit(EXIT_FAILURE, "rate must be in [1, 100] Mpps\n");

	if (rte_lcore_count() < 4)
		rte_exit(EXIT_FAILURE,
			 "need at least 4 lcores, use -l 0-3\n");

	main_lcore = rte_get_main_lcore();
	prod_lcore = rte_get_next_lcore(main_lcore, 1, 0);
	cons_lcore = rte_get_next_lcore(prod_lcore, 1, 0);
	mon_lcore = rte_get_next_lcore(cons_lcore, 1, 0);
	if (mon_lcore >= RTE_MAX_LCORE)
		rte_exit(EXIT_FAILURE, "not enough worker lcores\n");

	memset(&qi_ctx, 0, sizeof(qi_ctx));
	qi_ctx.hz = g_hz;
	qi_ctx.prod_pps = (uint64_t)rate_mpps * 1000000ULL;
	rte_atomic_store_explicit(&qi_ctx.stop, 0,
				  rte_memory_order_relaxed);
	/*
	 * Over-provision the consumer during the warmup so that scheduling
	 * jitter cannot slowly fill the ring; any warning seen there would
	 * then be a genuine predictor false positive.
	 */
	rte_atomic_store_explicit(&qi_ctx.cons_pps,
				  qi_ctx.prod_pps +
				  (uint64_t)WARMUP_EXTRA_MPPS * 1000000ULL,
				  rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_ctx.first_fail_ns, 0,
				  rte_memory_order_relaxed);

	qi_ctx.ring = rte_ring_create(RING_NAME, ring_size, SOCKET_ID_ANY,
				      RING_F_SP_ENQ | RING_F_SC_DEQ);
	if (qi_ctx.ring == NULL)
		rte_exit(EXIT_FAILURE, "cannot create ring %s\n", RING_NAME);
	capacity = rte_ring_get_capacity(qi_ctx.ring);

	snprintf(kvargs, sizeof(kvargs),
		 "lcore=%u,sched=spin,interval_us=10,high_wm=50,low_wm=25,"
		 "warn_horizon_us=150,clear_horizon_us=300,"
		 "confirm_samples=2,min_net_pps=100000,predictor=%s,log=0",
		 mon_lcore, predictor);

	if (rte_qi_init(kvargs) != 0)
		rte_exit(EXIT_FAILURE, "queue_index init failed\n");

	if (rte_qi_ring_register(RING_NAME, NULL) != 0)
		rte_exit(EXIT_FAILURE, "queue_index register failed\n");

	if (rte_qi_start() != 0)
		rte_exit(EXIT_FAILURE, "queue_index start failed\n");

	printf("queue_index verification: %u Mpps, ring %u (capacity %u), "
	       "predictor %s, monitor lcore %u\n",
	       rate_mpps, ring_size, capacity, predictor, mon_lcore);

	if (rte_eal_remote_launch(producer, &qi_ctx, prod_lcore) != 0)
		rte_exit(EXIT_FAILURE, "cannot start producer\n");
	if (rte_eal_remote_launch(consumer, &qi_ctx, cons_lcore) != 0)
		rte_exit(EXIT_FAILURE, "cannot start consumer\n");

	memset(&rep, 0, sizeof(rep));

	printf("[A] no-pressure warmup %" PRIu64 " ms\n",
	       (uint64_t)WARMUP_NS / 1000000);
	rep.warmup_alarms = run_warmup(WARMUP_NS);

	printf("[B] consumer stall %" PRIu64 " ms\n",
	       (uint64_t)STALL_NS / 1000000);
	run_stall(&rep, STALL_NS);

	printf("[C] drain backlog\n");
	rep.recovery_us = run_recovery(RECOVERY_TIMEOUT_NS) / 1000;
	/* Keep draining so the next phase starts from an empty ring. */
	spin_ns(DRAIN_HOLD_NS);
	rte_atomic_store_explicit(&qi_ctx.cons_pps, qi_ctx.prod_pps,
				  rte_memory_order_release);
	spin_ns(SETTLE_NS);

	printf("[D] consumer slow %u Mpps\n", (unsigned int)SLOW_RATE_MPPS);
	run_slow(&rep, SLOW_TIMEOUT_NS);

	rte_atomic_store_explicit(&qi_ctx.stop, 1, rte_memory_order_release);
	rte_eal_wait_lcore(prod_lcore);
	rte_eal_wait_lcore(cons_lcore);

	memset(&ms, 0, sizeof(ms));
	rte_qi_monitor_status(&ms);
	rte_qi_stop();
	rte_qi_ring_unregister(RING_NAME);
	rte_qi_fini();
	rte_ring_free(qi_ctx.ring);

	lead_us = (rep.warn_ns != 0 && rep.full_ns > rep.warn_ns) ?
		  (rep.full_ns - rep.warn_ns) / 1000 : 0;

	if (rep.eta_actual_us > 0 && rep.eta_us != RTE_QI_ETA_INFINITE) {
		uint64_t diff = rep.eta_us > rep.eta_actual_us ?
				rep.eta_us - rep.eta_actual_us :
				rep.eta_actual_us - rep.eta_us;

		eta_err_pct = diff * 100 / rep.eta_actual_us;
	} else {
		eta_err_pct = 100;
	}

	slow_lead_us = (rep.slow_warn_ns > rep.slow_start_ns) ?
		       (rep.slow_warn_ns - rep.slow_start_ns) / 1000 : 0;

	reasons_str(rep.warn_reasons, reasons, sizeof(reasons));

	printf("\n");
	printf("====================================================\n");
	printf(" Queue Index traffic verification\n");
	printf("====================================================\n");
	printf(" ring                    : %s (capacity %u)\n",
	       RING_NAME, capacity);
	printf(" producer rate           : %u Mpps\n", rate_mpps);
	printf(" predictor               : %s\n", predictor);
	printf(" monitor lcore           : %u\n", mon_lcore);
	printf("----------------------------------------------------\n");
	printf(" [A] no-pressure warmup\n");
	printf("     false alarms        : %" PRIu64 "  %s\n",
	       rep.warmup_alarms, rep.warmup_alarms == 0 ? "PASS" : "FAIL");
	printf(" [B] consumer stall\n");
	printf("     stall to warning    : %" PRIu64 " us\n",
	       rep.warn_ns > rep.stall_start_ns ?
	       (rep.warn_ns - rep.stall_start_ns) / 1000 : 0);
	printf("     stall to full       : %" PRIu64 " us\n",
	       rep.full_ns > rep.stall_start_ns ?
	       (rep.full_ns - rep.stall_start_ns) / 1000 : 0);
	printf("     warning to full     : %" PRIu64 " us  %s (>= %d us)\n",
	       lead_us, lead_us >= MIN_LEAD_US ? "PASS" : "FAIL",
	       MIN_LEAD_US);
	printf("     reasons             : %s\n", reasons);
	if (rep.eta_us == RTE_QI_ETA_INFINITE)
		printf("     eta / actual        : inf / %" PRIu64 " us\n",
		       rep.eta_actual_us);
	else
		printf("     eta / actual        : %" PRIu64 " / %" PRIu64
		       " us\n", rep.eta_us, rep.eta_actual_us);
	printf("     eta error           : %" PRIu64 "%%  %s (<= %d%%)\n",
	       eta_err_pct, eta_err_pct <= MAX_ETA_ERROR_PCT ? "PASS" : "FAIL",
	       MAX_ETA_ERROR_PCT);
	printf("     critical observed   : %s  %s\n",
	       rep.critical_seen ? "yes" : "no",
	       rep.critical_seen ? "PASS" : "FAIL");
	printf(" [C] recovery\n");
	printf("     recovery latency    : %" PRIu64 " us  %s (<= %d us)\n",
	       rep.recovery_us,
	       (rep.recovery_us > 0 && rep.recovery_us <= MAX_RECOVERY_US) ?
	       "PASS" : "FAIL", MAX_RECOVERY_US);
	printf(" [D] consumer slow\n");
	printf("     warning lead        : %" PRIu64 " us  %s (>= %d us)\n",
	       slow_lead_us, slow_lead_us >= MIN_LEVEL_LEAD_US ? "PASS" : "FAIL",
	       MIN_LEVEL_LEAD_US);
	printf(" [E] monitor health\n");
	printf("     ticks               : %" PRIu64 "\n", ms.ticks);
	printf("     missed / late       : %" PRIu64 " / %" PRIu64 "\n",
	       ms.missed_ticks, ms.late_ticks);
	printf("     enqueued / dequeued : %" PRIu64 " / %" PRIu64 "\n",
	       qi_ctx.enqueued, qi_ctx.dequeued);
	printf("     enqueue failures    : %" PRIu64 "\n", qi_ctx.enq_fail);
	printf("====================================================\n");

	pass = rep.warmup_alarms == 0 &&
	       lead_us >= MIN_LEAD_US &&
	       rep.critical_seen &&
	       eta_err_pct <= MAX_ETA_ERROR_PCT &&
	       rep.recovery_us > 0 && rep.recovery_us <= MAX_RECOVERY_US &&
	       slow_lead_us >= MIN_LEVEL_LEAD_US;

	printf(" RESULT: %s\n", pass ? "PASS" : "FAIL");

	return pass ? 0 : 1;
}
