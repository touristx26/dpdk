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
 */

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <rte_atomic.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_lcore.h>
#include <rte_memory.h>
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
	return ns * qi_ctx.hz / 1000000000ULL;
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
	uint64_t sample_ts = 0;
	struct rte_qi_ring_state st;

	rte_atomic_store_explicit(&qi_ctx.first_fail_ns, 0,
				  rte_memory_order_relaxed);
	rep->stall_start_ns = now_ns();
	rte_atomic_store_explicit(&qi_ctx.cons_pps, 0,
				  rte_memory_order_release);

	while (rte_rdtsc() < end) {
		if (get_state(&st)) {
			if (rep->warn_ns == 0 && st.state >= RTE_QI_WARN &&
			    st.warn_ts_ns >= rep->stall_start_ns) {
				rep->warn_ns = st.warn_ts_ns;
				rep->eta_us = st.eta_us;
				rep->warn_reasons = st.reasons;
				sample_ts = st.ts_ns;
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

	if (sample_ts != 0 && rep->full_ns > sample_ts)
		rep->eta_actual_us = (rep->full_ns - sample_ts) / 1000;
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
		if (get_state(&st) && st.state == RTE_QI_OK) {
			uint64_t latency = now_ns() - start_ns;

			rte_atomic_store_explicit(&qi_ctx.cons_pps,
						  qi_ctx.prod_pps,
						  rte_memory_order_release);
			return latency;
		}
		spin_ns(POLL_NS);
	}

	rte_atomic_store_explicit(&qi_ctx.cons_pps, qi_ctx.prod_pps,
				  rte_memory_order_release);
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
		if (get_state(&st) && st.state >= RTE_QI_WARN &&
		    st.warn_ts_ns >= rep->slow_start_ns) {
			rep->slow_warn_ns = st.warn_ts_ns;
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
	printf("Usage: %s [EAL options] -- [--ring-size=N] [--rate=N] "
	       "[--predictor=peak|ewma|linreg]\n", prog);
	printf("  --ring-size  ring size, power of two, default %u\n",
	       (unsigned int)DEFAULT_RING_SIZE);
	printf("  --rate       producer rate in Mpps, default %u\n",
	       (unsigned int)DEFAULT_RATE_MPPS);
	printf("  --predictor  queue_index predictor, default peak\n");
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
	const char *predictor = "peak";
	unsigned int main_lcore, prod_lcore, cons_lcore, mon_lcore;
	uint64_t lead_us, slow_lead_us, eta_err_pct;
	int ret, i;
	bool pass;

	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "EAL initialization failed\n");

	argc -= ret;
	argv += ret;

	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0)
			continue;
		if (strncmp(argv[i], "--ring-size=", 12) == 0) {
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

	if (rate_mpps < 1 || rate_mpps > 100)
		rte_exit(EXIT_FAILURE, "rate must be in [1, 100] Mpps\n");

	if (ring_size < 64 || ring_size > (1u << 24))
		rte_exit(EXIT_FAILURE, "ring size must be in [64, 16777216]\n");

	if ((ring_size & (ring_size - 1)) != 0) {
		uint32_t rounded = 64;

		while (rounded < ring_size)
			rounded <<= 1;
		printf("ring size %u rounded up to %u\n", ring_size, rounded);
		ring_size = rounded;
	}

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
	qi_ctx.hz = rte_get_tsc_hz();
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
	printf("     false alarms        : %" PRIu64 "\n", rep.warmup_alarms);
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
	printf("     critical observed   : %s\n",
	       rep.critical_seen ? "yes" : "no");
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
