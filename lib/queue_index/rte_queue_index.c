/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2026 Queue Index authors
 */

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <rte_atomic.h>
#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_kvargs.h>
#include <rte_launch.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_memory.h>
#include <rte_pause.h>
#include <rte_ring.h>
#include <rte_rwlock.h>

#include "rte_queue_index.h"

RTE_LOG_REGISTER_DEFAULT(queue_index_logtype, INFO);
#define RTE_LOGTYPE_QUEUE_INDEX queue_index_logtype

#define QI_MAX_WINDOW 64
#define QI_SAMPLE_COST_NS 100
#define QI_LCORE_UNSET (-1)
#define QI_LCORE_AUTO (-2)

enum qi_sched {
	QI_SCHED_SPIN = 0,
	QI_SCHED_SLEEP,
	QI_SCHED_HYBRID,
};

struct qi_global_conf {
	struct rte_qi_conf ring;
	uint32_t min_interval_us;
	uint32_t spin_us;
	uint32_t budget_pct;
	uint32_t log_rate;
	int lcore;
	enum qi_sched sched;
	uint8_t stagger;
	uint8_t log_enabled;
};

struct qi_sample {
	uint64_t tsc;
	uint32_t enq_total;
	uint32_t deq_total;
	uint32_t count_lo;
	uint32_t count_hi;
};

struct qi_slot;

/**
 * Predictor: derive the net accumulation rate (in packets per second) and
 * the estimated time to full (in microseconds) from the sample window.
 */
typedef void (*qi_predict_fn)(struct qi_slot *slot, uint64_t now,
			      uint32_t count_hi, uint64_t *net_pps,
			      uint64_t *eta_us);

struct qi_slot {
	RTE_ATOMIC(uint32_t) seq;
	RTE_ATOMIC(uint32_t) in_use;
	void *queue;
	const struct rte_qi_queue_ops *ops;
	uint32_t capacity;
	char name[RTE_RING_NAMESIZE];
	struct rte_qi_conf conf;
	uint64_t interval_tsc;
	qi_predict_fn predict;
	int64_t net_ewma;
	struct qi_sample win[QI_MAX_WINDOW];
	uint32_t win_head;
	uint32_t win_cnt;
	uint32_t prev_state;
	uint32_t trend_hits;
	uint32_t recovery_hits;
	uint64_t enter_state_ns;
	uint64_t next_due_tsc;
	uint64_t alarm_count;
	uint64_t warn_ts_ns;
	uint64_t last_log_tsc;
	struct rte_qi_ring_state pub;
} __rte_cache_aligned;

struct qi_monitor {
	RTE_ATOMIC(uint32_t) stop;
	RTE_ATOMIC(uint32_t) running;
	RTE_ATOMIC(uint64_t) generation;
	RTE_ATOMIC(uint64_t) ticks;
	RTE_ATOMIC(uint64_t) missed_ticks;
	RTE_ATOMIC(uint64_t) sample_errors;
	RTE_ATOMIC(uint64_t) late_ticks;
	RTE_ATOMIC(uint64_t) last_tick_ns;
	uint32_t lcore;
	enum qi_sched sched;
	uint64_t tsc_hz;
	uint64_t interval_tsc;
	uint64_t spin_tsc;
	uint64_t base_tsc;
	uint64_t base_ns;
};

static struct qi_global_conf qi_conf;
static struct qi_slot *qi_slots;
static struct qi_monitor qi_mon;
static rte_rwlock_t qi_lock;
static uint32_t qi_active[RTE_QI_MAX_RINGS];
static uint32_t qi_nactive;
static int qi_initialized;
static uint64_t qi_last_log_tsc;

static const char * const qi_valid_keys[] = {
	"lcore",
	"sched",
	"interval_us",
	"min_interval_us",
	"spin_us",
	"window",
	"high_wm",
	"low_wm",
	"warn_horizon_us",
	"clear_horizon_us",
	"confirm_samples",
	"min_net_pps",
	"hold_us",
	"predictor",
	"stagger",
	"budget_pct",
	"log",
	"log_rate",
	NULL,
};

static uint64_t
qi_us_to_tsc(uint64_t us)
{
	uint64_t tsc = us * qi_mon.tsc_hz / 1000000ULL;

	return tsc == 0 ? 1 : tsc;
}

static uint64_t
qi_tsc_to_ns(uint64_t tsc)
{
	uint64_t delta;

	if (tsc < qi_mon.base_tsc)
		tsc = qi_mon.base_tsc;

	delta = tsc - qi_mon.base_tsc;
	return qi_mon.base_ns +
	       (delta / qi_mon.tsc_hz) * 1000000000ULL +
	       ((delta % qi_mon.tsc_hz) * 1000000000ULL) / qi_mon.tsc_hz;
}

static uint64_t
qi_rate(uint32_t items, uint64_t delta_tsc)
{
	double rate;

	if (delta_tsc == 0)
		return 0;

	rate = (double)items * (double)qi_mon.tsc_hz / (double)delta_tsc;
	if (rate >= 1.8e19)
		return UINT64_MAX;

	return (uint64_t)rate;
}

static void
qi_name_set(char *dst, const char *src)
{
	snprintf(dst, RTE_RING_NAMESIZE, "%s", src);
}

static int
qi_parse_u32(const char *value, uint32_t *out)
{
	char *end = NULL;
	unsigned long long v;

	errno = 0;
	v = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' || v > UINT32_MAX)
		return -EINVAL;

	*out = (uint32_t)v;
	return 0;
}

static int
qi_parse_u64(const char *value, uint64_t *out)
{
	char *end = NULL;
	unsigned long long v;

	errno = 0;
	v = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0')
		return -EINVAL;

	*out = (uint64_t)v;
	return 0;
}

static int
qi_parse_bool(const char *value, uint8_t *out)
{
	uint32_t v;

	if (qi_parse_u32(value, &v) < 0 || v > 1)
		return -EINVAL;

	*out = (uint8_t)v;
	return 0;
}

static int
qi_parse_param(const char *key, const char *value, void *opaque)
{
	struct qi_global_conf *conf = opaque;
	uint32_t v32;
	uint64_t v64;

	if (strcmp(key, "lcore") == 0) {
		if (strcmp(value, "auto") == 0) {
			conf->lcore = QI_LCORE_AUTO;
			return 0;
		}
		if (qi_parse_u32(value, &v32) < 0 || v32 >= RTE_MAX_LCORE)
			return -EINVAL;
		conf->lcore = (int)v32;
		return 0;
	}

	if (strcmp(key, "sched") == 0) {
		if (strcmp(value, "spin") == 0)
			conf->sched = QI_SCHED_SPIN;
		else if (strcmp(value, "sleep") == 0)
			conf->sched = QI_SCHED_SLEEP;
		else if (strcmp(value, "hybrid") == 0)
			conf->sched = QI_SCHED_HYBRID;
		else
			return -EINVAL;
		return 0;
	}

	if (strcmp(key, "interval_us") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.interval_us = v32;
		return 0;
	}

	if (strcmp(key, "min_interval_us") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->min_interval_us = v32;
		return 0;
	}

	if (strcmp(key, "spin_us") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->spin_us = v32;
		return 0;
	}

	if (strcmp(key, "window") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.window = v32;
		return 0;
	}

	if (strcmp(key, "high_wm") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.high_wm = v32;
		return 0;
	}

	if (strcmp(key, "low_wm") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.low_wm = v32;
		return 0;
	}

	if (strcmp(key, "warn_horizon_us") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.warn_horizon_us = v32;
		return 0;
	}

	if (strcmp(key, "clear_horizon_us") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.clear_horizon_us = v32;
		return 0;
	}

	if (strcmp(key, "confirm_samples") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.confirm_samples = v32;
		return 0;
	}

	if (strcmp(key, "min_net_pps") == 0) {
		if (qi_parse_u64(value, &v64) < 0)
			return -EINVAL;
		conf->ring.min_net_pps = v64;
		return 0;
	}

	if (strcmp(key, "hold_us") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->ring.hold_us = v32;
		return 0;
	}

	if (strcmp(key, "predictor") == 0) {
		if (strcmp(value, "peak") == 0)
			conf->ring.predictor = RTE_QI_PRED_PEAK;
		else if (strcmp(value, "ewma") == 0)
			conf->ring.predictor = RTE_QI_PRED_EWMA;
		else if (strcmp(value, "linreg") == 0)
			conf->ring.predictor = RTE_QI_PRED_LINREG;
		else
			return -EINVAL;
		return 0;
	}

	if (strcmp(key, "stagger") == 0)
		return qi_parse_bool(value, &conf->stagger);

	if (strcmp(key, "budget_pct") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->budget_pct = v32;
		return 0;
	}

	if (strcmp(key, "log") == 0)
		return qi_parse_bool(value, &conf->log_enabled);

	if (strcmp(key, "log_rate") == 0) {
		if (qi_parse_u32(value, &v32) < 0)
			return -EINVAL;
		conf->log_rate = v32;
		return 0;
	}

	return -EINVAL;
}

static int
qi_check_conf(const struct rte_qi_conf *conf)
{
	if (conf->high_wm < 1 || conf->high_wm > 99)
		return -EINVAL;
	if (conf->low_wm > 98 || conf->low_wm >= conf->high_wm)
		return -EINVAL;
	if (conf->warn_horizon_us == 0 || conf->warn_horizon_us > 1000000)
		return -EINVAL;
	if (conf->clear_horizon_us <= conf->warn_horizon_us)
		return -EINVAL;
	if (conf->interval_us == 0 ||
	    conf->interval_us < qi_conf.min_interval_us ||
	    conf->interval_us > 1000000)
		return -EINVAL;
	if (conf->window < 2 || conf->window > QI_MAX_WINDOW)
		return -EINVAL;
	if (conf->confirm_samples < 1 || conf->confirm_samples > QI_MAX_WINDOW)
		return -EINVAL;
	if (conf->predictor < RTE_QI_PRED_PEAK ||
	    conf->predictor > RTE_QI_PRED_LINREG)
		return -EINVAL;

	return 0;
}

static void
qi_defaults(void)
{
	memset(&qi_conf, 0, sizeof(qi_conf));
	qi_conf.ring = (struct rte_qi_conf) {
		.high_wm = 50,
		.low_wm = 25,
		.warn_horizon_us = 150,
		.clear_horizon_us = 300,
		.interval_us = 10,
		.window = 8,
		.confirm_samples = 2,
		.hold_us = 0,
		.min_net_pps = 100000,
		.predictor = RTE_QI_PRED_PEAK,
	};
	qi_conf.min_interval_us = 5;
	qi_conf.spin_us = 0;
	qi_conf.budget_pct = 50;
	qi_conf.log_rate = 10;
	qi_conf.lcore = QI_LCORE_UNSET;
	qi_conf.sched = QI_SCHED_SPIN;
	qi_conf.stagger = 1;
	qi_conf.log_enabled = 1;
}

static void
qi_fill_conf(struct rte_qi_conf *dst, const struct rte_qi_conf *src)
{
	*dst = qi_conf.ring;

	if (src == NULL)
		return;

	if (src->high_wm != 0)
		dst->high_wm = src->high_wm;
	if (src->low_wm != 0)
		dst->low_wm = src->low_wm;
	if (src->warn_horizon_us != 0)
		dst->warn_horizon_us = src->warn_horizon_us;
	if (src->clear_horizon_us != 0)
		dst->clear_horizon_us = src->clear_horizon_us;
	if (src->interval_us != 0)
		dst->interval_us = src->interval_us;
	if (src->window != 0)
		dst->window = src->window;
	if (src->confirm_samples != 0)
		dst->confirm_samples = src->confirm_samples;
	if (src->hold_us != 0)
		dst->hold_us = src->hold_us;
	if (src->min_net_pps != 0)
		dst->min_net_pps = src->min_net_pps;
	if (src->predictor != 0)
		dst->predictor = src->predictor;
}

static void
qi_generation_bump(void)
{
	uint64_t gen = rte_atomic_load_explicit(&qi_mon.generation,
						rte_memory_order_relaxed);

	rte_atomic_store_explicit(&qi_mon.generation, gen + 1,
				  rte_memory_order_release);
}

static void
qi_publish(struct qi_slot *slot, const struct rte_qi_ring_state *st)
{
	uint32_t seq = rte_atomic_load_explicit(&slot->seq,
						rte_memory_order_relaxed);

	rte_atomic_store_explicit(&slot->seq, seq + 1,
				  rte_memory_order_relaxed);
	slot->pub = *st;
	rte_atomic_store_explicit(&slot->seq, seq + 2,
				  rte_memory_order_release);
}

static int
qi_read_slot(struct qi_slot *slot, struct rte_qi_ring_state *st)
{
	uint32_t s1, s2;

	for (;;) {
		s1 = rte_atomic_load_explicit(&slot->seq,
					      rte_memory_order_acquire);
		if ((s1 & 1u) != 0)
			continue;

		if (rte_atomic_load_explicit(&slot->in_use,
					     rte_memory_order_acquire) == 0)
			return -ENOENT;

		*st = slot->pub;

		s2 = rte_atomic_load_explicit(&slot->seq,
					      rte_memory_order_acquire);
		if (s1 == s2)
			return 0;
	}
}

/*
 * Provider: local rte_ring.
 *
 * Reads the producer/consumer cursors of a plain rte_ring, including the
 * HTS variant. The modulo uses the ring mask because rte_ring cursors
 * wrap at the ring size; queues whose cursors do not wrap mask their own
 * values and return them through their usage() callback instead.
 */
static int
qi_rte_ring_usage(void *queue, struct rte_qi_usage *u)
{
	struct rte_ring *ring = queue;
	uint32_t prod_head, prod_tail, cons_tail;
	enum rte_ring_sync_type prod_sync = rte_ring_get_prod_sync_type(ring);
	enum rte_ring_sync_type cons_sync = rte_ring_get_cons_sync_type(ring);

	if (prod_sync == RTE_RING_SYNC_MT_HTS) {
		prod_head = rte_atomic_load_explicit(
				&ring->hts_prod.ht.pos.head,
				rte_memory_order_relaxed);
		prod_tail = rte_atomic_load_explicit(
				&ring->hts_prod.ht.pos.tail,
				rte_memory_order_relaxed);
	} else {
		prod_head = rte_atomic_load_explicit(
				&ring->prod.head, rte_memory_order_relaxed);
		prod_tail = rte_atomic_load_explicit(
				&ring->prod.tail, rte_memory_order_relaxed);
	}

	if (cons_sync == RTE_RING_SYNC_MT_HTS)
		cons_tail = rte_atomic_load_explicit(
				&ring->hts_cons.ht.pos.tail,
				rte_memory_order_relaxed);
	else
		cons_tail = rte_atomic_load_explicit(
				&ring->cons.tail, rte_memory_order_relaxed);

	u->capacity = rte_ring_get_capacity(ring);
	u->count_lo = (prod_tail - cons_tail) & ring->mask;
	u->count_hi = (prod_head - cons_tail) & ring->mask;
	u->enq_total = prod_tail;
	u->deq_total = cons_tail;

	if (u->count_lo > u->capacity)
		u->count_lo = u->capacity;
	if (u->count_hi > u->capacity)
		u->count_hi = u->capacity;

	return 0;
}

static int
qi_rte_ring_check(void *queue)
{
	struct rte_ring *ring = queue;

	if (rte_ring_get_prod_sync_type(ring) == RTE_RING_SYNC_MT_RTS ||
	    rte_ring_get_cons_sync_type(ring) == RTE_RING_SYNC_MT_RTS) {
		RTE_LOG(ERR, QUEUE_INDEX, "RTS sync mode is not supported\n");
		return -ENOTSUP;
	}

	return 0;
}

static const struct rte_qi_queue_ops qi_rte_ring_ops = {
	.name = "rte_ring",
	.check = qi_rte_ring_check,
	.usage = qi_rte_ring_usage,
	.cost_ns = QI_SAMPLE_COST_NS,
};

static void
qi_compute_rates(const struct qi_slot *slot, uint64_t now,
		 uint64_t *enq_pps, uint64_t *deq_pps, uint32_t *peak_hi)
{
	const struct qi_sample *win = slot->win;
	uint32_t window = slot->conf.window;
	uint32_t count = slot->win_cnt;
	uint32_t first, last, i;
	uint64_t span;

	*enq_pps = 0;
	*deq_pps = 0;
	*peak_hi = 0;

	if (count == 0)
		return;

	last = (slot->win_head + window - 1) % window;
	first = (slot->win_head + window - count) % window;

	*peak_hi = win[first].count_hi;
	for (i = 1; i < count; i++) {
		uint32_t cur = (first + i) % window;

		if (win[cur].count_hi > *peak_hi)
			*peak_hi = win[cur].count_hi;
	}

	if (count < 2)
		return;

	span = now - win[first].tsc;
	if (span == 0)
		return;

	*enq_pps = qi_rate(win[last].enq_total - win[first].enq_total, span);
	*deq_pps = qi_rate(win[last].deq_total - win[first].deq_total, span);
}

/**
 * Common ETA finishing rule shared by all predictors:
 * a full ring has eta 0, a net rate below min_net_pps yields no prediction.
 */
static void
qi_finish_eta(const struct qi_slot *slot, uint32_t count_hi,
	      uint64_t net_pps, uint64_t *eta_us)
{
	uint32_t free_slots = slot->capacity > count_hi ?
			      slot->capacity - count_hi : 0;

	*eta_us = RTE_QI_ETA_INFINITE;
	if (free_slots == 0)
		*eta_us = 0;
	else if (net_pps >= slot->conf.min_net_pps && net_pps > 0)
		*eta_us = ((uint64_t)free_slots * 1000000ULL) / net_pps;
}

/**
 * PEAK predictor: maximum of the per-interval net rates over the window.
 * Fastest response to a sudden consumer stall; conservative under bursts.
 */
static void
qi_predict_peak(struct qi_slot *slot, uint64_t now __rte_unused,
		uint32_t count_hi, uint64_t *net_pps, uint64_t *eta_us)
{
	const struct qi_sample *win = slot->win;
	uint32_t window = slot->conf.window;
	uint32_t count = slot->win_cnt;
	uint32_t first, i;
	uint64_t peak_net = 0;

	if (count >= 2) {
		first = (slot->win_head + window - count) % window;
		for (i = 1; i < count; i++) {
			uint32_t prev = (first + i - 1) % window;
			uint32_t cur = (first + i) % window;
			uint64_t delta_tsc = win[cur].tsc - win[prev].tsc;
			uint64_t enq, deq, net;

			if (delta_tsc == 0)
				continue;

			enq = qi_rate(win[cur].enq_total - win[prev].enq_total,
				      delta_tsc);
			deq = qi_rate(win[cur].deq_total - win[prev].deq_total,
				      delta_tsc);
			net = enq > deq ? enq - deq : 0;

			if (net > peak_net)
				peak_net = net;
		}
	}

	*net_pps = peak_net;
	qi_finish_eta(slot, count_hi, peak_net, eta_us);
}

/**
 * EWMA predictor: exponentially weighted moving average of the net rate
 * with a fixed smoothing factor of 1/2 (state kept in the slot).
 */
static void
qi_predict_ewma(struct qi_slot *slot, uint64_t now __rte_unused,
		uint32_t count_hi, uint64_t *net_pps, uint64_t *eta_us)
{
	const struct qi_sample *win = slot->win;
	uint32_t window = slot->conf.window;
	uint32_t count = slot->win_cnt;
	int64_t net_now = 0;

	if (count >= 2) {
		uint32_t last = (slot->win_head + window - 1) % window;
		uint32_t prev = (slot->win_head + window - 2) % window;
		uint64_t delta_tsc = win[last].tsc - win[prev].tsc;

		if (delta_tsc > 0) {
			int64_t enq = (int64_t)qi_rate(
				win[last].enq_total - win[prev].enq_total,
				delta_tsc);
			int64_t deq = (int64_t)qi_rate(
				win[last].deq_total - win[prev].deq_total,
				delta_tsc);

			net_now = enq - deq;
		}
	}

	/* ewma = alpha * net_now + (1 - alpha) * ewma, with alpha = 1/2 */
	slot->net_ewma = (slot->net_ewma + net_now) / 2;

	*net_pps = slot->net_ewma > 0 ? (uint64_t)slot->net_ewma : 0;
	qi_finish_eta(slot, count_hi, *net_pps, eta_us);
}

/**
 * LINREG predictor: least-squares slope of the published occupancy
 * (count_lo) over the window, in items per second.
 * Best for slow drift; requires at least 3 samples.
 */
static void
qi_predict_linreg(struct qi_slot *slot, uint64_t now __rte_unused,
		  uint32_t count_hi, uint64_t *net_pps, uint64_t *eta_us)
{
	const struct qi_sample *win = slot->win;
	uint32_t window = slot->conf.window;
	uint32_t count = slot->win_cnt;
	uint32_t first, i;
	double sx = 0.0, sy = 0.0, sxx = 0.0, sxy = 0.0;
	double denom, slope, net;

	*net_pps = 0;
	*eta_us = RTE_QI_ETA_INFINITE;

	if (count < 3)
		return;

	first = (slot->win_head + window - count) % window;

	/* x: elapsed cycles since the first sample,
	 * y: published occupancy of that sample. */
	for (i = 0; i < count; i++) {
		uint32_t idx = (first + i) % window;
		double x = (double)(win[idx].tsc - win[first].tsc);
		double y = (double)win[idx].count_lo;

		sx += x;
		sy += y;
		sxx += x * x;
		sxy += x * y;
	}

	denom = (double)count * sxx - sx * sx;
	if (denom <= 0.0)
		return;

	slope = ((double)count * sxy - sx * sy) / denom; /* items per cycle */
	net = slope * (double)qi_mon.tsc_hz;              /* items per second */
	if (net <= 0.0 || net >= 1.8e19)
		return;

	*net_pps = (uint64_t)net;
	qi_finish_eta(slot, count_hi, *net_pps, eta_us);
}

static const qi_predict_fn qi_predictors[RTE_QI_PRED_LINREG + 1] = {
	[RTE_QI_PRED_PEAK] = qi_predict_peak,
	[RTE_QI_PRED_EWMA] = qi_predict_ewma,
	[RTE_QI_PRED_LINREG] = qi_predict_linreg,
};

static bool
qi_log_allowed(struct qi_slot *slot, uint64_t now)
{
	uint64_t global_gap;

	if (!qi_conf.log_enabled || qi_conf.log_rate == 0)
		return false;

	global_gap = qi_mon.tsc_hz / qi_conf.log_rate;
	if (now - qi_last_log_tsc < global_gap)
		return false;
	if (now - slot->last_log_tsc < qi_mon.tsc_hz)
		return false;

	qi_last_log_tsc = now;
	slot->last_log_tsc = now;
	return true;
}

static void
qi_log_transition(struct qi_slot *slot, uint64_t now,
		  enum rte_qi_state prev, const struct rte_qi_ring_state *st)
{
	if (!qi_log_allowed(slot, now))
		return;

	if (st->state == RTE_QI_CRITICAL)
		RTE_LOG(ERR, QUEUE_INDEX,
			"ring=%s state=CRITICAL prev=%d reasons=0x%x "
			"count=%"PRIu32"/%"PRIu32" enq=%"PRIu64"pps "
			"deq=%"PRIu64"pps net=%"PRIu64"pps eta=%"PRIu64"us\n",
			st->name, prev, st->reasons, st->count_hi, st->capacity,
			st->enq_pps, st->deq_pps, st->net_pps, st->eta_us);
	else if (st->state == RTE_QI_WARN)
		RTE_LOG(WARNING, QUEUE_INDEX,
			"ring=%s state=WARN prev=%d reasons=0x%x "
			"count=%"PRIu32"/%"PRIu32" enq=%"PRIu64"pps "
			"deq=%"PRIu64"pps net=%"PRIu64"pps eta=%"PRIu64"us\n",
			st->name, prev, st->reasons, st->count_hi, st->capacity,
			st->enq_pps, st->deq_pps, st->net_pps, st->eta_us);
	else
		RTE_LOG(NOTICE, QUEUE_INDEX,
			"ring=%s state=OK prev=%d count=%"PRIu32"/%"PRIu32"\n",
			st->name, prev, st->count_hi, st->capacity);
}

static void
qi_sample_slot(struct qi_slot *slot, uint64_t now)
{
	struct rte_qi_ring_state st;
	struct rte_qi_usage usage;
	struct qi_sample *sm;
	uint32_t capacity, count_lo, count_hi, peak_hi;
	uint32_t reasons = 0, prev_state, next_state;
	uint64_t enq_pps = 0, deq_pps = 0, net_pps;
	uint64_t eta_us, now_ns;
	bool level, trend, full, recovery;

	if (slot->ops->usage(slot->queue, &usage) != 0) {
		/*
		 * The provider could not be observed this tick. Keep the last
		 * published snapshot instead of publishing a half-filled one,
		 * so a transient backend failure does not look like a state
		 * change or a recovery.
		 */
		rte_atomic_store_explicit(
			&qi_mon.sample_errors,
			rte_atomic_load_explicit(&qi_mon.sample_errors,
						 rte_memory_order_relaxed) + 1,
			rte_memory_order_relaxed);
		return;
	}

	capacity = usage.capacity;
	slot->capacity = capacity;
	count_lo = usage.count_lo > capacity ? capacity : usage.count_lo;
	count_hi = usage.count_hi > capacity ? capacity : usage.count_hi;
	if (count_hi < count_lo)
		count_hi = count_lo;

	sm = &slot->win[slot->win_head];
	sm->tsc = now;
	sm->enq_total = usage.enq_total;
	sm->deq_total = usage.deq_total;
	sm->count_lo = count_lo;
	sm->count_hi = count_hi;
	slot->win_head = (slot->win_head + 1) % slot->conf.window;
	if (slot->win_cnt < slot->conf.window)
		slot->win_cnt++;

	qi_compute_rates(slot, now, &enq_pps, &deq_pps, &peak_hi);

	/* net_pps and eta_us come from the selected predictor. */
	slot->predict(slot, now, count_hi, &net_pps, &eta_us);

	level = capacity > 0 &&
		(uint64_t)count_hi * 100 >=
		(uint64_t)slot->conf.high_wm * capacity;
	full = capacity > 0 && count_hi >= capacity;
	trend = eta_us != RTE_QI_ETA_INFINITE &&
		eta_us <= slot->conf.warn_horizon_us;

	if (level)
		reasons |= RTE_QI_REASON_LEVEL;
	if (trend)
		reasons |= RTE_QI_REASON_TREND;
	if (full)
		reasons |= RTE_QI_REASON_FULL;

	prev_state = slot->prev_state;
	if (prev_state == RTE_QI_UNKNOWN)
		prev_state = RTE_QI_OK;
	next_state = prev_state;

	if (full) {
		next_state = RTE_QI_CRITICAL;
		slot->trend_hits = 0;
		slot->recovery_hits = 0;
	} else if (level) {
		next_state = RTE_QI_WARN;
		slot->trend_hits = 0;
		slot->recovery_hits = 0;
	} else if (trend) {
		slot->recovery_hits = 0;
		if (prev_state >= RTE_QI_WARN) {
			next_state = RTE_QI_WARN;
		} else {
			slot->trend_hits++;
			if (slot->trend_hits >= slot->conf.confirm_samples)
				next_state = RTE_QI_WARN;
		}
	} else {
		slot->trend_hits = 0;
		if (prev_state >= RTE_QI_WARN) {
			now_ns = qi_tsc_to_ns(now);
			recovery = (uint64_t)count_lo * 100 <=
					   (uint64_t)slot->conf.low_wm * capacity &&
				   eta_us > slot->conf.clear_horizon_us &&
				   (now_ns < slot->enter_state_ns ||
				    now_ns - slot->enter_state_ns >=
					    (uint64_t)slot->conf.hold_us);
			if (recovery)
				slot->recovery_hits++;
			else
				slot->recovery_hits = 0;

			if (slot->recovery_hits >= slot->conf.confirm_samples)
				next_state = RTE_QI_OK;
		}
	}

	now_ns = qi_tsc_to_ns(now);

	memset(&st, 0, sizeof(st));
	qi_name_set(st.name, slot->name);
	st.state = next_state;
	st.reasons = reasons;
	st.capacity = capacity;
	st.count_lo = count_lo;
	st.count_hi = count_hi;
	st.peak_hi = peak_hi;
	st.enq_pps = enq_pps;
	st.deq_pps = deq_pps;
	st.net_pps = net_pps;
	st.eta_us = eta_us;
	st.ts_ns = now_ns;

	if (next_state != slot->prev_state) {
		if (next_state >= RTE_QI_WARN) {
			slot->alarm_count++;
			slot->warn_ts_ns = now_ns;
		}
		slot->enter_state_ns = now_ns;
		slot->prev_state = next_state;
		qi_generation_bump();
	}

	st.alarm_count = slot->alarm_count;
	st.warn_ts_ns = slot->warn_ts_ns;
	qi_publish(slot, &st);

	if (next_state != prev_state)
		qi_log_transition(slot, now, prev_state, &st);
}

static void qi_wait_until(uint64_t deadline);

static int
qi_monitor_loop(void *arg)
{
	uint64_t now, next, intv, late;
	uint32_t i;

	RTE_SET_USED(arg);

	while (rte_atomic_load_explicit(&qi_mon.stop,
					rte_memory_order_acquire) == 0) {
		now = rte_rdtsc();
		next = now + qi_mon.interval_tsc;

		rte_rwlock_read_lock(&qi_lock);
		for (i = 0; i < qi_nactive; i++) {
			struct qi_slot *slot = &qi_slots[qi_active[i]];

			if (now >= slot->next_due_tsc) {
				qi_sample_slot(slot, now);

				intv = slot->interval_tsc;
				if (now - slot->next_due_tsc >= intv) {
					uint64_t skip =
						(now - slot->next_due_tsc) / intv;
					slot->next_due_tsc += skip * intv;
					rte_atomic_store_explicit(
						&qi_mon.missed_ticks,
						rte_atomic_load_explicit(
							&qi_mon.missed_ticks,
							rte_memory_order_relaxed) +
							skip,
						rte_memory_order_relaxed);
				}
				slot->next_due_tsc += intv;
			}

			if (slot->next_due_tsc < next)
				next = slot->next_due_tsc;
		}
		rte_rwlock_read_unlock(&qi_lock);

		rte_atomic_store_explicit(&qi_mon.ticks,
					  rte_atomic_load_explicit(
						  &qi_mon.ticks,
						  rte_memory_order_relaxed) + 1,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&qi_mon.last_tick_ns, qi_tsc_to_ns(now),
					  rte_memory_order_relaxed);

		late = rte_rdtsc() - now;
		if (late > qi_mon.interval_tsc)
			rte_atomic_store_explicit(
				&qi_mon.late_ticks,
				rte_atomic_load_explicit(&qi_mon.late_ticks,
							 rte_memory_order_relaxed) + 1,
				rte_memory_order_relaxed);

		qi_wait_until(next);
	}

	return 0;
}

static void
qi_wait_until(uint64_t deadline)
{
	uint64_t now, remain, spin;

	if (deadline == 0)
		deadline = rte_rdtsc() + qi_mon.interval_tsc;

	switch (qi_conf.sched) {
	case QI_SCHED_SPIN:
		while (rte_atomic_load_explicit(&qi_mon.stop,
						rte_memory_order_acquire) == 0 &&
		       rte_rdtsc() < deadline)
			rte_pause();
		break;

	case QI_SCHED_SLEEP:
		now = rte_rdtsc();
		if (deadline > now) {
			remain = (deadline - now) * 1000000ULL / qi_mon.tsc_hz;
			if (remain > UINT32_MAX)
				remain = UINT32_MAX;
			if (remain > 0)
				rte_delay_us_sleep((unsigned int)remain);
		}
		while (rte_atomic_load_explicit(&qi_mon.stop,
						rte_memory_order_acquire) == 0 &&
		       rte_rdtsc() < deadline)
			rte_pause();
		break;

	case QI_SCHED_HYBRID:
	default:
		spin = qi_mon.spin_tsc;
		now = rte_rdtsc();
		if (deadline > now + spin) {
			remain = (deadline - now - spin) * 1000000ULL /
				 qi_mon.tsc_hz;
			if (remain > UINT32_MAX)
				remain = UINT32_MAX;
			if (remain > 0)
				rte_delay_us_sleep((unsigned int)remain);
		}
		while (rte_atomic_load_explicit(&qi_mon.stop,
						rte_memory_order_acquire) == 0 &&
		       rte_rdtsc() < deadline)
			rte_pause();
		break;
	}
}

static int
qi_find_slot(const char *name)
{
	uint32_t i;

	for (i = 0; i < qi_nactive; i++) {
		struct qi_slot *slot = &qi_slots[qi_active[i]];

		if (strncmp(slot->name, name, RTE_RING_NAMESIZE) == 0)
			return (int)qi_active[i];
	}

	return -1;
}

static int
qi_resolve_lcore(int *err)
{
	unsigned int i;
	int main_lcore;

	if (qi_conf.lcore == QI_LCORE_AUTO) {
		RTE_LCORE_FOREACH_WORKER(i) {
			if (rte_eal_get_lcore_state(i) == WAIT)
				return (int)i;
		}
		*err = -ENOENT;
		return -1;
	}

	if (qi_conf.lcore == QI_LCORE_UNSET) {
		*err = -ENOENT;
		return -1;
	}

	if (qi_conf.lcore < 0 || qi_conf.lcore >= RTE_MAX_LCORE ||
	    !rte_lcore_is_enabled((unsigned int)qi_conf.lcore)) {
		*err = -ENOENT;
		return -1;
	}

	main_lcore = (int)rte_get_main_lcore();
	if (qi_conf.lcore == main_lcore) {
		*err = -EINVAL;
		return -1;
	}

	if (rte_eal_get_lcore_state((unsigned int)qi_conf.lcore) != WAIT) {
		*err = -EBUSY;
		return -1;
	}

	return qi_conf.lcore;
}

static double
qi_estimate_cpu_pct(uint32_t n_rings, uint32_t cost_ns, uint32_t interval_us)
{
	return (double)n_rings * (double)cost_ns * 100.0 /
	       ((double)interval_us * 1000.0);
}

int
rte_qi_init(const char *kvargs)
{
	struct rte_kvargs *kvlist;
	struct timespec ts;
	int ret;
	uint32_t i;

	if (qi_initialized)
		return -EBUSY;

	qi_defaults();

	if (kvargs != NULL && kvargs[0] != '\0') {
		kvlist = rte_kvargs_parse(kvargs, qi_valid_keys);
		if (kvlist == NULL)
			return -EINVAL;

		ret = rte_kvargs_process(kvlist, NULL, qi_parse_param, &qi_conf);
		rte_kvargs_free(kvlist);
		if (ret < 0)
			return -EINVAL;
	}

	if (qi_check_conf(&qi_conf.ring) < 0) {
		RTE_LOG(ERR, QUEUE_INDEX,
			"invalid global configuration\n");
		return -EINVAL;
	}

	if (qi_conf.budget_pct < 1 || qi_conf.budget_pct > 100) {
		RTE_LOG(ERR, QUEUE_INDEX,
			"budget_pct must be in [1, 100]\n");
		return -EINVAL;
	}

	if (qi_conf.log_rate > 100000) {
		RTE_LOG(ERR, QUEUE_INDEX,
			"log_rate is too large\n");
		return -EINVAL;
	}

	qi_mon.tsc_hz = rte_get_tsc_hz();
	if (qi_mon.tsc_hz == 0) {
		RTE_LOG(ERR, QUEUE_INDEX, "invalid TSC frequency\n");
		return -EINVAL;
	}

	clock_gettime(CLOCK_MONOTONIC, &ts);
	qi_mon.base_tsc = rte_rdtsc();
	qi_mon.base_ns = (uint64_t)ts.tv_sec * 1000000000ULL +
			 (uint64_t)ts.tv_nsec;
	qi_mon.interval_tsc = qi_us_to_tsc(qi_conf.ring.interval_us);
	if (qi_conf.sched == QI_SCHED_HYBRID) {
		uint32_t spin_us = qi_conf.spin_us;

		if (spin_us == 0) {
			spin_us = qi_conf.ring.interval_us / 4;
			if (spin_us > 50)
				spin_us = 50;
		}
		qi_mon.spin_tsc = qi_us_to_tsc(spin_us);
	} else {
		qi_mon.spin_tsc = 0;
	}
	qi_mon.lcore = UINT32_MAX;
	qi_mon.sched = qi_conf.sched;
	rte_atomic_store_explicit(&qi_mon.stop, 0, rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.running, 0, rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.generation, 0,
				  rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.ticks, 0, rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.missed_ticks, 0,
				  rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.late_ticks, 0,
				  rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.last_tick_ns, 0,
				  rte_memory_order_relaxed);

	qi_slots = rte_zmalloc("queue_index_slots",
			       sizeof(struct qi_slot) * RTE_QI_MAX_RINGS,
			       RTE_CACHE_LINE_SIZE);
	if (qi_slots == NULL)
		return -ENOMEM;

	for (i = 0; i < RTE_QI_MAX_RINGS; i++) {
		rte_atomic_store_explicit(&qi_slots[i].seq, 0,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&qi_slots[i].in_use, 0,
					  rte_memory_order_relaxed);
	}

	rte_rwlock_init(&qi_lock);
	qi_nactive = 0;
	qi_last_log_tsc = 0;
	qi_initialized = 1;

	return 0;
}

int
rte_qi_fini(void)
{
	if (!qi_initialized)
		return -EINVAL;

	rte_qi_stop();

	rte_free(qi_slots);
	qi_slots = NULL;
	qi_nactive = 0;
	qi_initialized = 0;
	memset(&qi_conf, 0, sizeof(qi_conf));
	memset(&qi_mon, 0, sizeof(qi_mon));

	return 0;
}

int
rte_qi_start(void)
{
	uint32_t i, n_rings, k;
	uint64_t now;
	double pct;
	int lcore, err = -ENOENT;

	if (!qi_initialized)
		return -EINVAL;

	if (rte_atomic_load_explicit(&qi_mon.running,
				     rte_memory_order_acquire) != 0)
		return -EBUSY;

	lcore = qi_resolve_lcore(&err);
	if (lcore < 0) {
		RTE_LOG(ERR, QUEUE_INDEX,
			"no usable monitor lcore (err=%d)\n", err);
		return err;
	}

	qi_mon.interval_tsc = qi_us_to_tsc(qi_conf.ring.interval_us);
	now = rte_rdtsc();

	pct = 0;
	rte_rwlock_read_lock(&qi_lock);
	n_rings = qi_nactive;
	for (i = 0; i < qi_nactive; i++) {
		struct qi_slot *slot = &qi_slots[qi_active[i]];

		pct += qi_estimate_cpu_pct(1, slot->ops->cost_ns,
					   slot->conf.interval_us);
	}
	rte_rwlock_read_unlock(&qi_lock);

	if (pct > (double)qi_conf.budget_pct) {
		RTE_LOG(ERR, QUEUE_INDEX,
			"estimated monitor CPU %.1f%% exceeds budget %u%% "
			"(%u rings)\n",
			pct, qi_conf.budget_pct, n_rings);
		return -EINVAL;
	}

	k = 0;
	rte_rwlock_write_lock(&qi_lock);
	for (i = 0; i < qi_nactive; i++) {
		struct qi_slot *slot = &qi_slots[qi_active[i]];

		slot->interval_tsc = qi_us_to_tsc(slot->conf.interval_us);
		slot->next_due_tsc = now;
		if (qi_conf.stagger && n_rings > 1)
			slot->next_due_tsc +=
				(uint64_t)k * qi_mon.interval_tsc / n_rings;
		k++;
	}
	rte_rwlock_write_unlock(&qi_lock);

	qi_mon.lcore = (uint32_t)lcore;
	rte_atomic_store_explicit(&qi_mon.stop, 0, rte_memory_order_relaxed);
	rte_atomic_store_explicit(&qi_mon.running, 1,
				  rte_memory_order_release);

	if (rte_eal_remote_launch(qi_monitor_loop, NULL,
				  (unsigned int)lcore) != 0) {
		rte_atomic_store_explicit(&qi_mon.running, 0,
					  rte_memory_order_release);
		qi_mon.lcore = UINT32_MAX;
		RTE_LOG(ERR, QUEUE_INDEX,
			"failed to launch monitor on lcore %d\n", lcore);
		return -EBUSY;
	}

	RTE_LOG(INFO, QUEUE_INDEX,
		"monitor started on lcore %d, %u rings, interval %u us, "
		"estimated CPU %.1f%%\n",
		lcore, n_rings, qi_conf.ring.interval_us, pct);

	return 0;
}

int
rte_qi_stop(void)
{
	if (!qi_initialized)
		return -EINVAL;

	if (rte_atomic_load_explicit(&qi_mon.running,
				     rte_memory_order_acquire) == 0)
		return 0;

	if (rte_lcore_id() == qi_mon.lcore) {
		RTE_LOG(ERR, QUEUE_INDEX,
			"cannot stop the monitor from its own lcore\n");
		return -EINVAL;
	}

	rte_atomic_store_explicit(&qi_mon.stop, 1, rte_memory_order_release);
	rte_eal_wait_lcore((unsigned int)qi_mon.lcore);
	rte_atomic_store_explicit(&qi_mon.running, 0,
				  rte_memory_order_release);
	qi_mon.lcore = UINT32_MAX;

	return 0;
}

int
rte_qi_ring_register(const char *ring_name, const struct rte_qi_conf *conf)
{
	struct rte_ring *ring;

	if (ring_name == NULL)
		return -EINVAL;

	if (!qi_initialized)
		return -EINVAL;

	ring = rte_ring_lookup(ring_name);
	if (ring == NULL)
		return -ENOENT;

	return rte_qi_ring_register_ptr(ring, ring_name, conf);
}

int
rte_qi_queue_register(void *queue, const struct rte_qi_queue_ops *ops,
		      const char *alias, const struct rte_qi_conf *conf)
{
	struct rte_qi_conf merged;
	struct rte_qi_usage usage;
	const char *name;
	uint32_t i;
	int free_idx = -1;

	if (!qi_initialized || queue == NULL || ops == NULL ||
	    ops->usage == NULL || alias == NULL)
		return -EINVAL;

	if (ops->check != NULL) {
		int ret = ops->check(queue);

		if (ret != 0)
			return ret;
	}

	name = alias;
	if (name[0] == '\0' || strlen(name) >= RTE_RING_NAMESIZE)
		return -EINVAL;

	qi_fill_conf(&merged, conf);
	if (qi_check_conf(&merged) < 0)
		return -EINVAL;

	/* One probe now fixes the capacity and rejects unusable queues
	 * before they enter the registry. */
	if (ops->usage(queue, &usage) != 0 || usage.capacity == 0)
		return -EINVAL;

	rte_rwlock_write_lock(&qi_lock);
	for (i = 0; i < RTE_QI_MAX_RINGS; i++) {
		struct qi_slot *slot = &qi_slots[i];

		if (rte_atomic_load_explicit(&slot->in_use,
					     rte_memory_order_acquire) == 0) {
			if (free_idx < 0)
				free_idx = (int)i;
			continue;
		}

		if (slot->queue == queue ||
		    strncmp(slot->name, name, RTE_RING_NAMESIZE) == 0) {
			rte_rwlock_write_unlock(&qi_lock);
			return -EEXIST;
		}
	}

	if (free_idx < 0) {
		rte_rwlock_write_unlock(&qi_lock);
		return -ENOSPC;
	}

	{
		struct qi_slot *slot = &qi_slots[free_idx];

		memset(slot, 0, sizeof(*slot));
		slot->queue = queue;
		slot->ops = ops;
		slot->capacity = usage.capacity;
		qi_name_set(slot->name, name);
		slot->conf = merged;
		slot->interval_tsc = qi_us_to_tsc(merged.interval_us);
		slot->predict = qi_predictors[merged.predictor];
		slot->prev_state = RTE_QI_UNKNOWN;
		slot->enter_state_ns = qi_tsc_to_ns(rte_rdtsc());
		slot->next_due_tsc = rte_rdtsc();

		memset(&slot->pub, 0, sizeof(slot->pub));
		qi_name_set(slot->pub.name, name);
		slot->pub.state = RTE_QI_UNKNOWN;
		slot->pub.capacity = usage.capacity;
		slot->pub.eta_us = RTE_QI_ETA_INFINITE;

		rte_atomic_store_explicit(&slot->seq, 0,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&slot->in_use, 1,
					  rte_memory_order_release);
	}

	qi_active[qi_nactive++] = (uint32_t)free_idx;
	rte_rwlock_write_unlock(&qi_lock);

	return 0;
}

int
rte_qi_ring_register_ptr(struct rte_ring *ring, const char *alias,
			 const struct rte_qi_conf *conf)
{
	if (ring == NULL)
		return -EINVAL;

	return rte_qi_queue_register(ring, &qi_rte_ring_ops,
				     alias != NULL ? alias : ring->name, conf);
}

int
rte_qi_ring_unregister(const char *ring_name)
{
	uint32_t i;
	int slot_idx;

	if (!qi_initialized || ring_name == NULL)
		return -EINVAL;

	rte_rwlock_write_lock(&qi_lock);
	slot_idx = qi_find_slot(ring_name);
	if (slot_idx < 0) {
		rte_rwlock_write_unlock(&qi_lock);
		return -ENOENT;
	}

	{
		struct qi_slot *slot = &qi_slots[slot_idx];
		uint32_t seq = rte_atomic_load_explicit(&slot->seq,
							rte_memory_order_relaxed);

		rte_atomic_store_explicit(&slot->seq, seq + 1,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&slot->in_use, 0,
					  rte_memory_order_relaxed);
		rte_atomic_store_explicit(&slot->seq, seq + 2,
					  rte_memory_order_release);
	}

	for (i = 0; i < qi_nactive; i++) {
		if (qi_active[i] == (uint32_t)slot_idx) {
			qi_active[i] = qi_active[--qi_nactive];
			break;
		}
	}
	rte_rwlock_write_unlock(&qi_lock);

	return 0;
}

int
rte_qi_ring_handle_get(const char *ring_name, uint32_t *idx)
{
	int slot_idx;

	if (!qi_initialized || ring_name == NULL || idx == NULL)
		return -EINVAL;

	rte_rwlock_read_lock(&qi_lock);
	slot_idx = qi_find_slot(ring_name);
	rte_rwlock_read_unlock(&qi_lock);

	if (slot_idx < 0)
		return -ENOENT;

	*idx = (uint32_t)slot_idx;
	return 0;
}

int
rte_qi_state_get_by_idx(uint32_t idx, struct rte_qi_ring_state *st)
{
	if (!qi_initialized || st == NULL || idx >= RTE_QI_MAX_RINGS)
		return -EINVAL;

	return qi_read_slot(&qi_slots[idx], st);
}

int
rte_qi_state_get(const char *ring_name, struct rte_qi_ring_state *st)
{
	int slot_idx;

	if (!qi_initialized || ring_name == NULL || st == NULL)
		return -EINVAL;

	rte_rwlock_read_lock(&qi_lock);
	slot_idx = qi_find_slot(ring_name);
	rte_rwlock_read_unlock(&qi_lock);

	if (slot_idx < 0)
		return -ENOENT;

	return qi_read_slot(&qi_slots[slot_idx], st);
}

int
rte_qi_state_list_warn(struct rte_qi_ring_state *arr, uint16_t max)
{
	uint32_t i;
	int count = 0;

	if (!qi_initialized || arr == NULL)
		return -EINVAL;

	rte_rwlock_read_lock(&qi_lock);
	for (i = 0; i < qi_nactive && count < max; i++) {
		struct rte_qi_ring_state st;

		if (qi_read_slot(&qi_slots[qi_active[i]], &st) < 0)
			continue;
		if (st.state < RTE_QI_WARN)
			continue;

		arr[count++] = st;
	}
	rte_rwlock_read_unlock(&qi_lock);

	return count;
}

uint64_t
rte_qi_generation(void)
{
	return rte_atomic_load_explicit(&qi_mon.generation,
					rte_memory_order_acquire);
}

int
rte_qi_monitor_status(struct rte_qi_monitor_status *st)
{
	if (!qi_initialized || st == NULL)
		return -EINVAL;

	memset(st, 0, sizeof(*st));

	rte_rwlock_read_lock(&qi_lock);
	st->registered = qi_nactive;
	rte_rwlock_read_unlock(&qi_lock);

	st->running = rte_atomic_load_explicit(&qi_mon.running,
					       rte_memory_order_acquire);
	st->lcore = st->running != 0 ? qi_mon.lcore : UINT32_MAX;
	st->ticks = rte_atomic_load_explicit(&qi_mon.ticks,
					     rte_memory_order_relaxed);
	st->missed_ticks = rte_atomic_load_explicit(&qi_mon.missed_ticks,
						    rte_memory_order_relaxed);
	st->sample_errors = rte_atomic_load_explicit(&qi_mon.sample_errors,
						     rte_memory_order_relaxed);
	st->late_ticks = rte_atomic_load_explicit(&qi_mon.late_ticks,
						  rte_memory_order_relaxed);
	st->last_tick_ns = rte_atomic_load_explicit(&qi_mon.last_tick_ns,
						    rte_memory_order_relaxed);
	st->generation = rte_atomic_load_explicit(&qi_mon.generation,
						  rte_memory_order_acquire);

	return 0;
}

int
rte_qi_process_at(uint64_t tsc)
{
	uint32_t i;
	int count = 0;

	if (!qi_initialized)
		return -EINVAL;

	if (rte_atomic_load_explicit(&qi_mon.running,
				     rte_memory_order_acquire) != 0)
		return -EBUSY;

	if (tsc < qi_mon.base_tsc)
		tsc = qi_mon.base_tsc;

	rte_rwlock_write_lock(&qi_lock);
	for (i = 0; i < qi_nactive; i++) {
		struct qi_slot *slot = &qi_slots[qi_active[i]];

		qi_sample_slot(slot, tsc);
		count++;
	}
	rte_rwlock_write_unlock(&qi_lock);

	return count;
}
