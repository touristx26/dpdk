/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2026 Queue Index authors
 */

#ifndef _RTE_QUEUE_INDEX_H_
#define _RTE_QUEUE_INDEX_H_

/**
 * @file
 *
 * Queue Index: ring pressure monitoring and early warning.
 *
 * A monitoring lcore periodically samples registered rings, predicts the
 * time remaining before a ring becomes full, and publishes a per-ring state
 * that applications can query.
 *
 * The library never modifies the monitored rings and never performs any
 * remediation action.
 *
 * @defgroup queue_index Queue Index
 * @{
 */

#include <stdint.h>

#include <rte_compat.h>
#include <rte_ring.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Maximum number of rings that can be registered. */
#define RTE_QI_MAX_RINGS 256

/** Value returned in rte_qi_ring_state::eta_us when no prediction is possible. */
#define RTE_QI_ETA_INFINITE UINT64_MAX

/**
 * Ring state published by the monitor.
 */
enum rte_qi_state {
	RTE_QI_UNKNOWN = 0,
	/**< Not sampled yet, or the monitor is not running. */
	RTE_QI_OK,
	/**< Ring pressure is nominal. */
	RTE_QI_WARN,
	/**< Ring is under pressure and may become full soon. */
	RTE_QI_CRITICAL,
	/**< Ring is full (including fully reserved by producers). */
};

/**
 * Reasons why a ring entered its current state.
 */
enum rte_qi_reason {
	RTE_QI_REASON_LEVEL = 1u << 0,
	/**< Occupancy crossed the high watermark. */
	RTE_QI_REASON_TREND = 1u << 1,
	/**< Predicted time to full is below the warning horizon. */
	RTE_QI_REASON_FULL = 1u << 2,
	/**< Ring is full. */
};

/**
 * Per-ring configuration.
 *
 * A field set to 0 means "inherit the global value" when this structure is
 * passed to rte_qi_ring_register() or rte_qi_ring_register_ptr().
 * Passing NULL means "inherit everything".
 */
struct rte_qi_conf {
	uint32_t high_wm;
	/**< High watermark, in percent of capacity. */
	uint32_t low_wm;
	/**< Low watermark, in percent of capacity, used for recovery. */
	uint32_t warn_horizon_us;
	/**< Enter warning when the predicted time to full is below this value. */
	uint32_t clear_horizon_us;
	/**< Leave warning only when the predicted time to full is above this. */
	uint32_t interval_us;
	/**< Sampling period for this ring. */
	uint32_t window;
	/**< Number of samples used for rate estimation. */
	uint32_t confirm_samples;
	/**< Consecutive samples needed to confirm trend/recovery. */
	uint32_t hold_us;
	/**< Minimum time to stay in WARN/CRITICAL after entering it. */
	uint64_t min_net_pps;
	/**< Minimum net accumulation rate considered for trend prediction. */
};

/**
 * Published per-ring state.
 *
 * Returned by value from the query functions. The contents are a consistent
 * snapshot, but may be up to one sampling period old.
 */
struct rte_qi_ring_state {
	char name[RTE_RING_NAMESIZE];
	/**< Name used at registration. */
	enum rte_qi_state state;
	/**< Current state. */
	uint32_t reasons;
	/**< Bitmask of enum rte_qi_reason for the current sample. */
	uint32_t capacity;
	/**< Usable ring capacity. */
	uint32_t count_lo;
	/**< Published occupancy (lower bound). */
	uint32_t count_hi;
	/**< Occupancy including producer reservations (upper bound). */
	uint32_t peak_hi;
	/**< Peak upper bound observed in the current window. */
	uint64_t enq_pps;
	/**< Enqueue rate over the current window. */
	uint64_t deq_pps;
	/**< Dequeue rate over the current window. */
	uint64_t net_pps;
	/**< Net accumulation rate; 0 when dequeue is faster. */
	uint64_t eta_us;
	/**< Predicted time to full, or RTE_QI_ETA_INFINITE. */
	uint64_t ts_ns;
	/**< Sampling timestamp, in nanoseconds. */
	uint64_t warn_ts_ns;
	/**< Last time this ring entered WARN or CRITICAL, in nanoseconds. */
	uint64_t alarm_count;
	/**< Number of times this ring entered WARN or CRITICAL. */
};

/**
 * Monitor health information.
 */
struct rte_qi_monitor_status {
	uint32_t lcore;
	/**< Lcore running the monitor, or UINT32_MAX. */
	uint32_t running;
	/**< 1 if the monitor lcore is running. */
	uint32_t registered;
	/**< Number of registered rings. */
	uint64_t ticks;
	/**< Number of monitor loop iterations. */
	uint64_t missed_ticks;
	/**< Number of skipped sampling periods. */
	uint64_t late_ticks;
	/**< Number of iterations that overran a sampling period. */
	uint64_t last_tick_ns;
	/**< Timestamp of the last monitor iteration, in nanoseconds. */
	uint64_t generation;
	/**< Incremented on every ring state transition. */
};

/**
 * Initialize the library and parse global parameters.
 *
 * Must be called after rte_eal_init().
 *
 * @param kvargs
 *   Comma-separated key=value list, or NULL to use defaults:
 *   lcore, sched, interval_us, min_interval_us, spin_us, window, high_wm,
 *   low_wm, warn_horizon_us, clear_horizon_us, confirm_samples,
 *   min_net_pps, hold_us, stagger, budget_pct, log, log_rate.
 * @return
 *   0 on success, -EINVAL on bad parameters, -ENOMEM on allocation failure,
 *   -EBUSY if already initialized.
 */
__rte_experimental
int rte_qi_init(const char *kvargs);

/**
 * Stop the monitor and release all resources.
 *
 * @return
 *   0 on success, -EINVAL if the library is not initialized.
 */
__rte_experimental
int rte_qi_fini(void);

/**
 * Start the monitor on the configured lcore.
 *
 * The lcore must be enabled and idle (WAIT state). With lcore=auto, the first
 * idle non-main lcore is selected.
 *
 * @return
 *   0 on success, -EINVAL on configuration or budget error, -ENOENT if no
 *   usable lcore is available, -EBUSY if the monitor is already running or
 *   the requested lcore is busy.
 */
__rte_experimental
int rte_qi_start(void);

/**
 * Stop the monitor and wait for the lcore to exit.
 *
 * Must not be called from the monitor lcore itself.
 *
 * @return
 *   0 on success, -EINVAL if the library is not initialized.
 */
__rte_experimental
int rte_qi_stop(void);

/**
 * Register a ring by name.
 *
 * The ring must have been created with rte_ring_create().
 *
 * @param ring_name
 *   Ring name.
 * @param conf
 *   Per-ring configuration, or NULL to inherit everything.
 * @return
 *   0 on success, -ENOENT if the ring does not exist, -EEXIST if already
 *   registered, -ENOSPC if the registry is full, -ENOTSUP for unsupported
 *   sync modes (RTS), -EINVAL on invalid configuration.
 */
__rte_experimental
int rte_qi_ring_register(const char *ring_name, const struct rte_qi_conf *conf);

/**
 * Register a ring by pointer.
 *
 * Useful for rings embedded in other structures or not created by
 * rte_ring_create().
 *
 * @param ring
 *   Ring pointer.
 * @param alias
 *   Name used for queries and logs; NULL means ring->name.
 * @param conf
 *   Per-ring configuration, or NULL to inherit everything.
 * @return
 *   0 on success, or a negative errno as in rte_qi_ring_register().
 */
__rte_experimental
int rte_qi_ring_register_ptr(struct rte_ring *ring, const char *alias,
			     const struct rte_qi_conf *conf);

/**
 * Unregister a ring.
 *
 * The application must call this before freeing the ring.
 *
 * @param ring_name
 *   Name used at registration.
 * @return
 *   0 on success, -ENOENT if not registered.
 */
__rte_experimental
int rte_qi_ring_unregister(const char *ring_name);

/**
 * Convert a ring name to a stable index for fast queries.
 *
 * The index remains valid until the ring is unregistered.
 *
 * @param ring_name
 *   Name used at registration.
 * @param idx
 *   Output index.
 * @return
 *   0 on success, -ENOENT if not registered, -EINVAL on NULL arguments.
 */
__rte_experimental
int rte_qi_ring_handle_get(const char *ring_name, uint32_t *idx);

/**
 * Get the state of a ring by name.
 *
 * Lock-free; may be called from any lcore, including the data plane.
 *
 * @param ring_name
 *   Name used at registration.
 * @param st
 *   Output state.
 * @return
 *   0 on success, -ENOENT if not registered, -EINVAL on NULL arguments.
 */
__rte_experimental
int rte_qi_state_get(const char *ring_name, struct rte_qi_ring_state *st);

/**
 * Get the state of a ring by index.
 *
 * Lock-free; may be called from any lcore, including the data plane.
 *
 * @param idx
 *   Index returned by rte_qi_ring_handle_get().
 * @param st
 *   Output state.
 * @return
 *   0 on success, -ENOENT if the slot is free, -EINVAL on bad arguments.
 */
__rte_experimental
int rte_qi_state_get_by_idx(uint32_t idx, struct rte_qi_ring_state *st);

/**
 * Copy the states of all rings currently in WARN or CRITICAL.
 *
 * @param arr
 *   Output array.
 * @param max
 *   Maximum number of entries to write.
 * @return
 *   Number of entries written, or a negative errno.
 */
__rte_experimental
int rte_qi_state_list_warn(struct rte_qi_ring_state *arr, uint16_t max);

/**
 * Get the global transition generation.
 *
 * The value is incremented every time any ring changes state. Applications
 * can use it to avoid scanning the registry when nothing changed.
 *
 * @return
 *   Current generation.
 */
__rte_experimental
uint64_t rte_qi_generation(void);

/**
 * Get monitor health information.
 *
 * @param st
 *   Output status.
 * @return
 *   0 on success, -EINVAL on NULL argument or if not initialized.
 */
__rte_experimental
int rte_qi_monitor_status(struct rte_qi_monitor_status *st);

/**
 * Sample all registered rings immediately using the supplied TSC value.
 *
 * This bypasses the monitor scheduling and is intended for deterministic
 * unit tests. It must not be used while the monitor lcore is running.
 *
 * @param tsc
 *   Timestamp, in TSC cycles. Must not be earlier than the last call.
 * @return
 *   Number of rings sampled, -EINVAL if the library is not initialized,
 *   -EBUSY if the monitor is running.
 */
__rte_experimental
int rte_qi_process_at(uint64_t tsc);

#ifdef __cplusplus
}
#endif

/** @} */

#endif /* _RTE_QUEUE_INDEX_H_ */
