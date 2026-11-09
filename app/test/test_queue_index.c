/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2026 Queue Index authors
 */

#include <stdint.h>
#include <string.h>

#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_memory.h>
#include <rte_ring.h>
#include <rte_queue_index.h>

#include "test.h"

#define QI_TEST_RING_SIZE 1024

static void *
qi_dummy_obj(uint32_t i)
{
	return (void *)(uintptr_t)(i + 1);
}

static int
test_qi_params(void)
{
	RTE_TEST_ASSERT(rte_qi_init(NULL) == 0,
			"init with defaults failed");
	RTE_TEST_ASSERT(rte_qi_init(NULL) == -EBUSY,
			"double init must fail");
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	RTE_TEST_ASSERT(rte_qi_init("bad_key=1") == -EINVAL,
			"unknown key must fail");
	RTE_TEST_ASSERT(rte_qi_init("high_wm=10,low_wm=20") == -EINVAL,
			"low_wm >= high_wm must fail");
	RTE_TEST_ASSERT(rte_qi_init("interval_us=1") == -EINVAL,
			"interval below min_interval_us must fail");
	RTE_TEST_ASSERT(rte_qi_init("window=1") == -EINVAL,
			"window below minimum must fail");
	RTE_TEST_ASSERT(rte_qi_init("sched=foo") == -EINVAL,
			"invalid sched must fail");
	RTE_TEST_ASSERT(rte_qi_init("warn_horizon_us=200,"
				    "clear_horizon_us=100") == -EINVAL,
			"clear_horizon_us <= warn_horizon_us must fail");

	RTE_TEST_ASSERT(rte_qi_init("interval_us=20,window=4,"
				    "min_net_pps=1000,budget_pct=80") == 0,
			"valid custom parameters failed");
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_qi_register(void)
{
	struct rte_qi_ring_state st;
	struct rte_ring *ring;
	uint32_t idx;

	RTE_TEST_ASSERT(rte_qi_init(NULL) == 0, "init failed");

	ring = rte_ring_create("qi_ut_ring_a", QI_TEST_RING_SIZE,
			       SOCKET_ID_ANY, 0);
	RTE_TEST_ASSERT(ring != NULL, "ring creation failed");

	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_a", NULL) == 0,
			"register by name failed");
	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_a", NULL) == -EEXIST,
			"duplicate register must fail");
	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_missing", NULL) == -ENOENT,
			"missing ring must fail");

	RTE_TEST_ASSERT(rte_qi_ring_handle_get("qi_ut_ring_a", &idx) == 0,
			"handle lookup failed");
	RTE_TEST_ASSERT(idx < RTE_QI_MAX_RINGS, "bad index");

	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_a", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_UNKNOWN, "state must be UNKNOWN");
	RTE_TEST_ASSERT(st.capacity == QI_TEST_RING_SIZE - 1,
			"unexpected capacity");
	RTE_TEST_ASSERT(st.eta_us == RTE_QI_ETA_INFINITE,
			"unexpected eta");

	RTE_TEST_ASSERT(rte_qi_ring_unregister("qi_ut_ring_a") == 0,
			"unregister failed");
	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_a", &st) == -ENOENT,
			"state get after unregister must fail");
	RTE_TEST_ASSERT(rte_qi_ring_unregister("qi_ut_ring_a") == -ENOENT,
			"double unregister must fail");

	rte_ring_free(ring);
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_qi_rts_reject(void)
{
	struct rte_ring *ring;

	RTE_TEST_ASSERT(rte_qi_init(NULL) == 0, "init failed");

	ring = rte_ring_create("qi_ut_ring_rts", 128, SOCKET_ID_ANY,
			       RING_F_MP_RTS_ENQ | RING_F_MC_RTS_DEQ);
	RTE_TEST_ASSERT(ring != NULL, "RTS ring creation failed");

	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_rts", NULL) ==
			-ENOTSUP, "RTS ring must be rejected");

	rte_ring_free(ring);
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_qi_level(void)
{
	struct rte_qi_ring_state st;
	struct rte_qi_conf conf;
	struct rte_ring *ring;
	void *objs[32];
	uint64_t hz, t0;
	uint32_t i;
	int nwarn;

	RTE_TEST_ASSERT(rte_qi_init("interval_us=10,window=4,"
				    "min_net_pps=1000000") == 0,
			"init failed");

	ring = rte_ring_create("qi_ut_ring_level", 256, SOCKET_ID_ANY, 0);
	RTE_TEST_ASSERT(ring != NULL, "ring creation failed");

	memset(&conf, 0, sizeof(conf));
	conf.high_wm = 5;
	conf.low_wm = 2;
	conf.warn_horizon_us = 100;
	conf.clear_horizon_us = 200;
	conf.confirm_samples = 1;
	conf.interval_us = 10;
	conf.window = 4;
	conf.min_net_pps = 1000000;

	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_level", &conf) == 0,
			"register failed");

	for (i = 0; i < 20; i++)
		objs[i] = qi_dummy_obj(i);

	RTE_TEST_ASSERT(rte_ring_enqueue_bulk(ring, objs, 20, NULL) == 20,
			"enqueue failed");

	hz = rte_get_tsc_hz();
	t0 = rte_rdtsc();
	RTE_TEST_ASSERT(rte_qi_process_at(t0) == 1, "process failed");

	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_level", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_WARN, "level trigger must warn");
	RTE_TEST_ASSERT((st.reasons & RTE_QI_REASON_LEVEL) != 0,
			"level reason missing");
	RTE_TEST_ASSERT(st.count_lo == 20 && st.count_hi == 20,
			"unexpected occupancy");

	nwarn = rte_qi_state_list_warn(&st, 4);
	RTE_TEST_ASSERT(nwarn >= 1, "warn list must contain the ring");

	RTE_TEST_ASSERT(rte_ring_dequeue_bulk(ring, objs, 20, NULL) == 20,
			"dequeue failed");
	RTE_TEST_ASSERT(rte_qi_process_at(t0 + hz / 100000) == 1,
			"process failed");
	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_level", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_OK, "state must recover");

	RTE_TEST_ASSERT(rte_qi_ring_unregister("qi_ut_ring_level") == 0,
			"unregister failed");
	rte_ring_free(ring);
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_qi_trend(void)
{
	struct rte_qi_ring_state st;
	struct rte_qi_conf conf;
	struct rte_ring *ring;
	void *objs[128];
	uint64_t hz, t0, tick;
	uint32_t i;

	RTE_TEST_ASSERT(rte_qi_init("interval_us=10,window=8,"
				    "min_net_pps=1000000") == 0,
			"init failed");

	ring = rte_ring_create("qi_ut_ring_trend", QI_TEST_RING_SIZE,
			       SOCKET_ID_ANY, 0);
	RTE_TEST_ASSERT(ring != NULL, "ring creation failed");

	memset(&conf, 0, sizeof(conf));
	conf.high_wm = 99;
	conf.low_wm = 50;
	conf.warn_horizon_us = 150;
	conf.clear_horizon_us = 300;
	conf.confirm_samples = 2;
	conf.interval_us = 10;
	conf.window = 8;
	conf.min_net_pps = 1000000;

	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_trend", &conf) == 0,
			"register failed");

	hz = rte_get_tsc_hz();
	tick = hz / 100000; /* 10 us */

	for (i = 0; i < 100; i++)
		objs[i] = qi_dummy_obj(i);

	t0 = rte_rdtsc();
	RTE_TEST_ASSERT(rte_qi_process_at(t0) == 1, "process failed");

	RTE_TEST_ASSERT(rte_ring_enqueue_bulk(ring, objs, 100, NULL) == 100,
			"enqueue failed");
	RTE_TEST_ASSERT(rte_qi_process_at(t0 + tick) == 1, "process failed");
	RTE_TEST_ASSERT(rte_qi_process_at(t0 + 2 * tick) == 1,
			"process failed");

	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_trend", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_WARN, "trend trigger must warn");
	RTE_TEST_ASSERT((st.reasons & RTE_QI_REASON_TREND) != 0,
			"trend reason missing");
	RTE_TEST_ASSERT(st.enq_pps > 1000000, "enqueue rate too low");
	RTE_TEST_ASSERT(st.eta_us < RTE_QI_ETA_INFINITE, "eta must be valid");

	RTE_TEST_ASSERT(rte_ring_dequeue_bulk(ring, objs, 100, NULL) == 100,
			"dequeue failed");

	/* Flush the rate window so that the old peak net rate ages out. */
	for (i = 0; i < 16; i++)
		RTE_TEST_ASSERT(rte_qi_process_at(t0 + (3 + i) * tick) == 1,
				"process failed");

	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_trend", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_OK, "state must recover");

	RTE_TEST_ASSERT(rte_qi_ring_unregister("qi_ut_ring_trend") == 0,
			"unregister failed");
	rte_ring_free(ring);
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_qi_predictors(void)
{
	struct rte_qi_ring_state st;
	struct rte_qi_conf conf;
	struct rte_ring *ring;
	void *objs[128];
	uint64_t hz, t0, tick;
	uint32_t i;

	/* predictor kvargs parsing */
	RTE_TEST_ASSERT(rte_qi_init("predictor=linreg") == 0,
			"linreg predictor init failed");
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");
	RTE_TEST_ASSERT(rte_qi_init("predictor=foo") == -EINVAL,
			"invalid predictor must fail");

	/* EWMA: steady accumulation must trigger a trend warning */
	RTE_TEST_ASSERT(rte_qi_init("predictor=ewma,window=8") == 0,
			"ewma init failed");

	ring = rte_ring_create("qi_ut_ring_ewma", QI_TEST_RING_SIZE,
			       SOCKET_ID_ANY, 0);
	RTE_TEST_ASSERT(ring != NULL, "ring creation failed");

	memset(&conf, 0, sizeof(conf));
	conf.high_wm = 99;
	conf.low_wm = 50;
	conf.warn_horizon_us = 150;
	conf.clear_horizon_us = 300;
	conf.confirm_samples = 1;
	conf.interval_us = 10;
	conf.window = 8;
	conf.min_net_pps = 1000000;
	conf.predictor = RTE_QI_PRED_EWMA;

	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_ewma", &conf) == 0,
			"register failed");

	hz = rte_get_tsc_hz();
	tick = hz / 100000; /* 10 us */

	for (i = 0; i < 100; i++)
		objs[i] = qi_dummy_obj(i);

	t0 = rte_rdtsc();
	RTE_TEST_ASSERT(rte_qi_process_at(t0) == 1, "process failed");

	/* 100 items per 10 us tick = 10 Mpps net accumulation */
	for (i = 0; i < 4; i++) {
		RTE_TEST_ASSERT(
			rte_ring_enqueue_bulk(ring, objs, 100, NULL) == 100,
			"enqueue failed");
		RTE_TEST_ASSERT(rte_qi_process_at(t0 + (i + 1) * tick) == 1,
				"process failed");
	}

	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_ewma", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_WARN, "ewma trend must warn");
	RTE_TEST_ASSERT((st.reasons & RTE_QI_REASON_TREND) != 0,
			"trend reason missing");
	RTE_TEST_ASSERT(st.eta_us < RTE_QI_ETA_INFINITE, "eta must be valid");
	RTE_TEST_ASSERT(st.net_pps >= 1000000, "ewma net rate too low");

	RTE_TEST_ASSERT(rte_qi_ring_unregister("qi_ut_ring_ewma") == 0,
			"unregister failed");
	rte_ring_free(ring);
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	/* LINREG: gradual fill must trigger a trend warning */
	RTE_TEST_ASSERT(rte_qi_init("predictor=linreg,window=8") == 0,
			"linreg init failed");

	ring = rte_ring_create("qi_ut_ring_linreg", QI_TEST_RING_SIZE,
			       SOCKET_ID_ANY, 0);
	RTE_TEST_ASSERT(ring != NULL, "ring creation failed");

	memset(&conf, 0, sizeof(conf));
	conf.high_wm = 99;
	conf.low_wm = 50;
	conf.warn_horizon_us = 150;
	conf.clear_horizon_us = 300;
	conf.confirm_samples = 1;
	conf.interval_us = 10;
	conf.window = 8;
	conf.min_net_pps = 1000000;
	conf.predictor = RTE_QI_PRED_LINREG;

	RTE_TEST_ASSERT(rte_qi_ring_register("qi_ut_ring_linreg", &conf) == 0,
			"register failed");

	t0 = rte_rdtsc();
	RTE_TEST_ASSERT(rte_qi_process_at(t0) == 1, "process failed");

	/* Linear ramp: 100 items per tick over 8 ticks fills the window
	 * with a slope of 10 Mpps. */
	for (i = 0; i < 8; i++) {
		RTE_TEST_ASSERT(
			rte_ring_enqueue_bulk(ring, objs, 100, NULL) == 100,
			"enqueue failed");
		RTE_TEST_ASSERT(rte_qi_process_at(t0 + (i + 1) * tick) == 1,
				"process failed");
	}

	RTE_TEST_ASSERT(rte_qi_state_get("qi_ut_ring_linreg", &st) == 0,
			"state get failed");
	RTE_TEST_ASSERT(st.state == RTE_QI_WARN, "linreg trend must warn");
	RTE_TEST_ASSERT((st.reasons & RTE_QI_REASON_TREND) != 0,
			"trend reason missing");
	RTE_TEST_ASSERT(st.eta_us < RTE_QI_ETA_INFINITE, "eta must be valid");
	RTE_TEST_ASSERT(st.net_pps >= 1000000, "linreg slope too low");

	RTE_TEST_ASSERT(rte_qi_ring_unregister("qi_ut_ring_linreg") == 0,
			"unregister failed");
	rte_ring_free(ring);
	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_qi_start_stop(void)
{
	struct rte_qi_monitor_status status;
	int ret;

	RTE_TEST_ASSERT(rte_qi_init("lcore=auto,interval_us=20") == 0,
			"init failed");

	ret = rte_qi_start();
	RTE_TEST_ASSERT(ret == 0 || ret == -ENOENT || ret == -EBUSY,
			"start returned unexpected error");

	if (ret == 0) {
		rte_delay_us_sleep(20000);
		RTE_TEST_ASSERT(rte_qi_monitor_status(&status) == 0,
				"monitor status failed");
		RTE_TEST_ASSERT(status.running == 1, "monitor must run");
		RTE_TEST_ASSERT(status.ticks > 0, "monitor must tick");
		RTE_TEST_ASSERT(rte_qi_stop() == 0, "stop failed");
		RTE_TEST_ASSERT(rte_qi_monitor_status(&status) == 0,
				"monitor status failed");
		RTE_TEST_ASSERT(status.running == 0, "monitor must stop");
	}

	RTE_TEST_ASSERT(rte_qi_fini() == 0, "fini failed");

	return 0;
}

static int
test_queue_index(void)
{
	RTE_TEST_ASSERT(test_qi_params() == 0, "params test failed");
	RTE_TEST_ASSERT(test_qi_register() == 0, "register test failed");
	RTE_TEST_ASSERT(test_qi_rts_reject() == 0, "RTS test failed");
	RTE_TEST_ASSERT(test_qi_level() == 0, "level test failed");
	RTE_TEST_ASSERT(test_qi_trend() == 0, "trend test failed");
	RTE_TEST_ASSERT(test_qi_predictors() == 0, "predictors test failed");
	RTE_TEST_ASSERT(test_qi_start_stop() == 0, "start/stop test failed");

	return 0;
}

REGISTER_TEST_COMMAND(queue_index_autotest, test_queue_index);
