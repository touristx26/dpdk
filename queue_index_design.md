# DPDK Queue Index 详细设计

| 项目 | 内容 |
|---|---|
| 功能名称 | Queue Index（ring 压满预警） |
| 代码位置 | `lib/queue_index/` |
| 基线 | DPDK 24.11（`origin/24.11`，commit `cbccf2f0d5`） |
| 开发分支 | `feat/queue-index` |
| 文档状态 | 已确认，进入 P0 实现 |
| 更新日期 | 2026-11-04 |

---

## 1. 背景与目标

高速 pipeline 中，ring 作为阶段间/线程间的队列，一旦被压满就会出现丢包或阻塞。应用希望在有线速流量（约 30 Mpps）时，**提前**知道某个 ring 即将被压满，从而自行采取措施（降速、丢弃、切流等）。

本库提供：

1. 由**独立 lcore** 持续采样 ring 的入队/出队行为；
2. 基于水位和净堆积速率预测“填满剩余时间”（ETA）；
3. 对每个 ring 维护一个可查询的**状态标记**（OK / WARN / CRITICAL）；
4. 在状态迁移时输出限流日志。

应用通过查询接口读取标记，**不负责**任何处置动作。

### 设计基准

| 项 | 取值 | 说明 |
|---|---|---|
| 目标流量 | 30 Mpps | 单 ring 入队速率上限 |
| 设计 ring 深度 | 4096 | 最小支持 2048；1024 需下调参数 |
| 满速填满时间 | ≈137 µs | `4096 / 30 Mpps` |
| 采样周期 | 10 µs | 137 µs 内可采约 13 点 |
| 监控 ring 数 | 典型 ≤32 | 单 lcore 预算上限见 §10.4 |

---

## 2. 非目标

- 不做 ring 的创建、扩容、限速、丢包策略；
- 不做告警推送（无回调、无 eventfd、无 alarm ring）；
- v1 不支持 RTS 同步模式的 ring；
- v1 不支持跨进程共享标记（标记在进程私有内存中）；
- v1 不提供 telemetry 命令（后续版本再加）；
- 不监控 ethdev 收发包队列（仅监控 `struct rte_ring`）。

---

## 3. 术语

| 术语 | 含义 |
|---|---|
| slot | 注册表中一个被监控 ring 的内部记录 |
| 低边界 `count_lo` | `(prod.tail - cons.tail)`，已发布、确定在 ring 中的元素数 |
| 高边界 `count_hi` | `(prod.head - cons.tail)`，含生产者已预留但未发布的元素，用于告警的保守上界 |
| 净速率 `net_pps` | 窗口内的入队速率减出队速率 |
| ETA | `(capacity - count_hi) / peak_net_pps`，预计填满剩余时间 |
| 迁移 | ring 状态在 OK/WARN/CRITICAL 之间变化 |

---

## 4. 需求与约束

### 4.1 功能需求

| 编号 | 需求 |
|---|---|
| F1 | 独立 lcore 按固定周期采样，不受应用控制循环影响 |
| F2 | 每个 ring 维护状态标记，应用可按名称或索引查询 |
| F3 | 支持水位触发与趋势触发，带滞回、确认次数、最短保持时间 |
| F4 | 状态迁移时输出限流日志 |
| F5 | 支持运行期动态注册/注销 ring |
| F6 | 监控参数可通过启动参数配置，并支持 per-ring 覆盖 |

### 4.2 性能与约束

| 编号 | 约束 |
|---|---|
| C1 | 采样只读，不加锁、不修改 ring，不改变数据面行为 |
| C2 | 查询路径无锁、无系统调用，可在任意 lcore 调用 |
| C3 | monitor lcore 预估占用不得超过 `budget_pct`（默认 50%） |
| C4 | 采样周期不得低于 `min_interval_us`（默认 5 µs） |
| C5 | 所有内部时间为 TSC，对外时间为 ns |
| C6 | 仅支持 MT / ST / HTS；RTS 返回 `-ENOTSUP` |

---

## 5. 总体架构

### 5.1 组件图

```
┌──────────────────────────── monitor lcore（独占，纯自旋）────────────────────────────┐
│                                                                                      │
│  deadline 调度器 ──► 采样器 ──► 速率窗口 ──► 预测器 ──► 状态机 ──► 发布快照 ──► 日志 │
│   (TSC/stagger)      (ring读)    (K个样本)    (ETA)     (滞回/确认)   (seqlock)  (限流)│
│                                                                                      │
└───────────────────────────────────────┬──────────────────────────────────────────────┘
                                        │ 写（唯一写者）
                              ┌─────────▼──────────┐
                              │ slot[0..N-1]       │
                              │ seq + 状态快照     │
                              └─────────┬──────────┘
                                        │ 读（多读者，无锁）
        ┌───────────────────────────────┴────────────────────────────────┐
        │ 应用任意 lcore：state_get / state_get_by_idx / state_list_warn │
        └────────────────────────────────────────────────────────────────┘
```

### 5.2 数据流

1. monitor lcore 到达某 ring 的采样时刻；
2. 读取 `prod.head / prod.tail / cons.tail`（2 条 cache line）；
3. 计算 `count_lo`、`count_hi`，把 `(tsc, prod_tail, cons_tail)` 写入窗口环形缓冲；
4. 由窗口计算 `enq_pps / deq_pps / net_pps / peak_net_pps`；
5. 计算 ETA，执行状态机，得到新状态与 reasons；
6. 若状态变化或需要刷新，seqlock 写入 slot；
7. 状态变化时输出限流日志，并递增 `generation`。

### 5.3 线程模型

| 角色 | 线程/上下文 | 说明 |
|---|---|---|
| monitor | 独立 EAL lcore | 唯一写者，纯自旋 |
| 注册/注销 | 应用线程 | 写锁，非热路径 |
| 查询 | 任意 lcore/线程 | 无锁 seqlock 读 |
| init/fini/start/stop | 主线程 | 不可与其他生命周期调用并发 |

---

## 6. 模块与文件

```
lib/queue_index/
├── meson.build
├── rte_queue_index.h          # 公开 API、配置、状态结构
├── rte_queue_index.c          # 注册表、采样、预测、状态机、调度、日志
└── version.map                # EXPERIMENTAL 符号表

app/test/
└── test_queue_index.c         # 单元测试
```

内部逻辑在 `rte_queue_index.c` 中按以下段落组织（不拆私有头文件，减少暴露面）：

1. 参数解析（kvargs）
2. 注册表（slot 管理）
3. 采样与窗口
4. 预测与状态机
5. 快照发布（seqlock）
6. monitor 调度循环
7. 日志与查询 API

---

## 7. 数据结构

### 7.1 公开结构

```c
#define RTE_QI_MAX_RINGS 256
#define RTE_QI_ETA_INFINITE UINT64_MAX

enum rte_qi_state {
    RTE_QI_UNKNOWN = 0,   /* 尚未采样，或 monitor 未启动 */
    RTE_QI_OK,
    RTE_QI_WARN,
    RTE_QI_CRITICAL,      /* 已压满（含已全部预留） */
};

enum rte_qi_reason {
    RTE_QI_REASON_LEVEL = 1u << 0,   /* 触发水位条件 */
    RTE_QI_REASON_TREND = 1u << 1,   /* 触发 ETA 条件 */
    RTE_QI_REASON_FULL  = 1u << 2,   /* count_hi == capacity */
};

enum rte_qi_predictor {
    RTE_QI_PRED_INHERIT = 0,   /* 仅 per-ring：继承全局 */
    RTE_QI_PRED_PEAK,          /* 窗口峰值（默认） */
    RTE_QI_PRED_EWMA,          /* 指数加权移动平均，α = 0.5 */
    RTE_QI_PRED_LINREG,        /* 窗口内占用最小二乘斜率 */
};

/* 0 表示继承全局配置（仅 per-ring 使用） */
struct rte_qi_conf {
    uint32_t high_wm;           /* 高水位百分比 */
    uint32_t low_wm;            /* 低水位百分比 */
    uint32_t warn_horizon_us;   /* ETA 触发阈值 */
    uint32_t clear_horizon_us;  /* ETA 恢复阈值 */
    uint32_t interval_us;       /* 采样周期 */
    uint32_t window;            /* 速率窗口样本数 */
    uint32_t confirm_samples;   /* 趋势条件连续确认次数 */
    uint32_t hold_us;           /* 最短保持时间 */
    uint64_t min_net_pps;       /* 趋势判断的最小净速率 */
    uint32_t predictor;         /* 预测器选择，0 = 继承全局 */
};

struct rte_qi_ring_state {
    char     name[RTE_RING_NAMESIZE];
    uint32_t state;          /* enum rte_qi_state */
    uint32_t reasons;        /* enum rte_qi_reason 位图 */
    uint32_t capacity;
    uint32_t count_lo;       /* 已发布占用（下界） */
    uint32_t count_hi;       /* 含预留占用（上界） */
    uint32_t peak_hi;        /* 窗口内高边界峰值 */
    uint64_t enq_pps;
    uint64_t deq_pps;
    uint64_t net_pps;        /* 所选预测器给出的净堆积速率估计（≥ 0），见 §9.1/§9.2 */
    uint64_t eta_us;         /* RTE_QI_ETA_INFINITE 表示不收敛 */
    uint64_t ts_ns;          /* 本次采样时刻 */
    uint64_t warn_ts_ns;     /* 最近一次进入 WARN/CRITICAL 的时刻 */
    uint64_t alarm_count;    /* 累计进入 WARN/CRITICAL 次数 */
};

struct rte_qi_monitor_status {
    uint32_t lcore;
    uint32_t running;
    uint32_t registered;
    uint64_t ticks;
    uint64_t missed_ticks;   /* 跳过的采样周期数 */
    uint64_t late_ticks;     /* 超过 deadline 才完成的次数 */
    uint64_t last_tick_ns;
    uint64_t generation;
};
```

> `net_pps` 是所选预测器给出的净堆积速率估计，出队快于入队时按 0 处理
>（EWMA 内部为 int64 状态、输出钳为非负），状态机不使用负速率。

### 7.2 内部结构

```c
struct qi_sample {
    uint64_t tsc;
    uint32_t prod_tail;
    uint32_t cons_tail;
};

struct qi_slot {
    RTE_ATOMIC(uint32_t) seq;      /* 奇数：写入中 */
    RTE_ATOMIC(uint32_t) in_use;   /* 1：有效 */
    struct rte_ring *ring;
    char name[RTE_RING_NAMESIZE];
    struct rte_qi_conf conf;       /* 已与全局配置合并 */
    struct qi_sample win[64];      /* 窗口环形缓冲，最大 64 */
    uint32_t win_head;
    uint32_t win_cnt;
    uint32_t prev_state;
    uint32_t trend_hits;           /* 连续趋势命中次数 */
    uint32_t recovery_hits;        /* 连续恢复命中次数 */
    uint64_t enter_state_ns;
    uint64_t next_due_tsc;
    uint64_t alarm_count;
    struct rte_qi_ring_state pub;  /* seqlock 保护的发布快照 */
};

struct qi_monitor {
    RTE_ATOMIC(uint32_t) stop;
    uint32_t lcore;
    uint32_t sched;                /* spin / sleep / hybrid */
    uint64_t tsc_hz;
    uint64_t interval_tsc;
    uint64_t min_interval_tsc;
    uint64_t spin_tsc;
    uint64_t next_tick_tsc;
    uint64_t ticks;
    uint64_t missed_ticks;
    uint64_t late_ticks;
    uint64_t last_tick_ns;
    RTE_ATOMIC(uint64_t) generation;
    uint64_t base_tsc;
    uint64_t base_ns;
};
```

### 7.3 内存与对齐

- `qi_slot` 按 cache line 对齐，避免多个 ring 的快照互相伪共享；
- monitor 只写自己的 slot；读者只读 `seq + pub`；
- 注册表固定 `RTE_QI_MAX_RINGS`（256）个 slot，初始化时 `rte_zmalloc` 一次分配，运行期不再分配内存；
- 窗口最大 64 个样本（`window` 上限 64），每个 slot 约 1.5 KB，256 个 slot 约 380 KB。

---

## 8. 采样设计

### 8.1 读取序列

按同步模式分支读取，避免依赖联合体布局巧合：

```c
/* MT / ST */
ph = rte_atomic_load_explicit(&r->prod.head, rte_memory_order_relaxed);
pt = rte_atomic_load_explicit(&r->prod.tail, rte_memory_order_relaxed);
ct = rte_atomic_load_explicit(&r->cons.tail, rte_memory_order_relaxed);

/* HTS */
ph = rte_atomic_load_explicit(&r->hts_prod.ht.pos.head, rte_memory_order_relaxed);
pt = rte_atomic_load_explicit(&r->hts_prod.ht.pos.tail, rte_memory_order_relaxed);
ct = rte_atomic_load_explicit(&r->hts_cons.ht.pos.tail, rte_memory_order_relaxed);
```

计算：

```c
uint32_t lo = (pt - ct) & r->mask;
uint32_t hi = (ph - ct) & r->mask;
if (lo > r->capacity) lo = r->capacity;
if (hi > r->capacity) hi = r->capacity;
```

- 使用无符号减法天然处理 32 位回绕；
- `count_hi` 用于告警（保守，宁可早报）；
- `count_lo` 用于恢复判断（保守，避免抖动）。

### 8.2 同步模式支持

| 模式 | 支持 | 处理 |
|---|---|---|
| `RTE_RING_SYNC_MT` | 是 | 读 `prod.head/tail`、`cons.tail` |
| `RTE_RING_SYNC_ST` | 是 | 同上 |
| `RTE_RING_SYNC_MT_HTS` | 是 | 读 `hts_*.ht.pos.*` |
| `RTE_RING_SYNC_MT_RTS` | 否 | 注册时返回 `-ENOTSUP` |

注册时通过 `rte_ring_get_prod_sync_type()` / `rte_ring_get_cons_sync_type()` 检查，二者任一为 RTS 即拒绝。

### 8.3 速率窗口

- 每个 slot 保存最近 K 个 `(tsc, prod_tail, cons_tail)`；
- 速率用**累计计数器增量**计算，不用占用差：

```
enq_pps = Δprod_tail * 1e9 / Δns
deq_pps = Δcons_tail * 1e9 / Δns
net_pps = enq_pps - deq_pps
```

- 窗口未满时使用已有样本跨度（至少 2 个样本）；
- `enq_pps` / `deq_pps` 为窗口端到端速率，仅用于展示与日志；
- 净速率与 ETA 由所选预测器推导（见 §9.1），`PEAK` 即窗口内每个采样间隔净增量的最大值，保证对突发保守；
- 速率计算使用双精度或 128 位中间量，避免 `Δitems × hz` 溢出（见 §17.3）。

### 8.4 误差分析

| 误差源 | 量级 | 处理 |
|---|---|---|
| 快照撕裂 | ≤ 采样瞬间在途的 enq/deq 数量（通常 ≤ burst × 生产者数） | `count_hi` 取上界并 clamp |
| 周期抖动 | 纯自旋下 < 1 µs | 用实际 TSC 计算速率 |
| 速率量化 | 10 µs × 30 Mpps = 300 个样本 | 窗口累计，K=8 |
| 慢速 ring | K×interval 可能过长 | per-ring 覆盖 `interval_us` / `window` |

### 8.5 cache line 干扰

- 每次采样读 prod、cons 各一条 cache line，会使生产/消费核下一次写该行发生一次 RFO；
- 干扰次数 = 采样频率，与自旋/睡眠无关；
- 估算：单 ring 单次约 100 ns，周期 10 µs 时约占对应核 1%；
- 缓解：`stagger=1` 错开各 ring 的采样相位；monitor 与数据面同 NUMA socket。

---

## 9. 预测与状态机

### 9.1 预测器

预测器只负责从采样窗口推导 `net_pps` 与 `eta_us`；触发阈值、状态机、reasons 位图对所有预测器完全一致。切换预测器不改变任何阈值语义。

```c
typedef void (*qi_predict_fn)(struct qi_slot *slot, uint64_t now,
                              uint32_t count_hi,
                              uint64_t *net_pps, uint64_t *eta_us);

static const qi_predict_fn qi_predictors[] = {
    [RTE_QI_PRED_PEAK]   = qi_predict_peak,
    [RTE_QI_PRED_EWMA]   = qi_predict_ewma,
    [RTE_QI_PRED_LINREG] = qi_predict_linreg,
};
```

注册时按 `conf.predictor`（继承解析后）把函数指针存进 slot，采样时直接调用。新增算法 = 新增一个表项。

| 预测器 | net_pps 推导 | 适用场景 | 开销 |
|---|---|---|---|
| `PEAK`（默认） | 窗口内每个相邻采样对净速率的最大值 | 下游**阶跃停摆**（高速短 ring）：1~2 个 tick 得到真实净速率 | O(K)，无状态 |
| `EWMA` | `ewma = (ewma + net_now) / 2`，α 固定 0.5，状态在 slot | **微突发常态**（突发但长期均衡）：平滑后误报显著降低，阶跃响应慢 3~4 个 tick | O(1)，8 字节状态 |
| `LINREG` | 窗口内 `(tsc, prod_tail - cons_tail)` 最小二乘斜率 | **缓慢失配**（深 ring、长 horizon）：斜率稳定，ETA 更可信 | O(K)，复用窗口缓冲，零新增内存 |

公共收尾规则 `qi_finish_eta()`（三个预测器共享）：

```
free     = capacity - count_hi
eta_us   = (free == 0) ? 0
           : (net_pps >= min_net_pps) ? free * 1e6 / net_pps
           : RTE_QI_ETA_INFINITE
```

选型依据是**故障时间常数**：微秒级阶跃 → PEAK；十微秒级突发 → EWMA；百微秒级漂移 → LINREG。同一 pipeline 中不同阶段的 ring 可各选各的（per-ring 覆盖）。

各预测器的推荐 `window`：PEAK/EWMA 用 8（EWMA 自带平滑，窗口只影响展示速率）；LINREG 用 32（长窗稳斜率）。

注意事项：

- `net_pps` 发布值统一为所选预测器的估计（PEAK 下即窗口峰值净速率），与 `eta_us` 同源，应用看到的就是触发判断用的值；
- EWMA 的 α 是唯一隐藏自由度，编译期固定 0.5，不进 API；实测不合适再暴露全局 kvargs；
- LINREG 回归对象是未 clamp 的 `prod_tail - cons_tail`（u32 差值的 int64 语义），不受容量饱和段拉平斜率的影响；
- LINREG 需要 ≥3 个样本，不足时 ETA 无穷；
- 不实现 Holt：其场景（趋势+多步外推）被 LINREG 覆盖，双参数调优成本不划算。

### 9.2 公式

```
free     = capacity - count_hi
net_pred = predictor(slot)            /* PEAK / EWMA / LINREG */
eta_us   = (net_pred >= min_net_pps)
           ? (free * 1e6) / net_pred
           : RTE_QI_ETA_INFINITE
level    = (count_hi * 100 >= high_wm * capacity)
trend    = (eta_us <= warn_horizon_us)
full     = (count_hi >= capacity)
```

- ETA 分母是预测器给出的净速率估计（PEAK 为窗口峰值），不是平均值：突发场景下平均值会低估风险；
- 当 ring 已满时，成功的入队为 0，`net_pps` 会趋近 0，趋势判断失效，此时只能靠 `full` / 水位；
- `count_hi` 为 0 且 `net_pred` 很大时，ETA 可能瞬间很小，需靠 `confirm_samples` 抑制单点噪声。

### 9.3 状态迁移

```
                 首次采样
   UNKNOWN ──────────────────► OK
      ▲                         │
      │                         │ level（立即）
      │                         │ 或 trend 连续 confirm_samples 次
      │                         ▼
      │                       WARN ◄──────────────┐
      │                         │                 │
      │                         │ full            │ 仍满足 WARN 条件
      │                         ▼                 │
      │                     CRITICAL ─────────────┘
      │                         │
      └──── 恢复条件连续 confirm_samples 次 ────────┘
```

### 9.4 迁移条件

**进入 WARN**（从 OK/UNKNOWN）：

- `level == true`：立即进入，`reasons |= LEVEL`；
- 或 `trend == true` 连续 `confirm_samples` 次：进入，`reasons |= TREND`。

**进入 CRITICAL**（从 OK/WARN）：

- `full == true`：立即进入，`reasons |= FULL`。

**恢复 OK**（从 WARN/CRITICAL）：

同时满足才计数，连续 `confirm_samples` 次后恢复：

- `count_lo * 100 <= low_wm * capacity`；
- `eta_us > clear_horizon_us`；
- 距 `enter_state_ns` 已超过 `hold_us`。

**保持原状态**：其余情况。`reasons` 始终反映当前样本的触发条件位图（LEVEL/TREND/FULL），无任何条件时为 0；状态是否迁移另由 `confirm_samples`、滞回与 `hold_us` 决定。

状态迁移时：

- `warn_ts_ns = now`（进入 WARN/CRITICAL）；
- `alarm_count++`（进入 WARN/CRITICAL）；
- `generation++`（release store）；
- 输出日志（限流）。

### 9.5 特殊场景

| 场景 | 处理 |
|---|---|
| 首次采样 | 状态 UNKNOWN → OK，速率 0，不告警 |
| ring 被压满 | `count_hi == capacity` → CRITICAL，reasons=FULL |
| 下游完全阻塞 | 入队速率≈0，`net_pps`≈0 → 依赖水位/满 |
| ring 被应用 free 但未注销 | 未定义行为；文档强制要求先注销；`unregister` 与采样用读写锁互斥 |
| monitor 停止 | 状态保留最后值，`monitor_status.running=0`，应用据 `last_tick_ns` 判断过期 |
| 注册后长时间无流量 | 速率 0，ETA 无穷，状态 OK |

---

## 10. 调度设计

### 10.1 模式

| 模式 | 行为 | 适用 |
|---|---|---|
| `spin`（默认） | 纯自旋到 deadline，等待用 `rte_pause()` | 高速 ring（10 µs 级周期） |
| `sleep` | `rte_delay_us_sleep()` 睡到 deadline | 低频、省电场景 |
| `hybrid` | 先睡到 `deadline - spin_us`，尾段自旋 | 周期 ≥200 µs |

### 10.2 deadline 调度

```c
next_tick = start_tsc + n * interval_tsc;   /* 绝对时刻，不累积漂移 */

/* spin */
while (!stop && rte_rdtsc() < next_tick)
    rte_pause();

/* hybrid */
remain_ns = (next_tick - now) * 1e9 / tsc_hz;
if (remain_ns > spin_ns)
    rte_delay_us_sleep((remain_ns - spin_ns) / 1000);
while (!stop && rte_rdtsc() < next_tick)
    rte_pause();
```

- 落后超过一个周期时：跳过错过的 tick，`missed_ticks += 1`，不补采；
- 采样完成时刻晚于 deadline 时，`late_ticks += 1`；
- `next_tick` 始终基于 `start_tsc + n*interval` 计算，不做“当前时刻 + interval”。

### 10.3 stagger

- 每个 slot 维护 `next_due_tsc`；
- 注册时设置初始相位：`next_due = base + i * interval / N`（`stagger=1`）；
- monitor 每轮只采样 `now >= next_due` 的 slot，然后 `next_due += interval`；
- 效果：把 N 个 ring 的 cache line 冲击均匀分散到整个周期；
- `stagger=0` 时所有 slot 同相位（整批采样），便于对齐分析。

### 10.4 预算校验

```
预估占用(%) = Σ_i [ C_sample_ns / (interval_us_i × 1000) × 100 ]
C_sample_ns 默认 100（两条远端 cache line + 计算）
```

- 启动时按当前已注册 ring 数计算；
- 超过 `budget_pct` → `rte_qi_start()` 返回 `-EINVAL`，并打印预估占用和建议周期；
- 运行期新增 ring 不重新校验（文档提示）；`monitor_status` 暴露 ring 数供应用自查。

### 10.5 启停流程

```
rte_qi_init(kvargs)
      │
      ├── 解析参数、分配 slot 数组、注册 logtype
      ▼
rte_qi_ring_register*()   ← 可在 start 前/后调用
      │
rte_qi_start()
      │
      ├── 校验 lcore（enabled 且 state==WAIT，否则 -EBUSY/-ENOENT）
      ├── 校验预算
      ├── 初始化 next_due_tsc
      └── rte_eal_remote_launch(qi_monitor_loop, ctx, lcore)
      ▼
      ... 应用查询 ...
      ▼
rte_qi_stop()
      │
      ├── stop = 1
      ├── rte_eal_wait_lcore(lcore)
      └── running = 0
      ▼
rte_qi_fini()
```

---

## 11. 并发设计

### 11.1 快照 seqlock

写者唯一（monitor），读者任意：

```c
/* 写 */
uint32_t s = seq;
rte_atomic_store_explicit(&slot->seq, s + 1, rte_memory_order_relaxed); /* 奇数 */
/* 写 slot->pub 各字段 */
rte_atomic_store_explicit(&slot->seq, s + 2, rte_memory_order_release); /* 偶数 */

/* 读 */
do {
    s1 = rte_atomic_load_explicit(&slot->seq, rte_memory_order_acquire);
    if (s1 & 1)
        continue;
    *out = slot->pub;   /* 结构体拷贝 */
    s2 = rte_atomic_load_explicit(&slot->seq, rte_memory_order_acquire);
} while (s1 != s2);
```

- 读者返回结构体 **by value**，不返回内部指针；
- 结构中 `capacity`、`count_hi` 等有单调约束，测试中可用于校验无撕裂。

### 11.2 注册表锁

- `register/unregister`：`rte_rwlock_write_lock`；
- monitor 每轮：`rte_rwlock_read_lock` 一次，遍历所有 slot，结束后释放；
- 写锁与采样互斥，保证注销后 monitor 不再解引用该 ring；
- 注册/注销不在数据面热路径，锁开销可忽略。

### 11.3 句柄

```c
int rte_qi_ring_handle_get(const char *name, uint32_t *idx);
int rte_qi_state_get_by_idx(uint32_t idx, struct rte_qi_ring_state *st);
```

- `idx` 在 `[0, RTE_QI_MAX_RINGS)`；
- `state_get_by_idx` 先检查 `in_use`，释放后返回 `-ENOENT`；
- 句柄被注销后失效，应用需重新获取；文档明确此约束（避免返回指针导致的悬垂问题）。

### 11.4 generation

- `generation` 在任意 slot 状态迁移时递增（release）；
- 应用可先读 `generation`，与上次比较，变化时才遍历 `state_list_warn()`；
- 读取用 acquire，保证看到 generation 变化时也能看到对应状态。

### 11.5 多进程

- 标记位于进程私有内存，**不跨进程共享**；
- ring 本身在共享内存中，secondary 读取 ring 计数是安全的；
- v1 约定：monitor 只在需要查询标记的进程内运行；多进程同时监控同一 ring 是允许的，只是重复采样。

---

## 12. 日志

- logtype：`lib.queue_index`，使用 `RTE_LOG_REGISTER_DEFAULT(queue_index_logtype, INFO)` 注册；
- 级别：进入 WARN 用 `RTE_LOG_WARNING`，进入 CRITICAL 用 `RTE_LOG_ERR`，恢复 OK 用 `RTE_LOG_NOTICE`；
- 仅在状态迁移时输出；
- 限流：全局令牌桶 `log_rate` 条/秒；每 ring 最短间隔 1 秒；
- 时间戳使用采样时刻（`state.ts_ns`），不是打印时刻。

格式示例：

```
QUEUE_INDEX: ring=swq_0 state=WARN reasons=TREND count=2048/4096 \
enq=30012345pps deq=29900112pps net=112233pps eta=10200us
```

预算：单条日志在 100~200 字节量级，限流后对 monitor 的影响可忽略；若日志后端可能长时间阻塞，需在配置中关闭日志（`log=0`），异步日志留待后续版本。

---

## 13. API 规格

### 13.1 生命周期

| 函数 | 说明 | 返回值 |
|---|---|---|
| `int rte_qi_init(const char *kvargs)` | 初始化库，解析全局参数 | 0 / `-EINVAL` / `-ENOMEM` |
| `int rte_qi_fini(void)` | 停止并释放全部资源 | 0 |
| `int rte_qi_start(void)` | 在配置的 lcore 上启动 monitor | 0 / `-EBUSY` / `-ENOENT` / `-EINVAL` |
| `int rte_qi_stop(void)` | 停止 monitor，等待 lcore 退出 | 0 |

### 13.2 注册

| 函数 | 说明 | 返回值 |
|---|---|---|
| `int rte_qi_ring_register(const char *name, const struct rte_qi_conf *conf)` | 按名称查找并注册 | 0 / `-ENOENT` / `-EEXIST` / `-ENOSPC` / `-ENOTSUP` |
| `int rte_qi_ring_register_ptr(struct rte_ring *r, const char *alias, const struct rte_qi_conf *conf)` | 按指针注册，`alias` 可为 NULL（取 `r->name`） | 同上 + `-EINVAL` |
| `int rte_qi_ring_unregister(const char *name)` | 注销 | 0 / `-ENOENT` |

### 13.3 查询

| 函数 | 说明 | 线程安全 |
|---|---|---|
| `int rte_qi_ring_handle_get(const char *name, uint32_t *idx)` | 名称 → 索引 | 是（读锁） |
| `int rte_qi_state_get(const char *name, struct rte_qi_ring_state *st)` | 按名称查询 | 是（无锁） |
| `int rte_qi_state_get_by_idx(uint32_t idx, struct rte_qi_ring_state *st)` | 按索引查询（热路径用） | 是（无锁） |
| `int rte_qi_state_list_warn(struct rte_qi_ring_state *arr, uint16_t max)` | 返回所有 WARN/CRITICAL | 是（无锁） |
| `uint64_t rte_qi_generation(void)` | 状态变化代数 | 是（acquire 读） |
| `int rte_qi_monitor_status(struct rte_qi_monitor_status *st)` | monitor 健康状态 | 是 |
| `int rte_qi_process_at(uint64_t tsc)` | 用指定 TSC 立即采样全部 ring，供单测使用；monitor 运行中返回 `-EBUSY` | 否（测试专用） |

### 13.4 错误码

| 错误码 | 场景 |
|---|---|
| `-EINVAL` | 参数非法、预算超限、状态不允许 |
| `-ENOENT` | ring 不存在 / 未注册 / lcore 不可用 |
| `-EEXIST` | 同一 ring 或同名已注册 |
| `-ENOSPC` | 注册表已满 |
| `-ENOTSUP` | RTS 同步模式 |
| `-EBUSY` | lcore 非 WAIT / monitor 已运行 |

### 13.5 使用示例

```c
struct rte_qi_conf conf = {
    .high_wm = 50,
    .low_wm = 25,
    .warn_horizon_us = 150,
    .clear_horizon_us = 300,
    .interval_us = 10,
    .window = 8,
    .confirm_samples = 2,
    .hold_us = 0,
    .min_net_pps = 100000,
    .predictor = RTE_QI_PRED_PEAK,   /* 0 = 继承全局 */
};

rte_qi_init("lcore=5,sched=spin,interval_us=10,predictor=peak,budget_pct=50");
rte_qi_ring_register("swq_0", &conf);
rte_qi_ring_register("swq_1", NULL);   /* 全部继承全局 */

/* 微突发常态的 ring 单独换 EWMA */
struct rte_qi_conf ewma_conf = {
    .predictor = RTE_QI_PRED_EWMA,
};
rte_qi_ring_register("swq_2", &ewma_conf);
rte_qi_start();

/* 控制面，每 50 µs 查一次 */
uint32_t idx;
rte_qi_ring_handle_get("swq_0", &idx);
for (;;) {
    struct rte_qi_ring_state st;
    if (rte_qi_state_get_by_idx(idx, &st) == 0 && st.state >= RTE_QI_WARN)
        app_handle_warn(&st);
    rte_delay_us_sleep(50);
}

rte_qi_stop();
rte_qi_ring_unregister("swq_0");
rte_qi_ring_unregister("swq_1");
rte_qi_fini();
```

---

## 14. 配置参数

### 14.1 全局参数（kvargs）

| 参数 | 类型 | 默认 | 范围 | 说明 |
|---|---|---|---|---|
| `lcore` | int/string | 必填 | `0..RTE_MAX_LCORE-1` 或 `auto` | monitor lcore；`auto` 选第一个 `WAIT` 且非 main 的核 |
| `sched` | enum | `spin` | `spin/sleep/hybrid` | 调度模式 |
| `interval_us` | uint32 | 10 | ≥ `min_interval_us` | 全局采样周期 |
| `min_interval_us` | uint32 | 5 | 1..1000 | 周期下限保护 |
| `spin_us` | uint32 | 0 | 0..100000 | 仅 `hybrid`；0 表示 `min(interval/4, 50)` |
| `window` | uint32 | 8 | 2..64 | 速率窗口样本数 |
| `high_wm` | uint32 | 50 | 1..99 | 高水位百分比 |
| `low_wm` | uint32 | 25 | 0..98 | 低水位百分比，必须 < `high_wm` |
| `warn_horizon_us` | uint32 | 150 | 1..1000000 | 趋势告警阈值 |
| `clear_horizon_us` | uint32 | 300 | > `warn_horizon_us` | 恢复阈值 |
| `confirm_samples` | uint32 | 2 | 1..64 | 趋势/恢复连续确认次数 |
| `min_net_pps` | uint64 | 100000 | 0..UINT64_MAX | 趋势判断速率门限 |
| `hold_us` | uint32 | 0 | 0..10000000 | 最短保持时间 |
| `predictor` | enum | `peak` | `peak/ewma/linreg` | 全局默认预测器，见 §9.1 |
| `stagger` | bool | 1 | 0/1 | 采样相位错开 |
| `budget_pct` | uint32 | 50 | 1..100 | monitor 预估占用上限 |
| `log` | bool | 1 | 0/1 | 是否输出迁移日志 |
| `log_rate` | uint32 | 10 | 0..100000 | 全局日志限流（条/秒） |

解析规则：

- 未知 key、类型错误、越界 → `rte_qi_init()` 返回 `-EINVAL`，打印具体 key；
- `low_wm >= high_wm`、`clear_horizon_us <= warn_horizon_us` → `-EINVAL`；
- 允许 kvargs 为 NULL（全部使用默认值，但 `lcore` 缺失时 `start()` 失败）。

### 14.2 per-ring 覆盖

`rte_qi_ring_register*()` 的 `conf` 中字段为 0 表示继承全局；非 0 覆盖。可覆盖字段：

`high_wm`、`low_wm`、`warn_horizon_us`、`clear_horizon_us`、`interval_us`、`window`、`confirm_samples`、`hold_us`、`min_net_pps`、`predictor`（0 = 继承全局预测器）。

`conf == NULL` 表示全部继承。

> ABI 注意：`predictor` 追加在 `rte_qi_conf` 末尾。库处于 EXPERIMENTAL 阶段，
> 允许结构布局变更，但使用旧头文件编译的应用需重编译。转正式 ABI 后不得再
> 直接追加字段，需改用带 size 的 v2 结构。

### 14.3 编译期固定项

| 项 | 值 | 说明 |
|---|---|---|
| `RTE_QI_MAX_RINGS` | 256 | 注册表上限 |
| 窗口上限 | 64 | `window` 的最大值 |
| 采样成本常数 | 100 ns | 预算估算用 |
| EWMA α | 0.5 | 平滑因子，编译期固定，见 §9.1 |
| LINREG 最小样本数 | 3 | 不足时 ETA 无穷 |
| 支持同步模式 | MT/ST/HTS | RTS 拒绝 |

### 14.4 不同 ring 深度的推荐组合

| capacity | interval_us | high_wm | warn_horizon_us | predictor |
|---|---|---|---|---|
| 1024 | 5 | 40 | 80 | `peak` |
| 2048 | 10 | 45 | 120 | `peak` |
| 4096 | 10 | 50 | 150 | `peak`（默认） |
| 8192 | 20 | 60 | 300 | `peak` / `linreg`（`window=32`） |

按流量形态选择预测器（覆盖上表默认值）：下游阶跃停摆 → `peak`；微突发常态 → `ewma`；缓慢失配 → `linreg`（配 `window=32`）。

---

## 15. 生命周期与状态机

### 15.1 库状态

```
UNINIT ──init──► INITED ──start──► RUNNING ──stop──► STOPPED
                    ▲                                     │
                    └──────────── fini ◄──────────────────┘
```

| 状态 | 允许操作 |
|---|---|
| UNINIT | `init` |
| INITED | `register/unregister/query/start/fini` |
| RUNNING | `register/unregister/query/stop/fini` |
| STOPPED | `query/fini`；再次 `start` 允许（重新拉起 lcore） |

### 15.2 约束

- `init` 必须在 `rte_eal_init()` 之后调用；
- `start` 前注册的 ring 在首个 tick 得到 UNKNOWN → OK；
- `stop` 后查询返回最后快照，`monitor_status.running=0`；
- `fini` 内部若处于 RUNNING，先执行 `stop`；
- 同一进程只允许一个 monitor 实例（v1 无多实例需求）。

---

## 16. 构建集成

### 16.1 `lib/meson.build`

在 `libraries` 列表中加入（依赖 `ring`，放在 `'ring'` 之后）：

```meson
libraries = [
        ...
        'ring',
        'queue_index',
        'rcu',
        ...
]
```

### 16.2 `lib/queue_index/meson.build`

```meson
sources = files('rte_queue_index.c')
headers = files('rte_queue_index.h')
deps += ['ring', 'kvargs']
```

（`eal` 由构建系统隐式依赖；v1 不依赖 telemetry。）

### 16.3 `lib/queue_index/version.map`

```text
EXPERIMENTAL {
        global:

        rte_qi_fini;
        rte_qi_generation;
        rte_qi_init;
        rte_qi_monitor_status;
        rte_qi_ring_handle_get;
        rte_qi_ring_register;
        rte_qi_ring_register_ptr;
        rte_qi_ring_unregister;
        rte_qi_start;
        rte_qi_state_get;
        rte_qi_state_get_by_idx;
        rte_qi_state_list_warn;
        rte_qi_stop;

        local: *;
};
```

### 16.4 单元测试

`app/test/meson.build` 的 `source_file_deps` 中增加：

```meson
'test_queue_index.c': ['queue_index'],
```

测试文件使用 `REGISTER_TEST_COMMAND(queue_index_autotest, test_queue_index)` 注册。

### 16.5 文档

- `rte_queue_index.h` 增加 `\defgroup queue_index` Doxygen 分组；
- `doc/api/doxy-api-index.md` 增加 `queue_index` 分组；
- `MAINTAINERS` 增加 `lib/queue_index/` 条目（fork 内部可选）。

---

## 17. 测试计划

### 17.1 单元测试

| 用例 | 内容 | 判定 |
|---|---|---|
| T1 | 参数解析：默认值、非法 key、越界、组合冲突 | 返回码与日志 |
| T2 | 注册：按名/按指针/重复/满表/RTS 拒绝 | 返回码 |
| T3 | 边界计算：构造确定占用，校验 `count_lo/count_hi` | 与 `rte_ring_count()` 一致（单线程场景） |
| T4 | 速率：注入时间戳，入队固定数量，校验 pps 误差 | 误差 < 1% |
| T5 | 状态机：水位触发、趋势触发、confirm、滞回恢复、hold | 状态序列与预期一致 |
| T6 | 特殊场景：满 ring、空 ring、长时间无流量 | CRITICAL / OK |
| T7 | 注销与重注册：索引复用、句柄失效 | `-ENOENT` |
| T8 | seqlock：单写多读压力，校验 `count_hi <= capacity`、`state` 合法 | 无非法值 |
| T9 | 生命周期：重复 start、未 start 查询、stop 后查询 | 返回码与 `running` |
| T10 | 预算校验：构造超预算 ring 数 | `-EINVAL` |
| T11 | 预测器：`predictor` kvargs 解析（合法/非法值）；EWMA 阶跃堆积触发 TREND；LINREG 线性填充触发 TREND；net_pps/eta 与理论值一致 | 返回码、状态、`reasons` |

时间注入：内部提供 `qi_process_at(uint64_t tsc)`，单测直接调用；对外为 `rte_qi_process_at()`（实验接口，monitor 运行中 `-EBUSY`）。

### 17.2 集成测试

独立示例程序 `examples/queue_index`（构建：`-Dexamples=queue_index`）用 4 个 lcore
自动执行下列场景并打印量化结果，退出码 0/1 表示 PASS/FAIL：

| 阶段 | 内容 | 判定 |
|---|---|---|
| A | producer 30 Mpps、consumer 略快（无压力），持续 1 s | 误报数 = 0 |
| B | consumer 停摆 300 ms | WARN 早于首次入队失败，提前量 ≥ 50 µs；ETA 误差 ≤ 50% |
| C | consumer 以 60 Mpps 排空积压 | 恢复延迟 ≤ 5 ms |
| D | consumer 降到 29 Mpps | 水位告警提前量 ≥ 1 ms |
| E | monitor | `missed_ticks` / `late_ticks` 观测 |

完整用例：

| 用例 | 内容 |
|---|---|
| I1 | 4 个 lcore：control / producer / consumer / monitor，观察 WARN 时序 |
| I2 | 下游停摆，验证 CRITICAL 与首次丢包时刻的对照 |
| I3 | 恢复流量，验证滞回与恢复日志 |
| I4 | `stagger=0/1` 对比 monitor CPU 与数据面 pps |
| I5 | 跨 NUMA 采样，记录干扰差异 |

### 17.3 数值精度

- 速率：`(double)Δitems * 1e9 / Δns`，或 128 位整数；
- `ts_ns`：`(tsc - base_tsc)` 转 ns 时使用分段计算，避免 64 位溢出：
  `ns = (delta / hz) * 1000000000 + ((delta % hz) * 1000000000) / hz`；
- `eta_us`：`free * 1e6 / peak_net_pps` 使用 128 位或 double，结果 clamp 到 `UINT64_MAX-1`。

### 17.4 性能验收

- monitor lcore 占用与预算估算偏差 < 20%；
- 数据面 pps 下降 < 1%（16 ring、10 µs、同 socket）；
- 查询路径单次 < 100 ns（结构体拷贝 + seqlock）。

---

## 18. 性能预算

| 项 | 估算 |
|---|---|
| 单 ring 单次采样 | ~100 ns（2 条远端 cache line + 计算） |
| 16 ring × 10 µs | 约 16% 单核 |
| 32 ring × 10 µs | 约 32% 单核 |
| 64 ring × 10 µs | 约 64%，超过默认预算 |
| 查询单次 | 结构体拷贝约 80 B + 2 次原子读，< 100 ns |
| 注册/注销 | 写锁 + 线性扫描，非热路径 |
| 内存 | 256 slot × ~1.5 KB ≈ 380 KB |

预算公式与启动校验见 §10.4。

---

## 19. 已知限制与风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 采样周期决定提前量下限 | 一个周期内填满的 ring 无法预测 | 高速小 ring 降低 `high_wm`；必要时减小 `interval_us` |
| 独立 lcore 不能提高预测精度 | 精度受限于周期与窗口 | 在文档和日志中明确 ETA 语义 |
| cache line 争用 | 生产核额外 RFO | 低频、stagger、同 NUMA |
| `rte_log` 同步 I/O | 阻塞 monitor 导致采样缺席 | 限流、可关闭；异步日志后续实现 |
| ring 被 free 未注销 | use-after-free | 文档强制先 `unregister`；采样与注销用读写锁互斥 |
| 多进程状态不共享 | secondary 查不到标记 | 文档明确；或各进程独立运行 monitor |
| RTS 不支持 | 使用 RTS 的 ring 无法监控 | 注册返回 `-ENOTSUP` |
| monitor lcore 故障 | 状态过期 | `monitor_status` 暴露 `running/last_tick_ns/missed_ticks` |
| 注册表固定 256 | 超大规模不适用 | 编译期常量，按需调整 |

---

## 20. 后续版本

| 版本 | 内容 |
|---|---|
| v1.1 | telemetry 命令（`/queue_index/rings`）读快照；异步日志 |
| v1.2 | alarm ring（把迁移事件写入 `rte_ring`，供控制面异步消费） |
| v1.3 | 每 socket 一个 monitor lcore，按 ring 归属分组 |
| v1.4 | 自适应预测器：按窗口内净速率的突发度统计自动切换 PEAK↔EWMA（仅新增表项，不改 API） |
| v2.0 | 自定义队列类型（`rte_qi_ring_ops` 采样回调）；RTS 支持；ring 自动发现（`rte_memzone_walk` + `RG_` 前缀） |

---

## 21. 决策记录

| 决策 | 结论 | 依据 |
|---|---|---|
| 是否用独立 lcore | 是，独立 lcore 采样 | 采样节拍不能受应用循环影响；日志时机可控 |
| 调度模式 | 默认纯自旋 | 10 µs 级周期下睡眠抖动不可接受 |
| 是否需要回调 | 否 | 应用主动查询，库不做推送 |
| ring 形式 | 按名 + 按指针 | pipeline ring 是普通 `rte_ring` |
| 设计深度 | 4096（最小 2048） | 30 Mpps 下 137 µs 缓冲，可采 13 点 |
| `lcore` 参数 | 强制指定，`auto` 兜底 | 避免抢占业务核 |
| 参数数量 | 初期保留完整参数，后续收敛 | 便于调优和问题定位 |
| RTS | v1 不支持 | 打包 64 位 tail，读取路径不同 |
| 上报 | 查询 + 迁移日志 | 满足“标记 + 应用自取”的需求 |
| 预测算法 | PEAK / EWMA / LINREG 三件套，函数指针表切换 | 对应阶跃 / 突发 / 漂移三类故障时间常数；查询契约不变 |
| Holt | 不实现 | 场景被 LINREG 覆盖，α/β 双参数调优成本不划算 |
| EWMA α | 编译期固定 0.5，不进 API | 10 µs 窗口无调优经验可依；需要时再暴露 kvargs |
| `predictor` 字段 | 追加在 `rte_qi_conf` 末尾（0 = 继承） | 兼容既有“0 继承”约定；实验期允许结构变更，代价是重编译 |
