# hairpin模块实现设计

## 修订记录

| 日期       | 修订版本 | 修改描述                 | 修改人          |
| :----------- | :--------- | :------------------------- | :---------------- |
| 2026-03-01 | 1.0      | xx1需求实现       | xxx |
| 2026-04-09 | 2.0      |  xx2需求实现，新增xxx功能  | xxx |

## 模块历史需求

| AR ID            | 需求描述                          | 功能描述                                                          |
| ------------------ | ----------------------------------- | ------------------------------------------------------------------- |
| xxxxxx | 【BIFUR】工具增加流量分叉使能开关 | 注册命令字到hinicamd3；根据用户命令，填充msg信息，发送到bifur驱动 |

## 关键术语

| 关键术语 | 中文名称 | 英文全称       | 详细定义                                       |
| ---------- | ---------- | ---------------- | ------------------------------------------------ |
| NETDEV      | 网络设备 | Network Device  | 抽象出来的虚拟网络设备，和实际物理网卡设备映射 |
| WORKER   | 实例     | Worker            | 实例是指一套完整的协议上下文                      |
| PBUF     | 包缓存   | PacketBuffer      | 报文缓存，并可以存储协议上下文信息                |

## 模块介绍

Hairpin特性类似于交换机技术，通过rte_eth_hairpin_conf配置转发路径，允许数据包从RX端口队列接收后再自动转发至TX端口队列后发出，无需CPU干预，如下图所示。
![](这里放一张原理图，展示工作原理)

【who】DPDK ( 实现主体 )

【what】卡内转发网络报文 (功能描述 )

【where】1823网卡 (承载主体)

【why】减少CPU开销&提高网络报文转发性能 ( 实现目的 )

【how】dpdk下发控制面指令，固件直接对网卡进行转发不上送CPU (实现方法 )

【how much】不支持主机模式&独占内存模式，仅支持通过fdir action queue配置hairpin (规格限制 )

## 模块依赖

| 模块 | 描述 |
| ----------- | ------------ |
| 1823固件 | 提供1823卡内转发能力 |

## 模块功能

### 业务功能

| 功能名称                                           | 功能说明    |
| ---------------------------------------------------- | ------------- |
| hinic3驱动支持hairpin queue建立 | hinic3-DPDK用户态初始化时建立hairpin队列 |
| hinic3驱动支持hairpin模式下收发包  | hinic3-DPDK用户态驱动支持根据hairpin配置转发报文 |
| hinic3驱动hairpin模式下包率性能90M  | hinic3驱动hairpin模式下包率性能90M |

介绍业务功能如何实现

### DFX功能

| 类型     | 功能名称             | 功能说明                                                 |
| ---------- | ------------------ | ------------------------------------------------------ |
| DFS可服务性 | 支持hairpin map映射信息显示 | dpdk启动时回显hairpin map映射信息 |
| DFC兼容性 | hinic3驱动支持hairpin异常退出 | dpdk启动时查询网卡是否支持hairpin，不支持时异常退出 |

介绍DFX如何实现

#### 兼容性穿刺

## 线程模型分析

### 并发分析

涉及线程间交互，主要描述线程间交互过程（过程简单的话，可以和运行分析是一个时序图），plantuml作图

## 运行分析

1.时序图描述与其他模块的交互，plantuml作图
2.若本模块内部运行复杂，可用流程图或其他方式详细描述运行过程
3.若代码采用一定方法优化，讲述为什么可以优化，优化的思路，帮助更好的理解代码

![](时序图)

## 接口描述

### 关键数据结构

```
struct SelectFdInfo {
    fd_set *readfds;
    fd_set *writefds;
    fd_set *exceptfds;
    struct pollfd *dpPollFds;
    int selectNfds;
    int dpPollNfds;
    int osPollRet; // os poll返回的event个数
    int dpPollRet; // dp轮询得到的event个数
};
```

### 关键函数

```
/* 获取hairpin规格 */
int hinic3_hairpin_cap_get(struct rte_eth_dev *dev, struct rte_eth_hairpin_cap *cap);

/* 获取hairpin map信息 */
int hinic3_hairpin_get_peer_ports(struct rte_eth_dev *dev, uint16_t *peer_ports,
								  size_t len, uint32_t direction);
/* 创建rx hairpin队列 */
int hinic3_rx_hairpin_queue_setup(struct rte_eth_dev *dev, uint16_t qid,
								  uint16_t nb_desc,
								  const struct rte_eth_hairpin_conf *conf);
/* 创建tx hairpin队列 */
int hinic3_tx_hairpin_queue_setup(struct rte_eth_dev *dev, uint16_t qid,
								  uint16_t nb_desc,
								  const struct rte_eth_hairpin_conf *conf);
/* 绑定hairpin map */
int hinic3_hairpin_bind(struct rte_eth_dev *dev, uint16_t rx_port);

/* 解绑hairpin map */
int hinic3_hairpin_unbind(struct rte_eth_dev *dev, uint16_t rx_port);
​
```

## 开发者测试设计

| 测试设计check维度 | 说明                                                                 | 是否涉及 |
| ------------------- | ---------------------------------------------------------------------- | ---------- |
| 需求新增功能    | 新开发功能点，需要对该功能点新增相应测试用例                         | √       |
| 基础功能          | 涉及基础场景功能验证                             | √       |
| 交叉功能          | 涉及同其他需求的交叉，例如lro功能，需要也验证大报文功能              | ×       |
| 配置文件          | 涉及配置文件，需要配置文件边界测试                                   | ×      |
| 资源基线          | 涉及资源基线变更，需要资源基线测试 | ×     |
| 性能              | 涉及性能，需要跑基础性能、大连接+大压力性能                          | √       |
| 规格              | 涉及规格，需要规格边界测试                                           | √       |
| 安全              | 涉及安全，例如特定、root权限下执行，读写系统文件等                   | ×       |

| 测试维度 | 测试场景 | 测试名称         | 测试步骤              | 预期结果      |  能否自动化 | mlx5网卡对比结果 | 其他网卡对比结果 |
| ---------- | ---------- | ------------------ | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | ------------ | ------------ | ------------ |
| 功能 | dpdk-testpmd   | dpdk本口转发 | 1./dpdk-testpmd  --rxq=1 --txq=1 -i --hairpin-mode=0 --hairpinq=1<br>2.flow create 0 ingress pattern eth / ipv4 / end actions queue index 5 / end<br>3. 发包仪发送ipv4报文                 | 1. 成功<br>2. 成功<br>3.发包仪本口收到回包，dpdk查看无报文。| √  |  性能表现一致 | 不涉及 |
| 功能 | dpdk-testpmd   | dpdk跨口转发 | 1./dpdk-testpmd  --rxq=1 --txq=1 -i --hairpin-mode=0x12 --hairpinq=1<br>2.flow create 0 ingress pattern eth / ipv4 / end actions queue index 5 / end<br>3. 发包仪发送ipv4报文               | 1. 成功<br>2. 成功<br>3.发包仪跨口收到回包，dpdk查看无报文。| √  |  性能表现一致 | 不涉及 |
| 功能 | dpdk-testpmd   | dpdk自定义转发 | 1./dpdk-testpmd  --rxq=1 --txq=1 -i --hairpin-mode=0x12 --hairpinq=1 --hairpinq=2 --hairpin-map=0:1:0:2:1 --hairpin-map=0:2:1:2:1<br>2.flow create 0 ingress pattern eth / ipv4 / udp / end actions queue index 4 / end<br>3. flow create 0 ingress pattern eth / ipv4 / tcp / end actions queue index 5 / end<br>4. 发包仪发送udp报文<br>5. 发包仪发送tcp报文         | 1. 成功<br>2. 成功<br>3. 成功<br>4.发包仪本口收到回包，dpdk查看无报文。<br>5.发包仪跨口收到回包，dpdk查看无报文。| √  |  性能表现一致 | 不涉及 |