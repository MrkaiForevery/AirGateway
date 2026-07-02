# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 提供在本仓库中工作时的指导。

## 构建与运行

- **构建**：`mvn clean package`
- **运行**：`mvn spring-boot:run`（或执行 `AirGatewayApplication.main()`）
- **服务端口**：`10101`（在 `application.yml` 中配置）
- **运行依赖**：Java 17、Maven，以及一个可达的 Nacos 服务（默认 `127.0.0.1:8848`）—— 应用启动时会从 Nacos 导入所有运行时配置。

测试极为有限（classpath 上没有 JUnit/assertj），也没有 lint 框架。唯一文件 `src/test/java/com/airfree/access/accessTest.java` 是一个草稿，不是真正的测试。

## 项目概述

**AirGateway** 是一个基于 Spring WebFlux + Spring Cloud (Alibaba) 构建的响应式、多协议 API 网关。它**不是**标准的 Spring Cloud Gateway 部署——团队用自定义的、策略驱动的过滤器管道替代了默认的 `GatewayRouteLocator` 过滤器链模型。该代码库处于活跃开发中：`AirGatewayEngine` 中的许多路由方法目前返回的是 mock 响应，也有大量 `TODO` 标记表示逻辑未完成。

类名、配置文件和日志中的命名统一为 `AirGateway` / air-gateway / `air-gateway`。注意 `application.yml` 中存在一个拼写错误的 `com.airfee` 日志级别配置项。

## 架构

### 请求处理流程

```
HTTP 请求
  → AirGatewayEndpointConfig（RouterFunction：/api/**、/admin/**、_ai/**、/websocket/**、/smpp/**、/backend/**）
    → AirGatewayEngine（实现 AirGateway 接口，按路由类型分发）
      → CustomerAirGatewayFilterManager.executeFilterChain(strategy, exchange, chain)
        → CustomWebFilterChain（每次请求独立实例，线程安全的索引遍历）
          → 由 AirGatewayStrategy 构建的有序 AbstractAirGatewayFilter 列表
```

在这条自定义链之外，全局的 Spring `WebFilter` 会先执行——`AirGatewayLogFilter`（`@Order(HIGHEST_PRECEDENCE-1)`，负责访问日志 + 错误日志）和 `AirGatewayMetricsFilter`。这两个过滤器**不受**过滤器策略体系管理。

### 核心包

- **`access`** — `AirGatewayEndpointConfig`：唯一的 WebFlux `RouterFunction` bean，将路径断言映射到引擎方法。这是 HTTP 入口。
- **`core`** — `AirGateway`（路由类型接口）、`AirGatewayEngine`（实现类，目前为 mock 支撑），以及桩接口 `AirGatewayRouteLocator` / `AirGatewayLoadBalancer` / `AirGatewayAccessAdapter`（尚未实现）。
- **`filter`** — 自定义过滤器管道：
  - `AirGatewayFilterManager` / `CustomerAirGatewayFilterManager`：在启动时扫描所有 `AbstractAirGatewayFilter` 和 `AirGatewayStrategy` bean，构建一个由 `ReadWriteLock` 保护的 `Map<strategy, filter-list>`。`buildAllFilterChainByStrategy` 是原子性重建整个映射的热刷新入口。
  - `AirGatewayStrategy` / `ApiFilterStrategy`：策略接口；返回按执行顺序排列的过滤器 bean 名称 `List<String>`。目前仅有 `ApiFilterStrategy` 一个实现。
  - `AbstractAirGatewayFilter`：基类；`isDisable()` 控制过滤器是否执行。自定义过滤器放在 `filter/customizeFilter/` 下。
  - `CustomWebFilterChain`：**每次请求**创建的 `WebFilterChain`，通过 `AtomicInteger` 索引依次走过过滤器，最后回退到原始 Spring 链。
  - `filterEnums/ProtocolApiEnum`：附加到 exchange 属性上的 SOAP / REST 协议标识。

### 服务发现 (`discovery`) — 多注册中心

网关可以**同时对接多个注册中心**（Nacos、ZooKeeper、Consul、Eureka）。注册中心类型字符串通过 `AirServiceDiscoveryTypeEnum` 解析为对应类，由 `AirReactiveRegistryCenterFactory` 反射构造并初始化客户端。

- `AirReactiveServiceDiscoveryManager`：核心管理器。其构造方法会启动一个异步 `initializationMono`，完成两件事：(1) 初始化所有已配置的注册中心；(2) 将网关自身（`"air-gateway"`）作为服务实例注册到支持注册的客户端（`supportsRegistration()`）。每个公共方法都会先调用 `ensureInitialized()`（阻塞等待该 mono 完成）。它还持有 `AirServiceDiscoveryCache`（30s TTL、1000 容量），并集成了健康检查、指标（`service.discovery.*`）以及用于发现/刷新事件的 `ApplicationEvent` 发布。
- 配置通过 `MultiAirRegistryCenterConfig` + `AirRegistryCenterConfig` 从 `discovery.registries.*` 绑定（由 Nacos 的 `airDiscoveryConfig.yaml` 加载）。

### 负载均衡 (`LoadBalance`)

`AirReactiveLoadBalancer` 从服务发现管理器中选取健康实例，并委托给 `AirLoadBalanceStrategy`：`round-robin`（默认）、`random`、`weighted`。策略实现位于 `strategy/defaultImpl/`；`strategy/customerImpl/` 目前为空（为用户自定义策略提供的扩展点）。

### 流量控制 (`flow`) — Reactor 操作符管道

`AirFlowControlOperator<T>` 是一个 `Function<Mono<T>, Mono<T>>`——它包装一个 Reactor 操作符，通过 `mono.transform(...)` 应用。`AirFlowControlPipelineBuilder.buildPipeline(name, resources)` 从配置中读取 `flow-control.pipelines.<name>`，按 `orderId` 排序后串联各个操作符。每个操作符由 `AirFlowControlOperatorFactory` 从 `AirFlowControlType` 枚举反射构建。

操作符类型：`rateLimit`（Resilience4j / TokenBucket / SlidingWindow）、`circuitBreaker`、`degradeOnCondition`。配置（`airFlowControl.yaml`）支持从 Nacos 热刷新。

### RPC (`rpc`)

`AirGatewayRPCServiceClient` 抽象后端调用；`rpc/client/` 下的实现覆盖 HTTP、gRPC（桩已存在，starter 在 classpath 中）和 RSocket。`AirRPCTypeEnum` 枚举协议类型。`AirGatewayRPCServiceClient` 是引擎最终用来转发请求的接口（尚未对接——引擎目前返回 mock）。

### 消息队列 (`mq`)

支持 Kafka + RocketMQ，分别通过 `AirKafkaBatchSenderFactory` / `AirRocketMqBatchProducerFactory` 以及响应式/并发监听器实现。配置由 `AirKafkaConfigProperties` / `AirRocketMQConfigProperties` 绑定（来自 Nacos）。

### 缓存 (`cache`) + 事件 (`listener`)

- 存储后端：Caffeine（本地）、Redis（Redisson）、MongoDB。核心管理器：`AirCacheClientManager`；配置位于 `cache/config/redis/` 和 `cache/config/mongo/`。
- 缓存变更以 `ApplicationEvent`（`AirGatewayAbstractCacheEvent` 子类）形式传播，携带操作枚举、key、value 和 `reason`。
- `AirGatewayEventDispatcher` 是唯一的 `ApplicationListener`，将事件分发给有序的 `AirGatewayCacheEventListener`（按 `dependBase` 分为本地 / redis / mongo 分支）和 `AirGatewayLogListener`。这是缓存写入/刷新逻辑的集成点。

### 日志 (`log`)

自定义的异步日志体系，非 Spring 默认实现。`AirGatewayLogPublisher` 发布 `AirGatewayLogEvent`；`AirGatewayLogContext` 在 exchange 上携带 `traceId`/`spanId`/`requestId`；`AirGatewayLogAnnotation`（AspectJ——参见 `AirGatewayLogAspect`）用于声明式方法级日志。存储抽象在 `log/core/storage/`（`AirGatewayLogStorage` → `DefaultAirGatewayLogStorage`）。

### 健康检查与监控 (`health`, `monitor`)

- `health`：`AirServiceInstanceHealthCheckService`（通过 LENIENT/HYBRID/CLIENT_ACTIVE 策略过滤健康实例）和 `AirRegistryCenterHealthService`（注册中心级别的健康检查）。
- `monitor`：Micrometer + Prometheus。`application.yml` 暴露 `health,metrics,prometheus,info` Actuator 端点及 `http.server.request` 百分位/SLA 指标。

## 配置模型

运行时配置分为两层：

1. **`application.yml`**（已提交，在仓库中）——服务端口、Actuator/Prometheus、`air-gateway.log.*` 队列容量，以及一个 `com.airfee`（拼写错误）调试覆盖项。
2. **Nacos**（外部，通过 `bootstrap.yml` 的 `spring.config.import` 加载）——所有环境相关配置：服务发现注册中心、Redis、MongoDB、流量控制管道、RocketMQ、Kafka 以及 Seata 事务配置。`bootstrap.yml` 还禁用了多项与自定义实现冲突的 Spring Cloud 自动配置（`service-registry.auto-registration`、`gateway.discovery.locator`、`consul.enabled` 等）。

Nacos namespace 为 `72461fe0-689e-45fe-bacd-aed8948f99da`，group 为 `AIR_GROUP`。目前这些值是硬编码的——更换环境需要修改 `bootstrap.yml`。

## 编辑前必须了解的设计模式

- **策略模式**：过滤器（`AirGatewayStrategy`）和负载均衡。添加过滤器时，创建 `AbstractAirGatewayFilter` `@Component`，然后将其 bean 名称加入策略的执行顺序列表。
- **基于反射的插件加载**：注册中心客户端、流量控制操作符、以及（计划中的）负载均衡策略都通过枚举的 `Class` 字段反射构造，而非 Spring bean。新增类型需要在枚举中添加对应常量。
- **Reactor 操作符组合**：流量控制是 `Function<Mono,Mono>`，通过 `transform` 链式调用。错误以 `AirCircuitBreakerOpenException` / `AirRateLimitExceededException` / `AirServiceUnavailableException` 形式抛出。
- **过滤器映射上的 ReadWriteLock**：`CustomerAirGatewayFilterManager` 在写锁下重建 strategy→chain 映射，请求在读锁下读取。任何热刷新路径必须通过 `buildAllFilterChainByStrategy`，直接修改映射是不允许的。
- **每次请求独立的链实例**：`CustomWebFilterChain` 每次请求都必须新建实例（其 index 是有状态的）；而它包装的过滤器列表是共享的、不可变的。