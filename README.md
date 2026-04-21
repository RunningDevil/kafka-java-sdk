# kafka-java-sdk

一个面向 Java 业务代码的轻量 Kafka SDK。  
目标是把“创建主题、发送消息、消费消息”这三件基础能力封装成简单稳定的 JDK 接口，减少业务工程对原生 Kafka 客户端细节的直接依赖。

这个项目当前刻意保持精简：

- 只保留 4 个核心类
- 只保留 3 类核心能力：创建主题、发送消息、消费消息
- 能通过配置解决的参数，不在业务调用方法里反复传参
- 提供适合本地和 WSL2 的默认值，便于快速验证

## 适用场景

适合下面这类场景：

- 业务工程只想简单接入 Kafka，不希望直接维护大量 `Properties`
- 旧工程希望把 Kafka 访问逻辑从业务线程、服务启动代码里解耦出来
- 需要把 Kafka 能力以 JAR 包形式提供给其他 Java 项目使用
- 需要先在本地或 WSL2 上快速跑通，再逐步接入正式环境参数

不适合下面这类场景：

- 需要非常复杂的 Kafka 运维能力，例如 topic 删除、topic 描述、offset 查询、事务消息
- 需要高度定制的多线程消费编排框架
- 需要把完整业务运行时对象直接塞进 SDK，例如 `ThreadFlag`、`NorthTask`、`Stats`

## 核心设计

项目只有 4 个核心类：

- `KafkaConfig`
  统一管理 Kafka 配置，包括 broker 地址、topic、consumer group、poll 参数、topic 创建参数、SSL 参数等。
- `KafkaAdmin`
  只负责创建主题。
- `KafkaProducer`
  只负责向配置中的 topic 发送消息。
- `KafkaConsumer`
  只负责从配置中的 topic 拉取消息并提交 offset。

设计原则如下：

- 配置集中化：topic、groupId、序列化器、SSL、消费参数全部收敛到 `KafkaConfig`
- 接口最小化：业务代码调用时尽量只关心“创建”“发送”“消费”
- 默认值可运行：只配 broker 和 topic，默认情况下就可以启动示例
- 与旧系统解耦：不把旧工程里的业务对象直接耦合进 SDK

## 项目结构

```text
src/main/java/com/wyl/kafkasdk/
├── KafkaConfig.java
├── KafkaAdmin.java
├── KafkaProducer.java
├── KafkaConsumer.java
└── example/
    └── KafkaSdkUsageExample.java
```

测试代码：

```text
src/test/java/com/wyl/kafkasdk/
├── KafkaConfigTest.java
├── KafkaAdminTest.java
├── KafkaProducerTest.java
└── KafkaConsumerTest.java
```

## 快速开始

### 1. 环境要求

- JDK 11+
- Maven 3.9+
- 可访问的 Kafka broker

### 2. 构建项目

```bash
mvn clean package
```

构建产物：

- `target/kafka-java-sdk-1.0.0-SNAPSHOT.jar`
- `target/kafka-java-sdk-1.0.0-SNAPSHOT-all.jar`

推荐直接使用 fat jar：

- `target/kafka-java-sdk-1.0.0-SNAPSHOT-all.jar`

### 3. 最小示例

当前示例代码位于：

- `src/main/java/com/wyl/kafkasdk/example/KafkaSdkUsageExample.java`

最小调用方式如下：

```java
String bootstrapServers = "127.0.0.1:9092";

KafkaConfig config = KafkaConfig.builder(bootstrapServers)
        .topic("demo-topic")
        .build();

try (KafkaAdmin kafkaAdmin = new KafkaAdmin(config)) {
    kafkaAdmin.createTopic();
}

try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(config)) {
    kafkaProducer.send("order-1001", "{\"status\":\"created\"}");
}

try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config)) {
    List<KafkaConsumer.Message<String, String>> messages = kafkaConsumer.poll();
    kafkaConsumer.commit();
}
```

## 为什么只配两个参数也能运行

因为 `KafkaConfig.Builder` 已经内置了一组默认值，适合本地和 WSL2 环境快速验证。

当前关键默认值包括：

- `topic = demo-topic`
- `groupId = demo-group`
- `clientId = demo-client`
- `keySerializer = StringSerializer`
- `valueSerializer = StringSerializer`
- `keyDeserializer = StringDeserializer`
- `valueDeserializer = StringDeserializer`
- `autoOffsetReset = earliest`
- `enableAutoCommit = false`
- `pollTimeout = 100ms`
- `partitions = 1`
- `replicationFactor = 1`
- `securityProtocol = PLAINTEXT`

所以在示例里只设置：

- broker 地址
- topic 名

其他参数会自动使用默认值。

## 运行路径说明

示例执行顺序如下：

1. 构造 `KafkaConfig`
2. `KafkaAdmin` 根据配置创建 topic
3. `KafkaProducer` 根据配置向 topic 发送消息
4. `KafkaConsumer` 根据配置自动订阅 topic，执行 `poll()`
5. 消费完成后执行 `commit()`

也就是说，`topic`、`groupId`、序列化器、poll 参数都不是在每次调用时传入，而是先进入 `KafkaConfig`，后续由 SDK 内部在创建 Kafka 原生客户端时组装成 `Properties`。

## 与旧 NorthConsumer 参数的对应关系

你旧代码里的很多 Kafka 参数，已经可以在 `KafkaConfig` 中统一表达，例如：

- `bootstrap.servers`
- `group.id`
- `client.id`
- `session.timeout.ms`
- `max.poll.interval.ms`
- `receive.buffer.bytes`
- `fetch.min.bytes`
- `heartbeat.interval.ms`
- `max.poll.records`
- `auto.offset.reset`
- `enable.auto.commit`
- SSL 相关参数

示例映射方式：

```java
KafkaConfig config = KafkaConfig.builder("127.0.0.1:9092")
        .topic("north-topic")
        .groupId("north-service")
        .clientId("north-service-0")
        .consumerThreads(4)
        .sessionTimeoutMs(300000)
        .maxPollIntervalMs(300000)
        .receiveBufferBytes(8192000)
        .fetchMinBytes(100000)
        .heartbeatIntervalMs(100000)
        .maxPollRecords(1000)
        .sslEnabled(true)
        .sslDirectory("/tmp/kafka-certs")
        .sslPassword("changeit")
        .build();
```

注意：

- `consumerThreads` 目前是统一配置项，用来承载旧代码的线程数量语义
- 业务层的 `ThreadFlag`、`Stats`、`NorthTask` 不属于 Kafka SDK 配置，因此没有放进 `KafkaConfig`

## WSL2 运行

如果 Kafka 集群也运行在 WSL2 中，并且 broker 监听的是 `127.0.0.1:9092`，通常不需要改代码。

典型执行命令：

```bash
cd /mnt/d/my_proj/java_proj/helloworld
mvn clean package -DskipTests
java -cp target/kafka-java-sdk-1.0.0-SNAPSHOT-all.jar com.wyl.kafkasdk.example.KafkaSdkUsageExample
```

如果你需要先验证 Kafka 是否可访问：

```bash
ss -lntp | grep 9092
nc -vz 127.0.0.1 9092
```

如果需要验证消息是否真的进了 topic：

```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic demo-topic

$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic demo-topic \
  --from-beginning \
  --max-messages 1 \
  --property print.key=true \
  --property key.separator=" | "
```

## 测试

运行全部单元测试：

```bash
mvn test
```

当前测试覆盖：

- `KafkaConfigTest`
  验证默认配置和旧参数风格配置能正确生成 Kafka `Properties`
- `KafkaAdminTest`
  验证主题创建参数是否正确传递
- `KafkaProducerTest`
  验证发送消息时使用的是配置中的 topic
- `KafkaConsumerTest`
  验证消费、自动订阅和手动提交 offset 的基本路径

## 当前能力边界

当前版本是“基础能力 SDK”，故意没有做这些内容：

- topic 删除
- topic 查询和描述
- 事务消息
- 高级 rebalance 管理
- 消费线程框架
- 业务过滤、落盘、指标统计等旧系统逻辑

这样做的目的是让 SDK 更稳定、更容易维护，也更适合作为基础 JAR 包给外部项目调用。

## 后续建议

如果你准备把这个项目上传到 GitHub，建议同步补充：

- `LICENSE`
- 更完整的 `.gitignore`
- 发布版本标签，例如 `v1.0.0`
- 一个单独的迁移文档，说明旧 `NorthConsumer` 代码如何迁移到本 SDK

## License

如果准备公开发布，建议补充正式许可证文件，例如 `MIT` 或 `Apache-2.0`。
