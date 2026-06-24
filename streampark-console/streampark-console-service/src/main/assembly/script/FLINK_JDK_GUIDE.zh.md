# Flink JDK 与构建说明

StreamPark 通过版本 shims 模块支持多个 Flink 大版本。Flink 2.x 对 **构建** 和 **运行**
Flink 作业提出了更高的 JDK 要求。

## Flink 版本与 JDK 要求

| Flink 版本      | 最低 JDK（Flink 运行时） | StreamPark Console JDK |
|-----------------|--------------------------|-------------------------|
| Flink 1.12–1.20 | JDK 8                    | JDK 8 / JDK 11          |
| Flink 2.0–2.2   | JDK 11                   | JDK 8 / JDK 11          |

> **说明：** StreamPark Console 可以继续运行在 JDK 8 上。使用 Flink 2.x **不需要**升级
> StreamPark 服务的 JDK。

## StreamPark 自动处理的内容

1. **注册 Flink 环境**
   - 从 `$FLINK_HOME/lib/flink-dist*.jar` 解析版本，或 fallback 到
     `flink-dist --version`。
   - 注册 Flink 环境不要求 StreamPark 发行包中已包含 Flink 2.x shims。
2. **Flink 2.x shims 打包**
   - 仅当 StreamPark 使用 **JDK 11+** 构建时，才会打包 Flink 2.0/2.1/2.2 shims
     （Maven profile：`flink-2.x-shims`）。

## 构建 JDK 与产物差异

| 构建 JDK | Flink 1.12–1.20 shims | Flink 2.0–2.2 shims |
|----------|------------------------|----------------------|
| JDK 8    | 包含                   | **不包含**           |
| JDK 11+  | 包含                   | 包含                 |

若在 JDK 8 环境下执行 `./build.sh` 构建，发行包的 `lib/` 目录中 **不会** 包含 Flink 2.x
shims JAR。此时无法管理和提交 Flink 2.x 作业，需使用 JDK 11+ 重新构建。

### 推荐：完整支持 Flink 2.x 的构建方式

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
./build.sh
```

或直接使用 Maven：

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
./mvnw -Pshaded,webapp,dist -DskipTests clean install
```

当构建 JDK 为 11 及以上时，`flink-2.x-shims` profile 会自动激活。

## 何时需要手动配置 JDK

### 1. 构建包含 Flink 2.x 支持的发行包

当构建主机默认使用 JDK 8，但需要在发行包中包含 Flink 2.x shims 时，必须在执行
`./build.sh` 或 Maven 前将 `JAVA_HOME` 设置为 JDK 11+（见上文）。

### 2. 运行 Flink 2.x 作业

当 Flink 2.x 集群或客户端命令在运行时需要兼容的 JDK 时，请在 StreamPark 使用的 Flink
安装目录中配置：

```bash
# $FLINK_HOME/conf/flink-env.sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

如有需要，在 StreamPark 中更新或重新注册对应 Flink 环境。

## 验证方式

使用相同的 `FLINK_HOME` 验证 Flink CLI：

```bash
export FLINK_HOME=/path/to/flink
source $FLINK_HOME/conf/flink-env.sh
$FLINK_HOME/bin/flink --version
```

验证 StreamPark 构建产物是否包含 Flink 2.x shims：

```bash
ls $STREAMPARK_HOME/lib/streampark-flink-shims_flink-2.*
```

若上述命令无输出，说明该发行包由 JDK 8 构建，需使用 JDK 11+ 重新构建才能获得 Flink 2.x
支持。

## 相关配置文件

- StreamPark 服务 JDK：`$STREAMPARK_HOME/conf/streampark-env.sh`
- Flink 安装 JDK：`$FLINK_HOME/conf/flink-env.sh`
- Maven profile：`streampark-flink-shims/pom.xml` 中的 `flink-2.x-shims`
