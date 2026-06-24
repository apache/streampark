# Spark JDK 配置说明

StreamPark 负责注册 Spark 环境并代为提交 Spark 作业。不同 Spark 大版本对 JDK 的最低要求不同。
StreamPark 在多数场景下会自动解析 JDK，但部分部署环境仍需要手动配置。

## Spark 版本与 JDK 要求

| Spark 版本 | 最低 JDK | 推荐 JDK        |
|------------|----------|-----------------|
| Spark 2.x  | JDK 8    | JDK 8           |
| Spark 3.x  | JDK 8    | JDK 8 / JDK 11  |
| Spark 4.x  | JDK 17   | JDK 17 / JDK 21 |

> **说明：** StreamPark Console 本身可以继续运行在 JDK 8 上。注册 Spark 4.x **不需要**升级
> StreamPark 服务的 JDK。

## StreamPark 自动处理的内容

1. **注册 Spark 环境**
   - 从 `$SPARK_HOME/jars/spark-core_*.jar` 或 `RELEASE` 文件解析版本。
   - **不依赖** StreamPark 服务当前使用的 JDK。
2. **提交 Spark 作业**
   - 按以下顺序解析 `JAVA_HOME`：
     1. `$SPARK_HOME/conf/spark-env.sh`
     2. 进程环境变量 `JAVA_HOME`
     3. 系统自动探测（macOS `/usr/libexec/java_home`、Linux 常见 JDK 路径）

## 何时需要手动配置 JDK

当 **同时满足** 以下条件时，需要手动配置：

- 使用 Spark 4.x（或其他对 JDK 要求更高的版本），且
- `$SPARK_HOME/conf/spark-env.sh` 中未设置 `JAVA_HOME`，且
- StreamPark 所在主机无法自动探测到目标 JDK（Linux 多 JDK 环境较常见）。

### 推荐方式：配置 Spark 的 `spark-env.sh`

在 StreamPark 所在主机编辑 `$SPARK_HOME/conf/spark-env.sh`（若不存在，可从
`spark-env.sh.template` 复制）：

```bash
# Spark 4.x 示例
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

修改后，如 Spark 安装路径有变化，请在 StreamPark 中重新注册或更新对应 Spark 环境。

### 备选方式：配置 StreamPark 主机环境变量

仅当同一台 StreamPark 主机上的所有 Spark 版本共用同一 JDK 时，可在启动 StreamPark 前设置：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
./streampark.sh start
```

若同一 StreamPark 实例需要同时管理 Spark 2/3 与 Spark 4，请优先在各 Spark 安装的
`spark-env.sh` 中分别配置 `JAVA_HOME`，而不是修改 StreamPark 服务 JDK。

## 验证方式

在 StreamPark 主机上使用相同的 `SPARK_HOME` 验证：

```bash
export SPARK_HOME=/path/to/spark
source $SPARK_HOME/conf/spark-env.sh
$SPARK_HOME/bin/spark-submit --version
```

若上述命令可正常执行，StreamPark 通常也能使用该 Spark 安装提交作业。

## 相关配置文件

- StreamPark 服务 JDK：`$STREAMPARK_HOME/conf/streampark-env.sh`
- Spark 安装 JDK：`$SPARK_HOME/conf/spark-env.sh`
