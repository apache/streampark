# StreamPark JDK Upgrade Guide

StreamPark 3.0 requires **JDK 11 or later** to run the Console process.

## Console JDK vs Job JDK

| Component | JDK requirement | Configuration |
|-----------|-----------------|---------------|
| StreamPark Console | **JDK 11+** | `JAVA_HOME` in `streampark-env.sh` or system environment |
| Flink jobs | Depends on Flink version | `$FLINK_HOME/conf/flink-env.sh` → `JAVA_HOME` |
| Spark jobs | Depends on Spark version | `$SPARK_HOME/conf/spark-env.sh` → `JAVA_HOME` |

Upgrading the Console to JDK 11 **does not require** upgrading Flink/Spark cluster JDK at the same time.

## Compatibility Matrix

| Engine | Minimum job JDK | Recommended job JDK |
|--------|-----------------|---------------------|
| Flink 1.17–1.20 | 8 | 11 |
| Flink 2.x | 11 | 11 or 17 |
| Spark 3.5+ | 11 | 11 or 17 |

## Upgrade Steps

### 1. Standalone deployment

1. Install JDK 11 (e.g. OpenJDK 11, Amazon Corretto 11).
2. Set `JAVA_HOME` in `conf/streampark-env.sh`:
   ```bash
   export JAVA_HOME=/path/to/jdk-11
   ```
3. Restart Console:
   ```bash
   ./bin/streampark.sh restart
   ```
4. Verify:
   ```bash
   ./bin/streampark.sh status
   java -version   # should show 11+
   ```

### 2. Docker deployment

Official StreamPark Docker images from 3.0 onward use JDK 11 internally. Pull the latest image:

```bash
docker pull apache/streampark:latest
```

### 3. Build from source

JDK 11+ is required to build StreamPark 3.0:

```bash
export JAVA_HOME=/path/to/jdk-11
./build.sh
```

## JVM Options

StreamPark enables the following JVM options by default (see `bin/jvm_opts.sh`):

```
--add-opens java.base/jdk.internal.loader=ALL-UNNAMED
--add-opens jdk.zipfs/jdk.nio.zipfs=ALL-UNNAMED
```

These are required for dynamic classpath loading (Flink shims, user JARs). Do not remove them unless you know the impact.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `StreamPark requires JDK 11 or later` on start | Console running on JDK 8 | Upgrade `JAVA_HOME` to JDK 11+ |
| `NoSuchFieldException: ucp` | Missing `--add-opens` | Ensure `jvm_opts.sh` is not overridden incorrectly |
| `javax.annotation.PostConstruct` not found | Missing annotation API | Use StreamPark 3.0+ distribution (includes dependency) |
| Hadoop/YARN connection issues on JDK 11 | Jersey classpath | Verify Hadoop 3.3.x; check Hadoop client compatibility |

## Related Issues

- #4409 — JDK 11 migration proposal
- #4410 — StreamPark 3.0 roadmap
