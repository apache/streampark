# Spark JDK Configuration Guide

StreamPark registers Spark environments and submits Spark jobs on behalf of users. Spark major
versions have different minimum JDK requirements. StreamPark resolves the JDK automatically in
most cases, but some deployments still require explicit configuration.

## JDK Requirements by Spark Version

| Spark Version | Minimum JDK | Recommended JDK |
|---------------|-------------|-----------------|
| Spark 2.x     | JDK 8       | JDK 8           |
| Spark 3.x     | JDK 8       | JDK 8 / JDK 11  |
| Spark 4.x     | JDK 17      | JDK 17 / JDK 21 |

> **Note:** StreamPark Console itself can run on JDK 8. Registering Spark 4.x does **not** require
> upgrading the StreamPark service JDK.

## What StreamPark Does Automatically

1. **Spark environment registration**
   - Parses Spark version from `$SPARK_HOME/jars/spark-core_*.jar` or the `RELEASE` file.
   - Does **not** depend on the StreamPark service JDK.
2. **Spark job submission**
   - Resolves `JAVA_HOME` in the following order:
     1. `$SPARK_HOME/conf/spark-env.sh`
     2. process environment variable `JAVA_HOME`
     3. system auto-detection (macOS `/usr/libexec/java_home`, common Linux JDK paths)

## When Manual JDK Configuration Is Required

Configure JDK manually when **all** of the following are true:

- Spark 4.x (or another version with a higher JDK requirement) is used, and
- `JAVA_HOME` is not set in `$SPARK_HOME/conf/spark-env.sh`, and
- the target JDK cannot be auto-detected on the StreamPark host (common on Linux servers with
  multiple JDK installations).

### Recommended: configure Spark `spark-env.sh`

Edit `$SPARK_HOME/conf/spark-env.sh` on the StreamPark host (create it from
`spark-env.sh.template` if needed):

```bash
# Spark 4.x example
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

After updating `spark-env.sh`, re-register or update the Spark environment in StreamPark if the
Spark installation path changed.

### Alternative: configure StreamPark host environment

Set `JAVA_HOME` before starting StreamPark only when the same JDK should be used for all Spark
versions on that host:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
./streampark.sh start
```

For mixed Spark 2/3 and Spark 4 deployments on one StreamPark instance, prefer configuring
`JAVA_HOME` in each Spark installation's `spark-env.sh` instead of changing the StreamPark
service JDK.

## Verification

Check Spark CLI locally with the same `SPARK_HOME`:

```bash
export SPARK_HOME=/path/to/spark
source $SPARK_HOME/conf/spark-env.sh
$SPARK_HOME/bin/spark-submit --version
```

If this command succeeds, StreamPark can submit jobs with the same Spark installation.

## Related Files

- StreamPark service JDK: `$STREAMPARK_HOME/conf/streampark-env.sh`
- Spark installation JDK: `$SPARK_HOME/conf/spark-env.sh`
