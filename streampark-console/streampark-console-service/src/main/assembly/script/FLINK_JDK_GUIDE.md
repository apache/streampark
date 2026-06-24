# Flink JDK and Build Guide

StreamPark supports multiple Flink major versions through version-specific shims modules.
Flink 2.x introduces a higher JDK requirement for both **building** and **running** Flink workloads.

## JDK Requirements by Flink Version

| Flink Version | Minimum JDK (Flink runtime) | StreamPark Console JDK |
|---------------|----------------------------|-------------------------|
| Flink 1.12–1.20 | JDK 8                    | JDK 8 / JDK 11          |
| Flink 2.0–2.2   | JDK 11                   | JDK 8 / JDK 11          |

> **Note:** StreamPark Console can continue to run on JDK 8. Using Flink 2.x does **not**
> require upgrading the StreamPark service JDK.

## What StreamPark Does Automatically

1. **Flink environment registration**
   - Parses Flink version from `$FLINK_HOME/lib/flink-dist*.jar` or falls back to
     `flink-dist --version`.
   - Registration does not require StreamPark to be built with Flink 2.x shims.
2. **Flink 2.x shims packaging**
   - Flink 2.0/2.1/2.2 shims are packaged only when StreamPark is **built with JDK 11+**
     (Maven profile `flink-2.x-shims`).

## Build Output vs JDK Version

| Build JDK | Flink 1.12–1.20 shims | Flink 2.0–2.2 shims |
|-----------|------------------------|----------------------|
| JDK 8     | Included               | **Not included**     |
| JDK 11+   | Included               | Included             |

If you build StreamPark with JDK 8 (for example via `./build.sh` on a Java 8 host), the
distribution will **not** contain Flink 2.x shims JARs under `lib/`. Flink 2.x management
and job submission will not work until you rebuild with JDK 11+.

### Recommended build command for full Flink 2.x support

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
./build.sh
```

Or with Maven directly:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
./mvnw -Pshaded,webapp,dist -DskipTests clean install
```

The `flink-2.x-shims` profile activates automatically when the build JDK is 11 or higher.

## When Manual JDK Configuration Is Required

### 1. Building a release with Flink 2.x support

Required when the build host defaults to JDK 8 but you need Flink 2.x shims in the
distribution package.

Set `JAVA_HOME` to JDK 11+ before running `./build.sh` or Maven (see above).

### 2. Running Flink 2.x jobs

Required when Flink 2.x clusters or client commands need a compatible JDK at runtime.

Configure JDK in the Flink installation used by StreamPark:

```bash
# $FLINK_HOME/conf/flink-env.sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Then update or re-register the Flink environment in StreamPark if needed.

## Verification

Verify Flink CLI with the same `FLINK_HOME`:

```bash
export FLINK_HOME=/path/to/flink
source $FLINK_HOME/conf/flink-env.sh
$FLINK_HOME/bin/flink --version
```

Verify Flink 2.x shims are present in a StreamPark build:

```bash
ls $STREAMPARK_HOME/lib/streampark-flink-shims_flink-2.*
```

If this command returns no files, the distribution was built with JDK 8 and must be rebuilt
with JDK 11+ for Flink 2.x support.

## Related Files

- StreamPark service JDK: `$STREAMPARK_HOME/conf/streampark-env.sh`
- Flink installation JDK: `$FLINK_HOME/conf/flink-env.sh`
- Maven profile: `flink-2.x-shims` in `streampark-flink-shims/pom.xml`
