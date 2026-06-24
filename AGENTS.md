<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache StreamPark — Agent Instructions

Project conventions, architecture, and coding patterns for the StreamPark codebase.

## Architecture

### Module Boundaries

StreamPark is a Maven multi-module project with four top-level modules. Each has a clear responsibility boundary.

- **`streampark-common`** (`streampark-common/`): Shared foundation layer. Contains configuration management (`ConfigKeys`, `ConfigOption`), utility classes (`Utils`, `HadoopUtils`, `JsonUtils`, `YarnUtils`), file system abstraction (`FsOperator`, `HdfsOperator`, `LfsOperator`), and shared enums (`FlinkDeployMode`, `ApplicationType`, etc.). Must be engine-agnostic — no Flink or Spark core API dependencies. All other modules depend on this module.

- **`streampark-flink`** (`streampark-flink/`): Flink development framework and runtime integration. Contains the core development API (`FlinkStreaming`, `FlinkTable`, `FlinkSQL`), version-specific shims layers (`streampark-flink-shims_flink-1.xx`), job submission clients (`streampark-flink-client`), Kubernetes integration (`streampark-flink-kubernetes`), application packer (`streampark-flink-packer`), and connectors. The shims proxy (`FlinkShimsProxy`) is the central mechanism for multi-version Flink support.

- **`streampark-spark`** (`streampark-spark/`): Spark development framework. Optional module, activated via `-Pspark` Maven profile. Contains `SparkStreaming`, `SparkBatch` core traits, Spark SQL client, and connectors. Follows the same lifecycle pattern as Flink modules.

- **`streampark-console`** (`streampark-console/`): Web management platform. Contains two submodules:
  - **`streampark-console-service`**: Spring Boot 2.7 backend, with `base/` (infrastructure), `core/` (business logic, controllers, services), and `system/` (authentication, user/role/team management) packages.
  - **`streampark-console-webapp`**: Vue 3 + Vite frontend, with `api/` (API layer), `views/` (pages), `components/` (reusable UI), `store/` (Pinia state).

The `streampark-common` module has the strongest stability guarantees — changes here affect all other modules. `streampark-flink/streampark-flink-core` public API (the `FlinkStreaming`, `FlinkTable` traits) should also be treated as stable — breaking changes require careful migration planning.

### High-Sensitivity Areas

- **`FlinkShimsProxy`**: The multi-version classloader isolation mechanism. Uses `ChildFirstClassLoader` to dynamically load version-specific shims JARs. Cached per Flink version. Changes here affect all Flink jobs across all versions. Never introduce static state that could leak across classloader boundaries.

- **`FlinkStreaming` / `FlinkTable` lifecycle**: The `main` -> `init` -> `ready` -> `handle` -> `destroy` lifecycle is the contract all user applications depend on. Changes to the execution order or initialization behavior can break existing applications in production.

- **`ConfigKeys` / `CommonConfig`**: Central configuration key definitions. Adding, removing, or renaming keys affects application configuration files, the console UI, and deployment scripts. Key names must remain backward-compatible.

- **`FlinkApplicationController` / `FlinkApplicationManageService` / `FlinkApplicationActionService`**: The core application management flow. Operations (start, stop, cancel, deploy) must be idempotent and handle all Flink states correctly. The `AppChangeEvent` annotation triggers state synchronization.

- **SQL parsing and validation** (`FlinkSql`, `FlinkSqlService`, `SqlConvertUtils`): SQL validation must be version-aware (Flink 1.12-1.20 have different SQL syntax). The `sql-rev.dict` file handles MySQL-to-PostgreSQL dialect conversion.

- **Kubernetes integration** (`FlinkK8sWatchController`): Uses Caffeine caches (`TrackIdCache`, `JobStatusCache`, `MetricCache`) for tracking K8s-deployed Flink jobs. Cache invalidation and TTL must be correct to avoid stale state.

- **Database schema changes**: All schema changes must have corresponding upgrade scripts in `streampark-console/.../script/upgrade/` for both MySQL and PostgreSQL. The `sql-rev.dict` file must be updated if new SQL dialect differences are introduced.

- **Authentication & Authorization**: `ShiroConfig`, `JWTUtil`, `ShiroRealm` — changes here affect all user access. The `@Permission` annotation and `PermissionAspect` enforce team-level resource isolation. Never weaken RBAC checks.

## Design Patterns

- **Lifecycle trait pattern**: `FlinkStreaming`, `FlinkTable`, `SparkStreaming`, `SparkBatch` all follow the same trait-based lifecycle: `main` -> `init` -> `ready` -> `handle` -> `start` -> `destroy`. Users override `handle()` (required) and optionally `ready()`, `config()`, `destroy()`. Never add mandatory lifecycle methods to existing traits.

- **Shims / Proxy pattern**: `FlinkShimsProxy.proxy(flinkVersion, func)` isolates version-specific Flink API calls behind a `ChildFirstClassLoader`. Each Flink version has its own shims module (`streampark-flink-shims_flink-1.xx`) with the same interface. New shims methods must be added to all version modules.

- **Service layer separation**: Console services are split by responsibility — `FlinkApplicationManageService` (CRUD), `FlinkApplicationActionService` (start/stop/cancel), `FlinkApplicationInfoService` (query/info). Follow this pattern when adding new application operations.

- **Implicit enrichment**: Scala `implicit` conversions are used to extend Flink/Spark APIs (e.g., `DataStreamExt` adds methods to `DataStream`). New implicit conversions must be scoped to avoid polluting the global namespace.

- **MyBatis-Plus entity pattern**: Entities extend `BaseEntity` (auto-fill `createTime`/`modifyTime`). Mappers extend MyBatis-Plus `BaseMapper`. Pagination uses `MybatisPager` + `PaginationInterceptor`. Follow existing patterns rather than introducing new ORM approaches.

- **Enum-based configuration**: Both Java enums (`FlinkDeployMode`, `ApplicationType`) and Scala enumeratum enums (`ApiType`, `PlannerType`) are used. Prefer Scala `enumeratum` for new Scala-side enums — it provides better type safety and serialization.

- **REST response pattern**: All controller methods return `RestResponse.success(data)` or `RestResponse.fail(...)`. Never return raw objects. Use `@Permission` annotation for access control, `@AppChangeEvent` for state-change auditing.

- **File system abstraction**: Use `FsOperator` (with `HdfsOperator` / `LfsOperator` implementations) for file operations. Never use raw `java.io.File` or Hadoop `FileSystem` directly in business logic.

## Coding Conventions

### Java (Backend)

- **Formatting**: Eclipse formatter via Spotless (`tools/checkstyle/spotless_streampark_formatter.xml`). Run `./mvnw spotless:apply` before committing.
- **Import order**: `org.apache.streampark`, `org.apache.streampark.shaded`, `org.apache`, `javax`, `java`, `scala`, `\#` (all others).
- **Static checks**: Checkstyle (`tools/checkstyle/checkstyle.xml`) + Spotless. No wildcard imports. No `@author` tags. No JUnit 4 imports.
- **Lombok**: Use `@Slf4j`, `@Data`, `@Builder` where appropriate. Do not use `@EqualsAndHashCode` on JPA/Hibernate entities.
- **Testing**: JUnit 5 (`org.junit.jupiter`) + AssertJ. Use `@Test` (not `@Test` from JUnit 4). Use `assertThat(...).isEqualTo(...)` style. Test classes should be in the same package as the code under test.
- **Package structure**: Controllers in `controller/`, service interfaces in `service/`, implementations in `service/impl/`, entities in `entity/`, mappers in `mapper/`, enums in `enums/`.

### Scala (Framework / Common)

- **Formatting**: Scalafmt 3.7.5 (`tools/checkstyle/.scalafmt.conf`). Max column 160. Run `./mvnw spotless:apply` to format.
- **Import ordering**: `org.apache.streampark.*` first, then other third-party, then `javax.*`, `java.*`, `scala.*`.
- **Static checks**: Scalastyle (`tools/checkstyle/scalastyle-config.xml`). No wildcard imports. No `println` statements (use `Logger` trait).
- **Testing**: ScalaTest 3.2.9. Use `FlatSpec` or `FunSuite` style consistent with existing tests.
- **Style**: Use `val` over `var`. Prefer `Option` over `null` in public APIs. Use `lazy val` for expensive initialization. Use pattern matching instead of `isInstanceOf`/`asInstanceOf`.

### TypeScript / Vue (Frontend)

- **Formatting**: ESLint + Prettier. Run `pnpm lint:eslint` and `pnpm lint:prettier`.
- **Vue 3 Composition API**: Use `<script setup lang="ts">` for new components. Use Pinia for state management (not Vuex).
- **API layer**: All HTTP calls go through `src/api/` modules using `defHttp`. API URLs are defined as enum constants. Never use `axios` or `fetch` directly in components.
- **Component naming**: PascalCase for component files and names. Use `index.ts` barrel exports in component directories.
- **Unused variables**: Prefix with `_` to suppress ESLint warnings (`argsIgnorePattern: '^_'`).

### General

- **Apache License header**: Required on all new files. Use the copyright header from `tools/checkstyle/copyright.txt`. Enforced by Spotless and Apache RAT.
- **No wildcard imports**: Prohibited in both Java and Scala (enforced by Spotless).
- **No personal pronouns in comments**: Use descriptive, impersonal documentation.
- **Database**: Support both MySQL and PostgreSQL. All new SQL must be tested against both. Use `sql-rev.dict` for dialect differences.
- **Logging**: Use Lombok `@Slf4j` (Java) or `Logger` trait (Scala). Use `log.info`/`log.error` with parameterized messages. Never log credentials or sensitive data.

## Commands

- **Full build (backend + frontend, skip tests):**
  ```shell
  ./build.sh
  ```
  Equivalent to: `./mvnw -Pshaded,webapp,dist -DskipTests clean install`

- **Fast build (backend only, skip all checks):**
  ```shell
  ./mvnw -Pfast clean install -DskipTests
  ```

- **Build with Spark module:**
  ```shell
  ./mvnw -Pspark,shaded clean install -DskipTests
  ```

- **Build backend only:**
  ```shell
  ./mvnw clean install -DskipTests -pl '!streampark-console/streampark-console-webapp'
  ```

- **Run single test class (Java):**
  ```shell
  ./mvnw test -pl streampark-console/streampark-console-service -Dtest=FlinkApplicationControllerTest
  ```

- **Run single test class (Scala):**
  ```shell
  ./mvnw test -pl streampark-common -Dtest=org.apache.streampark.common.util.CommandUtilsTest
  ```

- **Format code (Java + Scala):**
  ```shell
  ./mvnw spotless:apply
  ```

- **Check formatting:**
  ```shell
  ./mvnw spotless:check
  ```

- **Run Checkstyle:**
  ```shell
  ./mvnw checkstyle:check
  ```

- **Frontend development server:**
  ```shell
  cd streampark-console/streampark-console-webapp && pnpm dev
  ```

- **Frontend lint:**
  ```shell
  cd streampark-console/streampark-console-webapp && pnpm lint:eslint && pnpm lint:prettier
  ```

- **Frontend build:**
  ```shell
  cd streampark-console/streampark-console-webapp && pnpm build
  ```

- **Run Docker Compose (local):**
  ```shell
  docker compose -f docker/docker-compose.yaml up -d
  ```

## PR & Commit Conventions

- **PR title format**: `[Module] Description` (e.g., `[Flink] Fix shims classloader isolation`, `[Console] Add team-level resource filtering`, `[Common] Support Hadoop 3.x configuration`).
- **Module prefixes**: `[Common]`, `[Flink]`, `[Spark]`, `[Console]`, `[K8s]`, `[Docs]`, `[CI]`, `[Build]`.
- **One concern per PR**: Unrelated whitespace, import, or formatting changes go in separate PRs. Do not mix refactoring with feature work.
- **Commit messages**: Describe the *what* and *why*, not implementation details. Reference related GitHub issues with `#xxx`.
- **Apache License header**: Required on all new files (enforced by Spotless and Apache RAT). The `spotless:check` and `apache-rat:check` goals run in CI.
- **Schema changes**: Must include upgrade scripts for both MySQL and PostgreSQL in `streampark-console/.../script/upgrade/`.
- **New shims methods**: When adding a new method to the shims interface, add implementations to all version-specific shims modules (`streampark-flink-shims_flink-1.12` through `streampark-flink-shims_flink-1.20`).

## Boundaries

### Never Without Explicit Discussion

- **Never** modify `.asf.yaml`, `LICENSE`, `NOTICE`, or `.gitignore` without explicit discussion.
- **Never** upgrade Flink, Spark, Scala, Spring Boot, or other major dependency versions without discussion — these changes have broad impact across the entire project.
- **Never** remove or rename keys in `ConfigKeys` or `CommonConfig` — these are user-facing configuration contracts.
- **Never** change the `FlinkStreaming` / `FlinkTable` lifecycle method signatures — this breaks all user applications.
- **Never** add new required lifecycle methods to the `FlinkStreaming` / `FlinkTable` traits.
- **Never** commit secrets, credentials, API keys, or cloud-specific tokens.
- **Never** introduce a new Flink version shims module without adding the corresponding CI build configuration.
- **Never** add a new database migration without providing both MySQL and PostgreSQL upgrade scripts.
- **Never** change the `SqlConvertUtils` dialect conversion logic without testing against both MySQL and PostgreSQL.

### Ask First

- **Ask first** before adding new third-party dependencies — license compatibility with Apache 2.0 matters.
- **Ask first** before promoting package-private classes/methods to public.
- **Ask first** before adding new Maven modules or restructuring the module hierarchy.
- **Ask first** before introducing new Scala implicits in the `common` module — they affect all downstream code.
- **Ask first** before changing the authentication model (Shiro/JWT/Pac4j configuration).