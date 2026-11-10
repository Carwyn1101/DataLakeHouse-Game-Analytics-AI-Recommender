# Isolated native integration harness

This harness is prepared for review. Docker startup, image builds, native tests and writes are pending separate authorization. Follow the [reliability test plan](reliability-test-plan.md) for the original fixture definitions and acceptance criteria.

## Scope and baselines

The runner generates synthetic CSVs and invokes the production Bronze MinIO upsert, Silver Iceberg transforms and SCD2, and Gold dimension, bridge, fact and verification functions. The runner calls the Gold tasks in a serial order permitted by the DAG. Airflow is an import dependency here. Scheduler, executor, retries, collector APIs, Windows file detection, Dremio and Power BI require separate validation. The source CSVs are generated in the runner. No original collection, runtime configuration or data is required.

| Revision | Full SHA | Initial focus |
| --- | --- | --- |
| Canonical main, includes Silver #10 and #11 and Gold #12 | `5fd358ab12a637b06b175d4da77d339026eac3bf` | Silver and Gold baseline |
| Bronze PR #9 | `bcea0e946bb7b199e797943a82cfe82b65a89883` | Bronze merge blockers |
| Gold PR #13 | `b0f733580c75b561893580a1dd15b7f344602d36` | Gold merge blockers and remaining risks |

Preparation reads only reviewed Git blobs from the recovery clone. Candidate refs need to be available locally as commit objects. A new SHA needs a reviewed allowlist update and static restaging. Preparation never checks out a candidate or combines two PR heads. Use the candidate's own source for each run and the pinned main for a separate comparison. The staged copies remove production endpoint, credential, bucket and JAR literals and the Bronze Windows/host directory discovery entry point. The runner injects only clients, disposable paths, a Spark factory and a fixed fixture year. Production transformation and table identifiers are preserved.

## Isolation checklist

- Use the recovery clone and a private, disposable Linux Docker context on a host that cannot route to thesis services or mount thesis directories. Clear overrides for Docker/Compose, and keep the generated directory under local, private system temp. Confirm the target Docker context before each command.
- Run `prepare.py verify` before building or launching any service. It reconstructs every expected byte of the run bundle, including Compose topology and staged sources, and rejects extra files or reparse points. The reviewed context contains no host mounts, named external volumes, published ports, fixed container names, Docker socket, devices, production bucket or production configuration. Never add override files or join external networks.
- A new Compose project and bucket are generated per scenario. The project owns two volumes and one internal IPv4-only network. The fixed test CIDR is `172.30.233.0/28`. Run one project at a time. Abort on an overlap with a Docker network, LAN or VPN. Reserve `.2` for MinIO, `.3` for the metastore and `.4` for Spark/runner.
- Build contexts contain only staged scripts or test metastore config. The generated `test.env` is outside both contexts. It contains a per-run MinIO identity and password used only by the fixtures. Keep it local, do not print environment sections or rendered Compose secrets, and never upload the bundle. Use only a trusted image/registry source for the runner and services.
- MinIO uses the new object volume at `/data`. Hive uses its own local Derby database and default warehouse at `/native-meta`. The runner verifies the fresh catalog and effective Spark/Hadoop settings before creating the test bucket. The Bronze API wrapper restricts bucket and object keys. Silver and Gold share only the new bucket, using separate `silver/`, `gold/` and `warehouse/` prefixes.
- Before launching the runner, verify the resolved configuration with `config --quiet`. After starting only the fixture services, inspect the exact generated network and volumes. Require the expected `com.docker.compose.project` labels, the internal network flag, exactly the two fixture endpoints on it, and only the expected project volumes. Inspect container mount and port metadata selectively. Abort on bind mounts, foreign containers, published ports, unexpected storage or routes.
- The runner defaults to writes disabled. With explicit opt-in it checks its staged scripts' hashes, per-run identity, DNS and TCP peer addresses, sole IPv4 route, local Spark master, S3A settings, HMS URI and fresh HMS/bucket. Failure aborts. These are additional checks alongside host isolation and the pre-start checklist. They do not authorize using a shared or production Docker context.
- Record the candidate SHA, scenario, image digests, preflight outcome and fixture-only evidence. Tear down only the reviewed Compose project after confirming its labels and retaining necessary test results. Use a fresh bundle for every rerun or comparison. Do not run global volume/network prune.

## Offline preparation and future runbook

Requirements for offline staging are Git, local candidate objects and Python 3.10 or newer. Staging and verification invoke no Docker commands.

```powershell
python -B -m unittest discover -s tests/native -v
python -B -m unittest discover -s tests/reliability -v
$run = python -B tests/native/prepare.py prepare --candidate bcea0e946bb7b199e797943a82cfe82b65a89883 --scenario bronze
if ($LASTEXITCODE) { throw 'Staging failed' }
python -B tests/native/prepare.py verify $run
if ($LASTEXITCODE) { throw 'Static preflight failed' }
```

The remaining commands are for a later authorized native run. Minimum runtime prerequisites are Docker Engine with Linux containers and Compose v2, an approved isolated context, access to the pinned image tags and build dependency sources, and adequate disk/RAM for a JVM-based runner and metastore. Start with at least 8 GB available to Docker. Record actual versions and image digests in the run evidence. Spark 3.3.2, Iceberg 1.3.1 and Hive 4.0.0 are pinned. The test runner uses Python 3.10 for Spark 3.3 compatibility and local Spark mode. Embedded Derby replaces the deployment's PostgreSQL for metadata only. These choices limit deployment parity.

```powershell
# Only after separate startup approval and the host-side checklist.
$project = (Get-Content -Raw (Join-Path $run 'runner/run.json') | ConvertFrom-Json).project
$compose = @('--project-name', $project, '--project-directory', $run,
             '--env-file', (Join-Path $run 'test.env'), '-f', (Join-Path $run 'compose.json'))
docker compose @compose --profile native config --quiet
if ($LASTEXITCODE) { throw 'Compose preflight failed' }
docker compose @compose --profile native build runner fixture-hms
if ($LASTEXITCODE) { throw 'Fixture build failed' }
docker compose @compose up -d fixture-minio fixture-hms
if ($LASTEXITCODE) { throw 'Fixture startup failed' }

# Run these scoped checks and review the resulting resource metadata before opt-in.
docker network inspect "${project}_isolated" --format '{{.Internal}} {{json .Labels}} {{json .Containers}}'
docker volume inspect "${project}_objects" "${project}_metastore" --format '{{.Name}} {{json .Labels}}'
foreach ($service in 'fixture-minio', 'fixture-hms') {
    $id = docker compose @compose ps -q $service
    if (!$id) { throw 'Fixture container unavailable' }
    docker inspect $id --format '{{json .Mounts}} {{json .NetworkSettings.Ports}}'
}

# Only after checking the identities, paths and endpoints above and authorizing test writes.
docker compose @compose --profile native run --rm --no-deps -e NATIVE_ALLOW_WRITES=1 runner
$scenarioExit = $LASTEXITCODE

# Save the scenario outcome and fixture-only evidence. Verify labels again before cleanup.
docker compose @compose --profile native down --volumes
```

## Native execution plan

Start with the candidate gate cases in order. Pass the scenario name to `--scenario` and repeat preparation for each row. The assertion result is independent of staging. A candidate can stage successfully and then fail the native contract. Fault injection is deterministic at the Python call boundary. It does not simulate an unobserved partial object upload, process crash, concurrent Spark job or network loss inside an Iceberg commit.

| Order | SHA / scenario | Synthetic input and acceptance evidence |
| --- | --- | --- |
| 1 | #9 / `bronze` | Bronze portions of TP-01/02/03/04/10/11/12/14 and the equal-time input of TP-09. F0 loads 101 and 202. F1/F2 inserts 303 and updates 101 while preserving 202. Unchanged replays, source alias `1.01e2`, stale input and equal-time conflict must leave bytes and PUT count unchanged. A legacy target ID such as 101.0 must also be preserved on a no-op. Malformed target, duplicate logical key, invalid target key/timestamp and synthetic AccessDenied must propagate with no PUT and identical target bytes. Use main as a separate negative control. |
| 2 | #13 / `gold-helpers` | TP-23. V2 existing/missing lookup and closed-port HMS failure. Verify empty, nonempty and all-null max results and missing table/column errors. Inject max lookup failure before dimension DELETE and compare game-key mapping and snapshots. Use main as a separate negative control. |
| 3 | #13 / `gold-create-collision`, `gold-partial` | TP-19/25 and a deterministic create collision related to TP-26. Create a marker table between absence and initial write. Verify it survives. Interrupt `dim_game` after DELETE and before APPEND. Read intermediate and retried snapshots and business/surrogate-key mapping. Current `createOrReplace()` and separate DELETE/APPEND can cause failed assertions. A helper test pass does not resolve these risks. |
| 4 | Main / `silver`, `silver-catalog`, `silver-create-collision` | Silver portions of TP-01/02/03/04 and catalog/create-only controls. F0 creates version 1 for three tables, F2 expires and increments the tracked versions, F1 adds one games key, and unchanged rerun preserves history and snapshot IDs. Verify lookup on existing and missing tables, closed-port failure, and survival of a racing create-only fixture table. |
| 5 | Main / `silver-before-update`, `silver-append-failure`, `silver-lost-ack` | TP-16/17. Inject before UPDATE, before APPEND commit or just after a successful APPEND. Use fresh Spark sessions to inspect committed history. Check version continuity by sequence number, expiry flags, current-row uniqueness and no extra snapshot on lost-ack retry. Observe the no-current interval after UPDATE without APPEND. |
| 6 | Main / `gold` | TP-01 and required-table reads in TP-24. A missing first or second required table must fail verification. On a complete synthetic run check 18 required tables, 11,323 calendar rows, two rows per fact and valid game-key references. |

For Gold downstream failure characterization, keep evidence for the first failing boundary. A separate native review should test repeated bridge refresh and failure after bridge DELETE, unchanged Gold fact reruns, invalid surrogate references after retry, real concurrent creation and genuine authorization/storage errors. Silver still needs cached/lazy snapshot behavior under executor or network faults and interval timestamp continuity. PR #12 also needs an intermediate-table native read failure and a native diagnostic-only JOIN failure. The current scenarios cover selected deterministic controls, while these remaining cases require separately designed fault injection. An Airflow task state is not inferred from a direct Python call.

## Verification status

Static verification covers allowlisted exact-SHA staging, sanitization, exact build-context membership, Compose topology, XML endpoint/volume configuration, fixture schema, and rejection of opt-in/DNS/object-scope violations using offline tests. Image availability and build, Compose resolution, actual volume ownership, network/DNS/route checks, Hive client/Derby startup, S3A authentication, Spark/Iceberg behavior and every native result are **NOT VERIFIED** until the isolated run is authorized.

Upstream references for runtime assumptions are [Compose internal networking](https://docs.docker.com/reference/compose-file/networks/), [Hive Docker entrypoint](https://github.com/apache/hive/blob/rel/release-4.0.0/packaging/src/docker/entrypoint.sh), [Hadoop S3A dependencies](https://hadoop.apache.org/docs/r3.3.6/hadoop-aws/tools/hadoop-aws/index.html) and [Spark's Python 3.11 support boundary](https://spark.apache.org/releases/spark-release-3-4-0.html).
