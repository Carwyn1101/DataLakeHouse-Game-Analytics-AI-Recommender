# GitHub-hosted native integration pilot

## Scope and reviewed revisions

This pilot prepares a manually gated smoke check on GitHub-hosted Ubuntu. The smoke observes runner resources and Docker, runs the reviewed offline harness tests, and stages/verifies synthetic bundles and Compose configuration. Native workload status remains **NOT EXECUTED**.

| Purpose | Exact commit |
| --- | --- |
| PR #14 harness | `adc2d56803b9493c2e366f9a31cc3b85ad4e9816` |
| Bronze PR #9 | `bcea0e946bb7b199e797943a82cfe82b65a89883` |
| Gold PR #13 | `b0f733580c75b561893580a1dd15b7f344602d36` |
| Merged Silver #10/#11 and Gold #12 baseline | `5fd358ab12a637b06b175d4da77d339026eac3bf` |

The pilot branch is based on the verified canonical main baseline. Its workflow checks out the reviewed PR #14 commit directly with a sparse checkout. The three ETL candidates are fetched as exact, depth-one Git objects. Build inputs are prepared by the harness's own provenance checks. The pinned [harness runbook](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/blob/adc2d56803b9493c2e366f9a31cc3b85ad4e9816/docs/native-integration-harness.md) supplies the native isolation and fixture contracts.

## Manual authorization before a merge

GitHub documents that `workflow_dispatch` requires a workflow on the default branch. This pilot uses `pull_request` with only the `labeled` activity, which evaluates the workflow in the PR merge ref and supports a workflow introduced by an unmerged PR. A merge conflict blocks this event. [GitHub workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)

The initial smoke gate is the Owner's explicit application of `native-pilot-smoke-approved` to the open, ready-for-review PR from `codex/github-actions-native-pilot` in this repository. The job requires both the original actor and the rerun actor to match `github.repository_owner`. The workflow pins PR #14 as its test source, so the gate belongs on the pilot PR containing the workflow.

After Tech Lead reviews the pilot head and Project Owner authorizes the smoke, the Owner can create the label if needed and apply it using GitHub's PR UI. The event can then queue the job subject to repository Actions policies. Owner authorization should record the reviewed pilot SHA and harness SHA. Repeat the review and label action for a changed pilot head. GitHub's rerun control replays the earlier event and revision.

Opening or updating this PR does not match the configured event. The current task delivers the workflow and documentation for review. Owner authorization was exercised for the smoke at pilot head 92f5519f3c0c20c993c4794a63b3c61200689880. GitHub routed the label event and the smoke completed successfully.

## Runner assessment

The repository was observed as public during preparation. GitHub's published standard `ubuntu-24.04` x64 runner offers 4 vCPUs, 16 GB RAM and 14 GB SSD for public repositories. Private repository allocations are 2 vCPUs, 8 GB RAM and 14 GB SSD. The advertised SSD allocation does not establish actual free space for Docker's layers and the job's build context. Larger runners, billing allowances and account Actions restrictions are outside this pilot. [Hosted runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)

The Ubuntu 24.04 image manifest currently lists Docker client/server 28.0.4, Compose 2.38.2 and Python 3.12.3. The label pins the OS family while the image is updated over time. The smoke records the image version and installed Docker/Compose versions and verifies that the daemon is local and serves Linux containers. [Ubuntu 24.04 image manifest](https://github.com/actions/runner-images/blob/main/images/ubuntu/Ubuntu2404-Readme.md)

The reviewed harness uses Spark `local[2]` and recommends at least 8 GB available to Docker. The smoke requires at least two logical CPUs and 8,000,000,000 bytes currently available RAM as an initial budget. This is a pilot check based on the runbook. Successful native processing and peak memory use remain unmeasured. The public runner is a plausible first target for small synthetic data. A private runner's 8 GB total leaves little room for the runner OS, Hive JVM, Spark JVM and build processes together.

The smoke records free storage for runner temp and Docker's data directory. Image download, unpacked layers, pip/apt caches, JARs and test volumes compete for the available disk. Full-stack fit within the published 14 GB budget is **NOT VERIFIED**. A smoke pass establishes the checks in the workflow, with native workloads still pending.

## Image and build dependencies

| Component | Reviewed requirement | Current validation |
| --- | --- | --- |
| Runner image | `apache/airflow:3.0.6-python3.10`, Java 17 | Source reviewed; build NOT EXECUTED |
| Processing libraries | PySpark 3.3.2, numpy 1.24.3, pandas 1.5.3, pyarrow 12.0.1, MinIO SDK 7.2.20 | Pin inspection only |
| JARs | Iceberg Spark 3.3 runtime 1.3.1, hadoop-aws 3.3.2, AWS SDK bundle 1.11.1026 | Maven fetch and checks NOT EXECUTED |
| Metastore | `apache/hive:4.0.0` with disposable Derby and the image's bundled S3A/SDK pair | Image/JAR compatibility NOT VERIFIED |
| Object storage | MinIO `RELEASE.2025-07-23T15-54-02Z`, official binary in the Bronze-only fixture image | Binary checksum and signature verified, fixture image build NOT EXECUTED |
| Build sources | Docker Hub, Quay, Debian apt repositories, PyPI and Maven Central | Reachability, rate limits, digests and total download size NOT VERIFIED |

The harness retains its test-only Python 3.10, local Spark and Derby choices. Its runtime preflight checks HMS and creates a Spark session before every scenario, including Bronze. A Bronze-first native run therefore needs the current MinIO/Hive/Spark prerequisites. A lighter MinIO-only invocation would need a separately reviewed harness change. Runner Python performs offline staging only. Registry tags can change and anonymous pull limits can affect a later build. Record image digests, dependency resolution and resource use when a build is separately authorized.

## Security and execution boundaries

- `permissions` grants only `contents: read`. The official checkout action is pinned to the verified v6.0.2 commit and uses the automatic GitHub token for checkout with credential persistence disabled. The workload receives no configured repository secrets, personal credentials, cloud identity or private network integration.
- Public candidate fetches are anonymous and restricted to the canonical repository and fixed SHAs. A visibility change can make these fetches fail and requires a reviewed access decision.
- The job uses a fresh GitHub-hosted VM. The smoke rejects Docker connection overrides and requires the local Unix socket. Its source and temporary files belong to the disposable runner. This removes dependence on Windows host mounts and the protected thesis workspace.
- GitHub-hosted runners have Internet access. Private routes, VPNs, SSH tunnels or thesis credentials would change the boundary and require review. The workflow defines only fixed GitHub source-fetch destinations and the local Docker endpoint.
- `NATIVE_ALLOW_WRITES` is fixed to `0`. Docker commands are limited to version/context/info queries and `compose config --quiet`. Generated test credentials remain in temporary bundles and are removed after each candidate check. Evidence is written to the job log and step summary, with no bundle/artifact upload.

## Staged native execution plan after approval

| Priority | Revision and scope | Required next authorization and evidence |
| --- | --- | --- |
| 1 | Bronze #9, `bronze` | Authorize the current disposable MinIO/Hive/Spark prerequisites, then exercise the Bronze candidate. Record initial/update/rerun, legacy ID, stale/equal-time, invalid-target and read-failure outcomes. Measure the shared runner image and prerequisite cost before expanding native scope. |
| 2 | Gold #13, `gold-helpers`, then targeted collision/partial recovery | Approve the full disposable Spark/Iceberg/Hive stack. Record table lookup/max-key behavior, no-write-on-failure evidence, snapshots and key mappings. |
| 3 | Merged baseline, Silver and Gold verification | Approve selected `silver` and `gold` scenarios. Capture native behavior of PR #10/#11/#12 and cleanup. |
| 4 | Remaining reliability scenarios | Select individual faults or concurrency cases based on preceding results and unresolved risks. |

Each workload expansion needs a separately reviewed workflow change with the exact scenario set and cleanup. Retain a result table with PASS, FAIL, SKIP and NOT EXECUTED per scenario. Candidate changes require newly reviewed SHAs.

## Bounded Bronze native validation

The first native workload reuses the reviewed harness commit `adc2d56803b9493c2e366f9a31cc3b85ad4e9816` and runs only `bronze` for PR #9 commit `bcea0e946bb7b199e797943a82cfe82b65a89883`. The existing smoke workflow remains the source of offline runner evidence.

After Tech Lead reviews the new PR #15 head and Project Owner approves this specific native workload, the repository Owner applies the separate `native-bronze-approved` label to open, non-draft PR #15. Record the reviewed pilot head, harness SHA and candidate SHA before applying the label. The job requires the label event and the first attempt on the same-repository pilot branch. For a new authorized attempt, the Owner removes and reapplies the Bronze label after review. The existing smoke label authorizes the smoke workflow. A push alone does not match either label event.

The job checks the local Linux Docker daemon and verifies the bundle against the reviewed CLI before resolving Compose. It compares the resource-limited model with the reviewed model, checks that the generated project is fresh and rejects subnet overlap with local routes or Docker IPAM. It builds the reviewed runner and Hive images and the checksum-pinned MinIO fixture, then starts MinIO and Hive. It checks ownership labels, exact volumes, IP addresses, the internal network, port bindings and applied fixture limits before the one-off runner invocation. The pinned runtime then checks its route, DNS/TCP peers, script hashes, fresh Hive catalog and absent synthetic bucket before the first Bronze write.

| Bound | Budget |
| --- | --- |
| Runner preflight | At least 2 CPU, 8 GB available RAM and 8 GiB free on both Docker and temporary filesystems |
| Runtime CPU and RAM | Runner 2 CPU / 4 GiB, Hive 1 CPU / 2 GiB, MinIO 0.5 CPU / 1 GiB |
| Runtime logs | 10 MB per file, two files per container |
| Time | 30 min job cap, 15 min build, 3 min MinIO fixture build, 2 min fixture startup, 5 min Bronze invocation |

The overlay lives outside the verified bundle and permits only the intended CPU, memory and log limits and the exact MinIO build and project-local image. The running fixture must use the image ID recorded after that build. The build uses the hosted VM's resources with Compose parallelism set to one. Build disk usage and runtime peak usage remain unmeasured.

The workflow reports the first failed stage and exit code or timeout, with bounded diagnostics from build, startup or the runner. Generated fixture credentials are redacted. Successful runs record the Bronze checksum and the fixture image IDs. Cleanup removes only verified project containers, network and named volumes. A failed ownership check blocks Docker cleanup, and VM disposal covers cancellation and leftover build layers. Bundles and raw inspection output are kept off published logs and artifacts.

The existing scenario checks initial IDs, insertion and price update, byte-preserving replay, legacy/normalized AppIDs, stale/equal timestamps, malformed or duplicate targets and injected `AccessDenied` with unchanged bytes and successful PUT counts. Direct assertions center on game records. Reviews and popularity pass through the fixtures without independent byte assertions. The injected denial tests SDK error handling. Live IAM policies, wire-level PUT attempts, concurrent updates and interruption during uploads remain outside this run.

## MinIO fixture provenance

Run [43132609847](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43132609847) built the runner and Hive images and stopped at the anonymous pull of `quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z` with HTTP authorization failure. Fixture startup and Bronze assertions were NOT EXECUTED. Anonymous manifest requests for that exact tag returned 401 on Quay and Docker Hub. The historical archive checksum endpoint returned 410.

The Bronze-only fixture uses the [upstream GitHub release asset](https://github.com/minio/minio/releases/expanded_assets/RELEASE.2025-07-23T15-54-02Z). Its Linux AMD64 ELF has no dynamic loader and includes the release and signed source revision `7ced9663e6a791fef9dc6be798ff24cda9c730ac`. The downloaded bytes matched SHA-256 `eef6581f6509f43ece007a6f2eb4c5e3ce41498c8956e919a7ac7b4b170fa431` and passed detached Minisign verification with the key in the [upstream release Dockerfile](https://github.com/minio/minio/blob/7ced9663e6a791fef9dc6be798ff24cda9c730ac/Dockerfile.release). BuildKit checks the same SHA-256 on download.

The Dockerfile pins official Red Hat UBI Micro at the verified Linux AMD64 manifest digest `sha256:c55b3d2c48728e4a4ebc3f63a716e5ba135a5f44097667eaea8ccd3f8404fa66`. It uses the same base family as upstream, a direct executable entrypoint, UID 0, the existing `server /data --address :9000` command and the scoped data volume. The build context contains only the Dockerfile read from the approved PR #15 Git object. Fixture credentials remain in the existing disposable runtime bundle.

A publisher-verified mirror with an accessible immutable digest would be simpler. A full source build could pin the signed release, Go toolchain, module checksums and base digest, with additional build time and downloads. The selected binary build pins the inputs and requires a build on the hosted runner to verify image assembly and startup.

The overlay is specific to PR #15 Bronze. PR #14's original bundle remains byte-verified against its reviewed SHA, and PR #9's candidate pin remains the same. Other native consumers of PR #14 still use its Quay reference. Moving this fix into the shared harness would need a new PR #14 review and corresponding repins in consuming workflows.

## Review decision

**GO for review of the bounded Bronze workflow. HOLD for Bronze image builds, service startup and writes pending the separate Owner label.** [Smoke run 42955361680](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/42955361680) passed 13 harness tests and staged and parsed all three bundles. Its runner reported 4 CPU, 15,836,282,880 bytes of available RAM and 92,147,126,272 bytes of free Docker storage. These are measurements from the smoke job. Bronze resource use remains unmeasured. Local validation used Python 3.13.5 and PyYAML 6.0.2. Offline validation for this correction passed 17 CI checks and 29 reliability tests. The CI suite checks the pinned inputs, the permitted overlay delta and rejection of an unexpected MinIO image ID. The replacement MinIO build, fixture startup and native integration remain NOT EXECUTED at the corrected pilot head.
