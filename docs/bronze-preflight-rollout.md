# Bronze preflight and native validation

PR #9 remains on hold. The checker and tests in this phase provide source-level evidence. Production data preflight and native execution require separate Project Owner approval.

## Deployment boundary

The [Bronze DAG](../LakeHouse_Project/airflow/dags/auto_upload_data.py) runs every two minutes, with one retry after five minutes and catchup disabled. The [writer](../LakeHouse_Project/scripts/rawData_to_bronze.py) lists every entry in `/opt/airflow/source_data`. Phase 1E restricts publication to the three exact filenames consumed by Silver. Auxiliary entries are ignored by the writer and counted by the checker.

The checked-in [Compose file](../LakeHouse_Project/docker-compose.yml) mounts the thesis workspace's data directory at `/opt/airflow/source_data` using an absolute host path. The worker and DAG processor inherit `./scripts:/opt/airflow/scripts` and `PYTHONPATH=/opt/airflow`. A checkout under a live script mount can expose code to subsequent module imports without a rebuild. A GitHub merge alone does not establish that any host checkout or container changed. Actual mounts, Compose overrides, worker imports, installed SDK and concurrency limits were not inspected. Running the checked-in Compose file from a different clone still uses the configured thesis data path.

[Silver](../LakeHouse_Project/scripts/bronze_to_silver_etl.py) reads `games.csv`, `reviews.csv` and `popularity.csv` with inferred schema and multiline CSV parsing. The [collector contract](collector-staging.md) keeps 15-column raw games and the tags sidecar in staging. Enrichment produces the 16-column games file with embedded tags. There is no automatic publish or sidecar join in Bronze or Silver.

## Read-only checker

From the repository root, run only against explicitly selected synthetic fixture directories:

`python -B -m tools.bronze_preflight --source-dir <synthetic-source-dir> --target-dir <synthetic-target-dir>`

- The [checker](../tools/bronze_preflight.py) accepts local directories only. It does not create files, invoke MinIO, discover services, read environment credentials, or normalize input. It opens only the three exact filenames in read mode. It counts auxiliary entries without reading them and rejects direct symlink/reparse entries. Use stable, trusted, independently selected directories and keep the inputs unchanged during the check.
- Output is one JSON document with fixed dataset names, counts and PASS/FAIL. Exit codes are 0 for PASS and 1 for FAIL. No row, key, raw header, unknown filename, path or exception text is emitted. Errors during parsing can leave partial counts, with FAIL preserved.
- It checks header presence and uniqueness, required columns, row width, App IDs, duplicate keys, timestamp form and range, tags, and source/target header sets. `non_iso_timestamps` means the timestamp does not match the accepted shape. `invalid_timestamps` means a shape-matching timestamp is impossible or outside the supported nanosecond range. `duplicate_appids` counts distinct duplicated keys. `duplicate_appid_rows` counts occurrences after the first.
- It is a conservative readiness screen. Auxiliary entries and repeated source keys are counted for review. Target duplicate keys still fail the screen. Blank or null-like tags appear in a separate `tags_quality_status` for Silver. The checker does not resolve or validate source payload ties at the newest timestamp. The three valid headers can pass with zero rows. Matched extra columns are counted and allowed. A missing file in a supplied target directory fails the comparison because local absence does not prove a live object returned `NoSuchKey`.
- Omitting `--target-dir` sets `target_checked=false`. A source-only PASS does not establish source-to-target compatibility. The checker does not predict Spark type inference, compare payloads across source and target, detect changing files, or authorize rollout.

### Approval required for real data

Request approval for two specific read-only inputs: directory metadata and the three CSV contents under the resolved active Airflow input mount, plus GET of the corresponding three Bronze object bodies through an approved read-only identity. Permit GET and the minimum metadata required to confirm object identity and stability. No PUT, DELETE, bucket operations, private records in Git or copying data into test fixtures. Confirm mount identity before opening a file and confirm input stability before and after the audit. Publish only the checker counters, per-dataset PASS/FAIL and aggregate schema differences. Any confirmed missing live object must be recorded from the SDK response by the authorized operator.

The local CLI has no live target adapter. A later authorized audit can use operator-provided read-only local views of the three target objects, or reuse the CSV validator through a separately reviewed GET-only streaming adapter. Both routes must keep private records in the approved environment. Do not infer the live target schema from a source-only run.

## Policy review

| Policy | Current behavior | Recommendation and correctness impact | Silver/Gold compatibility | Change and approval |
| --- | --- | --- | --- | --- |
| Unexpected entries | The scan publishes only the three exact input filenames. | Keep the allowlist. Count ignored entries in preflight and keep sidecars in staging. | Matches the three Silver reads. External consumers were not inspected. | Implemented in Phase 1E. Verify external dependencies before rollout. |
| Equal timestamp, different content | Target wins and Bronze logs a conflict count. | Keep target by default. Require an explicit reviewed correction with a later observation timestamp. | Avoids a same-time update that Gold's strict bridge watermark can miss. Confirm downstream correction behavior. | No default code change. Owner approval required for a correction workflow. |
| Newer row with empty tags | The newer observation can contain blank tags. | Preserve raw missing metadata and report it separately for Silver. | Silver applies its existing critical-field filter and downstream handling. | No Bronze rejection based solely on missing business metadata. |
| Duplicate target keys | Fails closed. | Keep failure. If discovered, use an approved migration with evidence for the winning row. | Avoids ambiguous current state for Silver deduplication and Gold. | No writer change. Owner approval required for any repair. |
| Invalid legacy timestamps | Fails closed. | Keep failure. Normalize only via an approved migration when the original instant can be proven. | Avoids inventing a timestamp that changes Silver history or Gold watermark behavior. | No writer change. Owner approval required for any normalization. |
| Overlapping writers | Unconditional read/modify/write can lose an insert. | Enforce one writer across scheduled, manual and external paths as the minimum rollout gate. | Prevents lost Bronze keys. Multi-object publication remains independent. | Operational or orchestration change and Owner approval required. |

Phase 1E keeps key resolution and business metadata quality separate. The scanner now allows only the three supported filenames. No credential, DAG or downstream changes are included.

## Isolated native plan

**NOT EXECUTED.** After separate approval, prepare a fixture-only stack. Do not use the repository's deployment Compose file.

1. Use a unique Compose project, internal network, dedicated non-external volumes, synthetic bucket/prefix and disposable test identity. Bind only an approved synthetic directory. Do not publish host ports or mount the Docker socket, thesis workspace or existing data directories.
2. Validate with `docker compose -p bronze-validation-<id> -f <isolated-compose> config --quiet`. Before starting, inspect resolved services, bind sources, endpoints, networks and volume identities locally. Stop if any path or resource resolves outside the test boundary. Check approved image and SDK versions against the intended deployment. Keep resolved credentials and environment output private.
3. In the isolated harness, inject a MinIO client for the disposable bucket into the Bronze module. Use the actual SDK and server. Verify `NoSuchKey` initial load, existing-object success, incomplete read and duplicate target rejection. Verify errors leave object bytes unchanged. Inject a failed write before commit and a lost acknowledgement after commit around real SDK writes. Check object state and the expected number of successful puts after retry.
4. Run `spark-submit --master local[2] <synthetic-harness.py>` against the test bucket using the deployment Spark version and Silver's actual options: header, schema inference, double-quote escaping and multiline. Assert field names and inferred types, UTC observation instants, blank numeric cells, embedded quotes and multiline tags. Invoke the existing Silver preprocessing functions on fixture DataFrames. Stop before Iceberg writes.
5. Run a fixture-only Airflow task wired to `detect_and_upload` with a disposable client injected in the task process and synthetic files mounted at the expected input path. Use an isolated metadata database. Verify a task failure and scheduler-driven retry with the intended settings. Use a two-writer barrier to demonstrate the race and verify the approved writer control prevents both writers from committing from the same snapshot.
6. Generate representative synthetic target sizes and record peak process memory, read duration and write count. Include the in-memory response buffer and confirm results against approved worker resource limits.

## Release gates

**Merge.** Require Tech Lead review and Project Owner approval. Obtain approved aggregate preflight evidence for both source and target, resolve compatibility decisions that affect the merge, and pass the isolated native contract and retry checks on the candidate SHA. Document any accepted residual risk.

**Deployment.** Require a separate rollout authorization. Verify actual Compose overrides, mounts and loaded worker SHA, enforce the approved single-writer policy, confirm the input batch and rollback procedure, and rerun preflight on the rollout inputs. Treat any host checkout under a live scripts mount as a deployment action.
