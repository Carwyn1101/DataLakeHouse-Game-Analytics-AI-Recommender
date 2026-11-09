# Bronze fail-closed reads and safe upserts

## Dependency assessment

The [Bronze DAG](../LakeHouse_Project/airflow/dags/auto_upload_data.py) calls `detect_and_upload` every two minutes, with one retry after five minutes. The [Compose file](../LakeHouse_Project/docker-compose.yml) mounts input at `/opt/airflow/source_data` and provides the MinIO SDK to Airflow. The active [Bronze writer](../LakeHouse_Project/scripts/rawData_to_bronze.py) performs one read/modify/write per file in `lakehouse/bronze/`. The legacy direct-upload and append helpers have no repository callers.

[Silver](../LakeHouse_Project/scripts/bronze_to_silver_etl.py) reads `games.csv`, `reviews.csv` and `popularity.csv` with header and inferred types. Games requires embedded `tags`. The [CSV contracts](collector-staging.md) document 16, 7 and 7 columns, respectively. Upsert continues to publish under the same bucket, prefix and filenames. It preserves the existing target header order and accepts the same columns in a different order. The business key remains `steam_appid` and UTC timestamps retain their observation-time meaning.

## Before and after

| Area | Before Phase 1 | Phase 1 contract |
| --- | --- | --- |
| Object read | Broad exception handler treated access, transport and parsing failures as absence. | Only a MinIO SDK `S3Error` with code `NoSuchKey` permits initial creation. Other errors propagate and response cleanup is attempted even if reading fails. |
| CSV validation | The target could be replaced when its key column was missing. Differing source and target headers could be implicitly unioned. | Validate CSV shape, header uniqueness, keys and expected timestamp. The three Silver inputs require their existing columns. Extra columns are preserved when both headers have the same set. Invalid input or target fails before a write. |
| Key and timestamp | pandas inferred types and allowed fallback to other timestamps or content comparison. | Positive signed 64-bit decimal App IDs and valid ISO calendar dates or datetimes are required. Use `ts` for popularity and `ts_ingested` otherwise. Compare instants in UTC, treating naive values as UTC, and serialize UTC without an offset. |
| Source duplicates | Sorting could choose an invalid timestamp. Equal timestamps had no explicit tie policy. | Retain the newest valid row per key. Identical rows tied for newest collapse. Conflicting rows tied for newest or any invalid key/timestamp fail the object. |
| Target duplicates | Untouched duplicates survived and last row was used for comparisons. | Reject a duplicate target key before any write. Inspect affected objects under separate approval and resolve duplicates through a reviewed repair. |
| Existing key | Older or equal timestamps could replace the target on content differences. | Update only when the source timestamp is strictly newer. Older source is ignored. At equal time, target wins and conflicting CSV content is counted in a warning. Submit legitimate corrections with a later observation timestamp. |
| Empty input and reruns | Empty sources skipped. Unchanged nonempty runs rewrote the object. | Empty source is a no-op. A valid header-only target can accept inserts. An empty target without a header is invalid. An unchanged object skips the write and preserves its bytes. |
| Write failure and retry | Failure propagated. A retry reread the target and could rewrite the same object. | Failure propagates. A fresh read on retry writes a still-pending delta or skips if the prior write committed before its acknowledgement was lost. |
| Deployment configuration | Bronze has in-source MinIO connection settings and credentials. Airflow does not inject a dedicated Bronze credential contract. | Runtime settings remain compatible with the checked-in deployment. Credential migration requires separate approval. |

The [baseline Bronze assertions](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/blob/7342277e3ae812a1bb450d4e77581821db4dcac3/tests/reliability/test_characterization.py) reproduce the former behavior. The baseline evidence also remains in the [reliability audit](reliability-audit.md) and [test plan](reliability-test-plan.md). Equal-timestamp target precedence also avoids accepting corrections that Gold's strict bridge timestamp predicate may miss. Cross-object batch atomicity, target history repair, business-payload domain validation and concurrent-writer coordination remain outside this change.

## Rollout review

- Use a separately authorized read-only preflight for the three input and target headers, invalid or absent timestamps and duplicate keys. The source directory is still scanned as configured. Files without the Bronze CSV contract, such as a tags sidecar, will now fail validation and belong outside this input directory. Validate the configured mount before rollout.
- Plan migration to an Airflow-managed connection or deployment-injected secrets for the MinIO endpoint, TLS and scoped credentials. Check worker and DAG deployment wiring and MinIO permissions in an isolated stack before removing existing settings. Coordinate secret rotation separately.
- Validate the real SDK/server response, CSV reads in Spark, Airflow retry behavior and object integrity using a synthetic bucket. A single-object read/modify/write can still race with another writer and a multi-file retry can expose mixed publication epochs.

[MinIO SDK error contract](https://github.com/minio/minio-py/blob/7.2.20/minio/error.py)
