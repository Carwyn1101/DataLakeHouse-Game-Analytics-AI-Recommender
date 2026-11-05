# Recommender evaluation: metadata reference agreement

Evaluation is opt-in and **disabled by default**. It measures agreement with reference labels, not observed user satisfaction or proven recommendation quality.

## Configuration and UI

- `ENABLE_RECOMMENDER_EVAL=false` by default. Accepted enabled values: `1`, `true`, `yes`, `on` (case-insensitive).
- `RECOMMENDER_GROUND_TRUTH_PATH` is an explicit CSV path, empty by default. A container path must be visible inside `/app`, not a Windows host path.
- Compose forwards these two values without adding credentials or changing volumes/dependencies.
- Example for an existing, validated local artifact: set `RECOMMENDER_GROUND_TRUTH_PATH=/app/evaluation/tmp/recommendation_ground_truth.csv` and opt in explicitly.
- Disabled evaluation performs no GT reads and keeps the core recommendation flow unchanged.
- The Recommend tab uses `K=min(10, requested recommendations)`; chatbot recommendation uses `K=8`. K is never reduced because fewer valid recommendations were returned. Aggregate comparisons require the same K.
- Metrics use raw model values. Display percentages are copies; evaluation does not enter LLM prompts or explanations. Chat payloads store evaluation separately.
- The UI shows source/version, counts and a reference-agreement caveat. Missing/invalid/uncovered GT shows a status message, never a misleading zero-performance table.

## Ground-truth schema and validation

New generated CSVs contain:

```text
seed_game,seed_appid,relevant_game,relevant_appid,grade,metadata_score,source,dataset_version
```

The engine requires `relevant_appid`, `grade`, `source`, and either `seed_appid` or a legacy `seed_game`. `seed_appid` is the primary identity. A name-only lookup in an ID-based file must resolve to one seed identity; identical names associated with different IDs are ambiguous. Legacy name-only files cannot prove ID identity and are marked with a limitation.

IDs follow the PR #3 contract: positive integers, trimmed ASCII digit strings (leading zeros allowed), or finite integral floats below `2**53`. Boolean, null, fractional, non-positive, unsafe-float and malformed values are invalid. No invalid ID is converted to zero or truncated into another ID.

Grades must describe exact finite integers in `[0,3]`; boolean grades are invalid. Positive grades are relevant. Self-labels and invalid GT keys reject the file. Identical `(seed, relevant appid, grade)` duplicates collapse; conflicting grades reject it. A file must have one nonempty source and dataset version, never mixed provenance. Legacy files lacking `dataset_version` receive the explicit `legacy_unversioned` marker; they may be inspected per seed but cannot be aggregated until versioned.

`validate_ground_truth` raises static `GroundTruthError` codes. `load_ground_truth` translates absence into `gt_missing`, and malformed schema/labels or read failures into `gt_invalid`; exceptions/data/paths are not displayed as error details.

## Canonical list and metric contracts

`canonical_recommendations` validates IDs, excludes the seed and keeps the first valid occurrence of each canonical ID. Order is preserved; it never sorts by GT grade or refills from outside the returned list. UI rank numbers are renumbered to match this canonical list. `original_count`, `valid_unique_count` and `dropped_count` describe the full returned list before selecting Top-K. UI and evaluation use this same canonicalization.

K must be a positive integer, excluding boolean. Let `R` be the unique positive GT IDs for the seed and `hits` the unique relevant IDs in the canonical Top-K:

- `Precision@K = hits / K` (unfilled positions contribute zero).
- `Recall@K = hits / |R|`.
- `DCG@K = sum((2**grade_i - 1) / log2(i + 1))`, with rank `i` starting at 1.
- `IDCG@K` uses the largest GT grades in ideal order, at the same K.
- `NDCG@K = DCG@K / IDCG@K`.
- `RR@K = 1 / first relevant rank`, or zero if no hit.

Metrics are finite ratios in `[0,1]` without artificial clamping (the engine checks numerical tolerance). Presentation may multiply copies by 100. The engine never changes raw review ratios.

`aggregate_evaluations` computes `MRR@K` only across at least two distinct evaluable seed identities from one versioned benchmark and one K. Evaluable zero-hit/empty results are included. Missing/invalid/uncovered seeds are excluded with explicit coverage/status counts. Repeated evaluable seeds, mixed K, sources or versions are errors. One evaluable seed returns `insufficient_evaluable_seeds`, not a UI-labelled MRR.

## Explicit status contract

| Status | Evaluable | Metrics |
|---|---|---|
| `evaluated_hits` | yes | measured, at least one hit |
| `evaluated_zero_hits` | yes | zero-hit values |
| `evaluated_empty` | yes | zero values; GT is valid but canonical recommendations are empty |
| `gt_missing` | no | `None`; no path or missing file |
| `gt_invalid` | no | `None`; malformed/unreadable GT |
| `seed_not_covered` | no | `None` |
| `seed_ambiguous` | no | `None`; name lookup matches multiple seed IDs |
| `gt_no_positive_labels` | no | `None`; recall/IDCG denominator is undefined |

Invalid K or caller-supplied seed ID raises a safe `ValueError` instead of producing a metric. Unlabelled candidates have zero gain **for this reference benchmark**; this does not establish negative user preference.

## Explicit metadata-pseudo generator

`build_metadata_ground_truth.py` is a separate CLI. It is never imported or automatically executed by the Streamlit application. It reads current Silver game metadata via SELECT and writes local output only; there are no table mutations or rebuilds.

Required operator-provided credentials: `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`. There are no credential defaults; missing configuration fails before importing/initializing Spark. `HIVE_URI` and `MINIO_ENDPOINT` retain configurable internal-service defaults. Never put credential values in commands, commits, screenshots or manifests. Use the approved environment/secret mechanism of the deployment.

For an **authorized isolated fixture environment**, run:

```bash
python evaluation/build_metadata_ground_truth.py --max-silver-rows 30000 --seed-count 20 --per-seed 20 --min-score 0.18 --output evaluation/tmp/recommendation_ground_truth.csv
```

Do not use this example to run against persisted production Silver without separate authorization. `--help` is safe and does not require Spark or credentials.

- Row/seed/per-seed limits must be positive integers; zero/unbounded extraction is rejected. Minimum score must be finite in `(0,1]`.
- Weights: tags `.50`, genres `.25`, categories `.15`, developers `.10`.
- Null metadata has no tokens; mixed scalar/list values are normalized. No synthetic `nan` token is created from missing values.
- Input is normalized and sorted by canonical appid. Conflicting duplicate catalog keys are rejected.
- Seeds use preferred names followed by popularity descending, appid ascending for ties.
- Candidates use similarity descending and appid ascending for ties; self-matches are excluded.
- Grades: score `>= .42` → 3; `>= .30` → 2; positive lower scores → 1. No-overlap pairs are not positive labels.
- Default output is `evaluation/tmp/`, covered by the existing `tmp/` ignore rule. Both CSV and `.manifest.json` must remain ignored/local. An output outside the repository must be an operator-controlled private directory; output elsewhere inside the repository is rejected.
- Existing output/manifest files are never overwritten. The pair is not a multi-file transaction: after interruption, inspect artifacts and use a new path rather than overwrite an incomplete pair.

The manifest records source/methodology/weights, parameters, seed policy and selected IDs, UTC generation time, normalized input hash, deterministic dataset version, output SHA-256 and dataset/seed coverage. When available, metadata snapshot IDs observed before/after extraction are recorded **as observations only**. The read is not snapshot-pinned; even equal IDs do not prove atomic extraction. This limitation is explicit. The input hash identifies the extracted normalized rows; no snapshot identity is invented.

The loader validates CSV contents; operators should also verify the CSV SHA-256 against its manifest when provisioning the artifact. A matching manifest is not a substitute for input authorization or privacy review.

## Interpretation and testing

`metadata_pseudo` measures agreement with weighted metadata overlap. It shares signals with the recommender and has preferred/popularity seed selection, catalog limits and per-seed label truncation. These biases and unknown unlabelled candidates prevent claims about observed user satisfaction. Steam “More Like This”, if added later, is a platform reference, not automatically observed user behavior. Do not mix reference datasets without validation.

Run from repository root:

```bash
python -B -m unittest discover -s tests/evaluation -v
python -B -m unittest discover -s tests/core -v
python -B -m unittest discover -s tests/reliability -v
```

Acceptance tests use synthetic DataFrames, mocks and temporary directories. They cover independent math oracles, bounds, validation/status/coverage, deterministic generator output/provenance, redaction/configuration, opt-in UI integration and core regressions. Passing mocks does not establish native Streamlit/FAISS/Ollama/Spark integration. Native tests require an isolated runtime and synthetic warehouse; never point tests at production data.
