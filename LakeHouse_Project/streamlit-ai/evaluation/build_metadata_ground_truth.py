"""Explicit offline CLI for metadata_pseudo labels, never run by Streamlit startup."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from numbers import Real
import os
from pathlib import Path
import re
import sys

import pandas as pd

# Allow `python evaluation/build_metadata_ground_truth.py` without service imports.
APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
from src.recommender_evaluation import canonical_appid, normalize_name, validate_ground_truth

DEFAULT_OUTPUT = APP_ROOT / 'evaluation' / 'tmp' / 'recommendation_ground_truth.csv'
METHODOLOGY = 'weighted_jaccard_v1'
WEIGHTS = {'tags': .50, 'genres': .25, 'categories': .15, 'developers': .10}
PREFERRED_SEEDS = ['ELDEN RING', 'DARK SOULS', 'Stardew Valley', 'Counter-Strike',
                   'Cyberpunk 2077', 'The Witcher 3: Wild Hunt', 'Hades', 'Hollow Knight',
                   "Baldur's Gate 3", 'Monster Hunter: World']
RAW_COLUMNS = ['steam_appid', 'name', *WEIGHTS, 'recommendations_total']
OUTPUT_COLUMNS = ['seed_game', 'seed_appid', 'relevant_game', 'relevant_appid',
                  'grade', 'metadata_score', 'source', 'dataset_version']


def split_tokens(value):
    if isinstance(value, (list, tuple, set)):
        return set().union(*(split_tokens(item) for item in value)) if value else set()
    if value is None or pd.isna(value):
        return set()
    if isinstance(value, Real) and not math.isfinite(value):
        return set()
    return {part.strip() for part in re.split(r'[,;/|]', str(value).casefold()) if part.strip()}


def jaccard(left, right):
    return len(left & right) / len(left | right) if left and right else 0.0


def relevance_score(seed, candidate):
    return sum(weight * jaccard(seed[f'{field}_set'], candidate[f'{field}_set'])
               for field, weight in WEIGHTS.items())


def grade_from_score(score):
    return 3 if score >= .42 else 2 if score >= .30 else 1 if score > 0 else 0


def validate_parameters(max_silver_rows, seed_count, per_seed, min_score):
    for value in [max_silver_rows, seed_count, per_seed]:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError('Row, seed and per-seed limits must be positive integers')
    if isinstance(min_score, bool) or not isinstance(min_score, Real) or not math.isfinite(min_score) or not 0 < min_score <= 1:
        raise ValueError('Minimum similarity must be in (0, 1]')


def prepare_games(frame):
    if not set(RAW_COLUMNS).issubset(frame.columns) or frame.empty:
        raise ValueError('Catalog schema is missing or empty')
    games = frame[RAW_COLUMNS].copy()
    ids = [canonical_appid(value) for value in games.steam_appid]
    if any(value is None for value in ids) or any(not normalize_name(v) for v in games.name):
        raise ValueError('Catalog contains invalid business keys or names')
    games['steam_appid'] = pd.Series(ids, index=games.index, dtype=object)
    games['name_norm'] = games.name.map(normalize_name)
    games['recommendations_total'] = pd.to_numeric(games.recommendations_total, errors='coerce').fillna(0)
    games.loc[~games.recommendations_total.map(math.isfinite), 'recommendations_total'] = 0
    for field in WEIGHTS:
        games[f'{field}_set'] = games[field].map(split_tokens)
        games[field] = games[f'{field}_set'].map(lambda tokens: ', '.join(sorted(tokens)))
    games = games.drop_duplicates(subset=RAW_COLUMNS)
    if games.steam_appid.duplicated().any():
        raise ValueError('Catalog has conflicting duplicate IDs')
    return games.sort_values('steam_appid', kind='stable').reset_index(drop=True)


def select_seeds(games, seed_count):
    selected, used = [], set()
    ordered = games.sort_values(['recommendations_total', 'steam_appid'], ascending=[False, True], kind='stable')
    for name in PREFERRED_SEEDS:
        target = normalize_name(name)
        matches = ordered[ordered.name_norm == target]
        if matches.empty:
            matches = ordered[ordered.name_norm.str.contains(target, regex=False, na=False)]
        if not matches.empty:
            row = matches.iloc[0]
            if row.steam_appid not in used:
                selected.append(row)
                used.add(row.steam_appid)
    for _, row in ordered.iterrows():
        if len(selected) >= seed_count:
            break
        if row.steam_appid not in used and row.tags_set and row.genres_set:
            selected.append(row)
            used.add(row.steam_appid)
    return pd.DataFrame(selected).head(seed_count)


def input_identity(games):
    records = games[RAW_COLUMNS].to_dict('records')
    payload = json.dumps(records, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def build_ground_truth(games, seed_count, per_seed, min_score, *, dataset_version):
    rows = []
    seeds = select_seeds(games, seed_count)
    for _, seed in seeds.iterrows():
        candidates = []
        for _, candidate in games.iterrows():
            if candidate.steam_appid == seed.steam_appid:
                continue
            score = relevance_score(seed, candidate)
            if score >= min_score:
                candidates.append((score, candidate))
        candidates.sort(key=lambda pair: (-pair[0], int(pair[1].steam_appid)))
        for score, candidate in candidates[:per_seed]:
            rows.append(dict(seed_game=seed['name'], seed_appid=int(seed.steam_appid),
                             relevant_game=candidate['name'], relevant_appid=int(candidate.steam_appid),
                             grade=grade_from_score(score), metadata_score=round(score, 6),
                             source='metadata_pseudo', dataset_version=dataset_version))
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS), seeds


def build_benchmark(frame, *, max_silver_rows=30000, seed_count=20, per_seed=20, min_score=.18,
                    input_reference=None):
    validate_parameters(max_silver_rows, seed_count, per_seed, min_score)
    if len(frame) > max_silver_rows:
        raise ValueError('Extracted input exceeds its declared row limit')
    games = prepare_games(frame)
    identity = input_identity(games)
    parameters = dict(max_silver_rows=max_silver_rows, seed_count=seed_count, per_seed=per_seed, min_score=min_score)
    version_payload = json.dumps(dict(input_hash=identity, parameters=parameters, methodology=METHODOLOGY, weights=WEIGHTS, preferred_seeds=PREFERRED_SEEDS), sort_keys=True)
    version = METHODOLOGY + '-' + hashlib.sha256(version_payload.encode()).hexdigest()[:16]
    output, seeds = build_ground_truth(games, seed_count, per_seed, min_score, dataset_version=version)
    if output.empty:
        raise ValueError('No pseudo labels meet the configured threshold')
    validate_ground_truth(output)
    payload = output.to_csv(index=False, lineterminator='\n').encode('utf-8')
    covered = output.seed_appid.nunique()
    manifest = dict(source='metadata_pseudo', methodology=METHODOLOGY, weights=WEIGHTS,
                    interpretation='Agreement with metadata-derived labels; not observed user satisfaction.',
                    parameters=parameters, seed_selection_policy='preferred names, then popularity descending; appid tie-break',
                    preferred_seeds=PREFERRED_SEEDS, selected_seed_appids=[int(v) for v in seeds.steam_appid],
                    generated_at_utc=datetime.now(timezone.utc).isoformat(), dataset_version=version,
                    input_sha256=identity, input_reference=input_reference or dict(
                        snapshot_reference=None, limitation='Snapshot identity unavailable; hash describes extracted rows only.'),
                    output_sha256=hashlib.sha256(payload).hexdigest(),
                    coverage=dict(input_rows=len(frame), unique_games=len(games), selected_seeds=len(seeds),
                                  covered_seeds=int(covered), uncovered_selected_seeds=len(seeds) - int(covered),
                                  relevant_games=int(output.relevant_appid.nunique()), label_rows=len(output)))
    return output, manifest


def read_config(environ=None):
    environ = os.environ if environ is None else environ
    if any(not environ.get(name, '').strip() for name in ['MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY']):
        raise ValueError('Required MinIO credentials are not configured')
    return dict(hive_uri=environ.get('HIVE_URI', 'thrift://hive-metastore:9083'),
                endpoint=environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
                access=environ['MINIO_ACCESS_KEY'], secret=environ['MINIO_SECRET_KEY'])


def get_spark_session(config=None):
    config = read_config() if config is None else config
    if not isinstance(config, dict) or any(not isinstance(config.get(key), str) or not config[key].strip()
                                           for key in ['hive_uri', 'endpoint', 'access', 'secret']):
        raise ValueError('Required source configuration is missing')
    # Lazy import, after credential validation; --help and pure tests need no Spark.
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName('BuildRecommendationGroundTruth').master('local[*]')
            .config('spark.driver.memory', '4g')
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1')
            .config('spark.sql.catalog.hive', 'org.apache.iceberg.spark.SparkCatalog')
            .config('spark.sql.catalog.hive.type', 'hive').config('spark.sql.catalog.hive.uri', config['hive_uri'])
            .config('spark.sql.catalog.hive.warehouse', 's3a://lakehouse')
            .config('spark.hadoop.fs.s3a.endpoint', config['endpoint'])
            .config('spark.hadoop.fs.s3a.access.key', config['access'])
            .config('spark.hadoop.fs.s3a.secret.key', config['secret'])
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false').getOrCreate())


def _observed_snapshot(spark):
    try:
        rows = spark.sql('SELECT snapshot_id FROM hive.silver.games.snapshots ORDER BY committed_at DESC LIMIT 1').collect()
        return str(rows[0][0]) if rows else None
    except Exception:
        return None


def load_games(max_silver_rows, config):
    spark = get_spark_session(config)
    try:
        before = _observed_snapshot(spark)
        frame = spark.sql(f'''SELECT steam_appid, name, tags_cleaned AS tags, genres_cleaned AS genres,
            categories_cleaned AS categories, developers_cleaned AS developers, recommendations_total
            FROM hive.silver.games WHERE is_current = true
            ORDER BY COALESCE(recommendations_total, 0) DESC, steam_appid
            LIMIT {int(max_silver_rows)}''').toPandas()
        after = _observed_snapshot(spark)
        return frame, dict(table='hive.silver.games', observed_snapshot_before=before, observed_snapshot_after=after,
                           snapshot_reference=None, limitation='Read was not snapshot-pinned; observed IDs do not prove atomic extraction. Input hash identifies exported rows.')
    finally:
        spark.stop()


def validate_output_path(path):
    path = Path(path).resolve()
    repo = APP_ROOT.parent.parent.resolve()
    ignored = DEFAULT_OUTPUT.parent.resolve()
    if path.suffix.lower() != '.csv' or (path.is_relative_to(repo) and not path.is_relative_to(ignored)):
        raise ValueError('Output must be CSV in evaluation/tmp or an operator-controlled private directory outside the repository')
    manifest = path.with_suffix('.manifest.json')
    if path.exists() or manifest.exists():
        raise ValueError('Output already exists; choose a new path')
    return path


def write_benchmark(path, output, manifest):
    path = validate_output_path(path)
    payload = output.to_csv(index=False, lineterminator='\n').encode('utf-8')
    if hashlib.sha256(payload).hexdigest() != manifest['output_sha256']:
        raise ValueError('Output content differs from manifest')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as stream:
        stream.write(payload)
    with path.with_suffix('.manifest.json').open('x', encoding='utf-8') as stream:
        json.dump(manifest, stream, ensure_ascii=False, sort_keys=True, indent=2)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Generate metadata_pseudo labels explicitly; never a startup job.')
    parser.add_argument('--max-silver-rows', type=int, default=30000)
    parser.add_argument('--seed-count', type=int, default=20)
    parser.add_argument('--per-seed', type=int, default=20)
    parser.add_argument('--min-score', type=float, default=.18)
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    try:
        validate_parameters(args.max_silver_rows, args.seed_count, args.per_seed, args.min_score)
        path = validate_output_path(args.output)
        config = read_config()
        games, reference = load_games(args.max_silver_rows, config)
        output, manifest = build_benchmark(games, max_silver_rows=args.max_silver_rows, seed_count=args.seed_count,
                                           per_seed=args.per_seed, min_score=args.min_score, input_reference=reference)
        write_benchmark(path, output, manifest)
    except Exception:
        print('Generation failed; check configuration, validated input and a new private/ignored output path.', file=sys.stderr)
        return 1
    print('Metadata pseudo ground truth and provenance manifest generated. Not observed user behavior.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
