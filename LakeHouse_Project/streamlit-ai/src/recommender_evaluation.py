"""Pure graded-relevance evaluation; no FAISS, Spark, services or startup jobs."""
from collections import Counter
import csv
from decimal import Decimal, InvalidOperation
import math
from numbers import Integral, Real
import os
from pathlib import Path
import re

import pandas as pd


EVALUATED_STATUSES = {'evaluated_hits', 'evaluated_zero_hits', 'evaluated_empty'}
METRIC_NAMES = ('precision_at_k', 'recall_at_k', 'ndcg_at_k', 'rr_at_k')


class GroundTruthError(ValueError):
    """A static, safe reason code; never embed dataset contents or exception text."""


def canonical_appid(value):
    """Match PR #3: positive integers/digit strings and safe integral floats only."""
    if isinstance(value, bool):
        return None
    if isinstance(value, Integral):
        return int(value) if value > 0 else None
    if isinstance(value, str):
        value = value.strip()
        if not value.isascii() or not value.isdecimal():
            return None
        try:
            number = int(value)
        except ValueError:
            return None
        return number if number > 0 else None
    if isinstance(value, Real):
        number = float(value)
        if math.isfinite(number) and 0 < number < 2**53 and number.is_integer():
            return int(number)
    return None


def validate_k(k):
    if isinstance(k, bool) or not isinstance(k, Integral) or k <= 0:
        raise ValueError('K must be a positive integer')
    return int(k)


def normalize_name(value):
    if not isinstance(value, str):
        return ''
    return re.sub(r'\s+', ' ', value.strip().casefold())


def canonical_recommendations(recommendations, *, seed_appid=None):
    """Keep first valid canonical ID order, excluding the seed; never pad/refill."""
    if recommendations is None:
        recommendations = pd.DataFrame()
    if not isinstance(recommendations, pd.DataFrame):
        raise ValueError('Recommendations must be a DataFrame')
    seed = canonical_appid(seed_appid) if seed_appid is not None else None
    if seed_appid is not None and seed is None:
        raise ValueError('Invalid seed ID')
    seen, positions, ids = set(), [], []
    values = recommendations.get('steam_appid', pd.Series(None, index=recommendations.index, dtype=object))
    for position, value in enumerate(values):
        appid = canonical_appid(value)
        if appid is None or appid == seed or appid in seen:
            continue
        seen.add(appid)
        positions.append(position)
        ids.append(appid)
    canonical = recommendations.iloc[positions].copy()
    canonical['steam_appid'] = pd.Series(ids, index=canonical.index, dtype=object)
    if 'rank' in canonical:
        canonical['rank'] = range(1, len(canonical) + 1)
    counts = dict(original_count=len(recommendations), valid_unique_count=len(canonical),
                  dropped_count=len(recommendations) - len(canonical))
    return canonical, counts


def _grade(value):
    if isinstance(value, bool) or not isinstance(value, (str, Integral, Real)):
        raise GroundTruthError('invalid_grade')
    try:
        # Decimal avoids rounding a fractional CSV string into an integer grade.
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise GroundTruthError('invalid_grade') from None
    if not number.is_finite() or number != number.to_integral_value() or not 0 <= number <= 3:
        raise GroundTruthError('invalid_grade')
    return int(number)


def validate_ground_truth(frame):
    """Validate the entire file; conflicting sources/versions/grades never merge."""
    required = {'relevant_appid', 'grade', 'source'}
    if not isinstance(frame, pd.DataFrame) or frame.empty or not required.issubset(frame.columns):
        raise GroundTruthError('invalid_schema_or_empty')
    if not frame.columns.is_unique:
        raise GroundTruthError('duplicate_columns')
    has_ids = 'seed_appid' in frame
    if not has_ids and 'seed_game' not in frame:
        raise GroundTruthError('missing_seed_identity')
    versions = frame.get('dataset_version', pd.Series('legacy_unversioned', index=frame.index))
    sources = frame['source']
    for values in (sources, versions):
        if any(not isinstance(v, str) or not v.strip() for v in values):
            raise GroundTruthError('missing_provenance')
        if len({v.strip() for v in values}) != 1:
            raise GroundTruthError('incompatible_sources_or_versions')
    records, grades, aliases = [], {}, {}
    for position, (_, row) in enumerate(frame.iterrows()):
        relevant = canonical_appid(row['relevant_appid'])
        seed = canonical_appid(row['seed_appid']) if has_ids else None
        name = normalize_name(row.get('seed_game'))
        if relevant is None or (has_ids and seed is None) or (not has_ids and not name):
            raise GroundTruthError('invalid_business_key')
        if seed == relevant:
            raise GroundTruthError('self_relevance')
        identity = f'id:{seed}' if has_ids else f'name:{name}'
        if name:
            aliases.setdefault(name, set()).add(identity)
        grade = _grade(row['grade'])
        key = (identity, relevant)
        if key in grades:
            if grades[key] != grade:
                raise GroundTruthError('conflicting_grades')
            continue
        grades[key] = grade
        records.append(dict(seed_appid=seed, seed_norm=name, seed_identity=identity,
                            relevant_appid=relevant, grade=grade, source=str(sources.iloc[position]).strip(),
                            dataset_version=str(versions.iloc[position]).strip()))
    result = pd.DataFrame(records)
    result.attrs['aliases'] = aliases
    result.attrs['limitations'] = ([] if 'dataset_version' in frame else ['legacy_unversioned_ground_truth'])
    return result


def load_ground_truth(path=None):
    """Return explicit state. No configured path/file => missing, other errors => invalid."""
    configured = path if path is not None else os.getenv('RECOMMENDER_GROUND_TRUTH_PATH', '').strip()
    if not configured:
        return dict(status='gt_missing', reason='path_not_configured', data=None)
    try:
        with Path(configured).open(encoding='utf-8-sig', newline='') as stream:
            headers = [name.strip().casefold() for name in next(csv.reader(stream))]
            if len(headers) != len(set(headers)):
                raise GroundTruthError('duplicate_columns')
            stream.seek(0)
            frame = pd.read_csv(stream, dtype=object)
        validated = validate_ground_truth(frame)
    except FileNotFoundError:
        return dict(status='gt_missing', reason='file_not_found', data=None)
    except GroundTruthError as exc:
        return dict(status='gt_invalid', reason=str(exc), data=None)
    except Exception:
        return dict(status='gt_invalid', reason='unreadable_or_malformed_file', data=None)
    return dict(status='ready', reason=None, data=validated)


def _dcg(grades):
    return sum((2**grade - 1) / math.log2(rank + 1) for rank, grade in enumerate(grades, 1))


def evaluate_recommendations(seed_game, recommendations, *, k=10, seed_appid=None, gt=None, path=None):
    k = validate_k(k)
    canonical, counts = canonical_recommendations(recommendations, seed_appid=seed_appid)
    seed = canonical_appid(seed_appid) if seed_appid is not None else None
    identity = f'id:{seed}' if seed is not None else f'name:{normalize_name(seed_game)}'
    result = dict(status=None, evaluable=False, k=k, seed_identity=identity, seed_appid=seed,
                  source=None, dataset_version=None, metrics=None, ground_truth_size=0,
                  returned_count=min(k, len(canonical)), limitations=[], **counts)
    if gt is None:
        loaded = load_ground_truth(path)
    else:
        try:
            loaded = dict(status='ready', data=validate_ground_truth(gt), reason=None)
        except GroundTruthError as exc:
            loaded = dict(status='gt_invalid', reason=str(exc), data=None)
    if loaded['status'] != 'ready':
        result.update(status=loaded['status'], reason=loaded['reason'])
        return result
    truth = loaded['data']
    result.update(source=truth['source'].iloc[0], dataset_version=truth['dataset_version'].iloc[0],
                  limitations=truth.attrs.get('limitations', []))
    if seed is not None:
        # Legacy name-only files cannot establish whether appid matches their labels.
        identities = {identity} if identity in set(truth.seed_identity) else set()
        if not identities and truth.seed_appid.isna().all():
            identities = truth.attrs['aliases'].get(normalize_name(seed_game), set())
            result['limitations'] = result['limitations'] + ['legacy_name_only_identity']
    else:
        identities = truth.attrs['aliases'].get(normalize_name(seed_game), set())
    if len(identities) > 1:
        result['status'] = 'seed_ambiguous'
        return result
    if not identities:
        result['status'] = 'seed_not_covered'
        return result
    selected_identity = next(iter(identities))
    result['seed_identity'] = selected_identity
    selected = truth[truth.seed_identity == selected_identity]
    if seed is not None and seed in set(selected.relevant_appid):
        result.update(status='gt_invalid', reason='self_relevance')
        return result
    relevant = {int(row.relevant_appid): int(row.grade) for row in selected.itertuples() if row.grade > 0}
    result['ground_truth_size'] = len(relevant)
    if not relevant:
        result['status'] = 'gt_no_positive_labels'
        return result
    # A legacy name lookup may resolve a numeric seed; exclude it before taking K.
    if selected_identity.startswith('id:') and seed is None:
        canonical, counts = canonical_recommendations(recommendations, seed_appid=int(selected_identity[3:]))
        result.update(counts, returned_count=min(k, len(canonical)))
    grades = [relevant.get(appid, 0) for appid in canonical.steam_appid.iloc[:k]]
    hits = sum(grade > 0 for grade in grades)
    ideal = _dcg(sorted(relevant.values(), reverse=True)[:k])
    metrics = dict(precision_at_k=hits / k, recall_at_k=hits / len(relevant),
                   ndcg_at_k=_dcg(grades) / ideal,
                   rr_at_k=next((1 / rank for rank, grade in enumerate(grades, 1) if grade > 0), 0.0))
    if any(not math.isfinite(value) or not 0 <= value <= 1 + 1e-12 for value in metrics.values()):
        raise ArithmeticError('Metric contract violated')
    result.update(evaluable=True, metrics=metrics,
                  status='evaluated_hits' if hits else ('evaluated_empty' if canonical.empty else 'evaluated_zero_hits'))
    return result


def aggregate_evaluations(results):
    """Coverage-aware, same-benchmark MRR; require at least two distinct evaluable seeds."""
    results = list(results)
    coverage = dict(total_count=len(results), evaluable_count=sum(r['evaluable'] for r in results),
                    excluded_count=sum(not r['evaluable'] for r in results),
                    status_counts=dict(Counter(r['status'] for r in results)))
    valid = [r for r in results if r['evaluable']]
    if len({r['k'] for r in results}) > 1:
        raise ValueError('Cannot aggregate different K values')
    if any(not math.isfinite(r['metrics']['rr_at_k']) or not 0 <= r['metrics']['rr_at_k'] <= 1 for r in valid):
        raise ValueError('Invalid reciprocal-rank metric')
    if any(r['dataset_version'] == 'legacy_unversioned' for r in valid):
        raise ValueError('Versioned ground truth is required for aggregation')
    if len({(r['source'], r['dataset_version']) for r in valid}) > 1:
        raise ValueError('Cannot aggregate different benchmarks')
    if len({r['seed_identity'] for r in valid}) != len(valid):
        raise ValueError('Duplicate evaluable seed')
    if len(valid) < 2:
        return dict(status='insufficient_evaluable_seeds', mrr_at_k=None, **coverage)
    return dict(status='aggregated', k=valid[0]['k'], source=valid[0]['source'],
                dataset_version=valid[0]['dataset_version'],
                mrr_at_k=sum(r['metrics']['rr_at_k'] for r in valid) / len(valid), **coverage)


STATUS_MESSAGES = {
    'gt_missing': 'Chưa cấu hình hoặc chưa có ground truth; chưa đo hiệu năng.',
    'gt_invalid': 'Ground truth không hợp lệ hoặc không đọc được; chưa đo hiệu năng.',
    'seed_not_covered': 'Seed chưa có trong benchmark; chưa đo hiệu năng.',
    'seed_ambiguous': 'Tên seed không xác định duy nhất; cần seed_appid.',
    'gt_no_positive_labels': 'Seed không có positive labels; chưa đo hiệu năng.',
}


def format_metrics_summary(result):
    if not result.get('evaluable'):
        return STATUS_MESSAGES.get(result.get('status'), 'Chưa có kết quả đánh giá.')
    return f"Top-{result['k']}: {result['status']}; source={result['source']}"


def metrics_dataframe(result):
    if not result.get('evaluable'):
        return pd.DataFrame(columns=['metric', 'value'])
    k = result['k']
    return pd.DataFrame([{'metric': label, 'value': result['metrics'][key] * 100}
                         for label, key in [(f'Precision@{k}', 'precision_at_k'), (f'Recall@{k}', 'recall_at_k'),
                                            (f'NDCG@{k}', 'ndcg_at_k'), (f'RR@{k}', 'rr_at_k')]])
