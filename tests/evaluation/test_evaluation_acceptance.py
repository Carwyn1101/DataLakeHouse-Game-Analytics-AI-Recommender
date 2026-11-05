"""Synthetic acceptance tests. No production data, native services or startup jobs."""
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import hashlib
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / 'LakeHouse_Project/streamlit-ai'
# Import only the pure src package/module; never FAISS, app startup or Spark.
sys.path.insert(0, str(APP))
from src import recommender_evaluation as ev


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen = load_module('synthetic_ground_truth_generator', APP / 'evaluation/build_metadata_ground_truth.py')
core = load_module('evaluation_core_doubles', ROOT / 'tests/core/test_core_recovery.py')


def truth(seed=10):
    return pd.DataFrame([dict(seed_appid=seed, seed_game='Seed', relevant_appid=1, grade=3,
                              source='metadata_pseudo', dataset_version='fixture-v1'),
                         dict(seed_appid=seed, seed_game='Seed', relevant_appid=2, grade=1,
                              source='metadata_pseudo', dataset_version='fixture-v1')])


def recs(ids):
    return pd.DataFrame({'steam_appid': pd.Series(ids, dtype=object),
                         'review_positive_ratio': [.8] * len(ids)})


def evaluate(ids, *, k=3, gt=None, seed=10, name='Seed'):
    return ev.evaluate_recommendations(name, recs(ids), k=k, seed_appid=seed,
                                       gt=truth(seed) if gt is None else gt)


class MetricAcceptance(unittest.TestCase):
    def test_perfect_zero_hit_empty_statuses(self):
        perfect = evaluate([1, 2], k=2)
        self.assertEqual(perfect['status'], 'evaluated_hits')
        self.assertTrue(all(v == 1 for v in perfect['metrics'].values()))
        for ids, status in [([98, 99], 'evaluated_zero_hits'), ([], 'evaluated_empty')]:
            r = evaluate(ids, k=2)
            self.assertTrue(r['evaluable'])
            self.assertEqual(r['status'], status)
            self.assertTrue(all(v == 0 for v in r['metrics'].values()))

    def test_independent_graded_oracle_preserves_rank(self):
        result = evaluate([99, 1, 2])
        metrics = result['metrics']
        self.assertAlmostEqual(metrics['precision_at_k'], 2/3)
        self.assertEqual(metrics['recall_at_k'], 1)
        self.assertEqual(metrics['rr_at_k'], .5)
        self.assertAlmostEqual(metrics['ndcg_at_k'], (7/math.log2(3) + .5) / (7 + 1/math.log2(3)))

    def test_fixed_k_and_first_duplicate_occurrence(self):
        r = evaluate([2, '002', 2., 1], k=3)
        self.assertEqual(r['k'], 3)
        self.assertEqual(r['original_count'], 4)
        self.assertEqual(r['valid_unique_count'], 2)
        self.assertEqual(r['dropped_count'], 2)
        self.assertAlmostEqual(r['metrics']['precision_at_k'], 2/3)
        self.assertLess(r['metrics']['ndcg_at_k'], 1)  # Do not rerank by GT grades.
        short = evaluate([1], k=10)
        self.assertEqual(short['k'], 10)
        self.assertEqual(short['metrics']['precision_at_k'], .1)

    def test_ids_match_core_policy_and_never_truncate(self):
        faiss = core.functions('src/faiss_manager.py')
        invalid = [None, pd.NA, np.nan, np.inf, -np.inf, True, np.bool_(True), 0, -1, 1.9,
                   '', 'bad', '1.9', '+1', '1e3', float(2**53)]
        valid = [1, np.int64(1), 1., np.float64(1.), ' 001 ', '9007199254740993']
        for value in invalid + valid:
            with self.subTest(value=repr(value)):
                self.assertEqual(ev.canonical_appid(value), faiss['_business_key'](value))
        canonical, counts = ev.canonical_recommendations(recs(invalid + [1, '001', 2, 10]), seed_appid=10)
        self.assertEqual(canonical.steam_appid.tolist(), [1, 2])
        self.assertEqual(counts['dropped_count'], len(invalid) + 2)

    def test_missing_id_column_seed_exclusion_and_no_mutation(self):
        frame = recs([10, 1, 1, 2]); frame['rank'] = [1, 2, 3, 4]; before = frame.copy(deep=True)
        canonical, _ = ev.canonical_recommendations(frame, seed_appid=10)
        self.assertEqual(canonical.steam_appid.tolist(), [1, 2])
        self.assertEqual(canonical['rank'].tolist(), [1, 2])
        pd.testing.assert_frame_equal(frame, before)
        self.assertTrue(ev.canonical_recommendations(pd.DataFrame({'name': ['bad']}))[0].empty)

    def test_invalid_k(self):
        for k in [0, -1, True, np.bool_(True), 1.5, '3', None]:
            with self.subTest(k=k), self.assertRaises(ValueError):
                evaluate([1], k=k)

    def test_metric_bounds_for_all_small_rankings(self):
        from itertools import product
        for ids in product([1, 2, 99], repeat=3):
            result = evaluate(list(ids))
            self.assertTrue(all(math.isfinite(v) and 0 <= v <= 1 for v in result['metrics'].values()))

    def test_gt_identical_duplicates_conflicts_and_grades(self):
        base = truth()
        self.assertEqual(len(ev.validate_ground_truth(pd.concat([base, base]))), 2)
        conflicting = base.copy(); conflicting.loc[0, 'grade'] = 1
        self.assertEqual(evaluate([1], gt=pd.concat([base, conflicting]))['status'], 'gt_invalid')
        for grade in [None, pd.NA, np.nan, np.inf, -1, 4, 2.9, True, np.bool_(True), '2.00000000000000001', 'invalid']:
            frame = base.copy().astype({'grade': object}); frame.at[0, 'grade'] = grade
            with self.subTest(grade=repr(grade)):
                self.assertEqual(evaluate([1], gt=frame)['status'], 'gt_invalid')

    def test_gt_business_keys_self_matches_and_provenance(self):
        for field, value in [('seed_appid', None), ('seed_appid', 10.5), ('relevant_appid', 0),
                             ('relevant_appid', 1.5), ('source', ''), ('dataset_version', '')]:
            frame = truth().astype({field: object}); frame.at[0, field] = value
            with self.subTest(field=field, value=value):
                self.assertEqual(evaluate([1], gt=frame)['status'], 'gt_invalid')
        for column in ['source', 'dataset_version']:
            frame = truth(); frame.loc[1, column] = 'other'
            self.assertEqual(evaluate([1], gt=frame)['status'], 'gt_invalid')
        frame = truth(); frame.loc[0, 'relevant_appid'] = 10
        self.assertEqual(evaluate([1], gt=frame)['status'], 'gt_invalid')

    def test_seed_id_primary_and_ambiguous_legacy_name(self):
        mixed = pd.concat([truth(10), truth(20)], ignore_index=True)
        self.assertEqual(evaluate([1], gt=mixed)['ground_truth_size'], 2)
        self.assertEqual(ev.evaluate_recommendations('Seed', recs([1]), gt=mixed)['status'], 'seed_ambiguous')
        self.assertEqual(evaluate([1], seed=30, gt=mixed)['status'], 'seed_not_covered')
        legacy = truth().drop(columns=['seed_appid', 'dataset_version'])
        r = evaluate([1], gt=legacy)
        self.assertTrue(r['evaluable'])
        self.assertIn('legacy_unversioned_ground_truth', r['limitations'])
        self.assertIn('legacy_name_only_identity', r['limitations'])

    def test_no_positive_labels_and_malformed_gt(self):
        frame = truth(); frame['grade'] = 0
        self.assertEqual(evaluate([1], gt=frame)['status'], 'gt_no_positive_labels')
        for frame in [pd.DataFrame(), pd.DataFrame({'grade': [1]})]:
            result = evaluate([1], gt=frame)
            self.assertEqual(result['status'], 'gt_invalid')
            self.assertIsNone(result['metrics'])

    def test_legacy_self_labels_duplicate_columns_and_aggregate_guards(self):
        legacy = truth().drop(columns='seed_appid'); legacy.loc[0, 'relevant_appid'] = 10
        self.assertEqual(evaluate([1], gt=legacy)['status'], 'gt_invalid')
        duplicated = pd.concat([truth(), truth()[['source']]], axis=1)
        self.assertEqual(evaluate([1], gt=duplicated)['status'], 'gt_invalid')
        hit = evaluate([99, 1], seed=10)
        empty = evaluate([], seed=20)
        zero = evaluate([99], seed=30)
        uncovered = evaluate([1], seed=40, gt=truth(10))
        aggregate = ev.aggregate_evaluations([hit, empty, zero, uncovered])
        self.assertAlmostEqual(aggregate['mrr_at_k'], .5/3)
        self.assertEqual(aggregate['evaluable_count'], 3)
        bad = deepcopy(zero); bad['metrics']['rr_at_k'] = 2
        with self.assertRaises(ValueError):ev.aggregate_evaluations([hit, bad])
        unversioned = truth(20).drop(columns='dataset_version')
        with self.assertRaises(ValueError):ev.aggregate_evaluations([hit, evaluate([1], seed=20, gt=unversioned)])

    def test_file_statuses_and_safe_errors(self):
        with patch.dict(os.environ, {'RECOMMENDER_GROUND_TRUTH_PATH': ''}):
            self.assertEqual(ev.load_ground_truth()['status'], 'gt_missing')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'truth.csv'
            self.assertEqual(ev.load_ground_truth(path)['status'], 'gt_missing')
            truth().to_csv(path, index=False)
            self.assertEqual(ev.load_ground_truth(path)['status'], 'ready')
            path.write_text('bad,columns\n1,2\n', encoding='utf-8')
            self.assertEqual(ev.load_ground_truth(path)['status'], 'gt_invalid')
            path.write_text('seed_game,relevant_appid,grade,source,source\nSeed,1,3,metadata_pseudo,other\n', encoding='utf-8')
            self.assertEqual(ev.load_ground_truth(path)['status'], 'gt_invalid')
            truth().to_csv(path, index=False)
            with patch.object(ev.pd, 'read_csv', side_effect=PermissionError('PRIVATE_SENTINEL')):
                result = ev.evaluate_recommendations('Seed', recs([1]), seed_appid=10, path=path)
        self.assertEqual(result['status'], 'gt_invalid')
        self.assertNotIn('PRIVATE_SENTINEL', str(result))
        self.assertIsNone(result['metrics'])

    def test_rr_labels_and_coverage_aware_mrr(self):
        hit = evaluate([99, 1], seed=10)
        zero = evaluate([99], seed=20)
        excluded = evaluate([1], seed=30, gt=truth(10))
        result = ev.aggregate_evaluations([hit, zero, excluded])
        self.assertEqual(result['mrr_at_k'], .25)
        self.assertEqual((result['total_count'], result['evaluable_count'], result['excluded_count']), (3, 2, 1))
        self.assertIsNone(ev.aggregate_evaluations([hit])['mrr_at_k'])
        self.assertIn('RR@3', ev.metrics_dataframe(hit).metric.tolist())
        self.assertFalse(any('MRR' in label for label in ev.metrics_dataframe(hit).metric))
        with self.assertRaises(ValueError): ev.aggregate_evaluations([hit, hit])
        with self.assertRaises(ValueError): ev.aggregate_evaluations([hit, evaluate([1], seed=20, k=2)])
        other = truth(20); other['dataset_version'] = 'other'
        with self.assertRaises(ValueError): ev.aggregate_evaluations([hit, evaluate([1], seed=20, gt=other)])


class GeneratorAcceptance(unittest.TestCase):
    def catalog(self):
        return pd.DataFrame([dict(steam_appid=appid, name=f'Game {appid}', tags='Action, Space', genres='RPG',
                                   categories='Single-player', developers='Studio', recommendations_total=100)
                             for appid in [3, 1, 2]])

    def test_null_and_mixed_tokens_no_artificial_nan(self):
        for value in [None, np.nan, pd.NA, '', [], float('inf')]:
            self.assertEqual(gen.split_tokens(value), set())
        self.assertEqual(gen.split_tokens(['Action', None, np.nan, 'RPG; Space', 123]), {'action', 'rpg', 'space', '123'})
        self.assertEqual(gen.jaccard(set(), set()), 0)

    def test_weighted_jaccard_and_grade_boundaries(self):
        left = {f'{k}_set': {'x'} for k in gen.WEIGHTS}
        right = {f'{k}_set': set() for k in gen.WEIGHTS}
        self.assertEqual(gen.relevance_score(left, left), 1)
        for field, weight in gen.WEIGHTS.items():
            isolated = deepcopy(right); isolated[f'{field}_set'] = {'x'}
            self.assertEqual(gen.relevance_score(left, isolated), weight)
        self.assertEqual([gen.grade_from_score(x) for x in [0, .18, .30, .42]], [0, 1, 2, 3])

    def test_deterministic_ties_self_exclusion_hash_and_coverage(self):
        frame = self.catalog()
        a, ma = gen.build_benchmark(frame, seed_count=2, per_seed=2)
        b, mb = gen.build_benchmark(frame.iloc[::-1], seed_count=2, per_seed=2)
        pd.testing.assert_frame_equal(a, b)
        self.assertEqual(ma['dataset_version'], mb['dataset_version'])
        self.assertEqual(ma['input_sha256'], mb['input_sha256'])
        self.assertEqual(ma['selected_seed_appids'], [1, 2])
        self.assertFalse((a.seed_appid == a.relevant_appid).any())
        self.assertEqual(a[a.seed_appid == 1].relevant_appid.tolist(), [2, 3])
        self.assertEqual(ma['coverage']['covered_seeds'], 2)
        self.assertEqual(ma['output_sha256'], hashlib.sha256(a.to_csv(index=False, lineterminator='\n').encode()).hexdigest())
        self.assertIsNone(ma['input_reference']['snapshot_reference'])
        self.assertIn('limitation', ma['input_reference'])

    def test_parameters_and_no_labels(self):
        for values in [(0, 2, 2, .18), (10, -1, 2, .18), (10, 2, 0, .18),
                       (10, 2, 2, 0), (10, 2, 2, float('nan')), (10, 2, 2, 1.01)]:
            with self.subTest(values=values), self.assertRaises(ValueError):gen.validate_parameters(*values)
        one = self.catalog().iloc[:1]
        with self.assertRaises(ValueError):gen.build_benchmark(one, seed_count=1)

    def test_bad_catalog_keys_duplicates_and_null_metadata(self):
        frame = self.catalog().astype({'steam_appid': object}); frame.loc[0, 'steam_appid'] = 1.5
        with self.assertRaises(ValueError):gen.prepare_games(frame)
        frame = self.catalog(); other = frame.iloc[:1].copy(); other['name'] = 'Conflicting'
        with self.assertRaises(ValueError):gen.prepare_games(pd.concat([frame, other]))
        frame = self.catalog(); frame['tags'] = None; frame['developers'] = np.nan
        prepared = gen.prepare_games(frame)
        self.assertTrue(all(not tokens for tokens in prepared.tags_set))

    def test_config_is_required_before_spark_and_errors_are_redacted(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):gen.get_spark_session()
            with self.assertRaises(ValueError):gen.get_spark_session({})
            with patch.object(gen, 'get_spark_session') as spark, tempfile.TemporaryDirectory() as tmp:
                errors = io.StringIO()
                with redirect_stderr(errors):code = gen.main(['--output', str(Path(tmp)/'out.csv')])
                self.assertEqual(code, 1); spark.assert_not_called()
        with patch.object(gen, 'read_config', side_effect=ValueError('PRIVATE_SENTINEL')):
            errors = io.StringIO()
            with redirect_stderr(errors), tempfile.TemporaryDirectory() as tmp:
                self.assertEqual(gen.main(['--output', str(Path(tmp)/'out.csv')]), 1)
            self.assertNotIn('PRIVATE_SENTINEL', errors.getvalue())

    def test_private_outputs_manifest_and_no_overwrite(self):
        output, manifest = gen.build_benchmark(self.catalog(), seed_count=2)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'out.csv'
            gen.write_benchmark(path, output, manifest)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), manifest['output_sha256'])
            parsed = json.loads(path.with_suffix('.manifest.json').read_text())
            self.assertEqual(parsed['source'], 'metadata_pseudo')
            self.assertNotIn('secret', parsed)
            with self.assertRaises(ValueError):gen.write_benchmark(path, output, manifest)
        with self.assertRaises(ValueError):gen.validate_output_path(APP/'evaluation/unsafe.csv')
        with self.assertRaises(ValueError):gen.validate_output_path(APP/'evaluation/tmp/bad.json')

    def test_observed_snapshots_are_not_claimed_as_pinned_and_spark_stops(self):
        spark = Mock()
        before = Mock(); before.collect.return_value = [[111]]
        data = Mock(); data.toPandas.return_value = self.catalog()
        after = Mock(); after.collect.return_value = [[112]]
        spark.sql.side_effect = [before, data, after]
        with patch.object(gen, 'get_spark_session', return_value=spark):
            frame, reference = gen.load_games(10, {})
        self.assertEqual(len(frame), 3)
        self.assertEqual(reference['observed_snapshot_before'], '111')
        self.assertEqual(reference['observed_snapshot_after'], '112')
        self.assertIsNone(reference['snapshot_reference'])
        self.assertIn('not snapshot-pinned', reference['limitation'])
        spark.stop.assert_called_once()
        spark.sql.side_effect = RuntimeError('PRIVATE_SENTINEL')
        with patch.object(gen, 'get_spark_session', return_value=spark), self.assertRaises(RuntimeError):
            gen.load_games(10, {})
        self.assertEqual(spark.stop.call_count, 2)

    def test_cli_can_use_only_mocked_source_and_writes_fixture_output(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(gen, 'read_config', return_value={'synthetic': True}), \
             patch.object(gen, 'load_games', return_value=(self.catalog(), {'snapshot_reference': None, 'limitation': 'synthetic'})) as load:
            path = Path(tmp)/'out.csv'
            with redirect_stdout(io.StringIO()):self.assertEqual(gen.main(['--output', str(path), '--seed-count', '2']), 0)
            self.assertTrue(path.exists()); load.assert_called_once()


class UIAcceptance(unittest.TestCase):
    def case(self):
        return core.CoreTests(methodName='test_r12_whole_app_smoke_with_service_and_ui_doubles')

    def test_disabled_preserves_core_and_never_reads_gt(self):
        for action, intent in [('recommend', ''), ('chat', 'recommend_from_game')]:
            ui = self.case().smoke(action, intent)
            ui.evaluation_loader.assert_not_called()
            ui.explanation_mock.assert_called_once()

    def test_enabled_recommend_and_chat_use_raw_ratios_and_rr(self):
        frame = truth(1); frame['relevant_appid'] = [2, 3]
        for action, intent, k in [('recommend', '', 10), ('chat', 'recommend_from_game', 8)]:
            ui = self.case().smoke(action, intent, evaluation_enabled=True, truth=frame)
            ui.evaluation_loader.assert_called_once()
            ui.explanation_mock.assert_called_once()
            metrics = next(f for f in ui.frames if set(f.columns) == {'metric', 'value'})
            self.assertIn(f'RR@{k}', metrics.metric.tolist())
            self.assertFalse(any('MRR' in s for s in metrics.metric))
            if intent:
                result = ui.session_state.chat_messages[-1]
                self.assertEqual(result['evaluation']['source'], 'metadata_pseudo')
                self.assertEqual(result['content'], 'Synthetic explanation')

    def test_missing_and_invalid_gt_show_status_not_zero_table(self):
        for status in ['gt_missing', 'gt_invalid']:
            ui = self.case().smoke('recommend', '', evaluation_enabled=True, truth_status=status)
            self.assertFalse(any(set(f.columns) == {'metric', 'value'} for f in ui.frames))
            self.assertTrue(any('chưa đo' in message.casefold() for message in ui.infos))

    def empty_flow(self, action, intent, **kwargs):
        ui = self.case().smoke(action, intent, **kwargs)
        ui.explanation_mock.assert_not_called()
        self.assertEqual(ui.errors, [])
        self.assertFalse(any('steam_appid' in frame or 'name' in frame for frame in ui.frames))
        if intent:
            payload = ui.session_state.chat_messages[-1]
            self.assertEqual(payload['table_rows'], [])
            self.assertIn('không có recommendation', payload['content'])
            self.assertNotIn('Không tìm thấy', payload['content'])
        else:
            self.assertTrue(any('không có recommendation' in message for message in ui.infos))
        return ui

    def test_review_empty_recommendations_with_valid_gt_both_flows(self):
        labels = truth(1); labels['relevant_appid'] = [2, 3]
        for action, intent, k in [('recommend', '', 10), ('chat', 'recommend_from_game', 8)]:
            for empty in [pd.DataFrame(), pd.DataFrame(columns=['steam_appid', 'review_positive_ratio'])]:
                with self.subTest(action=action, columns=list(empty.columns)):
                    ui = self.empty_flow(action, intent, search_result=empty, evaluation_enabled=True, truth=labels)
                    ui.evaluation_loader.assert_called_once()
                    result = ui.evaluation_results[0]
                    self.assertEqual(result['status'], 'evaluated_empty')
                    self.assertTrue(result['evaluable'])
                    self.assertEqual(result['k'], k)
                    self.assertEqual((result['original_count'], result['valid_unique_count'], result['dropped_count']), (0, 0, 0))
                    self.assertTrue(all(value == 0 for value in result['metrics'].values()))
                    metrics = next(frame for frame in ui.frames if set(frame.columns) == {'metric', 'value'})
                    self.assertIn(f'Precision@{k}', metrics.metric.tolist())
                    self.assertTrue((metrics.value == 0).all())

    def test_review_empty_recommendations_with_missing_invalid_or_uncovered_gt(self):
        for action, intent in [('recommend', ''), ('chat', 'recommend_from_game')]:
            for status in ['gt_missing', 'gt_invalid', 'seed_not_covered']:
                with self.subTest(action=action, status=status):
                    kwargs = dict(truth_status=status) if status != 'seed_not_covered' else dict(truth=truth(10))
                    ui = self.empty_flow(action, intent, search_result=pd.DataFrame(), evaluation_enabled=True, **kwargs)
                    self.assertEqual(ui.evaluation_results[0]['status'], status)
                    self.assertIsNone(ui.evaluation_results[0]['metrics'])
                    self.assertFalse(any(set(frame.columns) == {'metric', 'value'} for frame in ui.frames))
                    self.assertTrue(any('chưa đo' in message.casefold() for message in ui.infos))

    def test_review_all_excluded_candidates_use_same_empty_canonical_list(self):
        labels = truth(1); labels['relevant_appid'] = [2, 3]
        for action, intent, k in [('recommend', '', 10), ('chat', 'recommend_from_game', 8)]:
            for frame in [recs([None, 'bad', 1, '001', True]), pd.DataFrame({'name': ['missing key']})]:
                with self.subTest(action=action, count=len(frame)):
                    before = frame.copy(deep=True)
                    ui = self.empty_flow(action, intent, search_result=frame, evaluation_enabled=True, truth=labels)
                    result = ui.evaluation_results[0]
                    self.assertEqual(result['status'], 'evaluated_empty')
                    self.assertEqual(result['k'], k)
                    self.assertEqual((result['original_count'], result['valid_unique_count'], result['dropped_count']), (len(frame), 0, len(frame)))
                    self.assertEqual(result['returned_count'], 0)
                    self.assertTrue(all(value == 0 for value in result['metrics'].values()))
                    pd.testing.assert_frame_equal(frame, before)

    def test_review_missing_source_does_not_evaluate_both_flows(self):
        for enabled in [False, True]:
            for action, intent in [('recommend', ''), ('chat', 'recommend_from_game')]:
                with self.subTest(enabled=enabled, action=action):
                    ui = self.case().smoke(action, intent, search_result=pd.DataFrame(), evaluation_enabled=enabled, source_found=False)
                    ui.evaluation_loader.assert_not_called()
                    ui.explanation_mock.assert_not_called()
                    self.assertEqual(ui.evaluation_results, [])
                    self.assertEqual(ui.frames, [])
                    if intent:
                        self.assertIn('Không tìm thấy', ui.session_state.chat_messages[-1]['content'])
                    else:
                        self.assertEqual(ui.errors, ['Seed game not found. Try Search tab first.'])

    def test_review_disabled_empty_recommendations_avoid_gt_reads(self):
        for action, intent in [('recommend', ''), ('chat', 'recommend_from_game')]:
            with self.subTest(action=action):
                ui = self.empty_flow(action, intent, search_result=pd.DataFrame(), evaluation_enabled=False)
                ui.evaluation_loader.assert_not_called()
                self.assertEqual(ui.evaluation_results, [])
                self.assertEqual(ui.frames, [])

    def test_canonical_list_for_ui_and_metrics_counts(self):
        frame = pd.DataFrame([dict(steam_appid=2, rank=1, name='Synthetic', tags='Action', genres='Action',
                                   price_category='Budget', chunk_id=0, final_score=.7, similarity_score=.7,
                                   review_positive_ratio=.8, release_year=2020)] * 2)
        labels = truth(1); labels['relevant_appid'] = [2, 3]
        ui = self.case().smoke('chat', 'recommend_from_game', search_result=frame, evaluation_enabled=True, truth=labels)
        payload = ui.session_state.chat_messages[-1]
        self.assertEqual(len(payload['table_rows']), 1)
        self.assertEqual(payload['evaluation']['original_count'], 2)
        self.assertEqual(payload['evaluation']['dropped_count'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
