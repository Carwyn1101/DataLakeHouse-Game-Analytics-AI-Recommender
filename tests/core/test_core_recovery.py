"""R01-R12 regression coverage. Synthetic only; no service or index persistence.

Run: python -B -m unittest discover -s tests/core -v
AST selection executes production function bodies without importing runtime services.
Whole-app smoke cases execute the module with explicit service/UI doubles.
Passing these tests does not validate real Streamlit, FAISS or Ollama integration.
"""
import ast
from contextlib import redirect_stdout
from copy import deepcopy
import importlib.util
import io
import math
from pathlib import Path
import re
import sys
import types
import typing
import unicodedata
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / 'LakeHouse_Project' / 'streamlit-ai'


def text(relative):
    return (APP / relative).read_text(encoding='utf-8-sig')


def normalized(values, **_kwargs):
    values = np.asarray(values)
    lengths = np.linalg.norm(values, axis=1, keepdims=True)
    return values / np.where(lengths == 0, 1, lengths)


def functions(relative, **extra):
    nodes = [n for n in ast.parse(text(relative)).body if isinstance(n, ast.FunctionDef)]
    for node in nodes:
        node.decorator_list = []
    ns = dict(pd=pd, np=np, re=re, unicodedata=unicodedata, Dict=typing.Dict,
              List=typing.List, Tuple=typing.Tuple, normalize=normalized, **extra)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), relative, 'exec'), ns)
    return ns


def weights():
    node = next(n for n in ast.parse(text('src/settings.py')).body
                if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'HYBRID_WEIGHTS' for t in n.targets))
    return ast.literal_eval(node.value)


class State(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class Surface:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class UI(types.ModuleType):
    def __init__(self, request='', action=''):
        super().__init__('streamlit')
        self.sidebar = Surface()
        self.session_state = State(chat_messages=[], pending_user_msg=request,
                                   chat_submit_requested=bool(request))
        self.action = action
        self.frames = []
        self.charts = []
        self.warnings = []
        self.column_config = types.SimpleNamespace(TextColumn=lambda *a, **k: None,
                                                  NumberColumn=lambda *a, **k: None)

    def __getattr__(self, name):
        if name in {'chat_message', 'expander', 'form'}:
            return lambda *a, **k: Surface()
        if name in {'columns', 'tabs'}:
            return lambda items, **k: [Surface() for _ in range(items if isinstance(items, int) else len(items))]
        if name == 'text_input':
            return lambda *a, **k: {'search_query': 'Synthetic', 'seed_game': 'Synthetic'}.get(k.get('key'), '')
        if name == 'number_input':
            return lambda *a, **k: k.get('value', 20)
        if name == 'slider':
            return lambda *a, **k: 10
        if name == 'form_submit_button':
            return lambda label, **k: self.action == 'search' and label == 'Lexical Search'
        if name == 'button':
            return lambda *a, **k: self.action == 'recommend'
        if name == 'stop':
            def stop():
                raise AssertionError('Unexpected application stop')
            return stop
        if name == 'cache_data':
            return lambda *a, **k: lambda fn: fn
        return lambda *a, **k: None

    def dataframe(self, frame, **_kwargs):
        self.frames.append(frame.copy())

    def altair_chart(self, chart, **_kwargs):
        self.charts.append(chart)

    def warning(self, message):
        self.warnings.append(message)


class Chart:
    def __init__(self, data=None, **_kwargs):
        self.data = data

    def __getattr__(self, _name):
        return lambda *a, **k: self

    def __add__(self, _other):
        return self


def altair_double():
    alt = types.ModuleType('altair')
    alt.Chart = Chart
    for name in ['X', 'Y', 'Axis', 'Scale', 'Tooltip', 'Color', 'Theta']:
        setattr(alt, name, lambda *a, **k: None)
    return alt


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.ui = UI()
        self.app = functions('app.py', st=self.ui, alt=altair_double())
        self.hybrid = functions('src/hybrid_chat.py', HYBRID_WEIGHTS=weights(),
                                USE_LLM_INTENT_PARSING=False, requests=Mock(),
                                OLLAMA_BASE_URL='http://synthetic.invalid', OLLAMA_CHAT_MODEL='synthetic',
                                is_ollama_available=Mock(return_value=False))
        self.faiss = functions('src/faiss_manager.py')

    def test_r01_no_evaluation_feature_or_syntax_errors(self):
        for name in ['app.py', 'src/faiss_manager.py', 'src/hybrid_chat.py', 'src/settings.py']:
            body = text(name)
            ast.parse(body)
            for forbidden in ['recommender_evaluation', 'ENABLE_RECOMMENDER_EVAL', 'evaluate_recommendations', 'ground_truth']:
                self.assertNotIn(forbidden, body)

    def test_r02_score_semantics_and_weights(self):
        self.assertEqual(set(weights()), {'faiss_structured', 'semantic', 'review_quality'})
        self.assertAlmostEqual(sum(weights().values()), 1.0)
        rows = pd.DataFrame({'steam_appid': [1, 2], 'similarity_score': [.8, .2],
                             'steam_review_positive_ratio': [.9, .8]})
        semantic = pd.DataFrame({'steam_appid': [1, 2], 'semantic_score': [.3, .9]})
        result = self.hybrid['hybrid_rerank'](rows, semantic, 2)
        expected = [.5 * .8 + .3 * .3 + (.1 + .1) * .9,
                    .5 * .2 + .3 * .9 + (.1 + .1) * .8]
        np.testing.assert_allclose(result.final_score, expected)
        missing = self.hybrid['hybrid_rerank'](rows, semantic.iloc[:0], 1)
        self.assertEqual(len(missing), 1)

    def search(self, query, rows, limit=20):
        return self.faiss['search_across_all_chunks'](query, {0: {'games_df': pd.DataFrame(rows)}}, limit)

    def test_r03_search_schema_priority_and_top_n(self):
        rows = {'name': ['match', 'other', 'other', 'other'],
                'tags_cleaned': ['', 'match', '', ''],
                'genres_cleaned': ['', '', 'match', ''],
                'developers_cleaned': ['', '', '', 'match']}
        found = self.search('match', rows, 3)
        self.assertEqual(found.relevance.tolist(), [3.0, 2.5, 2.0])
        self.assertTrue(found.relevance_label.notna().all())
        self.assertEqual(set(found.chunk_id), {0})

    def test_r04_mixed_null_and_missing_metadata_without_mutation(self):
        frame = pd.DataFrame({'name': [123, None, 'Synthetic'], 'tags_cleaned': [None, 456, pd.NA]})
        before = frame.copy(deep=True)
        indexes = {0: {'games_df': frame}}
        self.assertEqual(self.faiss['search_across_all_chunks']('123', indexes).relevance.tolist(), [3.0])
        self.assertEqual(self.faiss['search_across_all_chunks']('456', indexes).relevance.tolist(), [2.5])
        self.assertTrue(self.faiss['search_across_all_chunks']('None', indexes).empty)
        pd.testing.assert_frame_equal(frame, before)
        self.assertTrue(self.search('   ', {'name': ['Synthetic']}).empty)

    def test_r05_literal_regex_characters(self):
        for query in ['(', '[', 'C++', 'a.b', '*', '\\']:
            with self.subTest(query=query):
                found = self.search(query, {'name': [query, 'unrelated']})
                self.assertEqual(found.name.tolist(), [query])

    def test_r06_chunk_specific_vectors_dedup_seed_and_top_n(self):
        class Vectorizer:
            def __init__(self, width):
                self.width = width
                self.calls = []

            def transform(self, values):
                self.calls.append(values)
                return types.SimpleNamespace(toarray=lambda: np.ones((1, self.width), dtype='float32'))

        def row(appid, name):
            return dict(steam_appid=appid, name=name, tags_top7_str='tag', tags_remaining_str='more',
                        developers_cleaned='dev', categories_cleaned='cat', price_category_vnd='Budget',
                        publishers_cleaned='pub', steam_review_positive_ratio=.8, genres_cleaned='Action',
                        tags_cleaned='tag', price_vnd=1000, release_year=2020)

        indexes = {}
        for cid, width, rows, scores in [
            (0, 2, [row(1, 'Synthetic'), row(2, 'A'), row(3, 'B')], [1.0, .8, .7]),
            (1, 3, [row(1, 'Synthetic'), row(2, 'A'), row(4, 'C')], [1.0, .9, .6]),
        ]:
            vectors = {name: Vectorizer(width) for name in ['tfidf_tags_top7', 'tfidf_tags_remaining',
                       'tfidf_developers', 'tfidf_categories', 'tfidf_price', 'tfidf_publishers']}
            index = Mock()
            index.search.return_value = (np.array([scores + [0.0]]), np.array([[0, 1, 2, -1]]))
            indexes[cid] = dict(vectors, games_df=pd.DataFrame(rows), name_to_id={r['name'].lower(): i for i, r in enumerate(rows)},
                                weights={k: 1/7 for k in ['tags_top7', 'tags_remaining', 'developers', 'categories',
                                         'price_category', 'publishers', 'review_positive_ratio']},
                                review_scaler=types.SimpleNamespace(transform=lambda x: x), faiss_index=index)
        # Nullable/numeric source metadata must still reach each chunk vectorizer.
        indexes[0]['games_df'].loc[0, 'developers_cleaned'] = None
        indexes[0]['games_df'].loc[0, 'tags_remaining_str'] = None
        indexes[0]['games_df'].loc[0, 'steam_review_positive_ratio'] = np.nan
        indexes[1]['games_df'].loc[1, 'price_vnd'] = np.nan
        source, result = self.faiss['recommend_with_faiss']('Synthetic', indexes, 2)
        self.assertEqual(source.steam_appid, 1)
        self.assertEqual([r['steam_appid'] for r in result], [2, 3])
        self.assertEqual([r['rank'] for r in result], [1, 2])
        self.assertAlmostEqual(result[0]['similarity_score'], .9)
        self.assertEqual(result[0]['price_vnd'], 0)
        for cid, width in [(0, 2), (1, 3)]:
            vector, count = indexes[cid]['faiss_index'].search.call_args.args
            self.assertEqual(vector.shape, (1, 6 * width + 1))
            self.assertTrue(np.isfinite(vector).all())
            self.assertEqual(count, 5)
            for key in ['tfidf_tags_top7', 'tfidf_tags_remaining', 'tfidf_developers', 'tfidf_categories', 'tfidf_price', 'tfidf_publishers']:
                self.assertEqual(len(indexes[cid][key].calls), 1)
        self.assertEqual(self.faiss['recommend_with_faiss']('absent', indexes), (None, None))

    def test_r07_ratio_display_and_explanation_keep_raw_values(self):
        rows = pd.DataFrame([dict(name='Synthetic', review_positive_ratio=.8, similarity_score=.7)])
        before = rows.copy(deep=True)
        display = self.app['_prepare_recommend_display'](rows)
        self.assertEqual(display.review_positive_ratio.iloc[0], 80)
        self.assertEqual(display.final_score.iloc[0], .7)
        self.assertIn('review=80%', self.hybrid['_compact_candidate_context'](rows))
        pd.testing.assert_frame_equal(rows, before)

    def test_r08_charts_use_numeric_contract_after_localization(self):
        rows = [{'metric': 'avg_review_positive_ratio', 'value': .8},
                {'metric': 'avg_price_vnd', 'value': 100000},
                {'metric': 'top_category_1', 'label': 'Action', 'value': 2}]
        original = deepcopy(rows)
        self.app['_prepare_analysis_display'](rows)
        self.app['_render_analyze_charts'](rows)
        self.assertEqual(rows, original)
        self.assertEqual(len(self.ui.charts), 5)
        self.assertFalse(self.ui.warnings)

    def test_r09_category_price_percent_year_and_null_display(self):
        rows = [{'metric': 'price_vnd', 'value': 123456}, {'metric': 'review_positive_ratio', 'value': .805},
                {'metric': 'release_year', 'value': 2020}, {'metric': 'top_category_1', 'label': 'Action', 'value': 3},
                {'metric': 'price_vnd', 'value': None}]
        copy = deepcopy(rows)
        shown = self.app['_prepare_analysis_display'](rows)
        self.assertEqual(shown.value.tolist(), ['123,456', '80.5%', '2020', '3', 'N/A'])
        self.assertEqual(shown.label.iloc[3], 'Action')
        self.assertEqual(rows, copy)

    def test_r10_vietnamese_intents_and_ambiguous_voi(self):
        expected = {'so sánh A với B': 'compare_games', 'so sanh A voi B': 'compare_games',
                    'A vs B': 'compare_games', 'tìm game với đồ họa đẹp': 'search_game',
                    'tim game voi do hoa dep': 'search_game', 'tuong tu Synthetic': 'recommend_from_game',
                    'gợi ý từ game Synthetic': 'recommend_from_game', 'phân tích thị trường': 'analyze_data',
                    'thong ke thi truong': 'analyze_data', 'Convoy': 'search_game'}
        for phrase, intent in expected.items():
            with self.subTest(phrase=phrase):
                self.assertEqual(self.hybrid['route_intent'](phrase), intent)

    def test_r11_unavailable_timeout_truncation_and_sensitive_logging(self):
        sentinel = 'PRIVATE_SYNTHETIC_SENTINEL'
        messages = [{'role': 'user', 'content': sentinel}]
        stream = io.StringIO()
        with redirect_stdout(stream):
            self.assertEqual(self.hybrid['_llm_chat'](messages), '')
            self.assertEqual(self.hybrid['_llm_chat_with_meta'](messages), ('', ''))
            self.hybrid['is_ollama_available'].return_value = True
            self.hybrid['requests'].post.side_effect = TimeoutError(sentinel)
            self.assertEqual(self.hybrid['_llm_chat'](messages), '')
            self.assertEqual(self.hybrid['_llm_chat_with_meta'](messages), ('', ''))
            self.hybrid['requests'].post.side_effect = None
            response = self.hybrid['requests'].post.return_value
            response.json.return_value = {'message': {'content': sentinel}, 'done_reason': 'stop'}
            self.assertEqual(self.hybrid['_llm_chat_with_meta'](messages), (sentinel, 'stop'))
            self.hybrid['_llm_chat_with_meta'] = Mock(return_value=('unfinished', 'length'))
            rows = pd.DataFrame([dict(name='Synthetic', final_score=.8, tags='Action', genres='Action',
                                      price_category='Budget', review_positive_ratio=.8)])
            result = self.hybrid['explain_recommendations'](sentinel, 'Seed', rows)
            self.assertTrue(result)
            self.assertEqual(self.hybrid['_llm_chat_with_meta'].call_count, 2)
        self.assertNotIn(sentinel, stream.getvalue())
        for relative in ['app.py', 'src/hybrid_chat.py']:
            self.assertFalse(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'print'
                                 for n in ast.walk(ast.parse(text(relative)))))

    def test_r12_whole_app_smoke_with_service_and_ui_doubles(self):
        for action, intent in [('idle', ''), ('search', ''), ('recommend', ''),
                               ('chat', 'search_game'), ('chat', 'recommend_from_game'),
                               ('chat', 'analyze_data'), ('single', 'analyze_data'), ('chat', 'compare_games')]:
            with self.subTest(action=action, intent=intent):
                self.smoke(action, intent)

    def smoke(self, action, intent):
        request = 'phan tich thi truong' if intent == 'analyze_data' and action != 'single' else 'Synthetic request'
        ui = UI(request if intent else '', action)
        catalog = pd.DataFrame([dict(steam_appid=1, name='Seed', tags_cleaned='Action, RPG, Space, Extra',
                                    publishers_cleaned='Publisher', genres_cleaned='Action', price_category_vnd='Budget',
                                    categories_cleaned='Action, RPG', steam_review_positive_ratio=.8, price_vnd=100000, release_year=2020)])
        result = pd.DataFrame([dict(steam_appid=2, rank=1, name='Synthetic', tags='Action', genres='Action',
                                   tags_cleaned='Action', publishers_cleaned='Publisher', price_category='Budget',
                                   chunk_id=0, relevance_label='Synthetic label', similarity_score=.7, final_score=.7,
                                   review_positive_ratio=.8, release_year=2020)])
        raw = result.copy(deep=True)
        faiss = types.ModuleType('src.faiss_manager')
        faiss.load_all_faiss_indexes = Mock(return_value={})
        faiss.get_catalog_dataframe = Mock(return_value=catalog)
        faiss.search_across_all_chunks = Mock(return_value=result)
        faiss.recommend_with_faiss = Mock(return_value=(catalog.iloc[0], result.to_dict('records')))
        hybrid = types.ModuleType('src.hybrid_chat')
        for node in ast.parse(text('app.py')).body:
            if isinstance(node, ast.ImportFrom) and node.module == 'src.hybrid_chat':
                for alias in node.names:
                    setattr(hybrid, alias.name, Mock(return_value='Synthetic explanation'))
        hybrid.route_intent.return_value = intent
        hybrid.search_hybrid.return_value = result
        hybrid.recommend_hybrid.return_value = (catalog.iloc[0], result)
        hybrid.parse_user_filters.return_value = {}
        hybrid.extract_analyze_game_name.return_value = 'Seed' if action == 'single' else None
        hybrid.pick_game_by_name.return_value = catalog.iloc[0]
        hybrid.apply_structured_filters.return_value = catalog
        hybrid.parse_compare_names.return_value = ('Seed', 'Other')
        auto = types.ModuleType('src.auto_builder')
        auto.auto_build_indexes_if_needed = Mock(return_value=True)
        semantic = types.ModuleType('src.semantic_manager')
        semantic.ensure_semantic_index = Mock(return_value={'indexed': 1, 'new': 0, 'status': 'ok'})
        package = types.ModuleType('src')
        modules = {'streamlit': ui, 'altair': altair_double(), 'src': package,
                   'src.faiss_manager': faiss, 'src.hybrid_chat': hybrid,
                   'src.auto_builder': auto, 'src.semantic_manager': semantic}
        with patch.dict(sys.modules, modules), redirect_stdout(io.StringIO()) as output:
            exec(compile(text('app.py'), str(APP / 'app.py'), 'exec'), {'__name__': '__core_smoke__'})
        self.assertFalse(ui.warnings)
        self.assertEqual(output.getvalue(), '')
        if action == 'recommend' or intent == 'recommend_from_game':
            passed = hybrid.explain_recommendations.call_args.args[2]
            self.assertEqual(passed.review_positive_ratio.iloc[0], .8)
            self.assertTrue(any('review_positive_ratio' in frame and frame.review_positive_ratio.iloc[0] == 80 for frame in ui.frames))
        if intent == 'analyze_data':
            payload = ui.session_state.chat_messages[-1]['table_rows']
            self.assertTrue(all('metric' in row for row in payload))
            if action != 'single':
                self.assertTrue(any(row['metric'] == 'avg_review_positive_ratio' and row['value'] == .8 for row in payload))
                self.assertEqual(len(ui.charts), 5)
        if intent == 'compare_games':
            self.assertEqual(ui.session_state.chat_messages[-1]['table_rows'][1]['metric'], 'review_positive_ratio')
        # Search adds display-only metadata columns; model ratios remain raw.
        self.assertEqual(result.review_positive_ratio.iloc[0], raw.review_positive_ratio.iloc[0])

    @unittest.skipUnless(all(importlib.util.find_spec(name) is not None for name in ['streamlit', 'altair', 'faiss']),
                         'Native Streamlit/Altair/FAISS runtime is not installed; mocked smoke is separate')
    def test_r12_native_runtime_import_readiness(self):
        import streamlit
        import altair
        import faiss
        self.assertTrue(streamlit.__version__ and altair.__version__ and faiss.__version__)


if __name__ == '__main__':
    unittest.main(verbosity=2)
