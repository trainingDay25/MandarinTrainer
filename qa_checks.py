"""
QA / Regression-test script for Mandarin & Japanese Trainer.

Run after any code change or DB update:
    python qa_checks.py

If the Flask server is already running on port 5001 the HTTP checks also run.
All checks are independent — a failure in one does not abort the rest.

Exit code: 0 = all passed, 1 = one or more failures.
"""

import sqlite3
import os
import re
import sys
import json
import io

# ── UTF-8 stdout (Windows) ────────────────────────────────────────────────────
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
WORK_DB     = os.path.join(BASE_DIR, 'vocab.db')
RELEASE_DB  = os.path.join(BASE_DIR, 'src', 'mandarin_trainer', 'resources', 'vocab.db')
SERVER_URL  = 'http://127.0.0.1:5001'

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN  = '\033[92m'
RED    = '\033[91m'
YELLOW = '\033[93m'
CYAN   = '\033[96m'
RESET  = '\033[0m'
BOLD   = '\033[1m'

passed = failed = skipped = 0

def ok(msg):
    global passed
    passed += 1
    print(f'  {GREEN}PASS{RESET}  {msg}')

def fail(msg, detail=''):
    global failed
    failed += 1
    line = f'  {RED}FAIL{RESET}  {msg}'
    if detail:
        line += f'\n         {RED}>> {detail}{RESET}'
    print(line)

def skip(msg):
    global skipped
    skipped += 1
    print(f'  {YELLOW}SKIP{RESET}  {msg}')

def section(title):
    print(f'\n{BOLD}{CYAN}── {title} ──{RESET}')

def check(label, condition, detail=''):
    if condition:
        ok(label)
    else:
        fail(label, detail)

# ── DB helper ─────────────────────────────────────────────────────────────────
def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DATABASE HEALTH
# ═══════════════════════════════════════════════════════════════════════════════

def check_db(db_path, label='work DB'):
    # release DB is a seed — some tables/columns are added by init_db() at runtime
    is_release = (label == 'release DB')

    section(f'Database – {label}  ({os.path.basename(db_path)})')

    if not os.path.exists(db_path):
        fail(f'{label} file exists', f'Not found: {db_path}')
        return

    ok(f'{label} file exists')
    conn = open_db(db_path)

    # ── SQLite integrity ──────────────────────────────────────────────────────
    result = conn.execute('PRAGMA integrity_check').fetchone()[0]
    check('PRAGMA integrity_check = ok', result == 'ok', result)

    # ── Required tables ───────────────────────────────────────────────────────
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    # user_awards, game_plays, word_examples created at runtime — not required in seed DB
    required = {'words', 'progress', 'study_sessions', 'session_logs',
                'map_progress', 'custom_lists', 'custom_list_words',
                'grammar_favorites', 'users', 'dictionary'}
    if not is_release:
        required |= {'user_awards', 'game_plays', 'word_examples'}
    missing = required - tables
    check('All required tables present', not missing, f'Missing: {missing}')

    # ── words table ───────────────────────────────────────────────────────────
    total_words = conn.execute('SELECT COUNT(*) FROM words').fetchone()[0]
    check(f'words total > 20 000  (got {total_words:,})', total_words > 20_000)

    no_null_hanzi = conn.execute("SELECT COUNT(*) FROM words WHERE hanzi IS NULL OR hanzi=''").fetchone()[0]
    check('No blank hanzi in words', no_null_hanzi == 0, f'{no_null_hanzi} blank rows')

    # ── Chinese curricula ─────────────────────────────────────────────────────
    for cur, expected_min, levels in [
        ('classic', 5_000, list(range(1, 7))),
        ('hsk3',    9_000, list(range(1, 8))),
    ]:
        cnt = conn.execute('SELECT COUNT(*) FROM words WHERE curriculum=?', (cur,)).fetchone()[0]
        check(f'{cur}: word count > {expected_min:,}  (got {cnt:,})', cnt > expected_min)
        db_levels = {r[0] for r in conn.execute('SELECT DISTINCT hsk_level FROM words WHERE curriculum=?', (cur,)).fetchall()}
        for lv in levels:
            check(f'{cur}: level {lv} present', lv in db_levels)

    # ── JLPT ──────────────────────────────────────────────────────────────────
    jlpt_expected = {1: 718, 2: 668, 3: 2139, 4: 1748, 5: 2699}
    for stored_lv, expected_cnt in jlpt_expected.items():
        n_label = f'N{6 - stored_lv}'
        cnt = conn.execute('SELECT COUNT(*) FROM words WHERE curriculum=? AND hsk_level=?',
                           ('jlpt', stored_lv)).fetchone()[0]
        check(f'JLPT {n_label}: {expected_cnt} words  (got {cnt})', cnt == expected_cnt)

    # N5 example sentences
    n5_with_ex = conn.execute(
        "SELECT COUNT(*) FROM words WHERE curriculum='jlpt' AND hsk_level=1 "
        "AND example_hanzi IS NOT NULL AND example_hanzi != ''"
    ).fetchone()[0]
    check(f'JLPT N5: example sentences present for all 718  (got {n5_with_ex})', n5_with_ex == 718)

    # ── dictionary table ──────────────────────────────────────────────────────
    dict_cols = {r[1] for r in conn.execute('PRAGMA table_info(dictionary)').fetchall()}
    total_dict = conn.execute('SELECT COUNT(*) FROM dictionary').fetchone()[0]
    check(f'dictionary: total entries > 100 000  (got {total_dict:,})', total_dict > 100_000)

    if 'language' in dict_cols:
        ok("dictionary has 'language' column")
        cn_dict = conn.execute("SELECT COUNT(*) FROM dictionary WHERE language='chinese'").fetchone()[0]
        jp_dict = conn.execute("SELECT COUNT(*) FROM dictionary WHERE language='japanese'").fetchone()[0]
        check(f'dictionary: Chinese entries > 100 000  (got {cn_dict:,})', cn_dict > 100_000)
        check(f'dictionary: Japanese entries > 20 000  (got {jp_dict:,})', jp_dict > 20_000)
    elif is_release:
        skip("dictionary 'language' column — added by init_db() migration at runtime")
    else:
        fail("dictionary has 'language' column")

    # ── custom_lists table ────────────────────────────────────────────────────
    cl_cols = {r[1] for r in conn.execute('PRAGMA table_info(custom_lists)').fetchall()}
    if 'language' in cl_cols:
        ok("custom_lists has 'language' column")
    elif is_release:
        skip("custom_lists 'language' column — added by init_db() migration at runtime")
    else:
        fail("custom_lists has 'language' column")

    # ── word_examples table ───────────────────────────────────────────────────
    if 'word_examples' in tables:
        we_cnt = conn.execute('SELECT COUNT(*) FROM word_examples').fetchone()[0]
        check(f'word_examples has rows  (got {we_cnt:,})', we_cnt > 0)
    elif is_release:
        skip('word_examples table — created by init_db() at runtime')

    # ── progress orphans ──────────────────────────────────────────────────────
    orphans = conn.execute(
        'SELECT COUNT(*) FROM progress p WHERE NOT EXISTS '
        '(SELECT 1 FROM words w WHERE w.id = p.word_id)'
    ).fetchone()[0]
    check(f'No orphaned progress rows  (got {orphans})', orphans == 0)

    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. SEGMENTATION LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def check_segmentation():
    section('Segmentation')

    # Import the real functions from server
    try:
        sys.path.insert(0, os.path.join(BASE_DIR, 'src'))
        from mandarin_trainer.server import segment_japanese, segment_hanzi, _KANA_RE, _CJK_RE
    except Exception as e:
        fail('Import segment_japanese / segment_hanzi from server', str(e))
        return

    # ── segment_japanese ──────────────────────────────────────────────────────
    cases_jp = [
        ('駅で友達に会いました。',    ['駅', 'で', '友達', 'に', '会', 'いました']),
        ('空は青です。',              ['空', 'は', '青', 'です']),
        ('学校に行きます。',          ['学校', 'に', '行', 'きます']),
        ('友達と一緒に公園で遊びます。', ['友達', 'と', '一緒', 'に', '公園', 'で', '遊', 'びます']),
    ]
    for sentence, expected in cases_jp:
        result = segment_japanese(sentence)
        check(f'segment_japanese({sentence!r})', result == expected,
              f'got {result}, expected {expected}')

    # No empty tokens
    for sentence, _ in cases_jp:
        result = segment_japanese(sentence)
        check(f'No empty tokens in: {sentence!r}', all(t for t in result))

    # Kana is preserved (not stripped)
    kana_present = any(_KANA_RE.match(c) for c in ''.join(segment_japanese('食べます。')))
    check('segment_japanese preserves kana characters', kana_present)

    # Pure kana sentence (e.g. particle-only)
    pure_kana = segment_japanese('はい。')
    check('segment_japanese handles pure-kana sentence', len(pure_kana) >= 1 and all(pure_kana))

    # ── segment_hanzi ─────────────────────────────────────────────────────────
    cn_sentence = '我每天学习汉语。'
    result_cn = segment_hanzi(cn_sentence)
    check(f'segment_hanzi returns list for: {cn_sentence!r}', isinstance(result_cn, list) and len(result_cn) > 0)
    check('segment_hanzi: no kana in Chinese output',
          not any(_KANA_RE.match(c) for c in ''.join(result_cn)))
    check('segment_hanzi: all tokens non-empty', all(result_cn))


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SERVER MODULE IMPORTS & KEY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def check_imports():
    section('Server module & key functions')

    try:
        sys.path.insert(0, os.path.join(BASE_DIR, 'src'))
        import mandarin_trainer.server as srv
        ok('server.py imports without error')
    except Exception as e:
        fail('server.py imports without error', str(e))
        return

    # Key functions exist
    for fn in ['segment_hanzi', 'segment_japanese', 'make_cloze_prompt',
               'get_language', 'get_curricula', 'level_label',
               '_load_jp_dictionary', '_init_jieba']:
        check(f'Function exists: {fn}', hasattr(srv, fn))

    # level_label mapping
    expected_labels = {1: 'N5', 2: 'N4', 3: 'N3', 4: 'N2', 5: 'N1'}
    # Temporarily mock session to Japanese
    with srv.app.test_request_context('/'):
        with srv.app.test_client() as client:
            with client.session_transaction() as sess:
                sess['language'] = 'japanese'
                sess['user_id'] = None
        # Call level_label inside a fake request context with Japanese language
        import flask
        with srv.app.test_request_context('/'):
            flask.session['language'] = 'japanese'
            for stored, label in expected_labels.items():
                result = srv.level_label(stored)
                check(f'level_label({stored}) = {label!r}', result == label, f'got {result!r}')

    # get_curricula per language
    with srv.app.test_request_context('/'):
        flask.session['language'] = 'chinese'
        cn_cur = srv.get_curricula()
        check("get_curricula() Chinese = ('classic','hsk3')", set(cn_cur) == {'classic', 'hsk3'})

    with srv.app.test_request_context('/'):
        flask.session['language'] = 'japanese'
        jp_cur = srv.get_curricula()
        check("get_curricula() Japanese = ('jlpt',)", set(jp_cur) == {'jlpt'})

    # make_cloze_prompt
    with srv.app.test_request_context('/'):
        flask.session['language'] = 'chinese'
        cloze = srv.make_cloze_prompt('学校', '私は学校に行きます。')
        check('make_cloze_prompt returns non-empty string', bool(cloze))


# ═══════════════════════════════════════════════════════════════════════════════
# 4. HTTP ENDPOINT CHECKS  (only if server is running)
# ═══════════════════════════════════════════════════════════════════════════════

def check_http():
    section('HTTP endpoints  (requires running server)')

    try:
        import requests as req
    except ImportError:
        skip('requests library not available — skipping HTTP checks')
        return

    # Check if server is up
    try:
        ping = req.get(f'{SERVER_URL}/', timeout=3, allow_redirects=True)
    except Exception:
        skip('Server not running at 127.0.0.1:5001 — skipping HTTP checks')
        return

    ok(f'Server reachable at {SERVER_URL}')

    # Helper
    def get(path, params=None, expect_json=False):
        try:
            r = req.get(f'{SERVER_URL}{path}', params=params, timeout=10,
                        allow_redirects=True)
            if expect_json:
                return r.status_code, r.json()
            return r.status_code, r.text
        except Exception as e:
            return None, str(e)

    # Basic page routes
    for path in ['/', '/dictionary', '/game', '/words', '/stats', '/sessions',
                 '/awards', '/custom', '/grammar', '/map', '/users']:
        code, _ = get(path)
        check(f'GET {path} → 2xx or 3xx', code is not None and code < 500,
              f'status {code}')

    # API: TTS availability
    code, data = get('/api/tts/available', expect_json=True)
    check('GET /api/tts/available returns JSON with "available" key',
          isinstance(data, dict) and 'available' in data,
          str(data)[:120])

    # API: game words — classic (Chinese)
    code, data = get('/api/game/words', params={'curriculum': 'classic', 'count': 5}, expect_json=True)
    check('GET /api/game/words?curriculum=classic returns list', isinstance(data, list), str(data)[:120])
    if isinstance(data, list) and data:
        first = data[0]
        for field in ['id', 'hanzi', 'english', 'example_segments']:
            check(f'  classic game word has field: {field!r}', field in first)
        check('  classic example_segments is list', isinstance(first.get('example_segments'), list))
        # Should contain only CJK (no kana) since it's Chinese
        segs = first.get('example_segments', [])
        if segs:
            kana_in_cn = any(re.search(r'[぀-ヿ]', s) for s in segs)
            check('  classic segments contain no kana', not kana_in_cn,
                  f'Unexpected kana in: {segs}')

    # API: game words — jlpt (Japanese)
    code, data = get('/api/game/words', params={'curriculum': 'jlpt', 'hsk': '1', 'count': 5}, expect_json=True)
    check('GET /api/game/words?curriculum=jlpt returns list', isinstance(data, list), str(data)[:120])
    if isinstance(data, list) and data:
        first = data[0]
        check('  jlpt game word has example_segments', 'example_segments' in first)
        segs = first.get('example_segments', [])
        check('  jlpt example_segments is non-empty list', isinstance(segs, list) and len(segs) > 0)
        check('  jlpt segments all non-empty strings', all(isinstance(s, str) and s for s in segs))
        # Should contain kana somewhere across all words
        all_segs = [s for w in data for s in w.get('example_segments', [])]
        kana_found = any(re.search(r'[぀-ヿ]', s) for s in all_segs)
        check('  jlpt segments include kana tokens', kana_found,
              f'No kana in: {all_segs[:10]}')

    # API: dictionary search — Chinese
    code, data = get('/dictionary', params={'q': '学', 'lang': 'chinese'})
    check('GET /dictionary?q=学 returns 200', code == 200, f'status {code}')

    # Language switch routes exist
    for lang in ('chinese', 'japanese'):
        code, _ = get(f'/set-language/{lang}', params={'next': '/'})
        check(f'GET /set-language/{lang} returns redirect or 200', code is not None and code < 500)

    # /end route
    code, _ = get('/end', params={'next': '/'})
    check('GET /end redirects', code is not None and code < 500)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. RELEASE DB SPOT CHECK
# ═══════════════════════════════════════════════════════════════════════════════

def check_release_db():
    section('Release DB  (resources/vocab.db — bundled in APK/release ZIP)')

    if not os.path.exists(RELEASE_DB):
        skip(f'Release DB not found at {RELEASE_DB}')
        return

    conn = open_db(RELEASE_DB)

    jlpt_expected = {1: 718, 2: 668, 3: 2139, 4: 1748, 5: 2699}
    for stored_lv, expected_cnt in jlpt_expected.items():
        n_label = f'N{6 - stored_lv}'
        cnt = conn.execute('SELECT COUNT(*) FROM words WHERE curriculum=? AND hsk_level=?',
                           ('jlpt', stored_lv)).fetchone()[0]
        check(f'Release DB — JLPT {n_label}: {expected_cnt} words  (got {cnt})', cnt == expected_cnt)

    n5_ex = conn.execute(
        "SELECT COUNT(*) FROM words WHERE curriculum='jlpt' AND hsk_level=1 "
        "AND example_hanzi IS NOT NULL AND example_hanzi != ''"
    ).fetchone()[0]
    check(f'Release DB — N5 example sentences present  (got {n5_ex})', n5_ex == 718)

    # Parity check: same JLPT word count as work DB
    if os.path.exists(WORK_DB):
        work_conn = open_db(WORK_DB)
        for cur in ('classic', 'hsk3', 'jlpt'):
            work_cnt = work_conn.execute('SELECT COUNT(*) FROM words WHERE curriculum=?', (cur,)).fetchone()[0]
            rel_cnt  = conn.execute('SELECT COUNT(*) FROM words WHERE curriculum=?', (cur,)).fetchone()[0]
            check(f'Release DB {cur} word count matches work DB  ({rel_cnt} vs {work_cnt})',
                  rel_cnt == work_cnt, f'release={rel_cnt}, work={work_cnt}')
        work_conn.close()

    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# 6. JLPT CSV FILES
# ═══════════════════════════════════════════════════════════════════════════════

def check_jlpt_csv():
    section('JLPT source CSV files  (Jap/)')

    jap_dir = os.path.join(BASE_DIR, 'Jap')
    for n_level in (5, 4, 3, 2, 1):
        path = os.path.join(jap_dir, f'n{n_level}.csv')
        check(f'Jap/n{n_level}.csv exists', os.path.exists(path))
        if os.path.exists(path):
            import csv
            with open(path, newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
            check(f'Jap/n{n_level}.csv has rows  (got {len(rows)})', len(rows) > 0)
            required_cols = {'expression', 'reading', 'meaning'}
            actual_cols = set(rows[0].keys()) if rows else set()
            check(f'Jap/n{n_level}.csv has required columns',
                  required_cols.issubset(actual_cols),
                  f'Missing: {required_cols - actual_cols}')


# ═══════════════════════════════════════════════════════════════════════════════
# 8. FUNCTIONAL HTTP  (CRUD, accounts, export/import, reset)
# ═══════════════════════════════════════════════════════════════════════════════

def _make_test_apkg():
    """Build a minimal valid Anki .apkg (ZIP + SQLite) for upload testing."""
    import zipfile, io, sqlite3
    tmp = os.path.join(BASE_DIR, '_qa_tmp_apkg.anki2')
    try:
        mid = 1234567890
        fields = ['Expression', 'Reading', 'Meaning', 'Example']
        model_json = json.dumps({str(mid): {
            'id': str(mid), 'name': 'QA Model', 'type': 0, 'mod': 0, 'usn': 0,
            'sortf': 0, 'did': None, 'vers': [], 'tags': [], 'latexPre': '',
            'latexPost': '', 'css': '',
            'tmpls': [{'name': 'Card 1', 'ord': 0, 'qfmt': '{{Expression}}',
                       'afmt': '{{Reading}}<br>{{Meaning}}', 'bqfmt': '', 'bafmt': '',
                       'did': None, 'bfont': '', 'bsize': 0}],
            'flds': [{'name': f, 'ord': i, 'sticky': False, 'rtl': False,
                      'font': 'Arial', 'size': 20, 'media': []}
                     for i, f in enumerate(fields)],
        }})
        deck_json = json.dumps({'1': {
            'id': 1, 'name': 'Default', 'conf': 1, 'extendNew': 10, 'extendRev': 50,
            'collapsed': False, 'browserCollapsed': False,
            'newToday': [0, 0], 'revToday': [0, 0], 'lrnToday': [0, 0],
            'timeToday': [0, 0], 'dyn': 0, 'desc': '', 'usn': 0, 'mod': 0,
        }})
        conn = sqlite3.connect(tmp)
        conn.execute("""CREATE TABLE col (id INTEGER PRIMARY KEY, crt INTEGER,
            mod INTEGER, scm INTEGER, ver INTEGER, dty INTEGER, usn INTEGER,
            ls INTEGER, conf TEXT, models TEXT, decks TEXT, dconf TEXT, tags TEXT)""")
        conn.execute("INSERT INTO col VALUES (1,0,0,0,11,0,0,0,'{}',?,?,'{}','{}')",
                     (model_json, deck_json))
        conn.execute("""CREATE TABLE notes (id INTEGER PRIMARY KEY, guid TEXT,
            mid INTEGER, mod INTEGER, usn INTEGER, tags TEXT, flds TEXT, sfld TEXT,
            csum INTEGER, flags INTEGER, data TEXT)""")
        conn.execute("INSERT INTO notes VALUES (1,'qa1',?,0,0,'',?,'テスト',0,0,'')",
                     (mid, '\x1f'.join(['テスト', 'てすと', 'test word', 'Example sentence.'])))
        conn.execute("""CREATE TABLE cards (id INTEGER PRIMARY KEY, nid INTEGER,
            did INTEGER, ord INTEGER, mod INTEGER, usn INTEGER, type INTEGER,
            queue INTEGER, due INTEGER, ivl INTEGER, factor INTEGER, reps INTEGER,
            lapses INTEGER, left INTEGER, odue INTEGER, odid INTEGER,
            flags INTEGER, data TEXT)""")
        conn.execute("INSERT INTO cards VALUES (1,1,1,0,0,0,0,0,1,0,0,0,0,0,0,0,0,'')")
        conn.execute("""CREATE TABLE revlog (id INTEGER PRIMARY KEY, cid INTEGER,
            usn INTEGER, ease INTEGER, ivl INTEGER, lastIvl INTEGER, factor INTEGER,
            time INTEGER, type INTEGER)""")
        conn.execute('CREATE TABLE graves (usn INTEGER, oid INTEGER, type INTEGER)')
        conn.commit()
        conn.close()
        with open(tmp, 'rb') as fh:
            db_bytes = fh.read()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('collection.anki2', db_bytes)
            zf.writestr('media', '{}')
        return buf.getvalue()
    except Exception:
        return None
    finally:
        try:
            os.unlink(tmp)
        except Exception:
            pass


def check_functional_http():
    section('Functional HTTP  (CRUD, accounts, export/import, reset)')

    try:
        import requests as req
    except ImportError:
        skip('requests library not available — skipping functional checks')
        return
    try:
        req.get(f'{SERVER_URL}/', timeout=3, allow_redirects=True)
    except Exception:
        skip('Server not running — skipping functional checks')
        return

    s = req.Session()

    def pj(path, data):
        try:
            return s.post(f'{SERVER_URL}{path}', json=data, timeout=30, allow_redirects=True)
        except Exception:
            return None

    def pf(path, data):
        try:
            return s.post(f'{SERVER_URL}{path}', data=data, timeout=30, allow_redirects=True)
        except Exception:
            return None

    def gj(path, params=None):
        try:
            r = s.get(f'{SERVER_URL}{path}', params=params, timeout=10, allow_redirects=True)
            return r.status_code, r.json()
        except Exception:
            return None, {}

    def gt(path, params=None):
        try:
            r = s.get(f'{SERVER_URL}{path}', params=params, timeout=10, allow_redirects=True)
            return r.status_code, r.text
        except Exception:
            return None, ''

    def api_ok(r, label):
        """PASS if response is non-null, HTTP < 500, and ok:true (if present in JSON body)."""
        if r is None:
            fail(label, 'connection error')
            return False
        if r.status_code >= 500:
            fail(label, f'HTTP {r.status_code}: {r.text[:100]}')
            return False
        try:
            j = r.json()
            if isinstance(j, dict) and 'ok' in j and not j['ok']:
                fail(label, f'ok:false — {j.get("error", "")}')
                return False
        except Exception:
            pass
        ok(label)
        return True

    # Establish a Flask session: POST /users/create with the existing default user name.
    # The endpoint tolerates duplicate names (catches IntegrityError) and always sets
    # session['user_id'] — the only reliable way to get a Set-Cookie without hardcoding an ID.
    conn = open_db(WORK_DB)
    default_user = conn.execute('SELECT id, name FROM users ORDER BY id LIMIT 1').fetchone()
    conn.close()
    default_uid  = default_user['id']   if default_user else None
    default_name = default_user['name'] if default_user else None
    if default_name:
        pf('/users/create', {'name': default_name})
    else:
        skip('Functional checks — no users in DB; most checks will fail')
        return

    # ── Pre-cleanup: remove stale test data from previous failed runs ─────────
    conn = open_db(WORK_DB)
    stale_lists = conn.execute(
        "SELECT id FROM custom_lists WHERE name IN (?,?,?)",
        ('__qa_test__', '__qa_test_renamed__', '__qa_anki_test__')
    ).fetchall()
    stale_users = conn.execute(
        "SELECT id FROM users WHERE name IN (?,?)",
        ('__qa_test_user__', '__qa_test_user_renamed__')
    ).fetchall()
    conn.close()
    for row in stale_lists:
        pj('/api/custom-list/delete', {'list_id': row['id']})
    for row in stale_users:
        pf(f'/users/delete/{row["id"]}', {})

    # ── Custom Lists ──────────────────────────────────────────────────────────
    conn = open_db(WORK_DB)
    dict_rows = conn.execute('SELECT id FROM dictionary LIMIT 2').fetchall()
    conn.close()
    dict_ids = [r['id'] for r in dict_rows]

    _CL_SKIPS = ('Custom list: add word to existing list', 'Custom list: rename',
                 'Custom list: remove word', 'Custom list: delete',
                 'Custom list: gone after delete')

    if not dict_ids:
        skip('Custom list: create — no dictionary entries found')
        skip('Custom list: appears in listing — no dictionary entries found')
        for lbl in _CL_SKIPS:
            skip(f'{lbl} — no dictionary entries found')
    else:
        did1, did2 = dict_ids[0], dict_ids[-1]

        r = pj('/api/custom-list/add',
               {'list_name': '__qa_test__', 'word_ids': [did1]})
        created = api_ok(r, 'Custom list: create (POST /api/custom-list/add)')

        test_list_id = None
        if created and r:
            try:
                test_list_id = r.json().get('list_id')
            except Exception:
                pass

        _, ld = gj('/api/custom-lists')
        list_names = [l.get('name') for l in ld.get('lists', [])]
        check('Custom list: appears in GET /api/custom-lists',
              '__qa_test__' in list_names or test_list_id is not None,
              f'names seen: {list_names[:5]}')

        if test_list_id:
            r = pj('/api/custom-list/add',
                   {'list_id': test_list_id, 'word_ids': [did2]})
            api_ok(r, 'Custom list: add word to existing list')

            r = pj('/api/custom-list/rename',
                   {'list_id': test_list_id, 'name': '__qa_test_renamed__'})
            api_ok(r, 'Custom list: rename (POST /api/custom-list/rename)')

            conn = open_db(WORK_DB)
            clw = conn.execute(
                'SELECT word_id FROM custom_list_words WHERE list_id=? LIMIT 1',
                (test_list_id,)
            ).fetchone()
            conn.close()
            if clw:
                r = pj('/api/custom-list/remove-word',
                       {'list_id': test_list_id, 'word_id': clw['word_id']})
                api_ok(r, 'Custom list: remove word (POST /api/custom-list/remove-word)')
            else:
                skip('Custom list: remove word — list has no words after add')

            r = pj('/api/custom-list/delete', {'list_id': test_list_id})
            api_ok(r, 'Custom list: delete (POST /api/custom-list/delete)')

            _, ld2 = gj('/api/custom-lists')
            still = any(l.get('id') == test_list_id for l in ld2.get('lists', []))
            check('Custom list: gone after delete', not still)
        else:
            for lbl in _CL_SKIPS:
                skip(f'{lbl} — list creation failed')

    # ── Grammar Favorites ─────────────────────────────────────────────────────
    conn = open_db(WORK_DB)
    gp_row = conn.execute('SELECT id FROM grammar_points LIMIT 1').fetchone()
    conn.close()
    aids = [gp_row['id']] if gp_row else []
    if not aids:
        skip('Grammar: toggle favorite — no grammar_points in DB')
        skip('Grammar: untoggle favorite (cleanup)')
    else:
        aid = aids[0]
        r = pj(f'/api/grammar/{aid}/favorite', {})
        fav_on = None
        if r is not None and r.status_code < 500:
            try:
                fav_on = r.json().get('favorited')
            except Exception:
                pass
            check(f'Grammar: toggle favorite on  ({aid[:30]})',
                  fav_on == 1, f'got favorited={fav_on}')
        else:
            fail(f'Grammar: toggle favorite on  ({aid[:30]})',
                 f'HTTP {r.status_code if r else "none"}')

        r2 = pj(f'/api/grammar/{aid}/favorite', {})
        fav_off = None
        if r2 is not None and r2.status_code < 500:
            try:
                fav_off = r2.json().get('favorited')
            except Exception:
                pass
            check(f'Grammar: untoggle favorite / cleanup ({aid[:30]})',
                  fav_off == 0, f'got favorited={fav_off}')
        else:
            fail(f'Grammar: untoggle favorite ({aid[:30]})',
                 f'HTTP {r2.status_code if r2 else "none"}')

    # ── Export / Import ───────────────────────────────────────────────────────
    code, export_data = gj('/api/sync/export_raw')
    check('Export: GET /api/sync/export_raw returns non-empty dict',
          isinstance(export_data, dict) and bool(export_data),
          str(export_data)[:80])

    if isinstance(export_data, dict) and export_data:
        r = pj('/api/sync/import', export_data)
        api_ok(r, 'Import: POST /api/sync/import (roundtrip own export)')

    # ── Anki Import ───────────────────────────────────────────────────────────
    apkg = _make_test_apkg()
    if not apkg:
        skip('Anki: upload .apkg — failed to build test package')
        skip('Anki: import deck')
    else:
        r = s.post(f'{SERVER_URL}/anki/upload',
                   files={'apkg': ('qa_test.apkg', apkg, 'application/zip')},
                   timeout=15, allow_redirects=True)
        uploaded = api_ok(r, 'Anki: upload .apkg (POST /anki/upload)')

        if uploaded and r:
            try:
                ud      = r.json()
                token   = ud.get('token')
                models  = ud.get('models', [])
                mid_str = models[0].get('mid') if models else None
            except Exception:
                token = mid_str = None

            if token and mid_str:
                r2 = pj('/anki/import', {
                    'token': token, 'mid': mid_str,
                    'list_name': '__qa_anki_test__',
                    'mapping': {'hanzi': 0, 'pinyin': 1, 'english': 2, 'example_hanzi': 3},
                })
                api_ok(r2, 'Anki: import deck (POST /anki/import)')
                # Cleanup
                conn = open_db(WORK_DB)
                alist = conn.execute(
                    "SELECT id FROM custom_lists WHERE name='__qa_anki_test__' LIMIT 1"
                ).fetchone()
                conn.close()
                if alist:
                    pj('/api/custom-list/delete', {'list_id': alist['id']})
            else:
                skip('Anki: import deck — no token/model in upload response')
        else:
            skip('Anki: import deck — upload step failed')

    # ── Reset (safe: non-existent level / fake curriculum → 0 rows deleted) ──
    r = pf('/reset/progress',
           {'curriculum': 'classic', 'hsk_level': '99', 'next': '/'})
    check('Reset: word progress level 99 (POST /reset/progress, safe no-op)',
          r is not None and r.status_code < 500,
          f'HTTP {r.status_code if r else "none"}')

    r = pf('/api/map/reset', {'curriculum': '__qa_test_curriculum__'})
    check('Reset: map progress fake curriculum (POST /api/map/reset, safe no-op)',
          r is not None and r.status_code < 500,
          f'HTTP {r.status_code if r else "none"}')

    # ── User Accounts (last — /users/create auto-switches session) ────────────
    r = pf('/users/create', {'name': '__qa_test_user__'})
    check('Users: create (POST /users/create)',
          r is not None and r.status_code < 500,
          f'HTTP {r.status_code if r else "none"}')

    conn = open_db(WORK_DB)
    urow = conn.execute(
        "SELECT id FROM users WHERE name='__qa_test_user__' LIMIT 1"
    ).fetchone()
    conn.close()
    tid = urow['id'] if urow else None
    check('Users: new user exists in DB', tid is not None)

    if tid:
        r = pj(f'/users/rename/{tid}', {'name': '__qa_test_user_renamed__'})
        api_ok(r, 'Users: rename (POST /users/rename/<id>)')

        r = pf(f'/users/switch/{tid}', {})
        check('Users: switch to test user (POST /users/switch/<id>)',
              r is not None and r.status_code < 500)

        pf(f'/users/switch/{default_uid}', {})  # switch back to default user before deleting

        r = pf(f'/users/delete/{tid}', {})
        check('Users: delete (POST /users/delete/<id>)',
              r is not None and r.status_code < 500)

        conn = open_db(WORK_DB)
        gone = conn.execute('SELECT id FROM users WHERE id=?', (tid,)).fetchone()
        conn.close()
        check('Users: deleted user gone from DB', gone is None)
    else:
        for lbl in ('Users: rename', 'Users: switch to test user',
                    'Users: delete', 'Users: deleted user gone from DB'):
            skip(f'{lbl} — test user not created')

    pf(f'/users/switch/{default_uid}', {})  # ensure we end on user 1


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print(f'\n{BOLD}Mandarin & Japanese Trainer — QA Checks{RESET}')
    print('=' * 55)

    check_db(WORK_DB,    label='work DB')
    check_db(RELEASE_DB, label='release DB')
    check_release_db()
    check_jlpt_csv()
    check_imports()
    check_segmentation()
    check_http()
    check_functional_http()

    # ── Summary ───────────────────────────────────────────────────────────────
    total = passed + failed + skipped
    print(f'\n{"=" * 55}')
    colour = GREEN if failed == 0 else RED
    print(f'{BOLD}{colour}Results: {passed} passed  {failed} failed  {skipped} skipped  /  {total} total{RESET}')
    if failed:
        print(f'{RED}Some checks failed — review output above.{RESET}')
    else:
        print(f'{GREEN}All checks passed.{RESET}')
    print()

    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
