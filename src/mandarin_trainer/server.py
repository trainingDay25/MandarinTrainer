from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory, send_file, g
import sqlite3
import os
import sys
import json
import uuid
import random
import re
import unicodedata
import hashlib
import asyncio
import threading
import zipfile
import shutil
import requests as _requests

IS_ANDROID = hasattr(sys, 'getandroidapilevel')

# Shared state for Android native-to-WebView import picker.
# Written by __main__.py native command, consumed by Flask routes below.
# Keys are short hex tokens; values are dicts with 'files' and 'read_file'.
_import_pending = {}

if IS_ANDROID:
    edge_tts = None          # type: ignore[assignment]
    try:
        from gtts import gTTS as _gTTS
        _TTS_AVAILABLE = True
    except ImportError:
        _gTTS = None         # type: ignore[assignment]
        _TTS_AVAILABLE = False
else:
    _gTTS = None             # type: ignore[assignment]
    try:
        import edge_tts
        _TTS_AVAILABLE = True
    except ImportError:
        edge_tts = None      # type: ignore[assignment]
        _TTS_AVAILABLE = False

from datetime import datetime, timedelta, date as date_cls

try:
    import jieba
    _JIEBA_AVAILABLE = True
except ImportError:
    jieba = None             # type: ignore[assignment]
    _JIEBA_AVAILABLE = False

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__,
    template_folder=os.path.join(_MODULE_DIR, 'templates'),
    static_folder=os.path.join(_MODULE_DIR, 'static'))
app.secret_key = os.environ.get('SECRET_KEY', 'mandarin-srs-dev-key')

# ── Pinyin search helpers ─────────────────────────────────────────────────────

_TONE_MAP = {
    'ā':('a',1),'á':('a',2),'ǎ':('a',3),'à':('a',4),
    'ē':('e',1),'é':('e',2),'ě':('e',3),'è':('e',4),
    'ī':('i',1),'í':('i',2),'ǐ':('i',3),'ì':('i',4),
    'ō':('o',1),'ó':('o',2),'ǒ':('o',3),'ò':('o',4),
    'ū':('u',1),'ú':('u',2),'ǔ':('u',3),'ù':('u',4),
    'ǖ':('u:',1),'ǘ':('u:',2),'ǚ':('u:',3),'ǜ':('u:',4),
}
# Reverse: (base, tone) → diacritical char
_DIAC_MAP = {v: k for k, v in _TONE_MAP.items()}
_NEUTRAL  = {'a':'a','e':'e','i':'i','o':'o','u':'u','u:':'ü'}

def generate_tone_options(pinyin: str):
    """Return (options[5], correct_idx) varying the first toned syllable
    through tones 1-4 + neutral, or (None, None) if no tone mark found."""
    if not pinyin:
        return None, None
    syllables = pinyin.strip().split()
    for si, syl in enumerate(syllables):
        for ci, ch in enumerate(syl):
            if ch in _TONE_MAP:
                base, correct_tone = _TONE_MAP[ch]
                options, correct_idx = [], None
                for ti, tone in enumerate([1, 2, 3, 4, 0]):
                    repl = _NEUTRAL[base] if tone == 0 else _DIAC_MAP.get((base, tone), ch)
                    new_syls      = syllables[:]
                    new_syls[si]  = syl[:ci] + repl + syl[ci+1:]
                    options.append(' '.join(new_syls))
                    if tone == correct_tone:
                        correct_idx = ti
                return options, correct_idx
    return None, None

# Matches one pinyin syllable (with or without tone marks / tone numbers)
_SYLLABLE_RE = re.compile(
    r'(zh|ch|sh|[bpmfdtnlgkhzcsrwy])?'
    r'([aeiouüāáǎàēéěèīíǐìōóǒòūúǔùǖǘǚǜ]'
     r'[aeiouüāáǎàēéěèīíǐìōóǒòūúǔùǖǘǚǜ]*)'
    r'(ng|n|r)?'
    r'([1-5])?',
    re.IGNORECASE,
)

def pinyin_to_like_pattern(query):
    """Convert user pinyin input → LIKE pattern for CC-CEDICT format (spaced, tone-number).

    nǐhǎo   → ni3 hao3   (exact match)
    ni3hao3 → ni3 hao3   (exact match)
    nihao   → ni_ hao_   (any tone)
    nihao3  → ni_ hao3   (partial tone)
    ni3hao  → ni3 hao_   (partial tone)
    """
    q = query.lower().replace(' ', '')
    tokens = []
    pos = 0
    while pos < len(q):
        m = _SYLLABLE_RE.match(q, pos)
        if not m or not m.group(0):
            pos += 1
            continue
        initial    = m.group(1) or ''
        nucleus    = m.group(2) or ''
        final      = m.group(3) or ''
        digit      = m.group(4)        # existing tone number, or None

        # Strip tone marks from nucleus, collect tone
        base_chars = []
        mark_tone  = None
        for c in nucleus:
            if c in _TONE_MAP:
                b, t = _TONE_MAP[c]
                base_chars.append(b)
                mark_tone = str(t)
            else:
                base_chars.append(c)

        tone = digit or mark_tone      # explicit digit beats mark (same syllable)
        syl  = initial + ''.join(base_chars) + final
        tokens.append(syl + tone if tone else syl + '_')
        pos += len(m.group(0))

    return ' '.join(tokens) if tokens else None

def _resolve_data_dir():
    candidate = os.path.normpath(os.path.join(_MODULE_DIR, '..', '..'))
    if os.path.isfile(os.path.join(candidate, 'vocab.db')):
        return candidate
    return _MODULE_DIR

BASE_DIR = os.environ.get('MANDARIN_BASE_DIR') or _resolve_data_dir()
DB_PATH    = os.path.join(BASE_DIR, 'vocab.db')
TTS_CACHE = os.path.join(BASE_DIR, 'tts_cache')
os.makedirs(TTS_CACHE, exist_ok=True)
ANKI_TEMP_DIR = os.path.join(BASE_DIR, 'anki_temp')
os.makedirs(ANKI_TEMP_DIR, exist_ok=True)

LEMONADE_URL   = os.environ.get('LEMONADE_URL',   'http://127.0.0.1:8000')
LEMONADE_MODEL = os.environ.get('LEMONADE_MODEL', 'Qwen3-VL-8B-Instruct-GGUF')
LEMONADE_BATCH = 20

# task_id -> {done, total, status, message, finished, results}
_generation_tasks: dict = {}

_LEMONADE_SYSTEM = (
    "You are a JSON sentence generator. Output ONLY a raw JSON array — "
    "no markdown, no preamble, no explanation, no <think> tags.\n"
    "For each input word produce one Mandarin example sentence (10–18 characters) "
    "that contains that exact word, plus a natural English translation.\n"
    "Format: [{\"id\": <int>, \"example_hanzi\": \"<sentence>\", "
    "\"example_english\": \"<translation>\"}, ...]"
)


def _lemonade_call(batch: list[dict]) -> list[dict]:
    payload = [{'id': w['id'], 'hanzi': w['hanzi'], 'pinyin': w['pinyin'],
                'english': w['english']} for w in batch]
    resp = _requests.post(
        f'{LEMONADE_URL}/v1/chat/completions',
        json={
            'model': LEMONADE_MODEL,
            'messages': [
                {'role': 'system', 'content': _LEMONADE_SYSTEM},
                {'role': 'user',   'content': 'Generate:\n' + json.dumps(payload, ensure_ascii=False)},
            ],
            'temperature': 0.5,
            'max_tokens': len(batch) * 200,  # extra headroom for reasoning tokens
        },
        timeout=300,
    )
    resp.raise_for_status()
    import re as _re
    message = resp.json()['choices'][0]['message']
    # Some backends put thinking model output in reasoning_content instead of content
    content   = (message.get('content')           or '').strip()
    reasoning = (message.get('reasoning_content') or '').strip()
    raw = content or reasoning
    # Strip closed <think>...</think> blocks
    outside = _re.sub(r'<think>.*?</think>', '', raw, flags=_re.DOTALL).strip()
    # If nothing remains outside (model put everything inside think), extract the think content
    if not outside:
        m = _re.search(r'<think>(.*?)(?:</think>|$)', raw, flags=_re.DOTALL)
        candidate = m.group(1).strip() if m else raw
    else:
        candidate = outside
    # Strip markdown fences
    if candidate.startswith('```'):
        candidate = candidate.split('\n', 1)[1].rsplit('```', 1)[0].strip()
    # Find the JSON array between first '[' and last ']'
    start = candidate.find('[')
    end   = candidate.rfind(']')
    if start == -1 or end == -1:
        raise ValueError(f'No JSON array found in response: {candidate[:200]!r}')
    text = candidate[start:end + 1]
    results = json.loads(text)
    ids = {w['id'] for w in batch}
    return [r for r in results if r.get('id') in ids
            and r.get('example_hanzi') and r.get('example_english')]


def _generate_for_list(task_id: str, list_id: int):
    task = _generation_tasks[task_id]
    try:
        with get_db() as conn:
            rows = conn.execute('''
                SELECT w.id, w.hanzi, w.pinyin, w.english, w.hsk_level
                FROM words w
                JOIN custom_list_words clw ON w.id = clw.word_id
                WHERE clw.list_id = ?
                  AND (w.example_hanzi IS NULL OR w.example_hanzi = '')
                ORDER BY w.id
            ''', (list_id,)).fetchall()
        words = [dict(r) for r in rows]
        task['total'] = len(words)

        if not words:
            task.update(status='done', message='All words already have example sentences.', finished=True)
            return

        task['status'] = 'running'
        done    = 0
        skipped = 0
        last_err = ''
        for i in range(0, len(words), LEMONADE_BATCH):
            batch = words[i:i + LEMONADE_BATCH]
            for attempt in range(4):
                try:
                    results = _lemonade_call(batch)
                    with get_db() as conn:
                        conn.executemany(
                            'UPDATE words SET example_hanzi = ?, example_english = ? WHERE id = ?',
                            [(r['example_hanzi'], r['example_english'], r['id']) for r in results]
                        )
                    done += len(results)
                    task['done'] = done
                    for r in results:
                        w = next((x for x in batch if x['id'] == r['id']), {})
                        task['results'].append({
                            'id':              r['id'],
                            'hanzi':           w.get('hanzi', ''),
                            'example_hanzi':   r['example_hanzi'],
                            'example_english': r['example_english'],
                        })
                    break
                except (json.JSONDecodeError, ValueError) as e:
                    last_err = str(e)
                    if attempt < 3:
                        continue
                    skipped += len(batch)
                    break  # log and skip batch after repeated failures
                except _requests.RequestException as e:
                    task.update(status='error', message=f'Cannot reach Lemonade: {e}', finished=True)
                    return

        msg = f'{done} / {len(words)} sentences generated.'
        if skipped:
            msg += f' {skipped} skipped (parse error: {last_err[:120]})'
        task.update(status='done', message=msg, finished=True)

    except Exception as e:
        task.update(status='error', message=str(e), finished=True)

# Audio files live one level up (e.g. HSK1_ALL/word.wav)
AUDIO_BASE = os.environ.get('AUDIO_DIR', os.path.join(BASE_DIR, '..'))

# SRS ladder in minutes: 1m 5m 10m 30m 1h 6h 1d 3d 7d 15d 30d 90d
INTERVALS = [1, 5, 10, 30, 60, 360, 1440, 4320, 10080, 21600, 43200, 129600]
MAX_STEP   = len(INTERVALS) - 1

def fmt_interval(minutes):
    if minutes < 60:
        return f"{minutes}min"
    if minutes < 1440:
        return f"{minutes // 60}h"
    if minutes < 10080:
        return f"{minutes // 1440}d"
    if minutes < 43200:
        return f"{minutes // 10080}w"
    return f"{minutes // 43200}mo"

def grade_labels(step):
    """Return (wrong, medium, easy) interval labels for a word at the given step."""
    eff = max(step, -1)
    return (
        fmt_interval(INTERVALS[0]),
        fmt_interval(INTERVALS[max(1, eff - 1)]),
        fmt_interval(INTERVALS[min(MAX_STEP, max(2, eff + 1))]),
    )

def next_step(current_step, grade):
    """Compute new interval_step given current step and grade string."""
    eff = max(current_step, -1)
    if grade == 'wrong':
        return 0
    if grade == 'medium':
        return max(1, eff - 1)
    # easy
    return min(MAX_STEP, max(2, eff + 1))

# ── DB helpers ────────────────────────────────────────────────────────────────

def _pinyin_collation(a, b):
    def strip_tones(s):
        nfd = unicodedata.normalize('NFD', s.lower())
        return ''.join(c for c in nfd if not unicodedata.combining(c))
    sa, sb = strip_tones(a), strip_tones(b)
    return (sa > sb) - (sa < sb)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.create_collation('PINYIN', _pinyin_collation)
    return conn

def _load_jp_dictionary(conn):
    """Load jp_dictionary.json into the dictionary table (runs once, skipped if already loaded)."""
    if conn.execute("SELECT COUNT(*) FROM dictionary WHERE language='japanese'").fetchone()[0] > 0:
        return
    jp_path = os.path.join(os.path.dirname(__file__), 'jp_dictionary.json')
    if not os.path.exists(jp_path):
        return
    with open(jp_path, encoding='utf-8') as f:
        entries = json.load(f)
    conn.executemany(
        "INSERT INTO dictionary (traditional, simplified, pinyin, english, language) VALUES (?,?,?,?,'japanese')",
        [(e['word'], e['word'], e.get('romanization', ''), e.get('english_translation', ''))
         for e in entries]
    )


def init_db():
    with get_db() as conn:
        conn.execute('PRAGMA journal_mode=DELETE')
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS words (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                hanzi           TEXT NOT NULL,
                pinyin          TEXT,
                english         TEXT,
                example_hanzi   TEXT,
                example_pinyin  TEXT,
                example_english TEXT,
                audio_file      TEXT,
                hsk_level       INTEGER DEFAULT 0,
                curriculum      TEXT DEFAULT 'classic'
            );
            CREATE TABLE IF NOT EXISTS progress (
                word_id       INTEGER PRIMARY KEY REFERENCES words(id),
                interval_step INTEGER DEFAULT 0,
                last_grade    TEXT,
                last_seen     TEXT,
                due_at        TEXT
            );
            CREATE TABLE IF NOT EXISTS study_sessions (
                id         TEXT PRIMARY KEY,
                mode       TEXT,
                created_at TEXT,
                queue      TEXT
            );
        ''')
        # Migrate: add curriculum column if it doesn't exist yet
        cols = [r[1] for r in conn.execute('PRAGMA table_info(words)').fetchall()]
        if 'curriculum' not in cols:
            conn.execute("ALTER TABLE words ADD COLUMN curriculum TEXT DEFAULT 'classic'")
            conn.execute("UPDATE words SET curriculum = 'classic' WHERE curriculum IS NULL")

        # session_logs: persisted summary after each session ends
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS session_logs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at   TEXT,
                ended_at     TEXT,
                stack_size   INTEGER DEFAULT 0,
                total_seen   INTEGER DEFAULT 0,
                new_words    INTEGER DEFAULT 0,
                easy_count   INTEGER DEFAULT 0,
                medium_count INTEGER DEFAULT 0,
                wrong_count  INTEGER DEFAULT 0,
                selection    TEXT    DEFAULT '{}',
                score        REAL    DEFAULT 0
            );
        ''')

        # Migrate study_sessions: add history + stat tracking columns + user_id
        scols = [r[1] for r in conn.execute('PRAGMA table_info(study_sessions)').fetchall()]
        for col, dflt in [
            ('history',      "'[]'"),
            ('stack_size',   '0'),
            ('new_words',    '0'),
            ('easy_count',   '0'),
            ('medium_count', '0'),
            ('wrong_count',  '0'),
            ('selection',    "'{}'"),
            ('user_id',      'NULL'),
        ]:
            if col not in scols:
                conn.execute(f"ALTER TABLE study_sessions ADD COLUMN {col} TEXT DEFAULT {dflt}")

        # Custom lists
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS custom_lists (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS custom_list_words (
                list_id  INTEGER NOT NULL REFERENCES custom_lists(id),
                word_id  INTEGER NOT NULL REFERENCES words(id),
                PRIMARY KEY (list_id, word_id)
            );
        ''')

        # Multiple example sentences per word
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS word_examples (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                word_id         INTEGER NOT NULL REFERENCES words(id),
                example_hanzi   TEXT    NOT NULL,
                example_english TEXT    NOT NULL,
                created_at      TEXT    DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_word_examples_word_id ON word_examples(word_id);
        ''')
        # One-time migration: seed word_examples from existing words.example_hanzi
        migrated = conn.execute('SELECT COUNT(*) FROM word_examples').fetchone()[0]
        if migrated == 0:
            conn.execute('''
                INSERT INTO word_examples (word_id, example_hanzi, example_english)
                SELECT id, example_hanzi, example_english
                FROM words
                WHERE example_hanzi IS NOT NULL AND example_hanzi != ''
                  AND example_english IS NOT NULL AND example_english != ''
            ''')

        # Map progress (single-user, persists across sessions)
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS map_progress (
                curriculum  TEXT    NOT NULL,
                level       INTEGER NOT NULL,
                circle_num  INTEGER NOT NULL,
                best_score  REAL    DEFAULT 0,
                passed      INTEGER DEFAULT 0,
                attempts    INTEGER DEFAULT 0,
                last_at     TEXT,
                PRIMARY KEY (curriculum, level, circle_num)
            );
        ''')

        # Dictionary table (populated by import_cedict.py / jp_dictionary.json)
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS dictionary (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                traditional TEXT NOT NULL,
                simplified  TEXT NOT NULL,
                pinyin      TEXT,
                english     TEXT,
                language    TEXT DEFAULT 'chinese'
            );
            CREATE INDEX IF NOT EXISTS idx_dict_simplified  ON dictionary(simplified);
            CREATE INDEX IF NOT EXISTS idx_dict_traditional ON dictionary(traditional);
        ''')
        dict_cols = {r[1] for r in conn.execute('PRAGMA table_info(dictionary)').fetchall()}
        if 'language' not in dict_cols:
            conn.execute("ALTER TABLE dictionary ADD COLUMN language TEXT DEFAULT 'chinese'")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dict_language ON dictionary(language)")
        _load_jp_dictionary(conn)

        # ── Users (multi-user support) ─────────────────────────────────────────
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL UNIQUE,
                created_at TEXT    DEFAULT (datetime('now'))
            );
        ''')

        # Migrate progress: add user_id, rebuild with composite PK (user_id, word_id)
        prog_cols = {r[1] for r in conn.execute('PRAGMA table_info(progress)').fetchall()}
        if 'user_id' not in prog_cols:
            conn.executescript('''
                ALTER TABLE progress ADD COLUMN user_id INTEGER DEFAULT 1;
                UPDATE progress SET user_id = 1 WHERE user_id IS NULL;
                CREATE TABLE progress_new (
                    user_id       INTEGER NOT NULL DEFAULT 1,
                    word_id       INTEGER NOT NULL,
                    interval_step INTEGER DEFAULT 0,
                    last_grade    TEXT,
                    last_seen     TEXT,
                    due_at        TEXT,
                    PRIMARY KEY (user_id, word_id)
                );
                INSERT INTO progress_new
                    SELECT user_id, word_id, interval_step, last_grade, last_seen, due_at
                    FROM progress;
                DROP TABLE progress;
                ALTER TABLE progress_new RENAME TO progress;
            ''')

        # Migrate map_progress: add user_id, rebuild with composite PK
        map_cols = {r[1] for r in conn.execute('PRAGMA table_info(map_progress)').fetchall()}
        if 'user_id' not in map_cols:
            conn.executescript('''
                CREATE TABLE map_progress_new (
                    user_id    INTEGER NOT NULL DEFAULT 1,
                    curriculum TEXT    NOT NULL,
                    level      INTEGER NOT NULL,
                    circle_num INTEGER NOT NULL,
                    best_score REAL    DEFAULT 0,
                    passed     INTEGER DEFAULT 0,
                    attempts   INTEGER DEFAULT 0,
                    last_at    TEXT,
                    PRIMARY KEY (user_id, curriculum, level, circle_num)
                );
                INSERT INTO map_progress_new
                    SELECT 1, curriculum, level, circle_num, best_score, passed, attempts, last_at
                    FROM map_progress;
                DROP TABLE map_progress;
                ALTER TABLE map_progress_new RENAME TO map_progress;
            ''')

        # Migrate map_progress: add passed_at column
        if 'passed_at' not in map_cols:
            conn.execute("ALTER TABLE map_progress ADD COLUMN passed_at TEXT")
            conn.execute("UPDATE map_progress SET passed_at = last_at WHERE passed = 1")

        # Migrate session_logs: add user_id column
        sl_cols = {r[1] for r in conn.execute('PRAGMA table_info(session_logs)').fetchall()}
        if 'user_id' not in sl_cols:
            conn.execute('ALTER TABLE session_logs ADD COLUMN user_id INTEGER DEFAULT 1')
            conn.execute('UPDATE session_logs SET user_id = 1')

        # Migrate custom_lists: add user_id column
        cl_cols = {r[1] for r in conn.execute('PRAGMA table_info(custom_lists)').fetchall()}
        if 'user_id' not in cl_cols:
            conn.execute('ALTER TABLE custom_lists ADD COLUMN user_id INTEGER DEFAULT 1')
            conn.execute('UPDATE custom_lists SET user_id = 1')

        # Per-user grammar favorites
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS grammar_favorites (
                user_id  INTEGER NOT NULL,
                point_id TEXT    NOT NULL,
                PRIMARY KEY (user_id, point_id)
            );
        ''')
        # Awards and game-play tracking
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS user_awards (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                award_key  TEXT    NOT NULL,
                awarded_at TEXT,
                UNIQUE (user_id, award_key)
            );
            CREATE TABLE IF NOT EXISTS game_plays (
                user_id   INTEGER NOT NULL,
                game_type TEXT    NOT NULL,
                PRIMARY KEY (user_id, game_type)
            );
        ''')

        # Migrate game_plays: add plays count column
        gp_cols = {r[1] for r in conn.execute('PRAGMA table_info(game_plays)').fetchall()}
        if 'plays' not in gp_cols:
            conn.execute('ALTER TABLE game_plays ADD COLUMN plays INTEGER DEFAULT 1')

        # One-time migration: seed from legacy grammar_points.favorited for trainingDay (id=1)
        seeded = conn.execute('SELECT COUNT(*) FROM grammar_favorites').fetchone()[0]
        if seeded == 0:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO grammar_favorites (user_id, point_id)
                    SELECT 1, id FROM grammar_points WHERE favorited = 1
                ''')
            except sqlite3.OperationalError:
                pass  # grammar_points table may not exist yet

        # Seed JLPT vocabulary from CSV files if not already present
        # Stored levels: N5→1 (easiest), N4→2, N3→3, N2→4, N1→5 (hardest)
        jlpt_count = conn.execute("SELECT COUNT(*) FROM words WHERE curriculum='jlpt'").fetchone()[0]
        if jlpt_count == 0:
            import csv as _csv
            jap_dir = os.path.join(_MODULE_DIR, '..', '..', 'Jap')
            for n_level, stored_level in [(5, 1), (4, 2), (3, 3), (2, 4), (1, 5)]:
                csv_path = os.path.join(jap_dir, f'n{n_level}.csv')
                if not os.path.exists(csv_path):
                    continue
                with open(csv_path, newline='', encoding='utf-8') as f:
                    for row in _csv.DictReader(f):
                        expr = (row.get('expression') or '').strip()
                        read = (row.get('reading') or '').strip()
                        mean = (row.get('meaning') or '').strip()
                        if not expr:
                            continue
                        conn.execute(
                            'INSERT OR IGNORE INTO words (hanzi, pinyin, english, hsk_level, curriculum) VALUES (?,?,?,?,?)',
                            (expr, read, mean, stored_level, 'jlpt')
                        )

        # Migrate custom_lists: add language column
        cl2_cols = {r[1] for r in conn.execute('PRAGMA table_info(custom_lists)').fetchall()}
        if 'language' not in cl2_cols:
            conn.execute("ALTER TABLE custom_lists ADD COLUMN language TEXT DEFAULT 'chinese'")


# ── Time helpers ──────────────────────────────────────────────────────────────

def now_str():
    return datetime.now().isoformat()

def future_str(minutes):
    return (datetime.now() + timedelta(minutes=minutes)).isoformat()


# ── Awards ────────────────────────────────────────────────────────────────────

AWARDS = [
    # ── Flashcard reviews ──
    {'cat': 'flashcard', 'key': 'reviews_50',    'icon': '🃏', 'title': 'Getting Started',   'desc': 'Review 50 flashcards',        'threshold': 50},
    {'cat': 'flashcard', 'key': 'reviews_100',   'icon': '🃏', 'title': 'Committed',          'desc': 'Review 100 flashcards',       'threshold': 100},
    {'cat': 'flashcard', 'key': 'reviews_500',   'icon': '📖', 'title': 'On a Roll',          'desc': 'Review 500 flashcards',       'threshold': 500},
    {'cat': 'flashcard', 'key': 'reviews_1k',    'icon': '📚', 'title': 'Enthusiast',         'desc': 'Review 1,000 flashcards',     'threshold': 1000},
    {'cat': 'flashcard', 'key': 'reviews_5k',    'icon': '📚', 'title': 'Dedicated',          'desc': 'Review 5,000 flashcards',     'threshold': 5000},
    {'cat': 'flashcard', 'key': 'reviews_10k',   'icon': '🎓', 'title': 'Scholar',            'desc': 'Review 10,000 flashcards',    'threshold': 10000},
    {'cat': 'flashcard', 'key': 'reviews_50k',   'icon': '🏆', 'title': 'Master',             'desc': 'Review 50,000 flashcards',    'threshold': 50000},
    {'cat': 'flashcard', 'key': 'reviews_100k',  'icon': '👑', 'title': 'Legend',             'desc': 'Review 100,000 flashcards',   'threshold': 100000},
    # ── Words learned ──
    {'cat': 'words',     'key': 'words_10',      'icon': '✨', 'title': 'First Steps',        'desc': 'Learn 10 words',              'threshold': 10},
    {'cat': 'words',     'key': 'words_100',     'icon': '💯', 'title': 'Century',            'desc': 'Learn 100 words',             'threshold': 100},
    {'cat': 'words',     'key': 'words_500',     'icon': '🧱', 'title': 'Building Blocks',    'desc': 'Learn 500 words',             'threshold': 500},
    {'cat': 'words',     'key': 'words_1k',      'icon': '🏅', 'title': 'Milestone',          'desc': 'Learn 1,000 words',           'threshold': 1000},
    {'cat': 'words',     'key': 'words_5k',      'icon': '🌟', 'title': 'Fluency in Sight',   'desc': 'Learn 5,000 words',           'threshold': 5000},
    {'cat': 'words',     'key': 'words_10k',     'icon': '🎖', 'title': 'Linguist',           'desc': 'Learn 10,000 words',          'threshold': 10000},
    # ── Day streaks ──
    {'cat': 'streak',    'key': 'streak_5',        'icon': '🔥', 'title': 'Warming Up',         'desc': '5-day study streak',                            'threshold': 5},
    {'cat': 'streak',    'key': 'streak_10',        'icon': '🔥', 'title': 'On Fire',            'desc': '10-day study streak',                           'threshold': 10},
    {'cat': 'streak',    'key': 'streak_50',        'icon': '🔥', 'title': 'Unstoppable',        'desc': '50-day study streak',                           'threshold': 50},
    {'cat': 'streak',    'key': 'streak_100',       'icon': '💥', 'title': 'Century Streak',     'desc': '100-day study streak',                          'threshold': 100},
    {'cat': 'streak',    'key': 'streak_365',       'icon': '🌈', 'title': 'Year of Study',      'desc': '365-day study streak',                          'threshold': 365},
    # ── Sessions & Consistency ──
    {'cat': 'sessions',  'key': 'session_10',       'icon': '📅', 'title': 'First Habit',        'desc': 'Complete 10 study sessions',                    'threshold': 10},
    {'cat': 'sessions',  'key': 'session_50',       'icon': '📅', 'title': 'Regular Student',    'desc': 'Complete 50 study sessions',                    'threshold': 50},
    {'cat': 'sessions',  'key': 'session_100',      'icon': '🏅', 'title': 'Centurion',          'desc': 'Complete 100 study sessions',                   'threshold': 100},
    {'cat': 'sessions',  'key': 'session_500',      'icon': '💫', 'title': 'Devoted Learner',    'desc': 'Complete 500 study sessions',                   'threshold': 500},
    {'cat': 'sessions',  'key': 'one_month',        'icon': '🗓️', 'title': 'One Month',          'desc': '30 different days with at least one activity',  'threshold': 30},
    {'cat': 'sessions',  'key': 'early_bird',       'icon': '🌅', 'title': 'Early Bird',         'desc': 'Study before 7 AM'},
    {'cat': 'sessions',  'key': 'night_owl',        'icon': '🌙', 'title': 'Night Owl',          'desc': 'Study after 11 PM'},
    {'cat': 'sessions',  'key': 'flawless',         'icon': '🎯', 'title': 'Flawless',           'desc': 'Complete a session with no wrong answers'},
    {'cat': 'sessions',  'key': 'marathon',         'icon': '💪', 'title': 'Marathon',           'desc': 'Review 100 or more cards in a single session'},
    {'cat': 'sessions',  'key': 'comeback',         'icon': '🏁', 'title': 'Comeback',           'desc': 'Resume studying after a 7-day break'},
    {'cat': 'sessions',  'key': 'weekend_warrior',  'icon': '🏖️', 'title': 'Weekend Warrior',    'desc': 'Study on both Saturday and Sunday of the same weekend'},
    {'cat': 'sessions',  'key': 'rainbow_week',     'icon': '🌈', 'title': 'Full Week',          'desc': 'Study every single day of a calendar week'},
    {'cat': 'sessions',  'key': 'true_beginner',    'icon': '🤔', 'title': 'True Beginner',      'desc': 'Get a wrong answer in your very first session'},
    # ── Map ──
    {'cat': 'map',       'key': 'map_circle_1',     'icon': '🗺️', 'title': 'First Circle',       'desc': 'Complete your first map circle'},
    {'cat': 'map',       'key': 'map_skip',         'icon': '⚡', 'title': 'Fast Lane',          'desc': 'Pass circle 20 to unlock all circles of a level at once'},
    {'cat': 'map',       'key': 'map_level_done',   'icon': '🏆', 'title': 'Level Master',       'desc': 'Complete all 20 circles of any level'},
    {'cat': 'map',       'key': 'map_classic',      'icon': '🎓', 'title': 'Classic HSK Complete','desc': 'Complete all Classic HSK levels on the map'},
    {'cat': 'map',       'key': 'map_hsk3',         'icon': '🎓', 'title': 'New HSK 3.0 Complete','desc': 'Complete all New HSK 3.0 levels on the map'},
    {'cat': 'map',       'key': 'map_jlpt',         'icon': '🗾', 'title': 'JLPT Complete',        'desc': 'Complete all JLPT levels on the map'},
    {'cat': 'map',       'key': 'explorer_10',      'icon': '🗺️', 'title': 'Explorer',           'desc': 'Pass 10 map circles',                           'threshold': 10},
    {'cat': 'map',       'key': 'explorer_25',      'icon': '🧭', 'title': 'Trailblazer',        'desc': 'Pass 25 map circles',                           'threshold': 25},
    {'cat': 'map',       'key': 'explorer_50',      'icon': '🌍', 'title': 'Globetrotter',       'desc': 'Pass 50 map circles',                           'threshold': 50},
    {'cat': 'map',       'key': 'explorer_100',     'icon': '🚀', 'title': 'Century Mapper',     'desc': 'Pass 100 map circles',                          'threshold': 100},
    {'cat': 'map',       'key': 'perfect_circle',   'icon': '💎', 'title': 'Perfect Circle',     'desc': 'Score 100% on any map circle'},
    # ── Games ──
    {'cat': 'games',     'key': 'all_games',        'icon': '🎮', 'title': 'Game On',            'desc': 'Play all 6 game types at least once'},
    {'cat': 'games',     'key': 'game_bingo',       'icon': '🔊', 'title': 'Bingo Pro',          'desc': 'Play Audio Bingo 10 times',                     'threshold': 10},
    {'cat': 'games',     'key': 'game_tone',        'icon': '🎵', 'title': 'Tone Master',        'desc': 'Play Tone ID 10 times',                         'threshold': 10},
    {'cat': 'games',     'key': 'game_match',       'icon': '🃏', 'title': 'Card Sharp',         'desc': 'Play Match Pairs 10 times',                     'threshold': 10},
    {'cat': 'games',     'key': 'game_mc',          'icon': '📝', 'title': 'The Scholar',        'desc': 'Play Multiple Choice 10 times',                 'threshold': 10},
    {'cat': 'games',     'key': 'game_draw',        'icon': '✍️', 'title': 'Calligrapher',       'desc': 'Play Draw Hanzi 10 times',                      'threshold': 10},
    {'cat': 'games',     'key': 'game_scram',       'icon': '🔀', 'title': 'Scramble King',      'desc': 'Play Scrambled 10 times',                       'threshold': 10},
    # ── Misc / event ──
    {'cat': 'misc',      'key': 'first_user',       'icon': '👋', 'title': 'Welcome!',           'desc': 'Create a user account'},
    {'cat': 'misc',      'key': 'custom_list',      'icon': '📝', 'title': 'Curator',            'desc': 'Create a custom list and add at least one word'},
    {'cat': 'misc',      'key': 'list_maker_3',     'icon': '📋', 'title': 'List Maker',         'desc': 'Create 3 custom lists'},
    {'cat': 'misc',      'key': 'big_deck',         'icon': '📦', 'title': 'Big Deck',           'desc': 'Have a custom list with 20+ words'},
    {'cat': 'misc',      'key': 'exported',         'icon': '💾', 'title': 'Backed Up',          'desc': 'Export your progress'},
    {'cat': 'misc',      'key': 'imported',         'icon': '📥', 'title': 'Fresh Start',        'desc': 'Import progress from a file'},
    {'cat': 'misc',      'key': 'anki_imported',    'icon': '📦', 'title': 'Deck Builder',       'desc': 'Import an Anki deck'},
    {'cat': 'misc',      'key': 'grammar_fav',      'icon': '📖', 'title': 'Grammar Nerd',       'desc': 'Add a grammar lesson to favourites'},
    {'cat': 'misc',      'key': 'grammar_fan',      'icon': '📖', 'title': 'Grammar Fan',        'desc': 'Add 5 grammar lessons to favourites',           'threshold': 5},
    {'cat': 'misc',      'key': 'grammar_master',   'icon': '📚', 'title': 'Grammar Master',     'desc': 'Add 10 grammar lessons to favourites',          'threshold': 10},
    # ── Level mastery — Classic HSK ──
    {'cat': 'mastery',   'key': 'known_classic_1','icon': '⭐', 'title': 'HSK 1 Complete',    'desc': 'Know all Classic HSK 1 words'},
    {'cat': 'mastery',   'key': 'known_classic_2','icon': '⭐', 'title': 'HSK 2 Complete',    'desc': 'Know all Classic HSK 2 words'},
    {'cat': 'mastery',   'key': 'known_classic_3','icon': '⭐', 'title': 'HSK 3 Complete',    'desc': 'Know all Classic HSK 3 words'},
    {'cat': 'mastery',   'key': 'known_classic_4','icon': '🌟', 'title': 'HSK 4 Complete',    'desc': 'Know all Classic HSK 4 words'},
    {'cat': 'mastery',   'key': 'known_classic_5','icon': '🌟', 'title': 'HSK 5 Complete',    'desc': 'Know all Classic HSK 5 words'},
    {'cat': 'mastery',   'key': 'known_classic_6','icon': '👑', 'title': 'HSK 6 Complete',    'desc': 'Know all Classic HSK 6 words'},
    # ── Level mastery — New HSK 3.0 ──
    {'cat': 'mastery',   'key': 'known_hsk3_1',  'icon': '⭐', 'title': 'New HSK 1 Complete', 'desc': 'Know all New HSK 3.0 Level 1 words'},
    {'cat': 'mastery',   'key': 'known_hsk3_2',  'icon': '⭐', 'title': 'New HSK 2 Complete', 'desc': 'Know all New HSK 3.0 Level 2 words'},
    {'cat': 'mastery',   'key': 'known_hsk3_3',  'icon': '⭐', 'title': 'New HSK 3 Complete', 'desc': 'Know all New HSK 3.0 Level 3 words'},
    {'cat': 'mastery',   'key': 'known_hsk3_4',  'icon': '🌟', 'title': 'New HSK 4 Complete', 'desc': 'Know all New HSK 3.0 Level 4 words'},
    {'cat': 'mastery',   'key': 'known_hsk3_5',  'icon': '🌟', 'title': 'New HSK 5 Complete', 'desc': 'Know all New HSK 3.0 Level 5 words'},
    {'cat': 'mastery',   'key': 'known_hsk3_6',  'icon': '🌟', 'title': 'New HSK 6 Complete', 'desc': 'Know all New HSK 3.0 Level 6 words'},
    {'cat': 'mastery',   'key': 'known_hsk3_7',  'icon': '👑', 'title': 'New HSK 7-9 Complete','desc': 'Know all New HSK 3.0 Level 7-9 words'},
    # ── Level mastery — JLPT ──
    {'cat': 'mastery',   'key': 'known_jlpt_1',  'icon': '⭐', 'title': 'N5 Complete',          'desc': 'Know all JLPT N5 words'},
    {'cat': 'mastery',   'key': 'known_jlpt_2',  'icon': '⭐', 'title': 'N4 Complete',          'desc': 'Know all JLPT N4 words'},
    {'cat': 'mastery',   'key': 'known_jlpt_3',  'icon': '🌟', 'title': 'N3 Complete',          'desc': 'Know all JLPT N3 words'},
    {'cat': 'mastery',   'key': 'known_jlpt_4',  'icon': '🌟', 'title': 'N2 Complete',          'desc': 'Know all JLPT N2 words'},
    {'cat': 'mastery',   'key': 'known_jlpt_5',  'icon': '👑', 'title': 'N1 Complete',          'desc': 'Know all JLPT N1 words'},
]

AWARD_KEYS = {a['key'] for a in AWARDS}
ALL_GAME_TYPES = {'bingo', 'tone', 'match', 'mc', 'draw', 'scram'}


def grant_award(conn, uid, key):
    if key not in AWARD_KEYS:
        return
    conn.execute(
        'INSERT OR IGNORE INTO user_awards (user_id, award_key, awarded_at) VALUES (?, ?, ?)',
        (uid, key, now_str())
    )


def check_lazy_awards(conn, uid):
    """Check all milestone awards and grant any not yet earned. Called on awards page load."""
    from datetime import date as date_cls, timedelta as _td

    # ── Reviews ──────────────────────────────────────────────────────────────
    total_reviews = conn.execute('''
        SELECT COALESCE(SUM(CAST(easy_count AS INTEGER) +
                            CAST(medium_count AS INTEGER) +
                            CAST(wrong_count  AS INTEGER)), 0)
        FROM session_logs WHERE user_id = ?
    ''', (uid,)).fetchone()[0]
    for threshold, key in [
        (50,'reviews_50'),(100,'reviews_100'),(500,'reviews_500'),
        (1000,'reviews_1k'),(5000,'reviews_5k'),(10000,'reviews_10k'),
        (50000,'reviews_50k'),(100000,'reviews_100k'),
    ]:
        if total_reviews >= threshold:
            grant_award(conn, uid, key)

    # ── Words known ───────────────────────────────────────────────────────────
    known = conn.execute('''
        SELECT COUNT(*) FROM (
            SELECT DISTINCT w.hanzi, w.pinyin
            FROM progress p JOIN words w ON w.id = p.word_id
            WHERE p.interval_step >= 6 AND p.user_id = ?
        )
    ''', (uid,)).fetchone()[0]
    for threshold, key in [
        (10,'words_10'),(100,'words_100'),(500,'words_500'),
        (1000,'words_1k'),(5000,'words_5k'),(10000,'words_10k'),
    ]:
        if known >= threshold:
            grant_award(conn, uid, key)

    # ── Streak ────────────────────────────────────────────────────────────────
    date_rows = conn.execute('''
        SELECT DISTINCT DATE(d) AS d FROM (
            SELECT started_at AS d FROM session_logs WHERE user_id = ?
            UNION
            SELECT passed_at  AS d FROM map_progress
            WHERE user_id = ? AND passed = 1 AND passed_at IS NOT NULL
        ) ORDER BY d DESC
    ''', (uid, uid)).fetchall()
    streak = 0
    if date_rows:
        most_recent = date_cls.fromisoformat(date_rows[0]['d'])
        today = date_cls.today()
        if (today - most_recent).days <= 1:
            for i, row in enumerate(date_rows):
                if date_cls.fromisoformat(row['d']) == most_recent - __import__('datetime').timedelta(days=i):
                    streak += 1
                else:
                    break
    for threshold, key in [
        (5,'streak_5'),(10,'streak_10'),(50,'streak_50'),
        (100,'streak_100'),(365,'streak_365'),
    ]:
        if streak >= threshold:
            grant_award(conn, uid, key)

    # ── Map: first circle ─────────────────────────────────────────────────────
    any_circle = conn.execute(
        'SELECT 1 FROM map_progress WHERE user_id = ? AND passed = 1 LIMIT 1', (uid,)
    ).fetchone()
    if any_circle:
        grant_award(conn, uid, 'map_circle_1')

    # ── Map: all 20 circles of any level ─────────────────────────────────────
    level_done = conn.execute('''
        SELECT curriculum, level FROM map_progress
        WHERE user_id = ? AND passed = 1
        GROUP BY curriculum, level
        HAVING COUNT(*) = 20
        LIMIT 1
    ''', (uid,)).fetchone()
    if level_done:
        grant_award(conn, uid, 'map_level_done')

    # ── Map: all classic HSK levels complete ──────────────────────────────────
    classic_levels = conn.execute(
        "SELECT DISTINCT hsk_level FROM words WHERE curriculum = 'classic' ORDER BY hsk_level"
    ).fetchall()
    if classic_levels:
        classic_done = all(
            conn.execute('''
                SELECT COUNT(*) FROM map_progress
                WHERE user_id=? AND curriculum='classic' AND level=? AND passed=1
            ''', (uid, r[0])).fetchone()[0] == 20
            for r in classic_levels
        )
        if classic_done:
            grant_award(conn, uid, 'map_classic')

    # ── Map: all HSK3.0 levels complete ──────────────────────────────────────
    hsk3_levels = conn.execute(
        "SELECT DISTINCT hsk_level FROM words WHERE curriculum = 'hsk3' ORDER BY hsk_level"
    ).fetchall()
    if hsk3_levels:
        hsk3_done = all(
            conn.execute('''
                SELECT COUNT(*) FROM map_progress
                WHERE user_id=? AND curriculum='hsk3' AND level=? AND passed=1
            ''', (uid, r[0])).fetchone()[0] == 20
            for r in hsk3_levels
        )
        if hsk3_done:
            grant_award(conn, uid, 'map_hsk3')

    # ── Map: all JLPT levels complete ────────────────────────────────────────
    jlpt_levels = conn.execute(
        "SELECT DISTINCT hsk_level FROM words WHERE curriculum = 'jlpt' ORDER BY hsk_level"
    ).fetchall()
    if jlpt_levels:
        jlpt_done = all(
            conn.execute('''
                SELECT COUNT(*) FROM map_progress
                WHERE user_id=? AND curriculum='jlpt' AND level=? AND passed=1
            ''', (uid, r[0])).fetchone()[0] == 20
            for r in jlpt_levels
        )
        if jlpt_done:
            grant_award(conn, uid, 'map_jlpt')

    # ── All games played ──────────────────────────────────────────────────────
    game_rows = conn.execute(
        'SELECT game_type, plays FROM game_plays WHERE user_id = ?', (uid,)
    ).fetchall()
    played = {r['game_type'] for r in game_rows}
    if played >= ALL_GAME_TYPES:
        grant_award(conn, uid, 'all_games')
    game_key_map = {'bingo': 'game_bingo', 'tone': 'game_tone', 'match': 'game_match',
                    'mc': 'game_mc', 'draw': 'game_draw', 'scram': 'game_scram'}
    for r in game_rows:
        if r['plays'] >= 10 and r['game_type'] in game_key_map:
            grant_award(conn, uid, game_key_map[r['game_type']])

    # ── Level mastery ─────────────────────────────────────────────────────────
    mastery_map = {
        'classic': [(1,'known_classic_1'),(2,'known_classic_2'),(3,'known_classic_3'),
                    (4,'known_classic_4'),(5,'known_classic_5'),(6,'known_classic_6')],
        'hsk3':    [(1,'known_hsk3_1'),(2,'known_hsk3_2'),(3,'known_hsk3_3'),
                    (4,'known_hsk3_4'),(5,'known_hsk3_5'),(6,'known_hsk3_6'),(7,'known_hsk3_7')],
        'jlpt':    [(1,'known_jlpt_1'),(2,'known_jlpt_2'),(3,'known_jlpt_3'),
                    (4,'known_jlpt_4'),(5,'known_jlpt_5')],
    }
    for curr, levels in mastery_map.items():
        for lvl, key in levels:
            total_in_level = conn.execute(
                'SELECT COUNT(*) FROM words WHERE curriculum=? AND hsk_level=?', (curr, lvl)
            ).fetchone()[0]
            if total_in_level == 0:
                continue
            known_in_level = conn.execute('''
                SELECT COUNT(DISTINCT w.hanzi || '|' || w.pinyin)
                FROM progress p JOIN words w ON w.id = p.word_id
                WHERE p.user_id=? AND w.curriculum=? AND w.hsk_level=? AND p.interval_step >= 6
            ''', (uid, curr, lvl)).fetchone()[0]
            if known_in_level >= total_in_level:
                grant_award(conn, uid, key)

    # ── Sessions milestones ───────────────────────────────────────────────────
    total_sessions = conn.execute(
        'SELECT COUNT(*) FROM session_logs WHERE user_id = ?', (uid,)
    ).fetchone()[0]
    for threshold, key in [(10,'session_10'),(50,'session_50'),(100,'session_100'),(500,'session_500')]:
        if total_sessions >= threshold:
            grant_award(conn, uid, key)

    # ── Time-of-day sessions ─────────────────────────────────────────────────
    if conn.execute(
        "SELECT 1 FROM session_logs WHERE user_id=? AND CAST(strftime('%H',started_at) AS INTEGER) < 7 LIMIT 1",
        (uid,)
    ).fetchone():
        grant_award(conn, uid, 'early_bird')
    if conn.execute(
        "SELECT 1 FROM session_logs WHERE user_id=? AND CAST(strftime('%H',started_at) AS INTEGER) >= 23 LIMIT 1",
        (uid,)
    ).fetchone():
        grant_award(conn, uid, 'night_owl')

    # ── Flawless session ─────────────────────────────────────────────────────
    if conn.execute(
        '''SELECT 1 FROM session_logs WHERE user_id=?
           AND CAST(wrong_count AS INTEGER) = 0
           AND (CAST(easy_count AS INTEGER) + CAST(medium_count AS INTEGER)) > 0
           LIMIT 1''',
        (uid,)
    ).fetchone():
        grant_award(conn, uid, 'flawless')

    # ── Marathon session ─────────────────────────────────────────────────────
    if conn.execute(
        '''SELECT 1 FROM session_logs WHERE user_id=?
           AND (CAST(easy_count AS INTEGER) + CAST(medium_count AS INTEGER) +
                CAST(wrong_count AS INTEGER)) >= 100
           LIMIT 1''',
        (uid,)
    ).fetchone():
        grant_award(conn, uid, 'marathon')

    # ── True Beginner (wrong answer in first session) ─────────────────────────
    first_sess = conn.execute(
        'SELECT wrong_count FROM session_logs WHERE user_id=? ORDER BY started_at ASC LIMIT 1',
        (uid,)
    ).fetchone()
    if first_sess and int(first_sess['wrong_count'] or 0) > 0:
        grant_award(conn, uid, 'true_beginner')

    # ── All activity dates (sessions + map passes) ────────────────────────────
    all_date_rows = conn.execute('''
        SELECT DISTINCT DATE(d) AS d FROM (
            SELECT started_at AS d FROM session_logs WHERE user_id = ?
            UNION
            SELECT passed_at AS d FROM map_progress
            WHERE user_id = ? AND passed = 1 AND passed_at IS NOT NULL
        ) ORDER BY d
    ''', (uid, uid)).fetchall()
    study_day_set = {date_cls.fromisoformat(r['d']) for r in all_date_rows}
    dates_asc = [date_cls.fromisoformat(r['d']) for r in all_date_rows]

    # ── One Month (30 distinct activity days) ─────────────────────────────────
    if len(dates_asc) >= 30:
        grant_award(conn, uid, 'one_month')

    # ── Comeback (7+ day gap then resumed) ───────────────────────────────────
    for i in range(1, len(dates_asc)):
        if (dates_asc[i] - dates_asc[i - 1]).days >= 7:
            grant_award(conn, uid, 'comeback')
            break

    # ── Weekend Warrior (Saturday + Sunday same weekend) ──────────────────────
    for d in study_day_set:
        if d.weekday() == 5 and (d + _td(days=1)) in study_day_set:
            grant_award(conn, uid, 'weekend_warrior')
            break

    # ── Full Week (all 7 days of any Mon–Sun week) ────────────────────────────
    for d in study_day_set:
        week_start = d - _td(days=d.weekday())
        if all(week_start + _td(days=i) in study_day_set for i in range(7)):
            grant_award(conn, uid, 'rainbow_week')
            break

    # ── Map: explorer circle counts ───────────────────────────────────────────
    total_circles = conn.execute(
        'SELECT COUNT(*) FROM map_progress WHERE user_id=? AND passed=1', (uid,)
    ).fetchone()[0]
    for threshold, key in [(10,'explorer_10'),(25,'explorer_25'),(50,'explorer_50'),(100,'explorer_100')]:
        if total_circles >= threshold:
            grant_award(conn, uid, key)

    # ── Map: perfect circle (100% score) ─────────────────────────────────────
    if conn.execute(
        'SELECT 1 FROM map_progress WHERE user_id=? AND best_score >= 1.0 AND passed=1 LIMIT 1',
        (uid,)
    ).fetchone():
        grant_award(conn, uid, 'perfect_circle')

    # ── Grammar favourites ────────────────────────────────────────────────────
    try:
        grammar_count = conn.execute(
            'SELECT COUNT(*) FROM grammar_favorites WHERE user_id=?', (uid,)
        ).fetchone()[0]
        for threshold, key in [(5,'grammar_fan'),(10,'grammar_master')]:
            if grammar_count >= threshold:
                grant_award(conn, uid, key)
    except Exception:
        pass

    # ── Custom lists ──────────────────────────────────────────────────────────
    list_count = conn.execute(
        'SELECT COUNT(*) FROM custom_lists WHERE user_id=?', (uid,)
    ).fetchone()[0]
    if conn.execute('''
        SELECT 1 FROM custom_list_words clw
        JOIN custom_lists cl ON cl.id = clw.list_id
        WHERE cl.user_id = ? LIMIT 1
    ''', (uid,)).fetchone():
        grant_award(conn, uid, 'custom_list')
    if list_count >= 3:
        grant_award(conn, uid, 'list_maker_3')
    max_list_words = conn.execute('''
        SELECT COALESCE(MAX(cnt), 0) FROM (
            SELECT COUNT(*) AS cnt
            FROM custom_list_words clw
            JOIN custom_lists cl ON cl.id = clw.list_id
            WHERE cl.user_id = ?
            GROUP BY clw.list_id
        )
    ''', (uid,)).fetchone()[0]
    if max_list_words >= 20:
        grant_award(conn, uid, 'big_deck')


_CURR_LABELS = {'classic': 'Classic HSK', 'hsk3': 'New HSK 3.0'}

def get_hsk_labels(conn, hanzi: str) -> list:
    """Return [{curriculum, label, level}] for every curriculum this hanzi appears in."""
    rows = conn.execute(
        """SELECT DISTINCT curriculum, hsk_level FROM words
           WHERE hanzi = ? AND curriculum IN ('classic','hsk3')
           ORDER BY curriculum DESC, hsk_level""",
        (hanzi,)
    ).fetchall()
    result = []
    for r in rows:
        lvl = '7-9' if (r['curriculum'] == 'hsk3' and r['hsk_level'] == 7) else str(r['hsk_level'])
        result.append({'curriculum': r['curriculum'],
                       'label': _CURR_LABELS.get(r['curriculum'], r['curriculum']),
                       'level': lvl})
    return result


_CJK_RE     = re.compile(r'[一-鿿]')
_KANA_RE    = re.compile(r'[぀-ヿ]')   # hiragana + katakana
_vocab_set  = set()   # populated by _init_jieba()
_max_word   = 1

def _init_jieba():
    """Load all vocab words into jieba and build _vocab_set for post-processing."""
    global _vocab_set, _max_word
    with get_db() as conn:
        rows = conn.execute('SELECT DISTINCT hanzi FROM words WHERE hanzi IS NOT NULL').fetchall()
    _vocab_set = {r['hanzi'] for r in rows}
    _max_word  = max((len(w) for w in _vocab_set), default=1)
    if not _JIEBA_AVAILABLE:
        return
    for w in _vocab_set:
        jieba.add_word(w)
    jieba.initialize()

def _resplit(seg: str) -> list[str]:
    """Greedily split a segment into known vocab words (longest-match first)."""
    result, i = [], 0
    while i < len(seg):
        for length in range(min(_max_word, len(seg) - i), 0, -1):
            if seg[i:i+length] in _vocab_set:
                result.append(seg[i:i+length])
                i += length
                break
        else:
            result.append(seg[i])
            i += 1
    return result

def segment_hanzi(text: str) -> list[str]:
    """Segment a Chinese sentence into word-level tokens, dropping punctuation/spaces.

    Jieba occasionally merges adjacent vocab words (e.g. 今天+天气 → 今天天气).
    We post-process any segment not in our vocab: if the resplit produces at least one
    multi-char word we use it; otherwise we trust jieba's original (e.g. 每天, 喝水).
    """
    if not text:
        return []
    if not _JIEBA_AVAILABLE:
        cjk = ''.join(c for c in text if _CJK_RE.match(c))
        return _resplit(cjk)
    result = []
    for seg in jieba.cut(text):
        if not seg or not all(_CJK_RE.match(c) for c in seg):
            continue
        if seg not in _vocab_set:
            parts = _resplit(seg)
            result.extend(parts if any(len(p) > 1 for p in parts) else [seg])
        else:
            result.append(seg)
    return result if result else [c for c in text if _CJK_RE.match(c)]

def segment_japanese(text: str) -> list[str]:
    """Segment a Japanese sentence into alternating kanji-run / kana-run tokens.

    Keeps consecutive kanji together (e.g. 友達) and consecutive kana together
    (e.g. いました), dropping punctuation/spaces.  Results are used as scramble
    tiles so the player reassembles the sentence word-by-word.
    """
    if not text:
        return []
    tokens: list[str] = []
    current: list[str] = []
    cur_type: str | None = None
    for c in text:
        if _CJK_RE.match(c):
            t = 'cjk'
        elif _KANA_RE.match(c):
            t = 'kana'
        else:
            t = None
        if t is None:
            if current:
                tokens.append(''.join(current))
                current = []
                cur_type = None
        elif t == cur_type:
            current.append(c)
        else:
            if current:
                tokens.append(''.join(current))
            current = [c]
            cur_type = t
    if current:
        tokens.append(''.join(current))
    if len(tokens) > 1:
        return tokens
    # Fallback: every Japanese character as its own tile
    return [c for c in text if _CJK_RE.match(c) or _KANA_RE.match(c)]


def make_cloze_prompt(hanzi: str, example: str) -> str:
    if not example or not hanzi or hanzi not in example:
        return ''
    # Find the jieba segment that contains the vocab word; blank the whole segment
    # so we never produce a mid-word blank like ＿习 when vocab is 学 inside 学习.
    for seg in segment_hanzi(example):
        if hanzi in seg:
            return example.replace(seg, '＿' * len(seg), 1)
    return example.replace(hanzi, '＿' * len(hanzi), 1)

# ── Session queue (stored in DB, keyed by cookie sid) ─────────────────────────

def _shuffle_queue(queue_map):
    """Build a queue from {wid: status}, shuffled within each priority block."""
    groups = {}
    for wid, status in queue_map.items():
        groups.setdefault(status, []).append(wid)
    queue = []
    for status in ['overdue', 'new', 'wrong', 'medium', 'easy']:
        wids = groups.get(status, [])
        random.shuffle(wids)
        queue.extend({'wid': wid, 'status': status, 'due_at': None} for wid in wids)
    return queue

def get_sid():
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())
    return session['sid']

def get_user_id():
    if 'validated_user_id' in g:
        return g.validated_user_id
    uid = session.get('user_id')
    if uid is not None:
        with get_db() as conn:
            if not conn.execute('SELECT id FROM users WHERE id = ?', (uid,)).fetchone():
                session.pop('user_id', None)
                uid = None
    g.validated_user_id = uid
    return uid

def load_queue(sid):
    with get_db() as conn:
        row = conn.execute('SELECT queue, mode FROM study_sessions WHERE id = ?', (sid,)).fetchone()
        if row:
            return json.loads(row['queue']), row['mode']
    return [], 'mix'

def save_queue(sid, queue, mode=None):
    with get_db() as conn:
        existing = conn.execute('SELECT id FROM study_sessions WHERE id = ?', (sid,)).fetchone()
        if existing:
            if mode:
                conn.execute('UPDATE study_sessions SET queue = ?, mode = ? WHERE id = ?',
                             (json.dumps(queue), mode, sid))
            else:
                conn.execute('UPDATE study_sessions SET queue = ? WHERE id = ?',
                             (json.dumps(queue), sid))
        else:
            conn.execute(
                'INSERT INTO study_sessions (id, mode, created_at, queue, user_id) VALUES (?, ?, ?, ?, ?)',
                (sid, mode or 'mix', now_str(), json.dumps(queue), get_user_id())
            )

def load_history(sid):
    with get_db() as conn:
        row = conn.execute('SELECT history FROM study_sessions WHERE id = ?', (sid,)).fetchone()
        if row and row['history']:
            return json.loads(row['history'])
    return []

def save_history(sid, history):
    with get_db() as conn:
        conn.execute('UPDATE study_sessions SET history = ? WHERE id = ?',
                     (json.dumps(history), sid))

# ── Sidebar context processor ─────────────────────────────────────────────────

@app.context_processor
def inject_sidebar():
    uid = get_user_id()
    now = now_str()
    current_user_name = None
    try:
        with get_db() as conn:
            if uid:
                row = conn.execute('SELECT name FROM users WHERE id = ?', (uid,)).fetchone()
                current_user_name = row['name'] if row else None
                due_total = conn.execute(
                    "SELECT COUNT(*) FROM progress WHERE due_at <= ? AND user_id = ?", (now, uid)
                ).fetchone()[0]
                valid_cur = get_curricula()
                ph_cur = ','.join('?' * len(valid_cur))
                new_total = conn.execute(
                    f"""SELECT COUNT(*) FROM words w
                       LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                       WHERE p.word_id IS NULL AND w.curriculum IN ({ph_cur})""",
                    (uid, *valid_cur)
                ).fetchone()[0]
            else:
                due_total = 0
                new_total = 0
        return dict(sidebar_due=due_total, sidebar_new=new_total, current_user=current_user_name, current_language=get_language())
    except Exception:
        return dict(sidebar_due=0, sidebar_new=0, current_user=None, current_language='chinese')

# ── Language helpers ──────────────────────────────────────────────────────────

def get_language():
    return session.get('language', 'chinese')

def get_curricula():
    if get_language() == 'japanese':
        return ('jlpt',)
    return ('classic', 'hsk3')

def level_label(level: int) -> str:
    """Return display label for an hsk_level integer given the active language."""
    if get_language() == 'japanese':
        return f'N{6 - level}'  # stored 1→N5, 2→N4, 3→N3, 4→N2, 5→N1
    return str(level)

app.jinja_env.globals['level_label'] = level_label
app.jinja_env.globals['get_language'] = get_language

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/set-language/<lang>')
def set_language(lang):
    if lang in ('chinese', 'japanese'):
        session['language'] = lang
    raw_next = request.args.get('next', '/')
    next_url = raw_next if (raw_next.startswith('/') and not raw_next.startswith('//')) else '/'
    return redirect(next_url)


@app.route('/')
def index():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    lang = get_language()
    valid_curricula = get_curricula()
    default_curriculum = valid_curricula[0]
    curriculum = request.args.get('curriculum', default_curriculum)
    if curriculum not in valid_curricula and curriculum != 'custom':
        curriculum = default_curriculum
    now = now_str()
    with get_db() as conn:
        # Build curricula tab list — only those relevant to the active language
        curricula = [c for c in valid_curricula if conn.execute(
            'SELECT 1 FROM words WHERE curriculum=? LIMIT 1', (c,)
        ).fetchone()]
        if conn.execute(
            "SELECT COUNT(*) FROM custom_lists WHERE user_id=? AND language=?", (uid, lang)
        ).fetchone()[0] > 0:
            curricula.append('custom')

        if curriculum == 'custom':
            cl_rows = conn.execute('''
                SELECT cl.id, cl.name,
                       COUNT(DISTINCT clw.word_id) AS total,
                       SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END) AS new_cnt,
                       SUM(CASE WHEN p.due_at <= ? AND p.word_id IS NOT NULL
                                THEN 1 ELSE 0 END) AS due_cnt
                FROM custom_lists cl
                LEFT JOIN custom_list_words clw ON cl.id = clw.list_id
                LEFT JOIN progress p ON clw.word_id = p.word_id AND p.user_id = ?
                WHERE cl.user_id = ? AND cl.language = ?
                GROUP BY cl.id
                ORDER BY cl.name
            ''', (now, uid, uid, lang)).fetchall()
            custom_lists = [dict(r) for r in cl_rows]
            total   = sum(cl['total']   for cl in custom_lists)
            due_cnt = sum(cl['due_cnt'] for cl in custom_lists)
            new_cnt = sum(cl['new_cnt'] for cl in custom_lists)
            hsk_stats = {}
        else:
            custom_lists = []
            total   = conn.execute(
                'SELECT COUNT(*) FROM words WHERE curriculum = ?', (curriculum,)
            ).fetchone()[0]
            new_cnt = conn.execute('''
                SELECT COUNT(*) FROM words w
                LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE p.word_id IS NULL AND w.curriculum = ?
            ''', (uid, curriculum)).fetchone()[0]
            due_cnt = conn.execute('''
                SELECT COUNT(*) FROM progress p
                JOIN words w ON w.id = p.word_id
                WHERE p.due_at <= ? AND p.user_id = ? AND w.curriculum = ?
            ''', (now, uid, curriculum)).fetchone()[0]
            hsk_rows = conn.execute('''
                SELECT
                    w.hsk_level,
                    COUNT(*) AS total,
                    SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END) AS new_cnt,
                    SUM(CASE WHEN p.due_at <= ? AND p.word_id IS NOT NULL
                             THEN 1 ELSE 0 END) AS due_cnt
                FROM words w
                LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE w.curriculum = ?
                GROUP BY w.hsk_level
                ORDER BY w.hsk_level
            ''', (now, uid, curriculum)).fetchall()
            hsk_stats = {row['hsk_level']: dict(row) for row in hsk_rows}

    return render_template('index.html', total=total, new_cnt=new_cnt, due_cnt=due_cnt,
                           hsk_stats=hsk_stats, custom_lists=custom_lists,
                           curriculum=curriculum, curricula=curricula, lang=lang)


@app.route('/start', methods=['POST'])
def start():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    mode            = request.form.get('mode', 'mix')
    limit           = int(request.form.get('limit', 20))
    hsk_levels      = [int(x) for x in request.form.getlist('hsk') if x.isdigit()]
    curriculum      = request.form.get('curriculum', 'classic')
    grade_filter    = set(request.form.getlist('grades'))   # e.g. {'new','wrong'}
    include_overdue = request.form.get('include_overdue') == '1'
    list_ids        = [int(x) for x in request.form.getlist('list_id') if x.isdigit()]
    sid             = get_sid()

    now = now_str()

    if curriculum == 'custom':
        if list_ids:
            ph          = ','.join('?' * len(list_ids))
            filter_frag = f'AND w.id IN (SELECT word_id FROM custom_list_words WHERE list_id IN ({ph}))'
            filters     = list_ids
        else:
            filter_frag = f'AND w.id IN (SELECT word_id FROM custom_list_words clw JOIN custom_lists cl ON cl.id = clw.list_id WHERE cl.user_id = {uid})'
            filters     = []
    else:
        filters     = [curriculum]
        filter_frag = 'AND w.curriculum = ?'
        if hsk_levels:
            ph           = ','.join('?' * len(hsk_levels))
            filter_frag += f' AND w.hsk_level IN ({ph})'
            filters     += hsk_levels

    # No grade checked → SRS default: overdue + new up to limit (original behaviour)
    default_mode = not grade_filter

    queue_map = {}   # wid -> status; first write wins (overdue beats grade)

    with get_db() as conn:
        # ── 1. Overdue words (all of them, oldest first) ─────────────────────
        if default_mode or include_overdue:
            rows = conn.execute(f'''
                SELECT w.id FROM words w
                JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE p.due_at <= ? {filter_frag}
                ORDER BY p.due_at ASC
            ''', [uid, now] + filters).fetchall()
            for r in rows:
                queue_map[r['id']] = 'overdue'

        # ── 2. New words (always add the full requested limit on top) ─────────
        if default_mode or 'new' in grade_filter:
            new_limit = limit
            rows = conn.execute(f'''
                SELECT w.id FROM words w
                LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE p.word_id IS NULL {filter_frag}
                ORDER BY RANDOM()
                LIMIT ?
            ''', [uid] + filters + [new_limit]).fetchall()
            for r in rows:
                if r['id'] not in queue_map:
                    queue_map[r['id']] = 'new'

        # ── 3. Seen-grade words (explicit filter only, no limit) ──────────────
        seen_grades = grade_filter - {'new'}
        if seen_grades:
            ph = ','.join('?' * len(seen_grades))
            rows = conn.execute(f'''
                SELECT w.id, p.last_grade FROM words w
                JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE p.last_grade IN ({ph}) {filter_frag}
            ''', [uid] + list(seen_grades) + filters).fetchall()
            for r in rows:
                if r['id'] not in queue_map:
                    queue_map[r['id']] = r['last_grade']

    queue = _shuffle_queue(queue_map)
    save_queue(sid, queue, mode)

    if curriculum == 'custom':
        if list_ids:
            with get_db() as conn:
                ph    = ','.join('?' * len(list_ids))
                names = [r['name'] for r in conn.execute(
                    f'SELECT name FROM custom_lists WHERE id IN ({ph}) ORDER BY name',
                    list_ids
                ).fetchall()]
        else:
            names = []
        selection = json.dumps({
            'type':       'custom_list',
            'list_ids':   list_ids,
            'list_names': names,
        })
    else:
        selection = json.dumps({
            'curriculum':      curriculum,
            'hsk_levels':      hsk_levels,
            'grades':          sorted(grade_filter),
            'include_overdue': include_overdue or default_mode,
        })
    with get_db() as conn:
        conn.execute(
            'UPDATE study_sessions SET stack_size = ?, selection = ? WHERE id = ?',
            (len(queue), selection, sid)
        )

    session['seen'] = list(queue_map.keys())
    return redirect(url_for('study'))


@app.route('/study')
def study():
    if 'sid' not in session:
        return redirect(url_for('index'))
    return render_template('session.html')


@app.route('/start-list/<int:list_id>')
def start_list(list_id):
    """Start a fresh SRS session for a custom list (used by sidebar mid-session switch)."""
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    sid = get_sid()
    now = now_str()
    with get_db() as conn:
        overdue = conn.execute('''
            SELECT w.id FROM words w
            JOIN progress p ON w.id = p.word_id AND p.user_id = ?
            JOIN custom_list_words clw ON w.id = clw.word_id
            WHERE clw.list_id = ? AND p.due_at <= ?
            ORDER BY p.due_at ASC
        ''', (uid, list_id, now)).fetchall()
        new_words = conn.execute('''
            SELECT w.id FROM words w
            JOIN custom_list_words clw ON w.id = clw.word_id
            LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
            WHERE clw.list_id = ? AND p.word_id IS NULL
            ORDER BY RANDOM()
            LIMIT ?
        ''', (uid, list_id, max(0, 20 - len(overdue)))).fetchall()
        lst = conn.execute('SELECT name FROM custom_lists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone()

    queue_map = {r['id']: 'overdue' for r in overdue}
    for r in new_words:
        if r['id'] not in queue_map:
            queue_map[r['id']] = 'new'

    queue = _shuffle_queue(queue_map)
    save_queue(sid, queue, 'mix')

    selection = json.dumps({
        'type':      'custom_list',
        'list_id':   list_id,
        'list_name': lst['name'] if lst else 'Custom list',
    })
    with get_db() as conn:
        conn.execute(
            'UPDATE study_sessions SET stack_size = ?, selection = ? WHERE id = ?',
            (len(queue), selection, sid)
        )
    session['seen'] = list(queue_map.keys())
    return redirect(url_for('study'))


@app.route('/api/next')
def api_next():
    sid        = get_sid()
    queue, mode = load_queue(sid)
    now        = datetime.now()

    if not queue:
        with get_db() as conn:
            row = conn.execute('SELECT selection FROM study_sessions WHERE id = ?', (sid,)).fetchone()
        sel = json.loads(row['selection']) if row and row['selection'] else {}
        return jsonify({'done': True, 'selection': sel})

    # Split into ready (due now) and waiting (future due_at)
    ready   = [c for c in queue if c['due_at'] is None or
               datetime.fromisoformat(c['due_at']) <= now]
    waiting = [c for c in queue if c not in ready]

    if not ready:
        waiting.sort(key=lambda c: c['due_at'])
        secs = (datetime.fromisoformat(waiting[0]['due_at']) - now).total_seconds()
        save_queue(sid, queue)
        return jsonify({'waiting': True, 'seconds': max(1, int(secs))})

    # Cards with an expired due_at (graded mid-session and now due again) get
    # overdue priority (0) so they surface before remaining new words.
    _pri = {'overdue': 0, 'new': 1, 'wrong': 2, 'medium': 3, 'easy': 4}
    def _card_pri(c):
        if c['due_at'] is not None:   # timer already expired (that's why it's in ready)
            return 0
        return _pri.get(c['status'], 5)
    ready.sort(key=lambda c: (_card_pri(c), random.random()))
    card = ready[0]
    queue.remove(card)

    save_queue(sid, queue)
    session['current_card'] = {'wid': card['wid'], 'status': card['status']}
    session.modified = True

    uid = get_user_id()
    with get_db() as conn:
        word = conn.execute('''
            SELECT w.*, p.interval_step, p.last_grade, p.last_seen
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
            WHERE w.id = ?
        ''', (uid, card['wid'])).fetchone()
        ex = conn.execute('''
            SELECT COALESCE(we.example_hanzi,   w.example_hanzi)   AS example_hanzi,
                   COALESCE(we.example_pinyin,  w.example_pinyin)  AS example_pinyin,
                   COALESCE(we.example_english, w.example_english) AS example_english
            FROM words w
            LEFT JOIN word_examples we ON we.id = (
                SELECT id FROM word_examples WHERE word_id = w.id ORDER BY RANDOM() LIMIT 1
            )
            WHERE w.id = ?
        ''', (card['wid'],)).fetchone()
        hsk_labels = get_hsk_labels(conn, word['hanzi']) if word else []

    if not word:
        return api_next()

    step = word['interval_step'] if word['interval_step'] is not None else -1
    wl, ml, el = grade_labels(step)

    ex_hanzi   = (ex['example_hanzi']   if ex else None) or ''
    ex_pinyin  = (ex['example_pinyin']  if ex else None) or ''
    ex_english = (ex['example_english'] if ex else None) or ''

    return jsonify({
        'word_id':          word['id'],
        'hanzi':            word['hanzi'],
        'pinyin':           word['pinyin'],
        'english':          word['english'],
        'example_hanzi':    ex_hanzi,
        'example_pinyin':   ex_pinyin,
        'example_english':  ex_english,
        'cloze_prompt':     make_cloze_prompt(word['hanzi'] or '', ex_hanzi),
        'audio_file':       word['audio_file'],
        'hsk_level':        word['hsk_level'],
        'hsk_labels':       hsk_labels,
        'interval_step':    step,
        'last_grade':       word['last_grade'],
        'status':           card['status'],
        'mode':             mode,
        'remaining':        len(ready) - 1,
        'wrong_label':      wl,
        'medium_label':     ml,
        'easy_label':       el,
        'can_go_back':      bool(load_history(sid)),
    })


@app.route('/api/grade', methods=['POST'])
def api_grade():
    uid     = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'no user selected'}), 401
    data    = request.json
    word_id = data['word_id']
    grade   = data['grade']   # 'wrong' | 'medium' | 'easy'
    sid       = get_sid()
    queue, _  = load_queue(sid)

    with get_db() as conn:
        prog = conn.execute(
            'SELECT interval_step, last_grade, due_at FROM progress WHERE word_id = ? AND user_id = ?',
            (word_id, uid)
        ).fetchone()

        step     = prog['interval_step'] if prog else -1
        new_step = next_step(step, grade)
        db_due   = future_str(INTERVALS[new_step])

        conn.execute('''
            INSERT INTO progress (user_id, word_id, interval_step, last_grade, last_seen, due_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, word_id) DO UPDATE SET
                interval_step = excluded.interval_step,
                last_grade    = excluded.last_grade,
                last_seen     = excluded.last_seen,
                due_at        = excluded.due_at
        ''', (uid, word_id, new_step, grade, now_str(), db_due))

        grade_col = {'wrong': 'wrong_count', 'medium': 'medium_count', 'easy': 'easy_count'}[grade]
        conn.execute(f'UPDATE study_sessions SET {grade_col} = {grade_col} + 1 WHERE id = ?', (sid,))
        if prog is None:
            conn.execute('UPDATE study_sessions SET new_words = new_words + 1 WHERE id = ?', (sid,))

        # Cross-curriculum sync: same hanzi in other curricula gets the same progress
        mirrors = conn.execute('''
            SELECT w2.id FROM words w1
            JOIN words w2 ON w2.hanzi = w1.hanzi AND w2.id != w1.id
            WHERE w1.id = ?
        ''', (word_id,)).fetchall()
        for m in mirrors:
            conn.execute('''
                INSERT INTO progress (user_id, word_id, interval_step, last_grade, last_seen, due_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, word_id) DO UPDATE SET
                    interval_step = excluded.interval_step,
                    last_grade    = excluded.last_grade,
                    last_seen     = excluded.last_seen,
                    due_at        = excluded.due_at
            ''', (uid, m['id'], new_step, grade, now_str(), db_due))

    # History uses a separate connection — keep outside the block above
    history = load_history(sid)
    history.append({
        'wid':         word_id,
        'status':      (session.get('current_card') or {}).get('status', 'unknown'),
        'prev_step':   prog['interval_step'] if prog else None,
        'prev_grade':  prog['last_grade']    if prog else None,
        'prev_due_at': prog['due_at']        if prog else None,
    })
    save_history(sid, history)

    queue.append({'wid': word_id, 'status': grade, 'due_at': future_str(INTERVALS[new_step])})
    save_queue(sid, queue)
    session['current_card'] = None
    session.modified = True

    return jsonify({'ok': True, 'new_step': new_step, 'next_interval': INTERVALS[new_step]})


@app.route('/api/undo', methods=['POST'])
def api_undo():
    sid     = get_sid()
    history = load_history(sid)
    if not history:
        return jsonify({'ok': False, 'error': 'nothing to undo'})

    entry   = history.pop()          # pop the most recent graded card
    save_history(sid, history)
    queue, mode = load_queue(sid)
    current = session.get('current_card')

    # Put the card currently on screen back at the front of the queue
    if current:
        queue.insert(0, {'wid': current['wid'], 'status': current['status'], 'due_at': None})

    # Remove the re-queued entry api/grade added for the previous card
    queue = [c for c in queue if not (c['wid'] == entry['wid'] and c['due_at'] is not None)]

    # Restore progress to the pre-grade state
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'no user selected'}), 401
    with get_db() as conn:
        if entry['prev_step'] is None:
            conn.execute('DELETE FROM progress WHERE word_id = ? AND user_id = ?', (entry['wid'], uid))
        else:
            conn.execute('''
                UPDATE progress
                SET interval_step = ?, last_grade = ?, due_at = ?
                WHERE word_id = ? AND user_id = ?
            ''', (entry['prev_step'], entry['prev_grade'], entry['prev_due_at'], entry['wid'], uid))

        word = conn.execute('''
            SELECT w.*, p.interval_step, p.last_grade, p.last_seen
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
            WHERE w.id = ?
        ''', (uid, entry['wid'])).fetchone()
        ex = conn.execute('''
            SELECT COALESCE(we.example_hanzi,   w.example_hanzi)   AS example_hanzi,
                   COALESCE(we.example_pinyin,  w.example_pinyin)  AS example_pinyin,
                   COALESCE(we.example_english, w.example_english) AS example_english
            FROM words w
            LEFT JOIN word_examples we ON we.id = (
                SELECT id FROM word_examples WHERE word_id = w.id ORDER BY RANDOM() LIMIT 1
            )
            WHERE w.id = ?
        ''', (entry['wid'],)).fetchone()
        hsk_labels = get_hsk_labels(conn, word['hanzi']) if word else []

    save_queue(sid, queue)

    if not word:
        return jsonify({'ok': False, 'error': 'word not found'})

    step = word['interval_step'] if word['interval_step'] is not None else -1
    wl, ml, el = grade_labels(step)

    ex_hanzi   = (ex['example_hanzi']   if ex else None) or ''
    ex_pinyin  = (ex['example_pinyin']  if ex else None) or ''
    ex_english = (ex['example_english'] if ex else None) or ''

    session['current_card'] = {'wid': entry['wid'], 'status': entry['status']}
    session.modified = True

    return jsonify({
        'ok':               True,
        'word_id':          word['id'],
        'hanzi':            word['hanzi'],
        'pinyin':           word['pinyin'],
        'english':          word['english'],
        'example_hanzi':    ex_hanzi,
        'example_pinyin':   ex_pinyin,
        'example_english':  ex_english,
        'cloze_prompt':     make_cloze_prompt(word['hanzi'] or '', ex_hanzi),
        'audio_file':       word['audio_file'],
        'hsk_level':        word['hsk_level'],
        'hsk_labels':       hsk_labels,
        'interval_step':    step,
        'last_grade':       word['last_grade'],
        'status':           entry['status'],
        'mode':             mode,
        'remaining':        sum(1 for c in queue if c['due_at'] is None or datetime.fromisoformat(c['due_at']) <= datetime.now()) - 1,
        'wrong_label':      wl,
        'medium_label':     ml,
        'easy_label':       el,
        'can_go_back':      bool(history),  # history already has the entry popped
    })


@app.route('/debug/queue')
def debug_queue():
    sid        = get_sid()
    queue, mode = load_queue(sid)
    now        = datetime.now()

    if not queue:
        return render_template('debug_queue.html', rows=[], mode=mode, sid=sid[:8])

    wids = [c['wid'] for c in queue]
    with get_db() as conn:
        word_rows = conn.execute(
            f'SELECT id, hanzi, pinyin, english, hsk_level, curriculum FROM words '
            f'WHERE id IN ({",".join("?"*len(wids))})',
            wids
        ).fetchall()
    word_map = {r['id']: dict(r) for r in word_rows}

    rows = []
    for i, c in enumerate(queue):
        w       = word_map.get(c['wid'], {})
        due_dt  = datetime.fromisoformat(c['due_at']) if c['due_at'] else None
        overdue = due_dt is not None and due_dt <= now
        secs    = int((due_dt - now).total_seconds()) if due_dt and not overdue else None
        rows.append({
            'pos':        i + 1,
            'wid':        c['wid'],
            'hanzi':      w.get('hanzi', '?'),
            'pinyin':     w.get('pinyin', ''),
            'english':    w.get('english', ''),
            'hsk_level':  w.get('hsk_level', ''),
            'curriculum': w.get('curriculum', ''),
            'status':     c['status'],
            'due_at':     c['due_at'],
            'overdue':    overdue,
            'secs_until': secs,
        })

    return render_template('debug_queue.html', rows=rows, mode=mode, sid=sid[:8])


@app.route('/debug/sentences')
def debug_sentences():
    curriculum = request.args.get('curriculum', '')
    level      = request.args.get('level', '')

    filters, params = [], []
    if curriculum:
        filters.append('w.curriculum = ?')
        params.append(curriculum)
    if level and level.isdigit():
        filters.append('w.hsk_level = ?')
        params.append(int(level))
    where = ('WHERE ' + ' AND '.join(filters)) if filters else ''

    with get_db() as conn:
        rows = conn.execute(f'''
            SELECT we.id        AS ex_id,
                   we.word_id,
                   we.example_hanzi,
                   we.example_english,
                   we.created_at,
                   w.hanzi,
                   w.pinyin,
                   w.english    AS word_english,
                   w.hsk_level,
                   w.curriculum,
                   COUNT(*) OVER (PARTITION BY we.word_id, we.example_hanzi) AS dup_count
            FROM word_examples we
            JOIN words w ON w.id = we.word_id
            {where}
            ORDER BY w.pinyin, we.id
        ''', params).fetchall()

        curricula = [r[0] for r in conn.execute(
            "SELECT DISTINCT curriculum FROM words ORDER BY curriculum"
        ).fetchall()]
        levels = [r[0] for r in conn.execute(
            "SELECT DISTINCT hsk_level FROM words ORDER BY hsk_level"
        ).fetchall()]

    rows = [dict(r) for r in rows]
    total_words    = len({r['word_id'] for r in rows})
    total_sentences = len(rows)
    total_dupes    = sum(1 for r in rows if r['dup_count'] > 1)

    return render_template('debug_sentences.html',
                           rows=rows,
                           curricula=curricula,
                           levels=levels,
                           sel_curriculum=curriculum,
                           sel_level=level,
                           total_words=total_words,
                           total_sentences=total_sentences,
                           total_dupes=total_dupes)


@app.route('/game')
def game():
    valid_curricula = get_curricula()
    with get_db() as conn:
        curricula = [c for c in valid_curricula if conn.execute(
            'SELECT 1 FROM words WHERE curriculum=? LIMIT 1', (c,)
        ).fetchone()]
        level_counts = {}
        for cur in curricula:
            lvls = [r[0] for r in conn.execute(
                'SELECT DISTINCT hsk_level FROM words WHERE curriculum=? AND hsk_level IS NOT NULL ORDER BY hsk_level',
                (cur,)
            ).fetchall()]
            level_counts[cur] = lvls
    return render_template('game.html', curricula=curricula,
                           level_counts_json=json.dumps(level_counts),
                           lang=get_language())


@app.route('/api/game/log', methods=['POST'])
def api_game_log():
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False})
    game_type = (request.json or {}).get('game')
    if game_type not in ALL_GAME_TYPES:
        return jsonify({'ok': False})
    with get_db() as conn:
        conn.execute(
            '''INSERT INTO game_plays (user_id, game_type, plays) VALUES (?, ?, 1)
               ON CONFLICT(user_id, game_type) DO UPDATE SET plays = plays + 1''',
            (uid, game_type)
        )
        plays = conn.execute(
            'SELECT plays FROM game_plays WHERE user_id=? AND game_type=?', (uid, game_type)
        ).fetchone()['plays']
        game_key_map = {'bingo': 'game_bingo', 'tone': 'game_tone', 'match': 'game_match',
                        'mc': 'game_mc', 'draw': 'game_draw', 'scram': 'game_scram'}
        if plays >= 10 and game_type in game_key_map:
            grant_award(conn, uid, game_key_map[game_type])
        played = {r[0] for r in conn.execute('SELECT game_type FROM game_plays WHERE user_id=?', (uid,)).fetchall()}
        if played >= ALL_GAME_TYPES:
            grant_award(conn, uid, 'all_games')
    return jsonify({'ok': True})


@app.route('/api/game/words')
def api_game_words():
    curriculum = request.args.get('curriculum', 'classic')
    hsk        = request.args.get('hsk', '').strip()
    count      = min(int(request.args.get('count', '20')), 80)

    filters = [curriculum]
    where   = 'curriculum = ?'
    if hsk and hsk.isdigit():
        where  += ' AND hsk_level = ?'
        filters.append(hsk)

    with get_db() as conn:
        rows = conn.execute(
            f'SELECT w.id, w.hanzi, w.pinyin, w.english,'
            f' COALESCE(we.example_hanzi,   w.example_hanzi)   AS example_hanzi,'
            f' COALESCE(we.example_pinyin,  w.example_pinyin)  AS example_pinyin,'
            f' COALESCE(we.example_english, w.example_english) AS example_english'
            f' FROM words w'
            f' LEFT JOIN word_examples we'
            f'   ON we.id = ('
            f'     SELECT id FROM word_examples'
            f'     WHERE word_id = w.id ORDER BY RANDOM() LIMIT 1'
            f'   )'
            f' WHERE w.{where} ORDER BY RANDOM() LIMIT ?',
            filters + [count]
        ).fetchall()

    is_japanese = (curriculum == 'jlpt')
    segmenter   = segment_japanese if is_japanese else segment_hanzi

    result = []
    for r in rows:
        opts, correct_idx = generate_tone_options(r['pinyin'] or '')
        result.append({
            'id':              r['id'],
            'hanzi':           r['hanzi'],
            'pinyin':          r['pinyin'],
            'english':         r['english'],
            'example_hanzi':    r['example_hanzi'],
            'example_pinyin':   r['example_pinyin'],
            'example_english':  r['example_english'],
            'example_segments': segmenter(r['example_hanzi'] or ''),
            'tone_options':     opts,
            'tone_correct':     correct_idx,
        })
    return jsonify(result)


@app.route('/words')
def words():
    uid        = get_user_id()
    if not uid:
        return redirect('/users')
    lang       = get_language()
    valid_curricula = get_curricula()
    default_curriculum = valid_curricula[0]
    hsk        = request.args.get('hsk', '1')
    curriculum = request.args.get('curriculum', default_curriculum)
    if curriculum not in valid_curricula and curriculum != 'custom':
        curriculum = default_curriculum
    raw_lid    = request.args.get('list_id', '').strip()
    list_id    = int(raw_lid) if raw_lid.isdigit() else None

    with get_db() as conn:
        # ── Standalone custom-list view (from /custom page or sidebar) ─────────
        if list_id and curriculum != 'custom':
            lst = conn.execute('SELECT id, name FROM custom_lists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone()
            rows = conn.execute('''
                SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                FROM words w
                JOIN custom_list_words clw ON w.id = clw.word_id
                LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE clw.list_id = ?
                ORDER BY w.id
            ''', (uid, list_id)).fetchall()
            return render_template('words.html', words=rows, intervals=INTERVALS,
                                   active_hsk=None, active_curriculum=None,
                                   level_counts=[], curricula=[],
                                   list_id=list_id,
                                   list_name=lst['name'] if lst else 'Custom list',
                                   custom_lists=[], active_custom_list_id=None,
                                   active_custom_list_name=None, lang=lang)

        # ── Build tab-bar curricula — filtered by active language ─────────────
        curricula = [c for c in valid_curricula if conn.execute(
            'SELECT 1 FROM words WHERE curriculum=? LIMIT 1', (c,)
        ).fetchone()]
        if conn.execute(
            "SELECT COUNT(*) FROM custom_lists WHERE user_id=? AND language=?", (uid, lang)
        ).fetchone()[0] > 0:
            curricula.append('custom')

        # ── Custom Lists browse view ───────────────────────────────────────────
        if curriculum == 'custom':
            cl_rows = conn.execute(
                'SELECT id, name FROM custom_lists WHERE user_id = ? AND language = ? ORDER BY name', (uid, lang)
            ).fetchall()
            custom_lists = [dict(r) for r in cl_rows]

            if list_id:
                rows = conn.execute('''
                    SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                    FROM words w
                    JOIN custom_list_words clw ON w.id = clw.word_id
                    LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                    WHERE clw.list_id = ?
                    ORDER BY w.pinyin COLLATE PINYIN
                ''', (uid, list_id)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT DISTINCT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                    FROM words w
                    JOIN custom_list_words clw ON w.id = clw.word_id
                    JOIN custom_lists cl ON cl.id = clw.list_id AND cl.user_id = ?
                    LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                    ORDER BY w.pinyin COLLATE PINYIN
                ''', (uid, uid)).fetchall()

            active_list_name = next(
                (cl['name'] for cl in custom_lists if cl['id'] == list_id), None
            ) if list_id else None

            return render_template('words.html', words=rows, intervals=INTERVALS,
                                   active_hsk=None, active_curriculum='custom',
                                   level_counts=[], curricula=curricula,
                                   list_id=None, list_name=None,
                                   custom_lists=custom_lists,
                                   active_custom_list_id=list_id,
                                   active_custom_list_name=active_list_name, lang=lang)

        # ── Standard curriculum view (classic / hsk3) ─────────────────────────
        level_counts = conn.execute('''
            SELECT w.hsk_level,
                   COUNT(*)                                               AS total,
                   SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END)    AS new_cnt,
                   SUM(CASE WHEN p.word_id IS NOT NULL THEN 1 ELSE 0 END) AS seen_cnt
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
            WHERE w.curriculum = ?
            GROUP BY w.hsk_level
            ORDER BY w.hsk_level
        ''', (uid, curriculum)).fetchall()

        if hsk != 'all' and hsk.isdigit():
            rows = conn.execute('''
                SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                FROM words w
                LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE w.hsk_level = ? AND w.curriculum = ?
                ORDER BY w.pinyin COLLATE PINYIN
            ''', (uid, int(hsk), curriculum)).fetchall()
        else:
            rows = conn.execute('''
                SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                FROM words w
                LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
                WHERE w.curriculum = ?
                ORDER BY w.hsk_level, w.pinyin COLLATE PINYIN
            ''', (uid, curriculum)).fetchall()

    return render_template('words.html', words=rows, intervals=INTERVALS,
                           active_hsk=hsk, active_curriculum=curriculum,
                           level_counts=level_counts, curricula=curricula,
                           list_id=None, list_name=None,
                           custom_lists=[], active_custom_list_id=None,
                           active_custom_list_name=None, lang=lang)


@app.route('/grammar')
def grammar():
    uid   = get_user_id()
    level = request.args.get('level', 'all')
    fav_join  = 'LEFT JOIN grammar_favorites gf ON gf.point_id = gp.id AND gf.user_id = ?' if uid else ''
    fav_col   = 'CASE WHEN gf.point_id IS NOT NULL THEN 1 ELSE 0 END AS favorited' if uid else '0 AS favorited'
    fav_param = [uid] if uid else []
    with get_db() as conn:
        if level != 'all':
            try:
                rows = conn.execute(
                    f'''SELECT gp.id, gp.title, gp.hsk_level, gp.used_for, gp.structures, gp.has_detail, {fav_col}
                       FROM grammar_points gp {fav_join}
                       WHERE gp.hsk_level = ?
                       ORDER BY gp.title''', fav_param + [int(level)]
                ).fetchall()
            except (ValueError, sqlite3.OperationalError):
                rows = []
        else:
            try:
                rows = conn.execute(
                    f'''SELECT gp.id, gp.title, gp.hsk_level, gp.used_for, gp.structures, gp.has_detail, {fav_col}
                       FROM grammar_points gp {fav_join}
                       ORDER BY gp.hsk_level, gp.title''', fav_param
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
    return render_template('grammar.html',
                           points=[dict(r) for r in rows],
                           active_level=level,
                           has_user=bool(uid))


@app.route('/api/grammar/<asg_id>/favorite', methods=['POST'])
def api_grammar_favorite(asg_id):
    uid = get_user_id()
    if not uid:
        return jsonify({'error': 'no user selected'}), 401
    with get_db() as conn:
        if not conn.execute('SELECT id FROM grammar_points WHERE id=?', (asg_id,)).fetchone():
            return jsonify({'error': 'not found'}), 404
        existing = conn.execute(
            'SELECT 1 FROM grammar_favorites WHERE user_id=? AND point_id=?', (uid, asg_id)
        ).fetchone()
        if existing:
            conn.execute('DELETE FROM grammar_favorites WHERE user_id=? AND point_id=?', (uid, asg_id))
            new_val = 0
        else:
            conn.execute('INSERT INTO grammar_favorites (user_id, point_id) VALUES (?,?)', (uid, asg_id))
            grant_award(conn, uid, 'grammar_fav')
            new_val = 1
    return jsonify({'favorited': new_val})


@app.route('/api/grammar/<asg_id>')
def api_grammar_point(asg_id):
    uid = get_user_id()
    fav_col  = 'CASE WHEN gf.point_id IS NOT NULL THEN 1 ELSE 0 END AS favorited' if uid else '0 AS favorited'
    fav_join = 'LEFT JOIN grammar_favorites gf ON gf.point_id = gp.id AND gf.user_id = ?' if uid else ''
    fav_param = [uid] if uid else []
    with get_db() as conn:
        point = conn.execute(
            f'''SELECT gp.id, gp.title, gp.hsk_level, gp.url, gp.used_for, gp.structures, gp.detail_html, {fav_col}
               FROM grammar_points gp {fav_join}
               WHERE gp.id = ?''', fav_param + [asg_id]
        ).fetchone()
        if not point:
            return jsonify({'error': 'not found'}), 404
        examples = conn.execute(
            '''SELECT hanzi, hanzi_split, pinyin, english, structure
               FROM grammar_examples WHERE point_id = ? ORDER BY id''',
            (asg_id,)
        ).fetchall()
    p = dict(point)
    p['structures'] = [s for s in (p.get('structures') or '').split('|') if s.strip()]
    p['used_for']   = [t for t in (p.get('used_for')   or '').split('|') if t.strip()]
    return jsonify({'point': p, 'examples': [dict(e) for e in examples]})


@app.route('/stats')
def stats():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    lang      = get_language()
    curricula = get_curricula()
    cur_ph    = ','.join('?' * len(curricula))
    with get_db() as conn:
        # ── 1. Top line numbers ───────────────────────────────────────────────
        known_total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT DISTINCT w.hanzi, w.pinyin
                FROM progress p JOIN words w ON w.id = p.word_id
                WHERE p.interval_step >= 6 AND p.user_id = ?
                AND w.curriculum IN ({cur_ph})
            )
        ''', (uid, *curricula)).fetchone()[0]

        ws_row = conn.execute(f'''
            SELECT COUNT(*)                                                              AS total,
                   COALESCE(SUM(CASE WHEN interval_step >= 6    THEN 1 ELSE 0 END), 0) AS known,
                   COALESCE(SUM(CASE WHEN last_grade = 'easy'   THEN 1 ELSE 0 END), 0) AS easy,
                   COALESCE(SUM(CASE WHEN last_grade = 'medium' THEN 1 ELSE 0 END), 0) AS medium,
                   COALESCE(SUM(CASE WHEN last_grade = 'wrong'  THEN 1 ELSE 0 END), 0) AS wrong
            FROM (
                SELECT MAX(p.interval_step) AS interval_step,
                       MIN(p.last_grade)    AS last_grade
                FROM progress p
                JOIN words w ON w.id = p.word_id
                WHERE p.user_id = ? AND w.curriculum IN ({cur_ph})
                GROUP BY w.hanzi, w.pinyin
            )
        ''', (uid, *curricula)).fetchone()
        _ws_total        = ws_row['total'] or 1
        _ws_easy_nk      = max(0, ws_row['easy'] - ws_row['known'])
        word_state_bars  = {
            'total':              ws_row['total'],
            'known':              ws_row['known'],
            'easy':               ws_row['easy'],
            'easy_not_known':     _ws_easy_nk,
            'medium':             ws_row['medium'],
            'wrong':              ws_row['wrong'],
            'known_pct':          round(ws_row['known']   / _ws_total * 100),
            'easy_not_known_pct': round(_ws_easy_nk        / _ws_total * 100),
            'medium_pct':         round(ws_row['medium']  / _ws_total * 100),
            'wrong_pct':          round(ws_row['wrong']   / _ws_total * 100),
        }

        totals = conn.execute('''
            SELECT
                COALESCE(SUM(CAST(easy_count   AS INTEGER)), 0) AS easy,
                COALESCE(SUM(CAST(medium_count AS INTEGER)), 0) AS medium,
                COALESCE(SUM(CAST(wrong_count  AS INTEGER)), 0) AS wrong
            FROM session_logs WHERE user_id = ?
        ''', (uid,)).fetchone()
        total_reviews = totals['easy'] + totals['medium'] + totals['wrong']

        session_dates = conn.execute(f'''
            SELECT DISTINCT DATE(d) AS d FROM (
                SELECT p.last_seen AS d
                FROM progress p JOIN words w ON w.id = p.word_id
                WHERE p.user_id = ? AND w.curriculum IN ({cur_ph})
                AND p.last_seen IS NOT NULL
                UNION
                SELECT passed_at AS d FROM map_progress
                WHERE user_id = ? AND passed = 1 AND passed_at IS NOT NULL
                AND curriculum IN ({cur_ph})
            ) ORDER BY d DESC
        ''', (uid, *curricula, uid, *curricula)).fetchall()
        streak = 0
        if session_dates:
            most_recent = date_cls.fromisoformat(session_dates[0]['d'])
            today_d     = date_cls.today()
            if (today_d - most_recent).days <= 1:
                for i, row in enumerate(session_dates):
                    if date_cls.fromisoformat(row['d']) == most_recent - timedelta(days=i):
                        streak += 1
                    else:
                        break

        # ── 2. Retention rate ─────────────────────────────────────────────────
        total_correct = totals['easy'] + totals['medium']
        retention_pct = round(total_correct / total_reviews * 100) if total_reviews else 0

        # ── 3. Progress by level ──────────────────────────────────────────────
        level_rows = conn.execute(f'''
            SELECT w.curriculum, w.hsk_level,
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END), 0)            AS new_cnt,
                COALESCE(SUM(CASE WHEN p.last_grade = 'wrong'  THEN 1 ELSE 0 END), 0)     AS wrong_cnt,
                COALESCE(SUM(CASE WHEN p.last_grade = 'medium' THEN 1 ELSE 0 END), 0)     AS medium_cnt,
                COALESCE(SUM(CASE WHEN p.last_grade = 'easy'   THEN 1 ELSE 0 END), 0)     AS easy_cnt,
                COALESCE(SUM(CASE WHEN p.interval_step >= 6    THEN 1 ELSE 0 END), 0)     AS known_cnt
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id AND p.user_id = ?
            WHERE w.curriculum IN ({cur_ph})
            GROUP BY w.curriculum, w.hsk_level
            ORDER BY w.curriculum DESC, w.hsk_level
        ''', (uid, *curricula)).fetchall()

        level_stats = {cur: [] for cur in curricula}
        for r in level_rows:
            curr = r['curriculum']
            if curr not in level_stats:
                continue
            t = r['total']
            easy_not_known = max(0, r['easy_cnt'] - r['known_cnt'])
            level_stats[curr].append({
                'level':              r['hsk_level'],
                'total':              t,
                'new':                r['new_cnt'],
                'wrong':              r['wrong_cnt'],
                'medium':             r['medium_cnt'],
                'easy':               r['easy_cnt'],
                'known':              r['known_cnt'],
                'easy_not_known':     easy_not_known,
                'seen':               t - r['new_cnt'],
                'new_pct':            round(r['new_cnt']        / t * 100) if t else 0,
                'wrong_pct':          round(r['wrong_cnt']      / t * 100) if t else 0,
                'medium_pct':         round(r['medium_cnt']     / t * 100) if t else 0,
                'known_pct':          round(r['known_cnt']      / t * 100) if t else 0,
                'easy_not_known_pct': round(easy_not_known      / t * 100) if t else 0,
            })

        # ── 4. Due forecast ───────────────────────────────────────────────────
        due = conn.execute(f'''
            SELECT
                COALESCE(SUM(CASE WHEN p.due_at < datetime('now') THEN 1 ELSE 0 END), 0) AS overdue,
                COALESCE(SUM(CASE WHEN p.due_at >= datetime('now')
                                   AND p.due_at <  datetime('now','+1 day')  THEN 1 ELSE 0 END), 0) AS today,
                COALESCE(SUM(CASE WHEN p.due_at >= datetime('now','+1 day')
                                   AND p.due_at <  datetime('now','+2 days') THEN 1 ELSE 0 END), 0) AS tomorrow,
                COALESCE(SUM(CASE WHEN p.due_at >= datetime('now','+2 days')
                                   AND p.due_at <  datetime('now','+7 days') THEN 1 ELSE 0 END), 0) AS rest_week
            FROM progress p
            JOIN words w ON w.id = p.word_id
            WHERE p.due_at IS NOT NULL AND p.user_id = ? AND w.curriculum IN ({cur_ph})
        ''', (uid, *curricula)).fetchone()

        # ── 5. Activity calendar (last 90 days) ───────────────────────────────
        activity_rows = conn.execute('''
            SELECT DATE(started_at) AS d,
                SUM(CAST(easy_count AS INTEGER) +
                    CAST(medium_count AS INTEGER) +
                    CAST(wrong_count  AS INTEGER)) AS reviews
            FROM session_logs
            WHERE started_at >= date('now','-90 days') AND user_id = ?
            GROUP BY DATE(started_at)
        ''', (uid,)).fetchall()
        activity = {r['d']: int(r['reviews']) for r in activity_rows}

        map_activity_rows = conn.execute(f'''
            SELECT DISTINCT DATE(passed_at) AS d
            FROM map_progress
            WHERE user_id = ? AND passed = 1 AND passed_at IS NOT NULL
              AND passed_at >= date('now','-90 days')
              AND curriculum IN ({cur_ph})
        ''', (uid, *curricula)).fetchall()
        map_activity_dates = {r['d'] for r in map_activity_rows}

        # ── 6. Library size ───────────────────────────────────────────────────
        earned_awards_rows = conn.execute(
            'SELECT award_key FROM user_awards WHERE user_id=?', (uid,)
        ).fetchall()
        earned_keys = {r['award_key'] for r in earned_awards_rows}
        earned_award_sample = random.sample(
            [aw for aw in AWARDS if aw['key'] in earned_keys],
            min(4, len(earned_keys))
        )

        library = conn.execute(f'''
            SELECT COUNT(DISTINCT w.hanzi) AS total_hanzi,
                   COUNT(we.id)            AS total_examples
            FROM words w
            LEFT JOIN word_examples we ON we.word_id = w.id
            WHERE w.curriculum IN ({cur_ph})
        ''', (*curricula,)).fetchone()

        # ── 7. Map progress ───────────────────────────────────────────────────
        map_progress_rows = conn.execute('SELECT * FROM map_progress WHERE user_id = ?', (uid,)).fetchall()
        map_levels = {}
        for cur in curricula:
            map_levels[cur] = [r[0] for r in conn.execute(
                'SELECT DISTINCT hsk_level FROM words WHERE curriculum=? AND hsk_level IS NOT NULL ORDER BY hsk_level',
                (cur,)
            ).fetchall()]

    # Build calendar grid (padded to Monday of first week)
    today_d      = date_cls.today()
    range_start  = today_d - timedelta(days=89)
    pad_start    = range_start - timedelta(days=range_start.weekday())
    cal_days = []
    d = pad_start
    while d <= today_d:
        cal_days.append({
            'date':     d.isoformat(),
            'reviews':  activity.get(d.isoformat(), 0),
            'in_range': d >= range_start,
            'month':    d.strftime('%b') + ' ' + str(d.day) if d.day == 1 else '',
        })
        d += timedelta(days=1)

    # Build map stats
    map_prog = {(r['curriculum'], r['level'], r['circle_num']): dict(r) for r in map_progress_rows}
    map_stats = {}
    map_total_passed = 0
    for cur in curricula:
        map_stats[cur] = []
        levels = map_levels.get(cur, [])
        for li, level in enumerate(levels):
            circles = []
            level_20_passed = bool(map_prog.get((cur, level, 20), {}).get('passed', 0))
            for c in range(1, 21):
                prog = map_prog.get((cur, level, c), {})
                if prog.get('passed', 0):
                    circles.append('p')
                elif prog.get('attempts', 0):
                    circles.append('t')
                else:
                    # Determine if unlocked (same logic as map route)
                    if c == 20 or (li == 0 and c == 1):
                        unlocked = True
                    elif c == 1:
                        prev_level = levels[li - 1]
                        unlocked = bool(map_prog.get((cur, prev_level, 20), {}).get('passed', 0)) or level_20_passed
                    else:
                        unlocked = bool(map_prog.get((cur, level, c - 1), {}).get('passed', 0)) or level_20_passed
                    circles.append('l' if unlocked else 'u')
            passed_count = circles.count('p')
            map_total_passed += passed_count
            map_stats[cur].append({
                'level':    level,
                'circles':  circles,
                'passed':   passed_count,
                'complete': passed_count == 20,
            })

    return render_template('stats.html',
        lang           = lang,
        curricula_list = list(curricula),
        streak        = streak,
        total_reviews = total_reviews,
        known_total   = known_total,
        retention_pct = retention_pct,
        total_easy    = totals['easy'],
        total_medium  = totals['medium'],
        total_wrong   = totals['wrong'],
        level_stats   = level_stats,
        due_overdue   = int(due['overdue']),
        due_today     = int(due['today']),
        due_tomorrow  = int(due['tomorrow']),
        due_rest_week     = int(due['rest_week']),
        cal_days          = cal_days,
        map_stats         = map_stats,
        map_total_passed  = map_total_passed,
        total_hanzi       = library['total_hanzi'],
        total_examples    = library['total_examples'],
        word_state_bars      = word_state_bars,
        is_android           = IS_ANDROID,
        earned_award_sample  = earned_award_sample,
        map_activity_dates   = map_activity_dates,
    )


@app.route('/awards')
def awards():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    from datetime import date as _date, timedelta as _td
    with get_db() as conn:
        check_lazy_awards(conn, uid)
        earned_rows = conn.execute(
            'SELECT award_key, awarded_at FROM user_awards WHERE user_id = ? ORDER BY awarded_at',
            (uid,)
        ).fetchall()
        total_reviews = conn.execute('''
            SELECT COALESCE(SUM(CAST(easy_count AS INTEGER) +
                               CAST(medium_count AS INTEGER) +
                               CAST(wrong_count  AS INTEGER)), 0)
            FROM session_logs WHERE user_id = ?
        ''', (uid,)).fetchone()[0]
        known_total = conn.execute('''
            SELECT COUNT(*) FROM (
                SELECT DISTINCT w.hanzi, w.pinyin
                FROM progress p JOIN words w ON w.id = p.word_id
                WHERE p.interval_step >= 6 AND p.user_id = ?
            )
        ''', (uid,)).fetchone()[0]
        # Streak
        date_rows = conn.execute('''
            SELECT DISTINCT DATE(d) AS d FROM (
                SELECT started_at AS d FROM session_logs WHERE user_id = ?
                UNION
                SELECT passed_at AS d FROM map_progress
                WHERE user_id = ? AND passed = 1 AND passed_at IS NOT NULL
            ) ORDER BY d DESC
        ''', (uid, uid)).fetchall()
        streak = 0
        if date_rows:
            most_recent = _date.fromisoformat(date_rows[0]['d'])
            if (_date.today() - most_recent).days <= 1:
                for i, row in enumerate(date_rows):
                    if _date.fromisoformat(row['d']) == most_recent - _td(days=i):
                        streak += 1
                    else:
                        break
        # Extra progress counters for new award categories
        total_sessions = conn.execute(
            'SELECT COUNT(*) FROM session_logs WHERE user_id=?', (uid,)
        ).fetchone()[0]
        total_circles = conn.execute(
            'SELECT COUNT(*) FROM map_progress WHERE user_id=? AND passed=1', (uid,)
        ).fetchone()[0]
        total_days = conn.execute(
            '''SELECT COUNT(DISTINCT DATE(d)) FROM (
                SELECT started_at AS d FROM session_logs WHERE user_id = ?
                UNION
                SELECT passed_at AS d FROM map_progress
                WHERE user_id = ? AND passed = 1 AND passed_at IS NOT NULL
            )''', (uid, uid)
        ).fetchone()[0]
        game_plays_counts = {
            r['game_type']: r['plays']
            for r in conn.execute('SELECT game_type, plays FROM game_plays WHERE user_id=?', (uid,)).fetchall()
        }
        grammar_fav_count = 0
        try:
            grammar_fav_count = conn.execute(
                'SELECT COUNT(*) FROM grammar_favorites WHERE user_id=?', (uid,)
            ).fetchone()[0]
        except Exception:
            pass

        # Mastery progress: known and total words per curriculum+level
        level_totals = {
            (r['curriculum'], r['hsk_level']): r['cnt']
            for r in conn.execute('''
                SELECT curriculum, hsk_level, COUNT(*) AS cnt
                FROM words WHERE curriculum IN ('classic','hsk3')
                GROUP BY curriculum, hsk_level
            ''').fetchall()
        }
        level_known = {
            (r['curriculum'], r['hsk_level']): r['cnt']
            for r in conn.execute('''
                SELECT w.curriculum, w.hsk_level,
                       COUNT(DISTINCT w.hanzi || '|' || w.pinyin) AS cnt
                FROM progress p JOIN words w ON w.id = p.word_id
                WHERE p.user_id = ? AND p.interval_step >= 6
                  AND w.curriculum IN ('classic','hsk3')
                GROUP BY w.curriculum, w.hsk_level
            ''', (uid,)).fetchall()
        }

    earned = {r['award_key']: r['awarded_at'][:10] for r in earned_rows}

    # Map mastery award keys → (curriculum, level)
    mastery_key_map = {
        **{f'known_classic_{l}': ('classic', l) for l in range(1, 7)},
        **{f'known_hsk3_{l}':    ('hsk3',    l) for l in range(1, 8)},
    }
    # Per-category scalar progress (legacy threshold awards)
    cat_progress = {
        'flashcard': total_reviews,
        'words':     known_total,
        'streak':    streak,
    }

    # Per-award progress overrides (current_value, threshold)
    award_progress_map = {
        'session_10':  (total_sessions, 10),
        'session_50':  (total_sessions, 50),
        'session_100': (total_sessions, 100),
        'session_500': (total_sessions, 500),
        'one_month':   (total_days, 30),
        'explorer_10':  (total_circles, 10),
        'explorer_25':  (total_circles, 25),
        'explorer_50':  (total_circles, 50),
        'explorer_100': (total_circles, 100),
        'game_bingo':  (game_plays_counts.get('bingo', 0), 10),
        'game_tone':   (game_plays_counts.get('tone',  0), 10),
        'game_match':  (game_plays_counts.get('match', 0), 10),
        'game_mc':     (game_plays_counts.get('mc',    0), 10),
        'game_draw':   (game_plays_counts.get('draw',  0), 10),
        'game_scram':  (game_plays_counts.get('scram', 0), 10),
        'grammar_fan':    (grammar_fav_count, 5),
        'grammar_master': (grammar_fav_count, 10),
    }

    cat_labels = {
        'flashcard': 'Flashcard Reviews',
        'words':     'Words Learned',
        'streak':    'Day Streaks',
        'sessions':  'Sessions & Consistency',
        'map':       'Learning Map',
        'games':     'Games',
        'misc':      'Miscellaneous',
        'mastery':   'Level Mastery',
    }
    categories = {}
    for a in AWARDS:
        cat = a['cat']
        categories.setdefault(cat, {'label': cat_labels.get(cat, cat), 'awards': []})
        aw = {**a, 'earned_at': earned.get(a['key']),
              'progress_pct': 0, 'progress_label': ''}
        if not aw['earned_at']:
            if a['key'] in award_progress_map:
                current, total = award_progress_map[a['key']]
                aw['progress_pct']   = min(100, int(current / total * 100)) if total else 0
                aw['progress_label'] = f"{current:,} / {total:,}"
            elif cat in cat_progress and 'threshold' in a:
                current = cat_progress[cat]
                total   = a['threshold']
                aw['progress_pct']   = min(100, int(current / total * 100)) if total else 0
                aw['progress_label'] = f"{current:,} / {total:,}"
            elif cat == 'mastery' and a['key'] in mastery_key_map:
                curr_lvl = mastery_key_map[a['key']]
                current  = level_known.get(curr_lvl, 0)
                total    = level_totals.get(curr_lvl, 0)
                aw['progress_pct']   = min(100, int(current / total * 100)) if total else 0
                aw['progress_label'] = f"{current:,} / {total:,}"
        categories[cat]['awards'].append(aw)

    total_earned = len(earned)
    total_awards = len(AWARDS)
    return render_template('awards.html',
        categories=categories, cat_order=list(cat_labels.keys()),
        total_earned=total_earned, total_awards=total_awards,
        is_android=IS_ANDROID,
    )


@app.route('/sessions')
def sessions():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    curriculum_labels = {'classic': 'Classic HSK', 'hsk3': 'New HSK 3.0'}
    with get_db() as conn:
        logs = conn.execute(
            'SELECT * FROM session_logs WHERE total_seen > 0 AND user_id = ? ORDER BY started_at DESC', (uid,)
        ).fetchall()

    entries = []
    for log in logs:
        sel = json.loads(log['selection'] or '{}')

        if sel.get('type') == 'custom_list':
            names = sel.get('list_names') or ([sel['list_name']] if sel.get('list_name') else [])
            parts = ['Custom: ' + (', '.join(names) if names else 'All lists')]
        else:
            parts = []
            parts.append(curriculum_labels.get(sel.get('curriculum', 'classic'), sel.get('curriculum', '')))
            levels = sel.get('hsk_levels', [])
            parts.append('HSK ' + ', '.join(str(l) for l in sorted(levels)) if levels else 'All HSK')
            grades = sel.get('grades', [])
            parts.append(', '.join(g.capitalize() for g in grades) if grades else 'All grades')
            if sel.get('include_overdue'):
                parts.append('Overdue ✓')

        started = log['started_at'] or ''
        ended   = log['ended_at']   or ''
        duration = '–'
        if started and ended:
            try:
                secs = int((datetime.fromisoformat(ended) - datetime.fromisoformat(started)).total_seconds())
                duration = f"{secs // 60}m {secs % 60}s" if secs >= 60 else f"{secs}s"
            except Exception:
                pass

        score_pct = f"{int(log['score'] * 100)}%" if log['score'] else '0%'
        score_val = log['score'] or 0

        entries.append({
            'date':      started[:10]  if started else '–',
            'start':     started[11:16] if started else '–',
            'end':       ended[11:16]   if ended   else '–',
            'duration':  duration,
            'stack':     int(log['stack_size']   or 0),
            'seen':      int(log['total_seen']   or 0),
            'new':       int(log['new_words']    or 0),
            'easy':      int(log['easy_count']   or 0),
            'medium':    int(log['medium_count'] or 0),
            'wrong':     int(log['wrong_count']  or 0),
            'selection': ' · '.join(parts),
            'score':     score_pct,
            'score_val': score_val,
        })

    return render_template('sessions.html', entries=entries)


@app.route('/audio/<path:filename>')
def audio(filename):
    return send_from_directory(AUDIO_BASE, filename)


@app.route('/api/tts/available')
def api_tts_available():
    return jsonify({'available': _TTS_AVAILABLE})


@app.route('/api/tts')
def api_tts():
    text = request.args.get('text', '').strip()
    if not text:
        return ('', 400)
    lang_key   = get_language()
    filename   = hashlib.md5(f'{lang_key}:{text}'.encode()).hexdigest() + '.mp3'
    cache_file = os.path.join(TTS_CACHE, filename)
    if os.path.exists(cache_file):
        return send_file(cache_file, mimetype='audio/mpeg')
    if not _TTS_AVAILABLE:
        return ('', 503)
    if IS_ANDROID:
        lang_code = 'ja' if get_language() == 'japanese' else 'zh-CN'
        _gTTS(text=text, lang=lang_code).save(cache_file)
    else:
        voice = 'ja-JP-NanamiNeural' if get_language() == 'japanese' else 'zh-CN-XiaoxiaoNeural'
        asyncio.run(edge_tts.Communicate(text, voice=voice, rate='-10%').save(cache_file))
    return send_file(cache_file, mimetype='audio/mpeg')


@app.route('/end')
def end_session():
    raw_next = request.args.get('next', '/')
    # Only allow relative paths to prevent open-redirect
    next_url = raw_next if (raw_next.startswith('/') and not raw_next.startswith('//')) else '/'

    uid = session.get('user_id')
    sid = session.get('sid')
    if sid:
        with get_db() as conn:
            row = conn.execute('''
                SELECT created_at, stack_size, new_words,
                       easy_count, medium_count, wrong_count, selection
                FROM study_sessions WHERE id = ?
            ''', (sid,)).fetchone()
            if row:
                easy    = int(row['easy_count']   or 0)
                medium  = int(row['medium_count'] or 0)
                wrong   = int(row['wrong_count']  or 0)
                new_w   = int(row['new_words']    or 0)
                total   = easy + medium + wrong
                stack   = int(row['stack_size']   or 0)
                denom   = stack if stack > 0 else total
                if denom > 0:
                    quality  = min(1.0, (easy + 0.5 * medium) / denom)
                    new_frac = min(1.0, new_w / denom)
                    score    = round(quality * (0.65 + 0.35 * new_frac), 4)
                else:
                    score = 0.0
                conn.execute('''
                    INSERT INTO session_logs
                        (started_at, ended_at, stack_size, total_seen, new_words,
                         easy_count, medium_count, wrong_count, selection, score, user_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row['created_at'], now_str(), row['stack_size'] or 0,
                      total, new_w, easy, medium, wrong, row['selection'], score, uid))
            conn.execute('DELETE FROM study_sessions WHERE id = ?', (sid,))
        if uid:
            with get_db() as conn:
                check_lazy_awards(conn, uid)
    lang = session.get('language')
    session.clear()
    if uid:
        session['user_id'] = uid   # preserve user across session end
    if lang:
        session['language'] = lang  # preserve language across session end
    return redirect(next_url)


def _dict_search(q, field='all', limit=80, offset=0, language='chinese'):
    """Search dictionary by field. Returns (results, has_more).

    Ranking uses 3 tiers so exact / prefix matches always surface first:
      0 = exact match (whole field or whole syllable)
      1 = prefix / starts-with match
      2 = substring anywhere
    """
    fetch = limit + 1
    like  = f'%{q}%'
    ql    = q.lower()

    with get_db() as conn:

        if language == 'japanese':
            is_jp = any('぀' <= c <= 'ヿ' or '一' <= c <= '鿿' for c in q)

            if field == 'simplified' or (field == 'all' and is_jp):
                rows = conn.execute('''
                    SELECT id, simplified, simplified AS traditional, pinyin, english
                    FROM dictionary WHERE language='japanese' AND simplified LIKE ?
                    ORDER BY
                      CASE WHEN simplified=? THEN 0 WHEN simplified LIKE ? THEN 1 ELSE 2 END,
                      length(simplified)
                    LIMIT ? OFFSET ?
                ''', [like, q, q + '%', fetch, offset]).fetchall()

            elif field == 'pinyin':
                rows = conn.execute('''
                    SELECT id, simplified, simplified AS traditional, pinyin, english
                    FROM dictionary WHERE language='japanese' AND lower(pinyin) LIKE ?
                    ORDER BY
                      CASE WHEN lower(pinyin)=? THEN 0 WHEN lower(pinyin) LIKE ? THEN 1 ELSE 2 END,
                      length(simplified)
                    LIMIT ? OFFSET ?
                ''', [like, ql, ql + '%', fetch, offset]).fetchall()

            elif field == 'english':
                rows = conn.execute('''
                    SELECT id, simplified, simplified AS traditional, pinyin, english
                    FROM dictionary WHERE language='japanese' AND english LIKE ?
                    ORDER BY
                      CASE WHEN lower(english)=? THEN 0 WHEN lower(english) LIKE ? THEN 1 ELSE 2 END,
                      length(english)
                    LIMIT ? OFFSET ?
                ''', [like, ql, ql + '%', fetch, offset]).fetchall()

            else:  # all, non-CJK query: search romaji (pinyin) and english together
                rows = conn.execute('''
                    SELECT id, simplified, simplified AS traditional, pinyin, english
                    FROM dictionary WHERE language='japanese'
                      AND (lower(pinyin) LIKE ? OR english LIKE ?)
                    ORDER BY
                      CASE
                        WHEN lower(pinyin)=? OR lower(english)=?          THEN 0
                        WHEN lower(pinyin) LIKE ? OR lower(english) LIKE ? THEN 1
                        ELSE                                                     2
                      END,
                      length(simplified)
                    LIMIT ? OFFSET ?
                ''', [like, like, ql, ql, ql + '%', ql + '%', fetch, offset]).fetchall()

        else:
            is_chinese = any('一' <= c <= '鿿' for c in q)

            # ── Hanzi (simplified / all-chinese) ─────────────────────────────
            if field == 'simplified' or (field == 'all' and is_chinese):
                if field == 'all':
                    where   = "language='chinese' AND (simplified LIKE ? OR traditional LIKE ?)"
                    wparams = [like, like]
                else:
                    where   = "language='chinese' AND simplified LIKE ?"
                    wparams = [like]
                rows = conn.execute(f'''
                    SELECT id, traditional, simplified, pinyin, english
                    FROM dictionary WHERE {where}
                    ORDER BY
                      CASE
                        WHEN simplified = ?    THEN 0
                        WHEN simplified LIKE ? THEN 1
                        ELSE                        2
                      END,
                      length(simplified)
                    LIMIT ? OFFSET ?
                ''', wparams + [q, q + '%', fetch, offset]).fetchall()

            # ── Traditional ───────────────────────────────────────────────────
            elif field == 'traditional':
                rows = conn.execute('''
                    SELECT id, traditional, simplified, pinyin, english
                    FROM dictionary WHERE language='chinese' AND traditional LIKE ?
                    ORDER BY
                      CASE
                        WHEN traditional = ?    THEN 0
                        WHEN traditional LIKE ? THEN 1
                        ELSE                         2
                      END,
                      length(simplified)
                    LIMIT ? OFFSET ?
                ''', [like, q, q + '%', fetch, offset]).fetchall()

            # ── Pinyin ────────────────────────────────────────────────────────
            elif field == 'pinyin':
                pattern = pinyin_to_like_pattern(q)
                if not pattern:
                    return [], False
                rows = conn.execute('''
                    SELECT id, traditional, simplified, pinyin, english
                    FROM dictionary WHERE language='chinese' AND pinyin LIKE ?
                    ORDER BY
                      CASE
                        WHEN pinyin LIKE ?    THEN 0
                        WHEN pinyin LIKE ?    THEN 1
                        ELSE                       2
                      END,
                      length(simplified)
                    LIMIT ? OFFSET ?
                ''', [f'%{pattern}%', pattern, pattern + ' %', fetch, offset]).fetchall()

            # ── English (also handles 'all' non-Chinese) ──────────────────────
            else:
                rows = conn.execute('''
                    SELECT id, traditional, simplified, pinyin, english
                    FROM dictionary WHERE language='chinese' AND english LIKE ?
                    ORDER BY
                      CASE
                        WHEN lower(english) = ?         THEN 0
                        WHEN lower(english) LIKE ?
                          OR lower(english) LIKE ?      THEN 1
                        ELSE                                 2
                      END,
                      length(english)
                    LIMIT ? OFFSET ?
                ''', [like, ql, ql + ' %', ql + ';%', fetch, offset]).fetchall()

    results  = [dict(r) for r in rows]
    has_more = len(results) > limit
    return results[:limit], has_more


@app.route('/dictionary')
def dictionary():
    lang   = get_language()
    q      = request.args.get('q', '').strip()
    field  = request.args.get('field', 'all')
    offset = max(0, int(request.args.get('offset', 0) or 0))
    fmt    = request.args.get('format', 'html')

    with get_db() as conn:
        has_data = conn.execute(
            "SELECT COUNT(*) FROM dictionary WHERE language=?", (lang,)
        ).fetchone()[0] > 0

    results  = []
    has_more = False
    ready    = False

    if q and has_data:
        results, has_more = _dict_search(q, field, limit=80, offset=offset, language=lang)
        ready = True

    if fmt == 'json':
        return jsonify({'results': results, 'has_more': has_more})

    return render_template('dictionary.html', q=q, field=field, results=results,
                           has_data=has_data, ready=ready, has_more=has_more,
                           has_user=bool(get_user_id()), lang=lang)


@app.route('/custom')
def custom_lists_page():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    lang = get_language()
    now = now_str()
    with get_db() as conn:
        rows = conn.execute('''
            SELECT cl.id, cl.name, cl.created_at,
                   COUNT(DISTINCT clw.word_id) AS total,
                   SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END) AS new_cnt,
                   SUM(CASE WHEN p.due_at <= ? AND p.word_id IS NOT NULL THEN 1 ELSE 0 END) AS due_cnt
            FROM custom_lists cl
            LEFT JOIN custom_list_words clw ON cl.id = clw.list_id
            LEFT JOIN progress p ON clw.word_id = p.word_id AND p.user_id = ?
            WHERE cl.user_id = ? AND cl.language = ?
            GROUP BY cl.id
            ORDER BY cl.name
        ''', (now, uid, uid, lang)).fetchall()
    return render_template('custom.html', lists=[dict(r) for r in rows], lang=lang)


@app.route('/api/custom-lists')
def api_custom_lists():
    uid = get_user_id()
    if not uid:
        return jsonify({'lists': []})
    lang = get_language()
    with get_db() as conn:
        rows = conn.execute(
            'SELECT id, name FROM custom_lists WHERE user_id=? AND language=? ORDER BY name',
            (uid, lang)
        ).fetchall()
    return jsonify({'lists': [dict(r) for r in rows]})


@app.route('/api/custom-list/add', methods=['POST'])
def api_custom_list_add():
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'no user selected'}), 401
    data      = request.json
    dict_ids  = data.get('word_ids', [])   # dictionary.id values
    list_id   = data.get('list_id')        # int or None
    list_name = (data.get('list_name') or '').strip()

    if not dict_ids:
        return jsonify({'ok': False, 'error': 'No words selected'})

    with get_db() as conn:
        if list_id:
            if not conn.execute('SELECT id FROM custom_lists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone():
                return jsonify({'ok': False, 'error': 'List not found'})
        else:
            if not list_name:
                return jsonify({'ok': False, 'error': 'List name required'})
            if conn.execute('SELECT id FROM custom_lists WHERE name = ? AND user_id = ?', (list_name, uid)).fetchone():
                return jsonify({'ok': False, 'error': f'A list named "{list_name}" already exists.'})
            cur = conn.execute(
                'INSERT INTO custom_lists (name, created_at, user_id, language) VALUES (?, ?, ?, ?)',
                (list_name, now_str(), uid, get_language())
            )
            list_id = cur.lastrowid

        added = 0
        cur_lang = get_language()
        for dict_id in dict_ids:
            d = conn.execute(
                'SELECT simplified, pinyin, english, language FROM dictionary WHERE id = ?', (dict_id,)
            ).fetchone()
            if not d:
                continue

            if d['language'] == 'japanese':
                # For Japanese words: prefer jlpt curriculum match, then custom-japanese
                existing = conn.execute(
                    "SELECT id FROM words WHERE hanzi=? AND curriculum IN ('jlpt','custom') LIMIT 1",
                    (d['simplified'],)
                ).fetchone()
            else:
                # For Chinese words: reuse any existing word row matched by hanzi
                existing = conn.execute(
                    'SELECT id FROM words WHERE hanzi=? LIMIT 1', (d['simplified'],)
                ).fetchone()

            if existing:
                word_id = existing['id']
            else:
                cur = conn.execute('''
                    INSERT INTO words (hanzi, pinyin, english, hsk_level, curriculum)
                    VALUES (?, ?, ?, 0, 'custom')
                ''', (d['simplified'], d['pinyin'], d['english']))
                word_id = cur.lastrowid

            try:
                conn.execute(
                    'INSERT INTO custom_list_words (list_id, word_id) VALUES (?, ?)',
                    (list_id, word_id)
                )
                added += 1
            except sqlite3.IntegrityError:
                pass  # already in list

    if added > 0:
        grant_award(conn, uid, 'custom_list')
    return jsonify({'ok': True, 'list_id': list_id, 'added': added})


@app.route('/api/custom-list/delete', methods=['POST'])
def api_custom_list_delete():
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'no user selected'}), 401
    list_id = request.json.get('list_id')
    if not list_id:
        return jsonify({'ok': False, 'error': 'list_id required'})
    with get_db() as conn:
        if not conn.execute('SELECT id FROM custom_lists WHERE id = ? AND user_id = ?', (list_id, uid)).fetchone():
            return jsonify({'ok': False, 'error': 'not found'})
        conn.execute('DELETE FROM custom_list_words WHERE list_id = ?', (list_id,))
        conn.execute('DELETE FROM custom_lists WHERE id = ? AND user_id = ?', (list_id, uid))
        # Clean up custom/anki words that no longer belong to any list
        for cur in ('custom', 'anki'):
            conn.execute('''
                DELETE FROM progress WHERE user_id = ? AND word_id IN (
                    SELECT id FROM words WHERE curriculum = ?
                    AND id NOT IN (SELECT word_id FROM custom_list_words)
                )
            ''', (uid, cur))
            conn.execute('''
                DELETE FROM word_examples WHERE word_id IN (
                    SELECT id FROM words WHERE curriculum = ?
                    AND id NOT IN (SELECT word_id FROM custom_list_words)
                )
            ''', (cur,))
            conn.execute('''
                DELETE FROM words WHERE curriculum = ?
                AND id NOT IN (SELECT word_id FROM custom_list_words)
            ''', (cur,))
        conn.commit()
    return jsonify({'ok': True})


@app.route('/api/custom-list/remove-word', methods=['POST'])
def api_custom_list_remove_word():
    data    = request.get_json()
    list_id = data.get('list_id')
    word_id = data.get('word_id')
    if not list_id or not word_id:
        return jsonify({'ok': False, 'error': 'list_id and word_id required'})
    with get_db() as conn:
        conn.execute(
            'DELETE FROM custom_list_words WHERE list_id = ? AND word_id = ?',
            (list_id, word_id)
        )
    return jsonify({'ok': True})


@app.route('/api/custom-list/rename', methods=['POST'])
def api_custom_list_rename():
    uid     = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'no user selected'}), 401
    data    = request.get_json()
    list_id = data.get('list_id')
    name    = (data.get('name') or '').strip()
    if not list_id or not name:
        return jsonify({'ok': False, 'error': 'list_id and name required'})
    with get_db() as conn:
        conn.execute('UPDATE custom_lists SET name = ? WHERE id = ? AND user_id = ?', (name, list_id, uid))
    return jsonify({'ok': True})


@app.route('/api/lists/<int:list_id>/generate-examples', methods=['POST'])
def api_generate_examples(list_id):
    task_id = str(uuid.uuid4())
    _generation_tasks[task_id] = {'done': 0, 'total': 0, 'status': 'starting', 'message': '', 'finished': False, 'results': []}
    threading.Thread(target=_generate_for_list, args=(task_id, list_id), daemon=True).start()
    return jsonify({'ok': True, 'task_id': task_id})


@app.route('/api/generate-status/<task_id>')
def api_generate_status(task_id):
    task = _generation_tasks.get(task_id)
    if not task:
        return jsonify({'ok': False, 'error': 'Task not found'})
    return jsonify({'ok': True, **task})


@app.route('/api/word/<int:word_id>/random-example')
def api_word_random_example(word_id):
    with get_db() as conn:
        ex = conn.execute(
            '''SELECT COALESCE(we.example_hanzi,   w.example_hanzi)   AS example_hanzi,
                      COALESCE(we.example_pinyin,  w.example_pinyin)  AS example_pinyin,
                      COALESCE(we.example_english, w.example_english) AS example_english,
                      w.hanzi
               FROM words w
               LEFT JOIN word_examples we ON we.id = (
                   SELECT id FROM word_examples WHERE word_id = w.id ORDER BY RANDOM() LIMIT 1
               )
               WHERE w.id = ?''',
            (word_id,)
        ).fetchone()
        hsk_labels = get_hsk_labels(conn, ex['hanzi']) if ex else []
    if ex and ex['example_hanzi']:
        return jsonify({'example_hanzi': ex['example_hanzi'], 'example_pinyin': ex['example_pinyin'] or '',
                        'example_english': ex['example_english'] or '', 'hsk_labels': hsk_labels})
    return jsonify({'hsk_labels': hsk_labels})


@app.route('/api/word/examples', methods=['POST'])
def update_word_examples():
    data            = request.get_json()
    word_id         = data.get('word_id')
    if not word_id:
        return jsonify({'ok': False, 'error': 'word_id required'})
    example_hanzi   = (data.get('example_hanzi')   or '').strip() or None
    example_english = (data.get('example_english') or '').strip() or None
    with get_db() as conn:
        conn.execute(
            'UPDATE words SET example_hanzi = ?, example_english = ? WHERE id = ?',
            (example_hanzi, example_english, word_id)
        )
    return jsonify({'ok': True})


@app.route('/reset/progress', methods=['POST'])
def reset_progress():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    curriculum = request.form.get('curriculum', 'classic')
    hsk_level  = request.form.get('hsk_level', '')
    raw_next   = request.form.get('next', '/words')
    next_url   = raw_next if (raw_next.startswith('/') and not raw_next.startswith('//')) else '/words'

    with get_db() as conn:
        if hsk_level and hsk_level.isdigit():
            conn.execute('''
                DELETE FROM progress WHERE user_id = ? AND word_id IN (
                    SELECT id FROM words WHERE curriculum = ? AND hsk_level = ?
                )
            ''', (uid, curriculum, int(hsk_level)))
        else:
            conn.execute('''
                DELETE FROM progress WHERE user_id = ? AND word_id IN (
                    SELECT id FROM words WHERE curriculum = ?
                )
            ''', (uid, curriculum,))

    return redirect(next_url)



@app.route('/map')
def map_page():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    valid_curricula = get_curricula()
    curriculum_labels = {
        'classic': 'Classic HSK', 'hsk3': 'New HSK 3.0', 'jlpt': 'JLPT'
    }
    with get_db() as conn:
        curricula = [c for c in valid_curricula if conn.execute(
            'SELECT 1 FROM words WHERE curriculum=? LIMIT 1', (c,)
        ).fetchone()]
        level_counts = {}
        for cur in curricula:
            lvls = [r[0] for r in conn.execute(
                'SELECT DISTINCT hsk_level FROM words WHERE curriculum=? AND hsk_level IS NOT NULL ORDER BY hsk_level',
                (cur,)
            ).fetchall()]
            level_counts[cur] = lvls
        progress_rows = conn.execute('SELECT * FROM map_progress WHERE user_id = ?', (uid,)).fetchall()

    progress = {}
    for row in progress_rows:
        progress[(row['curriculum'], row['level'], row['circle_num'])] = dict(row)

    map_data = {}
    for cur in curricula:
        levels = level_counts.get(cur, [])
        map_data[cur] = []
        for li, level in enumerate(levels):
            circles = []
            level_20_passed = bool(progress.get((cur, level, 20), {}).get('passed', 0))
            for c in range(1, 21):
                prog    = progress.get((cur, level, c), {})
                passed  = bool(prog.get('passed', 0))
                if c == 20:
                    # Always accessible as entrance exam
                    unlocked = True
                elif li == 0 and c == 1:
                    unlocked = True
                elif c == 1:
                    prev_level = levels[li - 1]
                    prev_gate  = bool(progress.get((cur, prev_level, 20), {}).get('passed', 0))
                    unlocked   = prev_gate or level_20_passed
                else:
                    prev_passed = bool(progress.get((cur, level, c - 1), {}).get('passed', 0))
                    unlocked    = prev_passed or level_20_passed
                circles.append({
                    'num':        c,
                    'passed':     passed,
                    'unlocked':   unlocked or passed,
                    'best_score': prog.get('best_score', 0) or 0,
                    'attempts':   prog.get('attempts',   0) or 0,
                })
            map_data[cur].append({'level': level, 'circles': circles})

    return render_template('map.html', curricula=curricula, map_data=map_data,
                           curriculum_labels=curriculum_labels, lang=get_language())


_MAP_THRESHOLDS = {
    1: 0.75, 2: 0.76, 3: 0.77, 4: 0.78,
    5: 0.79, 6: 0.80, 7: 0.81, 8: 0.82,
    9: 0.83, 10: 0.84, 11: 0.85, 12: 0.86,
    13: 0.87, 14: 0.88, 15: 0.89, 16: 0.90,
    17: 0.91, 18: 0.92, 19: 0.93, 20: 0.94,
}


@app.route('/api/map/complete', methods=['POST'])
def api_map_complete():
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'no user selected'}), 401
    data       = request.json
    curriculum = data.get('curriculum', 'classic')
    level      = int(data.get('level', 1))
    circle_num = int(data.get('circle_num', 1))
    score      = float(data.get('score', 0))
    threshold  = _MAP_THRESHOLDS.get(circle_num, 0.70)
    passed     = 1 if score >= threshold else 0

    with get_db() as conn:
        existing = conn.execute(
            'SELECT best_score, passed FROM map_progress WHERE user_id=? AND curriculum=? AND level=? AND circle_num=?',
            (uid, curriculum, level, circle_num)
        ).fetchone()
        if existing:
            new_best   = max(existing['best_score'] or 0, score)
            new_passed = 1 if (existing['passed'] or passed) else 0
            first_pass = passed and not existing['passed']
            if first_pass:
                conn.execute(
                    'UPDATE map_progress SET best_score=?, passed=?, passed_at=?, attempts=attempts+1, last_at=? '
                    'WHERE user_id=? AND curriculum=? AND level=? AND circle_num=?',
                    (new_best, new_passed, now_str(), now_str(), uid, curriculum, level, circle_num)
                )
            else:
                conn.execute(
                    'UPDATE map_progress SET best_score=?, passed=?, attempts=attempts+1, last_at=? '
                    'WHERE user_id=? AND curriculum=? AND level=? AND circle_num=?',
                    (new_best, new_passed, now_str(), uid, curriculum, level, circle_num)
                )
        else:
            first_pass = bool(passed)
            conn.execute(
                'INSERT INTO map_progress (user_id, curriculum, level, circle_num, best_score, passed, passed_at, attempts, last_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)',
                (uid, curriculum, level, circle_num, score, passed, now_str() if passed else None, now_str())
            )

        if first_pass and circle_num == 20 and uid:
            grant_award(conn, uid, 'map_skip')

    if passed and uid:
        with get_db() as conn:
            check_lazy_awards(conn, uid)
    return jsonify({'ok': True, 'passed': bool(passed)})


@app.route('/api/map/reset', methods=['POST'])
def api_map_reset():
    uid = get_user_id()
    if not uid:
        return redirect('/users')
    curriculum = request.form.get('curriculum', 'classic')
    with get_db() as conn:
        conn.execute('DELETE FROM map_progress WHERE curriculum=? AND user_id=?', (curriculum, uid))
    return redirect('/map')


@app.route('/family')
def family():
    return render_template('family.html', lang=get_language())


@app.route('/kana')
def kana():
    if get_language() != 'japanese':
        return redirect('/')
    return render_template('kana.html')


# ── User management ───────────────────────────────────────────────────────────

@app.route('/users')
def users_page():
    with get_db() as conn:
        users = conn.execute('SELECT id, name FROM users ORDER BY name').fetchall()
    return render_template('users.html', users=[dict(u) for u in users],
                           current_user_id=get_user_id(), is_android=IS_ANDROID)


@app.route('/users/create', methods=['POST'])
def users_create():
    name = (request.form.get('name') or '').strip()
    if not name:
        return redirect('/users')
    with get_db() as conn:
        try:
            conn.execute('INSERT INTO users (name) VALUES (?)', (name,))
        except sqlite3.IntegrityError:
            pass  # duplicate name — just switch to existing
        user = conn.execute('SELECT id FROM users WHERE name = ?', (name,)).fetchone()
    if user:
        session['user_id'] = user['id']
        with get_db() as conn:
            grant_award(conn, user['id'], 'first_user')
    return redirect('/')


@app.route('/users/delete/<int:user_id>', methods=['POST'])
def users_delete(user_id):
    with get_db() as conn:
        if not conn.execute('SELECT id FROM users WHERE id = ?', (user_id,)).fetchone():
            return redirect('/users')
        # Delete all user-owned data
        conn.execute('DELETE FROM progress WHERE user_id = ?', (user_id,))
        conn.execute('DELETE FROM session_logs WHERE user_id = ?', (user_id,))
        conn.execute('DELETE FROM map_progress WHERE user_id = ?', (user_id,))
        conn.execute('DELETE FROM custom_list_words WHERE list_id IN (SELECT id FROM custom_lists WHERE user_id = ?)', (user_id,))
        conn.execute('DELETE FROM custom_lists WHERE user_id = ?', (user_id,))
        # Clean up custom words no longer referenced by any list
        conn.execute('''
            DELETE FROM words WHERE curriculum = 'custom'
            AND id NOT IN (SELECT word_id FROM custom_list_words)
        ''')
        conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
    # If this was the active user, clear the session
    if session.get('user_id') == user_id:
        session.clear()
    return redirect('/users')


@app.route('/users/switch/<int:user_id>', methods=['POST'])
def users_switch(user_id):
    with get_db() as conn:
        user = conn.execute('SELECT id FROM users WHERE id = ?', (user_id,)).fetchone()
    if not user:
        return redirect('/users')
    # End any active study session cleanly (delete without logging)
    sid = session.get('sid')
    if sid:
        with get_db() as conn:
            conn.execute('DELETE FROM study_sessions WHERE id = ?', (sid,))
    session.clear()
    session['user_id'] = user_id
    return redirect('/')


@app.route('/users/rename/<int:user_id>', methods=['POST'])
def users_rename(user_id):
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'ok': False, 'error': 'Name cannot be empty.'})
    with get_db() as conn:
        if not conn.execute('SELECT id FROM users WHERE id = ?', (user_id,)).fetchone():
            return jsonify({'ok': False, 'error': 'User not found.'})
        if conn.execute('SELECT id FROM users WHERE name = ? AND id != ?', (name, user_id)).fetchone():
            return jsonify({'ok': False, 'error': f'An account named "{name}" already exists.'})
        conn.execute('UPDATE users SET name = ? WHERE id = ?', (name, user_id))
    return jsonify({'ok': True, 'name': name})


# ── Progress sync export / import ────────────────────────────────────────────

def _collect_sync_payload(uid, conn):
    """Build the sync data dict for a user. Caller must hold an open connection."""
    user = conn.execute('SELECT name FROM users WHERE id=?', (uid,)).fetchone()
    if not user:
        return None, None

    words = [dict(r) for r in conn.execute('''
        SELECT w.hanzi, w.pinyin, w.english, w.hsk_level, w.curriculum,
               w.example_hanzi, w.example_pinyin, w.example_english, w.audio_file
        FROM words w
        JOIN progress p ON p.word_id = w.id AND p.user_id = ?
    ''', (uid,)).fetchall()]

    progress = [dict(r) for r in conn.execute('''
        SELECT w.hanzi, w.curriculum, p.interval_step, p.last_grade, p.last_seen, p.due_at
        FROM progress p JOIN words w ON w.id = p.word_id
        WHERE p.user_id = ?
    ''', (uid,)).fetchall()]

    lists_raw = conn.execute(
        'SELECT id, name, created_at, language FROM custom_lists WHERE user_id=?', (uid,)
    ).fetchall()
    custom_lists = []
    for lst in lists_raw:
        members = conn.execute('''
            SELECT w.hanzi FROM custom_list_words clw
            JOIN words w ON w.id = clw.word_id WHERE clw.list_id = ?
        ''', (lst['id'],)).fetchall()
        custom_lists.append({
            'name': lst['name'],
            'created_at': lst['created_at'],
            'language': lst['language'] or 'chinese',
            'words': [r['hanzi'] for r in members],
        })

    word_ids = [r[0] for r in conn.execute(
        'SELECT word_id FROM progress WHERE user_id=?', (uid,)
    ).fetchall()]
    word_examples = []
    if word_ids:
        ph = ','.join('?' * len(word_ids))
        word_examples = [dict(r) for r in conn.execute(f'''
            SELECT w.hanzi AS word_hanzi, we.example_hanzi, we.example_english
            FROM word_examples we JOIN words w ON w.id = we.word_id
            WHERE we.word_id IN ({ph})
        ''', word_ids).fetchall()]

    map_progress = [dict(r) for r in conn.execute('''
        SELECT curriculum, level, circle_num, best_score, passed, attempts, last_at
        FROM map_progress WHERE user_id=?
    ''', (uid,)).fetchall()]

    grammar_favorites = [r[0] for r in conn.execute(
        'SELECT point_id FROM grammar_favorites WHERE user_id=?', (uid,)
    ).fetchall()]

    session_logs = [dict(r) for r in conn.execute(
        'SELECT * FROM session_logs WHERE user_id=? ORDER BY started_at', (uid,)
    ).fetchall()]
    for log in session_logs:
        log.pop('id', None)

    user_awards = [dict(r) for r in conn.execute(
        'SELECT award_key, awarded_at FROM user_awards WHERE user_id=? ORDER BY awarded_at', (uid,)
    ).fetchall()]

    game_plays = [{'game_type': r['game_type'], 'plays': r['plays']}
                  for r in conn.execute(
                      'SELECT game_type, plays FROM game_plays WHERE user_id=?', (uid,)
                  ).fetchall()]

    payload = {
        'version': 1,
        'user_name': user['name'],
        'exported_at': datetime.now().isoformat(),
        'words': words,
        'progress': progress,
        'custom_lists': custom_lists,
        'word_examples': word_examples,
        'map_progress': map_progress,
        'grammar_favorites': grammar_favorites,
        'session_logs': session_logs,
        'user_awards': user_awards,
        'game_plays': game_plays,
    }
    return user['name'], payload


@app.route('/api/sync/export')
def sync_export():
    import io as _io
    uid = get_user_id()
    if not uid:
        return jsonify({'error': 'No active user'}), 400
    with get_db() as conn:
        username, payload = _collect_sync_payload(uid, conn)
    if payload is None:
        return jsonify({'error': 'User not found'}), 404
    buf = _io.BytesIO(json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8'))
    fname = f"mandarin_progress_{username}_{datetime.now().strftime('%Y-%m-%d')}.json"
    return send_file(buf, mimetype='application/json', as_attachment=True,
                     download_name=fname)


@app.route('/api/sync/export_raw')
def sync_export_raw():
    """Returns sync payload as plain JSON — used by the native Android export command."""
    uid = get_user_id()
    if not uid:
        return jsonify({'error': 'No active user'}), 400
    with get_db() as conn:
        _, payload = _collect_sync_payload(uid, conn)
        if payload is not None:
            grant_award(conn, uid, 'exported')
    if payload is None:
        return jsonify({'error': 'User not found'}), 404
    return jsonify(payload)


@app.route('/api/sync/import', methods=['POST'])
def sync_import():
    raw = None
    if 'file' in request.files:
        raw = request.files['file'].read()
    elif request.content_type and 'json' in request.content_type:
        raw = request.data
    else:
        raw = request.form.get('json_text', '').encode('utf-8')

    try:
        data = json.loads(raw)
    except Exception:
        return jsonify({'error': 'Invalid JSON'}), 400
    if data.get('version') != 1:
        return jsonify({'error': 'Unsupported sync file version'}), 400

    # Always import as the user named in the file, creating them if needed
    import_username = data.get('user_name', '').strip()
    if not import_username:
        return jsonify({'error': 'No user_name in file'}), 400

    with get_db() as conn:
        row = conn.execute('SELECT id FROM users WHERE name=?', (import_username,)).fetchone()
        if row:
            uid = row['id']
        else:
            uid = conn.execute('INSERT INTO users (name) VALUES (?)', (import_username,)).lastrowid

    # Set the imported user as the active session user
    session['user_id'] = uid

    with get_db() as conn:
        # Build word lookup maps, creating missing words
        hanzi_to_id = {}        # hanzi → word_id (curriculum-agnostic; last-seen wins)
        hanzi_cur_to_id = {}    # (hanzi, curriculum) → word_id (precise lookup)
        for w in data.get('words', []):
            hanzi = w['hanzi']
            curr  = w.get('curriculum', 'classic')
            row = conn.execute(
                'SELECT id FROM words WHERE hanzi=? AND curriculum=?', (hanzi, curr)
            ).fetchone()
            if row:
                wid = row['id']
            else:
                wid = conn.execute('''
                    INSERT INTO words
                        (hanzi, pinyin, english, hsk_level, curriculum,
                         example_hanzi, example_pinyin, example_english, audio_file)
                    VALUES (?,?,?,?,?,?,?,?,?)
                ''', (hanzi, w.get('pinyin'), w.get('english'), w.get('hsk_level', 0),
                      curr, w.get('example_hanzi'),
                      w.get('example_pinyin'), w.get('example_english'), w.get('audio_file'))
                ).lastrowid
            hanzi_cur_to_id[(hanzi, curr)] = wid
            hanzi_to_id[hanzi] = wid  # last-seen wins; fine for unique hanzi

        # Ensure any progress entry's hanzi is resolved even if not in words list
        for p in data.get('progress', []):
            hanzi = p['hanzi']
            curr  = p.get('curriculum', 'classic')
            key   = (hanzi, curr)
            if key not in hanzi_cur_to_id:
                row = conn.execute(
                    'SELECT id FROM words WHERE hanzi=? AND curriculum=?', (hanzi, curr)
                ).fetchone()
                if not row:
                    row = conn.execute('SELECT id FROM words WHERE hanzi=?', (hanzi,)).fetchone()
                if row:
                    hanzi_cur_to_id[key] = row['id']
                    hanzi_to_id[hanzi] = row['id']

        # Replace progress
        # Deduplicate by (hanzi, curriculum-group).  Chinese curricula (classic/hsk3)
        # always have identical progress values and share the same group so they get
        # mirrored together.  JLPT (and other curricula) form their own groups, ensuring
        # a kanji that appears in both Chinese and Japanese gets separate progress entries.
        # Old export files have no curriculum field; treat them as Chinese (group='').
        conn.execute('DELETE FROM progress WHERE user_id=?', (uid,))
        seen_hc: dict = {}
        for p in data.get('progress', []):
            curr = p.get('curriculum', '')
            grp  = '' if not curr or curr in ('classic', 'hsk3') else curr
            key  = (p['hanzi'], grp)
            if key not in seen_hc:
                seen_hc[key] = p
        for (hanzi, grp), p in seen_hc.items():
            if not grp:
                # Old export or Chinese: mirror progress to all classic/hsk3 word IDs
                srs_wids = conn.execute(
                    "SELECT id FROM words WHERE hanzi=? AND curriculum IN ('classic','hsk3')",
                    (hanzi,)
                ).fetchall()
                wids = {r['id'] for r in srs_wids}
                if hanzi in hanzi_to_id:
                    wids.add(hanzi_to_id[hanzi])
            else:
                # Curriculum-specific (jlpt, anki, custom): write to exactly that word
                specific = hanzi_cur_to_id.get((hanzi, grp))
                wids = {specific} if specific else set()
            for wid in wids:
                conn.execute('''
                    INSERT OR REPLACE INTO progress (user_id, word_id, interval_step, last_grade, last_seen, due_at)
                    VALUES (?,?,?,?,?,?)
                ''', (uid, wid, p.get('interval_step', 0), p.get('last_grade'),
                      p.get('last_seen'), p.get('due_at')))
            if hanzi not in hanzi_to_id and wids:
                hanzi_to_id[hanzi] = next(iter(wids))

        # Replace custom lists
        old_list_ids = [r[0] for r in conn.execute(
            'SELECT id FROM custom_lists WHERE user_id=?', (uid,)
        ).fetchall()]
        if old_list_ids:
            ph = ','.join('?' * len(old_list_ids))
            conn.execute(f'DELETE FROM custom_list_words WHERE list_id IN ({ph})', old_list_ids)
        conn.execute('DELETE FROM custom_lists WHERE user_id=?', (uid,))
        for lst in data.get('custom_lists', []):
            cur = conn.execute(
                'INSERT INTO custom_lists (name, created_at, user_id, language) VALUES (?,?,?,?)',
                (lst['name'], lst.get('created_at'), uid, lst.get('language', 'chinese'))
            )
            lid = cur.lastrowid
            for hanzi in lst.get('words', []):
                wid = hanzi_to_id.get(hanzi)
                if not wid:
                    row = conn.execute('SELECT id FROM words WHERE hanzi=?', (hanzi,)).fetchone()
                    wid = row['id'] if row else None
                if wid:
                    conn.execute('INSERT OR IGNORE INTO custom_list_words (list_id, word_id) VALUES (?,?)',
                                 (lid, wid))

        # Word examples: add any that don't already exist (shared table, no per-user delete)
        for ex in data.get('word_examples', []):
            wid = hanzi_to_id.get(ex.get('word_hanzi'))
            if not wid:
                row = conn.execute('SELECT id FROM words WHERE hanzi=?',
                                   (ex.get('word_hanzi'),)).fetchone()
                wid = row['id'] if row else None
            if wid:
                exists = conn.execute(
                    'SELECT 1 FROM word_examples WHERE word_id=? AND example_hanzi=?',
                    (wid, ex['example_hanzi'])
                ).fetchone()
                if not exists:
                    conn.execute(
                        'INSERT INTO word_examples (word_id, example_hanzi, example_english) VALUES (?,?,?)',
                        (wid, ex['example_hanzi'], ex['example_english'])
                    )

        # Replace map progress
        conn.execute('DELETE FROM map_progress WHERE user_id=?', (uid,))
        for m in data.get('map_progress', []):
            conn.execute('''
                INSERT INTO map_progress
                    (user_id, curriculum, level, circle_num, best_score, passed, attempts, last_at)
                VALUES (?,?,?,?,?,?,?,?)
            ''', (uid, m['curriculum'], m['level'], m['circle_num'],
                  m.get('best_score', 0), m.get('passed', 0), m.get('attempts', 0), m.get('last_at')))

        # Replace grammar favorites
        conn.execute('DELETE FROM grammar_favorites WHERE user_id=?', (uid,))
        for point_id in data.get('grammar_favorites', []):
            conn.execute('INSERT OR IGNORE INTO grammar_favorites (user_id, point_id) VALUES (?,?)',
                         (uid, point_id))

        # Replace session logs
        conn.execute('DELETE FROM session_logs WHERE user_id=?', (uid,))
        for log in data.get('session_logs', []):
            log = {k: v for k, v in log.items() if k != 'id'}
            log['user_id'] = uid
            cols = list(log.keys())
            ph = ','.join('?' * len(cols))
            conn.execute(f"INSERT INTO session_logs ({','.join(cols)}) VALUES ({ph})",
                         list(log.values()))

        # Replace awards
        conn.execute('DELETE FROM user_awards WHERE user_id=?', (uid,))
        for aw in data.get('user_awards', []):
            conn.execute(
                'INSERT OR IGNORE INTO user_awards (user_id, award_key, awarded_at) VALUES (?,?,?)',
                (uid, aw['award_key'], aw.get('awarded_at'))
            )

        # Replace game plays (supports old list-of-strings and new list-of-dicts format)
        conn.execute('DELETE FROM game_plays WHERE user_id=?', (uid,))
        for item in data.get('game_plays', []):
            gt = item if isinstance(item, str) else item.get('game_type', '')
            plays = 1 if isinstance(item, str) else item.get('plays', 1)
            if gt in ALL_GAME_TYPES:
                conn.execute(
                    'INSERT OR IGNORE INTO game_plays (user_id, game_type, plays) VALUES (?,?,?)',
                    (uid, gt, plays)
                )

        # Grant after awards restore so the DELETE wipe can't remove it
        grant_award(conn, uid, 'first_user')
        grant_award(conn, uid, 'imported')

    return jsonify({'ok': True, 'words_imported': len(data.get('progress', [])),
                    'user_name': import_username})


@app.route('/android/importing')
def android_importing():
    return '''<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-dark d-flex align-items-center justify-content-center" style="height:100vh;margin:0">
<div class="text-center text-white">
  <div class="spinner-border text-primary mb-3" role="status" style="width:3rem;height:3rem">
    <span class="visually-hidden">Loading...</span>
  </div>
  <div class="fs-5">Importing progress&hellip;</div>
  <div class="text-muted small mt-2">Please wait</div>
</div>
</body></html>'''


@app.route('/android/import-anki')
def android_import_anki_trigger():
    return '''<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-dark d-flex align-items-center justify-content-center" style="height:100vh;margin:0">
<div class="text-center text-white">
  <div class="spinner-border text-primary mb-3" role="status" style="width:3rem;height:3rem">
    <span class="visually-hidden">Loading...</span>
  </div>
  <div class="fs-5">Opening file picker&hellip;</div>
</div>
</body></html>'''


@app.route('/android/import-picker')
def android_import_picker():
    token = request.args.get('token', '')
    pending = _import_pending.get(token)
    if not pending:
        return redirect('/users')
    filenames = [f[0] for f in pending['files']]
    return render_template('android_import_picker.html', token=token, filenames=filenames)


@app.route('/android/import-execute', methods=['POST'])
def android_import_execute():
    data = request.get_json(force=True) or {}
    token = data.get('token', '')
    idx = data.get('idx')
    pending = _import_pending.pop(token, None)
    if not pending or idx is None or idx >= len(pending['files']):
        return jsonify({'ok': False, 'error': 'Session expired — please try Import again'}), 400
    _, uri = pending['files'][idx]
    try:
        raw = pending['read_file'](uri)
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Could not read file: {e}'}), 500
    try:
        payload = json.loads(raw)
    except Exception:
        return jsonify({'ok': False, 'error': 'File is not valid JSON'}), 400
    if payload.get('version') != 1:
        return jsonify({'ok': False, 'error': 'Unsupported sync file version'}), 400
    import_username = payload.get('user_name', '').strip()
    if not import_username:
        return jsonify({'ok': False, 'error': 'No user_name in file'}), 400

    with get_db() as conn:
        row = conn.execute('SELECT id FROM users WHERE name=?', (import_username,)).fetchone()
        if row:
            uid = row['id']
        else:
            uid = conn.execute('INSERT INTO users (name) VALUES (?)', (import_username,)).lastrowid

    session['user_id'] = uid

    with get_db() as conn:
        hanzi_to_id = {}
        for w in payload.get('words', []):
            hanzi = w['hanzi']
            row = conn.execute('SELECT id FROM words WHERE hanzi=?', (hanzi,)).fetchone()
            if row:
                hanzi_to_id[hanzi] = row['id']
            else:
                cur = conn.execute('''
                    INSERT INTO words
                        (hanzi, pinyin, english, hsk_level, curriculum,
                         example_hanzi, example_pinyin, example_english, audio_file)
                    VALUES (?,?,?,?,?,?,?,?,?)
                ''', (hanzi, w.get('pinyin'), w.get('english'), w.get('hsk_level', 0),
                      w.get('curriculum', 'classic'), w.get('example_hanzi'),
                      w.get('example_pinyin'), w.get('example_english'), w.get('audio_file')))
                hanzi_to_id[hanzi] = cur.lastrowid

        for p in payload.get('progress', []):
            hanzi = p['hanzi']
            if hanzi not in hanzi_to_id:
                row = conn.execute('SELECT id FROM words WHERE hanzi=?', (hanzi,)).fetchone()
                if row:
                    hanzi_to_id[hanzi] = row['id']

        conn.execute('DELETE FROM progress WHERE user_id=?', (uid,))
        for p in payload.get('progress', []):
            wid = hanzi_to_id.get(p['hanzi'])
            if wid:
                conn.execute('''
                    INSERT OR REPLACE INTO progress
                        (user_id, word_id, interval_step, last_grade, last_seen, due_at)
                    VALUES (?,?,?,?,?,?)
                ''', (uid, wid, p.get('interval_step', 0), p.get('last_grade'),
                      p.get('last_seen'), p.get('due_at')))

        old_list_ids = [r[0] for r in conn.execute(
            'SELECT id FROM custom_lists WHERE user_id=?', (uid,)
        ).fetchall()]
        if old_list_ids:
            ph = ','.join('?' * len(old_list_ids))
            conn.execute(f'DELETE FROM custom_list_words WHERE list_id IN ({ph})', old_list_ids)
        conn.execute('DELETE FROM custom_lists WHERE user_id=?', (uid,))
        for lst in payload.get('custom_lists', []):
            cur = conn.execute(
                'INSERT INTO custom_lists (name, created_at, user_id, language) VALUES (?,?,?,?)',
                (lst['name'], lst.get('created_at'), uid, lst.get('language', 'chinese'))
            )
            lid = cur.lastrowid
            for hanzi in lst.get('words', []):
                wid = hanzi_to_id.get(hanzi) or (
                    conn.execute('SELECT id FROM words WHERE hanzi=?', (hanzi,)).fetchone() or {}
                ).get('id')
                if wid:
                    conn.execute('INSERT OR IGNORE INTO custom_list_words (list_id, word_id) VALUES (?,?)',
                                 (lid, wid))

        conn.execute('DELETE FROM map_progress WHERE user_id=?', (uid,))
        for m in payload.get('map_progress', []):
            conn.execute('''
                INSERT INTO map_progress
                    (user_id, curriculum, level, circle_num, best_score, passed, attempts, last_at)
                VALUES (?,?,?,?,?,?,?,?)
            ''', (uid, m['curriculum'], m['level'], m['circle_num'],
                  m.get('best_score', 0), m.get('passed', 0), m.get('attempts', 0), m.get('last_at')))

        conn.execute('DELETE FROM grammar_favorites WHERE user_id=?', (uid,))
        for point_id in payload.get('grammar_favorites', []):
            conn.execute('INSERT OR IGNORE INTO grammar_favorites (user_id, point_id) VALUES (?,?)',
                         (uid, point_id))

        conn.execute('DELETE FROM session_logs WHERE user_id=?', (uid,))
        for log in payload.get('session_logs', []):
            log = {k: v for k, v in log.items() if k != 'id'}
            log['user_id'] = uid
            cols = list(log.keys())
            ph = ','.join('?' * len(cols))
            conn.execute(f"INSERT INTO session_logs ({','.join(cols)}) VALUES ({ph})",
                         list(log.values()))

        # Replace awards
        conn.execute('DELETE FROM user_awards WHERE user_id=?', (uid,))
        for aw in payload.get('user_awards', []):
            conn.execute(
                'INSERT OR IGNORE INTO user_awards (user_id, award_key, awarded_at) VALUES (?,?,?)',
                (uid, aw['award_key'], aw.get('awarded_at'))
            )

        # Replace game plays
        conn.execute('DELETE FROM game_plays WHERE user_id=?', (uid,))
        for game_type in payload.get('game_plays', []):
            if game_type in ALL_GAME_TYPES:
                conn.execute(
                    'INSERT OR IGNORE INTO game_plays (user_id, game_type) VALUES (?,?)',
                    (uid, game_type)
                )

        # Grant after awards restore so the DELETE wipe can't remove it
        grant_award(conn, uid, 'first_user')
        grant_award(conn, uid, 'imported')

    return jsonify({'ok': True,
                    'words_imported': len(payload.get('progress', [])),
                    'user_name': import_username})


# ── Anki import helpers ───────────────────────────────────────────────────────

def _strip_anki(text):
    """Strip HTML tags and Anki sound/image references from a field value."""
    if not text:
        return ''
    text = re.sub(r'\[sound:[^\]]+\]', '', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _anki_db_path(token):
    return os.path.join(ANKI_TEMP_DIR, token, 'collection.anki2')


def _clean_old_anki_temps():
    from datetime import timezone as _tz
    cutoff = datetime.now(_tz.utc) - timedelta(hours=2)
    if not os.path.isdir(ANKI_TEMP_DIR):
        return
    for name in os.listdir(ANKI_TEMP_DIR):
        folder = os.path.join(ANKI_TEMP_DIR, name)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(folder), _tz.utc)
            if mtime < cutoff:
                shutil.rmtree(folder, ignore_errors=True)
        except OSError:
            pass


# ── Anki routes ───────────────────────────────────────────────────────────────

def _parse_anki_db(db_path):
    """Return (model_list, total_notes) by reading an extracted .anki2 DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    col_row = conn.execute('SELECT models FROM col').fetchone()
    raw_models = json.loads(col_row['models'])

    model_fields = {}
    for mid, m in raw_models.items():
        model_fields[str(mid)] = [f['name'] for f in m.get('flds', [])]

    notes = conn.execute('SELECT id, mid, flds FROM notes LIMIT 200').fetchall()
    conn.close()

    samples_by_model = {}
    for note in notes:
        mid  = str(note['mid'])
        flds = note['flds'].split('\x1f')
        if mid not in samples_by_model:
            samples_by_model[mid] = []
        if len(samples_by_model[mid]) < 5:
            samples_by_model[mid].append([_strip_anki(v) for v in flds])

    model_list = []
    for mid, fnames in model_fields.items():
        if mid in samples_by_model:
            model_list.append({'mid': mid, 'fields': fnames, 'samples': samples_by_model[mid]})

    total_notes = len(notes)
    try:
        conn2 = sqlite3.connect(db_path)
        total_notes = conn2.execute('SELECT COUNT(*) FROM notes').fetchone()[0]
        conn2.close()
    except Exception:
        pass

    return model_list, total_notes


def _extract_apkg(folder, apkg_path):
    """Unzip .apkg into folder, normalise DB name to collection.anki2. Returns error str or None."""
    try:
        with zipfile.ZipFile(apkg_path, 'r') as z:
            names = z.namelist()
            db_name = 'collection.anki2' if 'collection.anki2' in names else \
                      next((n for n in names if n.endswith('.anki2')), None)
            if not db_name:
                return 'No .anki2 database found in package'
            z.extract(db_name, folder)
            if db_name != 'collection.anki2':
                os.rename(os.path.join(folder, db_name),
                          os.path.join(folder, 'collection.anki2'))
    except zipfile.BadZipFile:
        return 'Invalid .apkg file'
    return None


@app.route('/anki')
def anki_page():
    uid = get_user_id()
    prefill_token = request.args.get('token', '')
    prefill_data  = None
    if prefill_token:
        db_path = _anki_db_path(prefill_token)
        if os.path.isfile(db_path):
            try:
                model_list, total = _parse_anki_db(db_path)
                prefill_data = json.dumps({'token': prefill_token, 'models': model_list, 'total': total})
            except Exception:
                prefill_data = None
    return render_template('anki_import.html', has_user=bool(uid), prefill_data=prefill_data)


@app.route('/anki/upload', methods=['POST'])
def anki_upload():
    """Accept .apkg via multipart form, unzip, inspect DB, return field names + samples."""
    _clean_old_anki_temps()
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'No user selected'}), 401

    f = request.files.get('apkg')
    if not f or not (f.filename or '').lower().endswith('.apkg'):
        return jsonify({'ok': False, 'error': 'Please upload a .apkg file'}), 400

    token = uuid.uuid4().hex
    folder = os.path.join(ANKI_TEMP_DIR, token)
    os.makedirs(folder, exist_ok=True)

    apkg_path = os.path.join(folder, 'upload.apkg')
    f.save(apkg_path)

    err = _extract_apkg(folder, apkg_path)
    if err:
        shutil.rmtree(folder, ignore_errors=True)
        return jsonify({'ok': False, 'error': err}), 400

    try:
        model_list, total_notes = _parse_anki_db(_anki_db_path(token))
    except Exception as e:
        shutil.rmtree(folder, ignore_errors=True)
        return jsonify({'ok': False, 'error': str(e)}), 500

    return jsonify({'ok': True, 'token': token, 'models': model_list, 'total': total_notes})


@app.route('/anki/upload_raw', methods=['POST'])
def anki_upload_raw():
    """Accept raw .apkg bytes (Android). Same response shape as /anki/upload."""
    _clean_old_anki_temps()
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'No user selected'}), 401

    raw = request.data
    if not raw:
        return jsonify({'ok': False, 'error': 'No data received'}), 400

    token = uuid.uuid4().hex
    folder = os.path.join(ANKI_TEMP_DIR, token)
    os.makedirs(folder, exist_ok=True)

    apkg_path = os.path.join(folder, 'upload.apkg')
    with open(apkg_path, 'wb') as fh:
        fh.write(raw)

    err = _extract_apkg(folder, apkg_path)
    if err:
        shutil.rmtree(folder, ignore_errors=True)
        return jsonify({'ok': False, 'error': err}), 400

    try:
        model_list, total_notes = _parse_anki_db(_anki_db_path(token))
    except Exception as e:
        shutil.rmtree(folder, ignore_errors=True)
        return jsonify({'ok': False, 'error': str(e)}), 500

    return jsonify({'ok': True, 'token': token, 'models': model_list, 'total': total_notes})


@app.route('/anki/import', methods=['POST'])
def anki_import():
    """Process field mapping and insert words + examples into the DB."""
    uid = get_user_id()
    if not uid:
        return jsonify({'ok': False, 'error': 'No user selected'}), 401

    data       = request.get_json()
    token      = data.get('token', '')
    mid        = str(data.get('mid', ''))
    list_name  = (data.get('list_name') or '').strip()
    mapping    = data.get('mapping', {})  # {hanzi, pinyin, english, example_hanzi, example_pinyin, example_english}

    if not token or not mid or not list_name:
        return jsonify({'ok': False, 'error': 'token, mid, and list_name are required'}), 400

    db_path = _anki_db_path(token)
    if not os.path.isfile(db_path):
        return jsonify({'ok': False, 'error': 'Upload session expired, please re-upload'}), 400

    hanzi_idx = mapping.get('hanzi')
    if hanzi_idx is None:
        return jsonify({'ok': False, 'error': 'Hanzi field mapping is required'}), 400

    # Index values come from JS as strings or ints
    def idx(key):
        v = mapping.get(key)
        return int(v) if v is not None and v != '' else None

    h_i  = idx('hanzi')
    py_i = idx('pinyin')
    en_i = idx('english')
    ex_h_i  = idx('example_hanzi')
    ex_py_i = idx('example_pinyin')
    ex_en_i = idx('example_english')

    try:
        anki_conn = sqlite3.connect(db_path)
        anki_conn.row_factory = sqlite3.Row
        notes = anki_conn.execute(
            'SELECT flds FROM notes WHERE mid = ?', (int(mid),)
        ).fetchall()
        anki_conn.close()
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

    now = now_str()
    imported = 0
    skipped  = 0

    with get_db() as conn:
        # Create the custom list
        cur = conn.execute(
            'INSERT INTO custom_lists (name, created_at, user_id, language) VALUES (?, ?, ?, ?)',
            (list_name, now, uid, get_language())
        )
        list_id = cur.lastrowid

        for note in notes:
            flds = note['flds'].split('\x1f')

            def get_field(i):
                if i is None or i >= len(flds):
                    return ''
                return _strip_anki(flds[i])

            hanzi   = get_field(h_i)
            pinyin  = get_field(py_i)
            english = get_field(en_i)

            if not hanzi:
                skipped += 1
                continue

            # Check if word already exists (any curriculum) to avoid exact dupes
            existing = conn.execute(
                'SELECT id FROM words WHERE hanzi = ? AND pinyin = ?',
                (hanzi, pinyin)
            ).fetchone()

            if existing:
                word_id = existing['id']
            else:
                cur2 = conn.execute(
                    '''INSERT INTO words (hanzi, pinyin, english, hsk_level, curriculum)
                       VALUES (?, ?, ?, ?, ?)''',
                    (hanzi, pinyin, english, 0, 'anki')
                )
                word_id = cur2.lastrowid

            # Link to list (ignore if already linked)
            conn.execute(
                'INSERT OR IGNORE INTO custom_list_words (list_id, word_id) VALUES (?, ?)',
                (list_id, word_id)
            )

            # Insert examples
            ex_h  = get_field(ex_h_i)
            ex_py = get_field(ex_py_i)
            ex_en = get_field(ex_en_i)
            if ex_h:
                conn.execute(
                    '''INSERT INTO word_examples (word_id, example_hanzi, example_pinyin, example_english, created_at)
                       VALUES (?, ?, ?, ?, ?)''',
                    (word_id, ex_h, ex_py, ex_en, now)
                )

            imported += 1

        conn.commit()

    # Clean up temp folder
    shutil.rmtree(os.path.join(ANKI_TEMP_DIR, token), ignore_errors=True)

    if uid:
        with get_db() as conn:
            grant_award(conn, uid, 'anki_imported')
    return jsonify({'ok': True, 'imported': imported, 'skipped': skipped, 'list_id': list_id})


if __name__ == '__main__':
    init_db()
    _init_jieba()
    app.run(port=5001, debug=True)
