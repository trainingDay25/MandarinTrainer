from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory, send_file
import sqlite3
import os
import json
import uuid
import random
import re
import unicodedata
import hashlib
import asyncio
import threading
import requests as _requests
import edge_tts
from datetime import datetime, timedelta, date as date_cls

app = Flask(__name__)
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, 'vocab.db')
TTS_CACHE  = os.path.join(BASE_DIR, 'tts_cache')
os.makedirs(TTS_CACHE, exist_ok=True)

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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.create_collation('PINYIN', _pinyin_collation)
    return conn

def init_db():
    with get_db() as conn:
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

        # Migrate study_sessions: add history + stat tracking columns
        scols = [r[1] for r in conn.execute('PRAGMA table_info(study_sessions)').fetchall()]
        for col, dflt in [
            ('history',      "'[]'"),
            ('stack_size',   '0'),
            ('new_words',    '0'),
            ('easy_count',   '0'),
            ('medium_count', '0'),
            ('wrong_count',  '0'),
            ('selection',    "'{}'"),
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

        # Dictionary table (populated by import_cedict.py)
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS dictionary (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                traditional TEXT NOT NULL,
                simplified  TEXT NOT NULL,
                pinyin      TEXT,
                english     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_dict_simplified  ON dictionary(simplified);
            CREATE INDEX IF NOT EXISTS idx_dict_traditional ON dictionary(traditional);
        ''')


# ── Time helpers ──────────────────────────────────────────────────────────────

def now_str():
    return datetime.now().isoformat()

def future_str(minutes):
    return (datetime.now() + timedelta(minutes=minutes)).isoformat()


def make_cloze_prompt(hanzi: str, example: str) -> str:
    if not example or not hanzi or hanzi not in example:
        return ''
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
                'INSERT INTO study_sessions (id, mode, created_at, queue) VALUES (?, ?, ?, ?)',
                (sid, mode or 'mix', now_str(), json.dumps(queue))
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
    now = now_str()
    try:
        with get_db() as conn:
            due_total = conn.execute(
                "SELECT COUNT(*) FROM progress WHERE due_at <= ?", (now,)
            ).fetchone()[0]
            new_total = conn.execute(
                """SELECT COUNT(*) FROM words w
                   LEFT JOIN progress p ON w.id = p.word_id
                   WHERE p.word_id IS NULL AND w.curriculum != 'custom'"""
            ).fetchone()[0]
        return dict(sidebar_due=due_total, sidebar_new=new_total)
    except Exception:
        return dict(sidebar_due=0, sidebar_new=0)

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    curriculum = request.args.get('curriculum', 'classic')
    now = now_str()
    with get_db() as conn:
        # Build curricula tab list
        curricula = [r[0] for r in conn.execute(
            "SELECT DISTINCT curriculum FROM words WHERE curriculum != 'custom' ORDER BY curriculum"
        ).fetchall()]
        if conn.execute('SELECT COUNT(*) FROM custom_lists').fetchone()[0] > 0:
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
                LEFT JOIN progress p ON clw.word_id = p.word_id
                GROUP BY cl.id
                ORDER BY cl.name
            ''', (now,)).fetchall()
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
                LEFT JOIN progress p ON w.id = p.word_id
                WHERE p.word_id IS NULL AND w.curriculum = ?
            ''', (curriculum,)).fetchone()[0]
            due_cnt = conn.execute('''
                SELECT COUNT(*) FROM progress p
                JOIN words w ON w.id = p.word_id
                WHERE p.due_at <= ? AND w.curriculum = ?
            ''', (now, curriculum)).fetchone()[0]
            hsk_rows = conn.execute('''
                SELECT
                    w.hsk_level,
                    COUNT(*) AS total,
                    SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END) AS new_cnt,
                    SUM(CASE WHEN p.due_at <= ? AND p.word_id IS NOT NULL
                             THEN 1 ELSE 0 END) AS due_cnt
                FROM words w
                LEFT JOIN progress p ON w.id = p.word_id
                WHERE w.curriculum = ?
                GROUP BY w.hsk_level
                ORDER BY w.hsk_level
            ''', (now, curriculum)).fetchall()
            hsk_stats = {row['hsk_level']: dict(row) for row in hsk_rows}

    return render_template('index.html', total=total, new_cnt=new_cnt, due_cnt=due_cnt,
                           hsk_stats=hsk_stats, custom_lists=custom_lists,
                           curriculum=curriculum, curricula=curricula)


@app.route('/start', methods=['POST'])
def start():
    mode            = request.form.get('mode', 'mix')
    limit           = int(request.form.get('limit', 20))
    hsk_levels      = [int(x) for x in request.form.getlist('hsk') if x.isdigit()]
    curriculum      = request.form.get('curriculum', 'classic')
    grade_filter    = set(request.form.getlist('grades'))   # e.g. {'new','wrong'}
    include_overdue = request.form.get('include_overdue') == '1'
    raw_list_id     = request.form.get('list_id', '').strip()
    list_id         = int(raw_list_id) if raw_list_id.isdigit() else None
    sid             = get_sid()

    now = now_str()

    if list_id:
        filter_frag = 'AND w.id IN (SELECT word_id FROM custom_list_words WHERE list_id = ?)'
        filters     = [list_id]
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
        # ── 1. Overdue words ──────────────────────────────────────────────────
        if default_mode or include_overdue:
            rows = conn.execute(f'''
                SELECT w.id FROM words w
                JOIN progress p ON w.id = p.word_id
                WHERE p.due_at <= ? {filter_frag}
                ORDER BY p.due_at ASC
            ''', [now] + filters).fetchall()
            for r in rows:
                queue_map[r['id']] = 'overdue'

        # ── 2. New words ──────────────────────────────────────────────────────
        if default_mode or 'new' in grade_filter:
            new_limit = max(0, limit - len(queue_map)) if default_mode else limit
            rows = conn.execute(f'''
                SELECT w.id FROM words w
                LEFT JOIN progress p ON w.id = p.word_id
                WHERE p.word_id IS NULL {filter_frag}
                ORDER BY RANDOM()
                LIMIT ?
            ''', filters + [new_limit]).fetchall()
            for r in rows:
                if r['id'] not in queue_map:
                    queue_map[r['id']] = 'new'

        # ── 3. Seen-grade words (explicit filter only, no limit) ──────────────
        seen_grades = grade_filter - {'new'}
        if seen_grades:
            ph = ','.join('?' * len(seen_grades))
            rows = conn.execute(f'''
                SELECT w.id, p.last_grade FROM words w
                JOIN progress p ON w.id = p.word_id
                WHERE p.last_grade IN ({ph}) {filter_frag}
            ''', list(seen_grades) + filters).fetchall()
            for r in rows:
                if r['id'] not in queue_map:
                    queue_map[r['id']] = r['last_grade']

    queue = _shuffle_queue(queue_map)
    save_queue(sid, queue, mode)

    if list_id:
        with get_db() as conn:
            lst = conn.execute('SELECT name FROM custom_lists WHERE id = ?', (list_id,)).fetchone()
        selection = json.dumps({
            'type':      'custom_list',
            'list_id':   list_id,
            'list_name': lst['name'] if lst else 'Custom list',
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
    sid = get_sid()
    now = now_str()
    with get_db() as conn:
        overdue = conn.execute('''
            SELECT w.id FROM words w
            JOIN progress p ON w.id = p.word_id
            JOIN custom_list_words clw ON w.id = clw.word_id
            WHERE clw.list_id = ? AND p.due_at <= ?
            ORDER BY p.due_at ASC
        ''', (list_id, now)).fetchall()
        new_words = conn.execute('''
            SELECT w.id FROM words w
            JOIN custom_list_words clw ON w.id = clw.word_id
            LEFT JOIN progress p ON w.id = p.word_id
            WHERE clw.list_id = ? AND p.word_id IS NULL
            ORDER BY RANDOM()
            LIMIT ?
        ''', (list_id, max(0, 20 - len(overdue)))).fetchall()
        lst = conn.execute('SELECT name FROM custom_lists WHERE id = ?', (list_id,)).fetchone()

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

    with get_db() as conn:
        word = conn.execute('''
            SELECT w.*, p.interval_step, p.last_grade, p.last_seen
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id
            WHERE w.id = ?
        ''', (card['wid'],)).fetchone()

    if not word:
        return api_next()

    step = word['interval_step'] if word['interval_step'] is not None else -1
    wl, ml, el = grade_labels(step)

    return jsonify({
        'word_id':          word['id'],
        'hanzi':            word['hanzi'],
        'pinyin':           word['pinyin'],
        'english':          word['english'],
        'example_hanzi':    word['example_hanzi'],
        'example_pinyin':   word['example_pinyin'],
        'example_english':  word['example_english'],
        'cloze_prompt':     make_cloze_prompt(word['hanzi'] or '', word['example_hanzi'] or ''),
        'audio_file':       word['audio_file'],
        'hsk_level':        word['hsk_level'],
        'interval_step':    step,
        'last_grade':       word['last_grade'],
        'status':           card['status'],
        'mode':             mode,
        'remaining':        len(queue),
        'wrong_label':      wl,
        'medium_label':     ml,
        'easy_label':       el,
        'can_go_back':      bool(load_history(sid)),
    })


@app.route('/api/grade', methods=['POST'])
def api_grade():
    data    = request.json
    word_id = data['word_id']
    grade   = data['grade']   # 'wrong' | 'medium' | 'easy'
    sid     = get_sid()
    queue, mode = load_queue(sid)

    with get_db() as conn:
        prog = conn.execute(
            'SELECT interval_step, last_grade, due_at FROM progress WHERE word_id = ?',
            (word_id,)
        ).fetchone()

        step     = prog['interval_step'] if prog else -1
        new_step = next_step(step, grade)
        db_due   = future_str(INTERVALS[new_step])

        conn.execute('''
            INSERT INTO progress (word_id, interval_step, last_grade, last_seen, due_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(word_id) DO UPDATE SET
                interval_step = excluded.interval_step,
                last_grade    = excluded.last_grade,
                last_seen     = excluded.last_seen,
                due_at        = excluded.due_at
        ''', (word_id, new_step, grade, now_str(), db_due))

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
                INSERT INTO progress (word_id, interval_step, last_grade, last_seen, due_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(word_id) DO UPDATE SET
                    interval_step = excluded.interval_step,
                    last_grade    = excluded.last_grade,
                    last_seen     = excluded.last_seen,
                    due_at        = excluded.due_at
            ''', (m['id'], new_step, grade, now_str(), db_due))

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
    with get_db() as conn:
        if entry['prev_step'] is None:
            conn.execute('DELETE FROM progress WHERE word_id = ?', (entry['wid'],))
        else:
            conn.execute('''
                UPDATE progress
                SET interval_step = ?, last_grade = ?, due_at = ?
                WHERE word_id = ?
            ''', (entry['prev_step'], entry['prev_grade'], entry['prev_due_at'], entry['wid']))

        word = conn.execute('''
            SELECT w.*, p.interval_step, p.last_grade, p.last_seen
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id
            WHERE w.id = ?
        ''', (entry['wid'],)).fetchone()

    save_queue(sid, queue)

    if not word:
        return jsonify({'ok': False, 'error': 'word not found'})

    step = word['interval_step'] if word['interval_step'] is not None else -1
    wl, ml, el = grade_labels(step)

    session['current_card'] = {'wid': entry['wid'], 'status': entry['status']}
    session.modified = True

    return jsonify({
        'ok':               True,
        'word_id':          word['id'],
        'hanzi':            word['hanzi'],
        'pinyin':           word['pinyin'],
        'english':          word['english'],
        'example_hanzi':    word['example_hanzi'],
        'example_pinyin':   word['example_pinyin'],
        'example_english':  word['example_english'],
        'cloze_prompt':     make_cloze_prompt(word['hanzi'] or '', word['example_hanzi'] or ''),
        'audio_file':       word['audio_file'],
        'hsk_level':        word['hsk_level'],
        'interval_step':    step,
        'last_grade':       word['last_grade'],
        'status':           entry['status'],
        'mode':             mode,
        'remaining':        len(queue),
        'wrong_label':      wl,
        'medium_label':     ml,
        'easy_label':       el,
        'can_go_back':      bool(history),  # history already has the entry popped
    })


@app.route('/debug/queue')
def debug_queue():
    now = datetime.now()

    with get_db() as conn:
        # Find the most recently created session that still has cards in its queue
        session_row = conn.execute('''
            SELECT id, mode, queue, created_at FROM study_sessions
            WHERE queue != '[]' AND queue IS NOT NULL AND queue != ''
            ORDER BY created_at DESC LIMIT 1
        ''').fetchone()

    if not session_row:
        return render_template('debug_queue.html', rows=[], mode='—', sid='—')

    sid   = session_row['id']
    mode  = session_row['mode']
    queue = json.loads(session_row['queue'])

    if not queue:
        return render_template('debug_queue.html', rows=[], mode=mode, sid=sid[:8])

    wids = [c['wid'] for c in queue]
    with get_db() as conn:
        word_rows = conn.execute(
            f'SELECT id, hanzi, pinyin, english, hsk_level, curriculum FROM words WHERE id IN ({",".join("?"*len(wids))})',
            wids
        ).fetchall()
    word_map = {r['id']: dict(r) for r in word_rows}

    rows = []
    for i, c in enumerate(queue):
        w = word_map.get(c['wid'], {})
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


@app.route('/words')
def words():
    hsk        = request.args.get('hsk', '1')
    curriculum = request.args.get('curriculum', 'classic')
    raw_lid    = request.args.get('list_id', '').strip()
    list_id    = int(raw_lid) if raw_lid.isdigit() else None

    with get_db() as conn:
        # ── Standalone custom-list view (from /custom page or sidebar) ─────────
        if list_id and curriculum != 'custom':
            lst = conn.execute('SELECT id, name FROM custom_lists WHERE id = ?', (list_id,)).fetchone()
            rows = conn.execute('''
                SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                FROM words w
                JOIN custom_list_words clw ON w.id = clw.word_id
                LEFT JOIN progress p ON w.id = p.word_id
                WHERE clw.list_id = ?
                ORDER BY w.id
            ''', (list_id,)).fetchall()
            return render_template('words.html', words=rows, intervals=INTERVALS,
                                   active_hsk=None, active_curriculum=None,
                                   level_counts=[], curricula=[],
                                   list_id=list_id,
                                   list_name=lst['name'] if lst else 'Custom list',
                                   custom_lists=[], active_custom_list_id=None, active_custom_list_name=None)

        # ── Build tab-bar curricula for all non-standalone views ──────────────
        curricula = [r[0] for r in conn.execute(
            "SELECT DISTINCT curriculum FROM words WHERE curriculum != 'custom' ORDER BY curriculum"
        ).fetchall()]
        if conn.execute('SELECT COUNT(*) FROM custom_lists').fetchone()[0] > 0:
            curricula.append('custom')

        # ── Custom Lists browse view ───────────────────────────────────────────
        if curriculum == 'custom':
            cl_rows = conn.execute(
                'SELECT id, name FROM custom_lists ORDER BY name'
            ).fetchall()
            custom_lists = [dict(r) for r in cl_rows]

            if list_id:
                rows = conn.execute('''
                    SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                    FROM words w
                    JOIN custom_list_words clw ON w.id = clw.word_id
                    LEFT JOIN progress p ON w.id = p.word_id
                    WHERE clw.list_id = ?
                    ORDER BY w.pinyin COLLATE PINYIN
                ''', (list_id,)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT DISTINCT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                    FROM words w
                    JOIN custom_list_words clw ON w.id = clw.word_id
                    LEFT JOIN progress p ON w.id = p.word_id
                    ORDER BY w.pinyin COLLATE PINYIN
                ''').fetchall()

            active_list_name = next(
                (cl['name'] for cl in custom_lists if cl['id'] == list_id), None
            ) if list_id else None

            return render_template('words.html', words=rows, intervals=INTERVALS,
                                   active_hsk=None, active_curriculum='custom',
                                   level_counts=[], curricula=curricula,
                                   list_id=None, list_name=None,
                                   custom_lists=custom_lists,
                                   active_custom_list_id=list_id,
                                   active_custom_list_name=active_list_name)

        # ── Standard curriculum view (classic / hsk3) ─────────────────────────
        level_counts = conn.execute('''
            SELECT w.hsk_level,
                   COUNT(*)                                               AS total,
                   SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END)    AS new_cnt,
                   SUM(CASE WHEN p.word_id IS NOT NULL THEN 1 ELSE 0 END) AS seen_cnt
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id
            WHERE w.curriculum = ?
            GROUP BY w.hsk_level
            ORDER BY w.hsk_level
        ''', (curriculum,)).fetchall()

        if hsk != 'all' and hsk.isdigit():
            rows = conn.execute('''
                SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                FROM words w
                LEFT JOIN progress p ON w.id = p.word_id
                WHERE w.hsk_level = ? AND w.curriculum = ?
                ORDER BY w.pinyin COLLATE PINYIN
            ''', (int(hsk), curriculum)).fetchall()
        else:
            rows = conn.execute('''
                SELECT w.*, p.interval_step, p.last_grade, p.last_seen, p.due_at
                FROM words w
                LEFT JOIN progress p ON w.id = p.word_id
                WHERE w.curriculum = ?
                ORDER BY w.hsk_level, w.pinyin COLLATE PINYIN
            ''', (curriculum,)).fetchall()

    return render_template('words.html', words=rows, intervals=INTERVALS,
                           active_hsk=hsk, active_curriculum=curriculum,
                           level_counts=level_counts, curricula=curricula,
                           list_id=None, list_name=None,
                           custom_lists=[], active_custom_list_id=None, active_custom_list_name=None)


@app.route('/grammar')
def grammar():
    level = request.args.get('level', 'all')
    with get_db() as conn:
        if level != 'all':
            try:
                rows = conn.execute(
                    '''SELECT id, title, hsk_level, used_for, structures, has_detail, favorited
                       FROM grammar_points WHERE hsk_level = ?
                       ORDER BY title''', (int(level),)
                ).fetchall()
            except (ValueError, sqlite3.OperationalError):
                rows = []
        else:
            try:
                rows = conn.execute(
                    '''SELECT id, title, hsk_level, used_for, structures, has_detail, favorited
                       FROM grammar_points ORDER BY hsk_level, title'''
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
    return render_template('grammar.html',
                           points=[dict(r) for r in rows],
                           active_level=level)


@app.route('/api/grammar/<asg_id>/favorite', methods=['POST'])
def api_grammar_favorite(asg_id):
    with get_db() as conn:
        row = conn.execute('SELECT favorited FROM grammar_points WHERE id=?', (asg_id,)).fetchone()
        if not row:
            return jsonify({'error': 'not found'}), 404
        new_val = 0 if row['favorited'] else 1
        conn.execute('UPDATE grammar_points SET favorited=? WHERE id=?', (new_val, asg_id))
        conn.commit()
    return jsonify({'favorited': new_val})


@app.route('/api/grammar/<asg_id>')
def api_grammar_point(asg_id):
    with get_db() as conn:
        point = conn.execute(
            '''SELECT id, title, hsk_level, url, used_for, structures, detail_html, favorited
               FROM grammar_points WHERE id = ?''', (asg_id,)
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
    with get_db() as conn:
        # ── 1. Top line numbers ───────────────────────────────────────────────
        known_total = conn.execute(
            'SELECT COUNT(*) FROM progress WHERE interval_step >= 6'
        ).fetchone()[0]

        totals = conn.execute('''
            SELECT
                COALESCE(SUM(CAST(easy_count   AS INTEGER)), 0) AS easy,
                COALESCE(SUM(CAST(medium_count AS INTEGER)), 0) AS medium,
                COALESCE(SUM(CAST(wrong_count  AS INTEGER)), 0) AS wrong
            FROM study_sessions
        ''').fetchone()
        total_reviews = totals['easy'] + totals['medium'] + totals['wrong']

        session_dates = conn.execute(
            "SELECT DISTINCT DATE(created_at) AS d FROM study_sessions ORDER BY d DESC"
        ).fetchall()
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
        level_rows = conn.execute('''
            SELECT w.curriculum, w.hsk_level,
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END), 0)            AS new_cnt,
                COALESCE(SUM(CASE WHEN p.last_grade = 'wrong'  THEN 1 ELSE 0 END), 0)     AS wrong_cnt,
                COALESCE(SUM(CASE WHEN p.last_grade = 'medium' THEN 1 ELSE 0 END), 0)     AS medium_cnt,
                COALESCE(SUM(CASE WHEN p.last_grade = 'easy'   THEN 1 ELSE 0 END), 0)     AS easy_cnt,
                COALESCE(SUM(CASE WHEN p.interval_step >= 6    THEN 1 ELSE 0 END), 0)     AS known_cnt
            FROM words w
            LEFT JOIN progress p ON w.id = p.word_id
            WHERE w.curriculum IN ('classic', 'hsk3')
            GROUP BY w.curriculum, w.hsk_level
            ORDER BY w.curriculum DESC, w.hsk_level
        ''').fetchall()

        level_stats = {'classic': [], 'hsk3': []}
        for r in level_rows:
            curr = r['curriculum']
            if curr not in level_stats:
                continue
            t = r['total']
            level_stats[curr].append({
                'level':      r['hsk_level'],
                'total':      t,
                'new':        r['new_cnt'],
                'wrong':      r['wrong_cnt'],
                'medium':     r['medium_cnt'],
                'easy':       r['easy_cnt'],
                'known':      r['known_cnt'],
                'seen':       t - r['new_cnt'],
                'new_pct':    round(r['new_cnt']    / t * 100) if t else 0,
                'wrong_pct':  round(r['wrong_cnt']  / t * 100) if t else 0,
                'medium_pct': round(r['medium_cnt'] / t * 100) if t else 0,
                'easy_pct':   round(r['easy_cnt']   / t * 100) if t else 0,
                'known_pct':  round(r['known_cnt']  / t * 100) if t else 0,
            })

        # ── 4. Due forecast ───────────────────────────────────────────────────
        due = conn.execute('''
            SELECT
                COALESCE(SUM(CASE WHEN due_at < datetime('now') THEN 1 ELSE 0 END), 0) AS overdue,
                COALESCE(SUM(CASE WHEN due_at >= datetime('now')
                                   AND due_at <  datetime('now','+1 day')  THEN 1 ELSE 0 END), 0) AS today,
                COALESCE(SUM(CASE WHEN due_at >= datetime('now','+1 day')
                                   AND due_at <  datetime('now','+2 days') THEN 1 ELSE 0 END), 0) AS tomorrow,
                COALESCE(SUM(CASE WHEN due_at >= datetime('now','+2 days')
                                   AND due_at <  datetime('now','+7 days') THEN 1 ELSE 0 END), 0) AS rest_week
            FROM progress WHERE due_at IS NOT NULL
        ''').fetchone()

        # ── 5. Activity calendar (last 90 days) ───────────────────────────────
        activity_rows = conn.execute('''
            SELECT DATE(started_at) AS d,
                SUM(CAST(easy_count AS INTEGER) +
                    CAST(medium_count AS INTEGER) +
                    CAST(wrong_count  AS INTEGER)) AS reviews
            FROM session_logs
            WHERE started_at >= date('now','-90 days')
            GROUP BY DATE(started_at)
        ''').fetchall()
        activity = {r['d']: int(r['reviews']) for r in activity_rows}

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

    return render_template('stats.html',
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
        due_rest_week = int(due['rest_week']),
        cal_days      = cal_days,
    )


@app.route('/sessions')
def sessions():
    curriculum_labels = {'classic': 'Classic HSK', 'hsk3': 'New HSK 3.0'}
    with get_db() as conn:
        logs = conn.execute(
            'SELECT * FROM session_logs WHERE total_seen > 0 ORDER BY started_at DESC'
        ).fetchall()

    entries = []
    for log in logs:
        sel = json.loads(log['selection'] or '{}')

        if sel.get('type') == 'custom_list':
            parts = ['Custom: ' + sel.get('list_name', 'Unknown')]
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


@app.route('/api/tts')
def api_tts():
    text = request.args.get('text', '').strip()
    if not text:
        return ('', 400)
    cache_file = os.path.join(TTS_CACHE, hashlib.md5(text.encode()).hexdigest() + '.mp3')
    if not os.path.exists(cache_file):
        asyncio.run(edge_tts.Communicate(text, voice='zh-CN-XiaoxiaoNeural', rate='-10%').save(cache_file))
    return send_file(cache_file, mimetype='audio/mpeg')


@app.route('/end')
def end_session():
    raw_next = request.args.get('next', '/')
    # Only allow relative paths to prevent open-redirect
    next_url = raw_next if (raw_next.startswith('/') and not raw_next.startswith('//')) else '/'

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
                if total > 0:
                    quality  = (easy + 0.5 * medium) / total
                    new_frac = new_w / total
                    score    = round(quality * (0.65 + 0.35 * new_frac), 4)
                else:
                    score = 0.0
                conn.execute('''
                    INSERT INTO session_logs
                        (started_at, ended_at, stack_size, total_seen, new_words,
                         easy_count, medium_count, wrong_count, selection, score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row['created_at'], now_str(), row['stack_size'] or 0,
                      total, new_w, easy, medium, wrong, row['selection'], score))
            conn.execute('DELETE FROM study_sessions WHERE id = ?', (sid,))
    session.clear()
    return redirect(next_url)


def _dict_search(q, field='all', limit=80, offset=0):
    """Search dictionary by field. Returns (results, has_more).

    Ranking uses 3 tiers so exact / prefix matches always surface first:
      0 = exact match (whole field or whole syllable)
      1 = prefix / starts-with match
      2 = substring anywhere
    """
    is_chinese = any('一' <= c <= '鿿' for c in q)
    fetch      = limit + 1
    like       = f'%{q}%'
    ql         = q.lower()

    with get_db() as conn:

        # ── Hanzi (simplified / all-chinese) ─────────────────────────────────
        if field == 'simplified' or (field == 'all' and is_chinese):
            if field == 'all':
                where  = 'simplified LIKE ? OR traditional LIKE ?'
                wparams = [like, like]
            else:
                where  = 'simplified LIKE ?'
                wparams = [like]
            rows = conn.execute(f'''
                SELECT id, traditional, simplified, pinyin, english
                FROM dictionary WHERE {where}
                ORDER BY
                  CASE
                    WHEN simplified = ?           THEN 0
                    WHEN simplified LIKE ?        THEN 1
                    ELSE                               2
                  END,
                  length(simplified)
                LIMIT ? OFFSET ?
            ''', wparams + [q, q + '%', fetch, offset]).fetchall()

        # ── Traditional ───────────────────────────────────────────────────────
        elif field == 'traditional':
            rows = conn.execute('''
                SELECT id, traditional, simplified, pinyin, english
                FROM dictionary WHERE traditional LIKE ?
                ORDER BY
                  CASE
                    WHEN traditional = ?    THEN 0
                    WHEN traditional LIKE ? THEN 1
                    ELSE                         2
                  END,
                  length(simplified)
                LIMIT ? OFFSET ?
            ''', [like, q, q + '%', fetch, offset]).fetchall()

        # ── Pinyin ────────────────────────────────────────────────────────────
        elif field == 'pinyin':
            pattern = pinyin_to_like_pattern(q)
            if not pattern:
                return [], False
            # Without surrounding % → exact syllable check (e.g. 'da_' ≠ 'dao3')
            rows = conn.execute('''
                SELECT id, traditional, simplified, pinyin, english
                FROM dictionary WHERE pinyin LIKE ?
                ORDER BY
                  CASE
                    WHEN pinyin LIKE ?        THEN 0
                    WHEN pinyin LIKE ?        THEN 1
                    ELSE                           2
                  END,
                  length(simplified)
                LIMIT ? OFFSET ?
            ''', [f'%{pattern}%', pattern, pattern + ' %', fetch, offset]).fetchall()

        # ── English (also handles 'all' non-Chinese) ──────────────────────────
        else:
            rows = conn.execute('''
                SELECT id, traditional, simplified, pinyin, english
                FROM dictionary WHERE english LIKE ?
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
    q      = request.args.get('q', '').strip()
    field  = request.args.get('field', 'all')
    offset = max(0, int(request.args.get('offset', 0) or 0))
    fmt    = request.args.get('format', 'html')

    with get_db() as conn:
        has_data = conn.execute('SELECT COUNT(*) FROM dictionary').fetchone()[0] > 0

    results  = []
    has_more = False
    ready    = False

    if q and has_data:
        results, has_more = _dict_search(q, field, limit=80, offset=offset)
        ready = True

    if fmt == 'json':
        return jsonify({'results': results, 'has_more': has_more})

    return render_template('dictionary.html', q=q, field=field, results=results,
                           has_data=has_data, ready=ready, has_more=has_more)


@app.route('/custom')
def custom_lists_page():
    now = now_str()
    with get_db() as conn:
        rows = conn.execute('''
            SELECT cl.id, cl.name, cl.created_at,
                   COUNT(DISTINCT clw.word_id) AS total,
                   SUM(CASE WHEN p.word_id IS NULL THEN 1 ELSE 0 END) AS new_cnt,
                   SUM(CASE WHEN p.due_at <= ? AND p.word_id IS NOT NULL THEN 1 ELSE 0 END) AS due_cnt
            FROM custom_lists cl
            LEFT JOIN custom_list_words clw ON cl.id = clw.list_id
            LEFT JOIN progress p ON clw.word_id = p.word_id
            GROUP BY cl.id
            ORDER BY cl.name
        ''', (now,)).fetchall()
    return render_template('custom.html', lists=[dict(r) for r in rows])


@app.route('/api/custom-lists')
def api_custom_lists():
    with get_db() as conn:
        rows = conn.execute('SELECT id, name FROM custom_lists ORDER BY name').fetchall()
    return jsonify({'lists': [dict(r) for r in rows]})


@app.route('/api/custom-list/add', methods=['POST'])
def api_custom_list_add():
    data      = request.json
    dict_ids  = data.get('word_ids', [])   # dictionary.id values
    list_id   = data.get('list_id')        # int or None
    list_name = (data.get('list_name') or '').strip()

    if not dict_ids:
        return jsonify({'ok': False, 'error': 'No words selected'})

    with get_db() as conn:
        if list_id:
            if not conn.execute('SELECT id FROM custom_lists WHERE id = ?', (list_id,)).fetchone():
                return jsonify({'ok': False, 'error': 'List not found'})
        else:
            if not list_name:
                return jsonify({'ok': False, 'error': 'List name required'})
            cur = conn.execute(
                'INSERT INTO custom_lists (name, created_at) VALUES (?, ?)',
                (list_name, now_str())
            )
            list_id = cur.lastrowid

        added = 0
        for dict_id in dict_ids:
            d = conn.execute(
                'SELECT simplified, pinyin, english FROM dictionary WHERE id = ?', (dict_id,)
            ).fetchone()
            if not d:
                continue

            # Reuse existing word row (any curriculum) matched by hanzi
            existing = conn.execute(
                'SELECT id FROM words WHERE hanzi = ? LIMIT 1', (d['simplified'],)
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

    return jsonify({'ok': True, 'list_id': list_id, 'added': added})


@app.route('/api/custom-list/delete', methods=['POST'])
def api_custom_list_delete():
    list_id = request.json.get('list_id')
    if not list_id:
        return jsonify({'ok': False, 'error': 'list_id required'})
    with get_db() as conn:
        conn.execute('DELETE FROM custom_list_words WHERE list_id = ?', (list_id,))
        conn.execute('DELETE FROM custom_lists WHERE id = ?', (list_id,))
        # Clean up custom words that no longer belong to any list
        conn.execute('''
            DELETE FROM progress WHERE word_id IN (
                SELECT id FROM words WHERE curriculum = 'custom'
                AND id NOT IN (SELECT word_id FROM custom_list_words)
            )
        ''')
        conn.execute('''
            DELETE FROM words WHERE curriculum = 'custom'
            AND id NOT IN (SELECT word_id FROM custom_list_words)
        ''')
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
    data    = request.get_json()
    list_id = data.get('list_id')
    name    = (data.get('name') or '').strip()
    if not list_id or not name:
        return jsonify({'ok': False, 'error': 'list_id and name required'})
    with get_db() as conn:
        conn.execute('UPDATE custom_lists SET name = ? WHERE id = ?', (name, list_id))
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
    curriculum = request.form.get('curriculum', 'classic')
    hsk_level  = request.form.get('hsk_level', '')
    raw_next   = request.form.get('next', '/words')
    next_url   = raw_next if (raw_next.startswith('/') and not raw_next.startswith('//')) else '/words'

    with get_db() as conn:
        if hsk_level and hsk_level.isdigit():
            conn.execute('''
                DELETE FROM progress WHERE word_id IN (
                    SELECT id FROM words WHERE curriculum = ? AND hsk_level = ?
                )
            ''', (curriculum, int(hsk_level)))
        else:
            conn.execute('''
                DELETE FROM progress WHERE word_id IN (
                    SELECT id FROM words WHERE curriculum = ?
                )
            ''', (curriculum,))

    return redirect(next_url)


@app.route('/reset/sessions', methods=['POST'])
def reset_session_logs():
    with get_db() as conn:
        conn.execute('DELETE FROM session_logs')
    return redirect('/sessions')


if __name__ == '__main__':
    init_db()
    app.run(port=5001, debug=True)
