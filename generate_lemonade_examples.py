"""
Generate Mandarin example sentences using a local Lemonade LLM server (OpenAI-compatible API).

No API key needed — runs entirely locally, no rate limits.

Usage:
    python generate_lemonade_examples.py                          # classic HSK 5 + 6, missing only
    python generate_lemonade_examples.py 5                        # classic HSK 5 only
    python generate_lemonade_examples.py 6 --curriculum hsk3     # New HSK 3.0 level 6
    python generate_lemonade_examples.py 6 --overwrite           # replace existing sentences too
    python generate_lemonade_examples.py 5 --url http://127.0.0.1:8000

Make sure Lemonade is running before starting.
"""

import sqlite3, json, sys, os, io, re
import requests

if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH    = os.path.join(os.path.dirname(__file__), 'vocab.db')
MODEL      = 'Qwen3-VL-8B-Instruct-GGUF'
BASE_URL   = 'http://127.0.0.1:8000'
BATCH_SIZE = 40

SYSTEM_PROMPT = """\
You generate short, natural Mandarin (普通话) example sentences for HSK vocabulary flashcards.

Rules:
- The target word MUST appear in the sentence exactly as given.
- Sentence length: 10–18 Chinese characters.
- Use vocabulary appropriate for the word's HSK level (HSK 5 = upper-intermediate, HSK 6 = advanced, HSK 7-9 = near-native/professional).
- English translation should be natural and idiomatic, not word-for-word.
- Return ONLY a valid JSON array — no markdown, no commentary, no explanation.

Output format (example):
[{"id": 123, "example_hanzi": "他的汉语水平很高。", "example_english": "His Chinese is excellent."}, ...]"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def pending_words(level: int, curriculum: str, overwrite: bool) -> list[dict]:
    if overwrite:
        where_example = ''
    else:
        where_example = "AND (example_hanzi LIKE '%Sentence%' OR example_hanzi LIKE '%Satz%' OR example_hanzi IS NULL OR example_hanzi = '')"
    with get_db() as conn:
        rows = conn.execute(f'''
            SELECT id, hanzi, pinyin, english, hsk_level
            FROM words
            WHERE curriculum = ? AND hsk_level = ?
              {where_example}
            ORDER BY id
        ''', (curriculum, level)).fetchall()
    return [dict(r) for r in rows]


def save_batch(results: list[dict], curriculum: str):
    with get_db() as conn:
        conn.executemany(
            'UPDATE words SET example_hanzi = ?, example_english = ? WHERE id = ? AND curriculum = ?',
            [(r['example_hanzi'], r['example_english'], r['id'], curriculum) for r in results]
        )


def call_api(base_url: str, batch: list[dict]) -> list[dict]:
    payload = [
        {'id': w['id'], 'hanzi': w['hanzi'], 'pinyin': w['pinyin'],
         'english': w['english'], 'hsk_level': w['hsk_level']}
        for w in batch
    ]
    user_msg = 'Generate example sentences for these words:\n' + \
               json.dumps(payload, ensure_ascii=False, indent=2)

    resp = requests.post(
        f'{base_url}/v1/chat/completions',
        json={
            'model': MODEL,
            'messages': [
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user',   'content': user_msg},
            ],
            'temperature': 0.7,
            'max_tokens': BATCH_SIZE * 200,
        },
        timeout=300,
    )
    resp.raise_for_status()

    message = resp.json()['choices'][0]['message']
    content   = (message.get('content')           or '').strip()
    reasoning = (message.get('reasoning_content') or '').strip()
    raw = content or reasoning

    outside = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()
    if not outside:
        m = re.search(r'<think>(.*?)(?:</think>|$)', raw, flags=re.DOTALL)
        candidate = m.group(1).strip() if m else raw
    else:
        candidate = outside
    if candidate.startswith('```'):
        candidate = candidate.split('\n', 1)[1].rsplit('```', 1)[0].strip()
    start = candidate.find('[')
    end   = candidate.rfind(']')
    if start == -1 or end == -1:
        raise ValueError(f'No JSON array in response: {candidate[:200]!r}')

    results = json.loads(candidate[start:end + 1])
    batch_ids = {w['id'] for w in batch}
    return [r for r in results if r.get('id') in batch_ids
            and r.get('example_hanzi') and r.get('example_english')]


def check_server(base_url: str):
    try:
        r = requests.get(f'{base_url}/v1/models', timeout=5)
        r.raise_for_status()
        models = [m['id'] for m in r.json().get('data', [])]
        print(f'Lemonade reachable. Available models: {models}')
        if MODEL not in models:
            print(f'  WARNING: "{MODEL}" not in model list above.')
    except Exception as e:
        print(f'ERROR: Cannot reach Lemonade at {base_url} — {e}')
        sys.exit(1)


def process_level(base_url: str, level: int, curriculum: str, overwrite: bool):
    words = pending_words(level, curriculum, overwrite)
    if not words:
        print(f'  HSK {level}: nothing to do, skipping.')
        return 0

    total   = len(words)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    print(f'\nHSK {level} ({curriculum}): {total} words, {batches} batches of {BATCH_SIZE}')

    done = 0
    for i in range(0, total, BATCH_SIZE):
        batch     = words[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f'  Batch {batch_num}/{batches} (words {i+1}–{min(i+len(batch), total)})... ',
              end='', flush=True)

        for attempt in range(4):
            try:
                results = call_api(base_url, batch)
                save_batch(results, curriculum)
                done += len(results)
                print(f'saved {len(results)}')
                break
            except json.JSONDecodeError as e:
                if attempt < 3:
                    print(f'JSON parse error, retrying... ', end='', flush=True)
                else:
                    print(f'JSON parse error after retries, skipping batch: {e}')
            except requests.RequestException as e:
                print(f'Request error: {e}')
                sys.exit(1)
            except Exception as e:
                print(f'Unexpected error: {e}')
                sys.exit(1)

    print(f'  HSK {level}: done. {done}/{total} sentences generated.')
    return done


def main():
    args = sys.argv[1:]

    base_url = BASE_URL
    if '--url' in args:
        idx = args.index('--url')
        base_url = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    curriculum = 'classic'
    if '--curriculum' in args:
        idx = args.index('--curriculum')
        curriculum = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    overwrite = '--overwrite' in args
    args = [a for a in args if a != '--overwrite']

    levels = [int(a) for a in args if a.isdigit()] or [5, 6]

    if not os.path.exists(DB_PATH):
        print(f'Database not found at {DB_PATH}')
        sys.exit(1)

    print(f'Curriculum: {curriculum}  |  Levels: {levels}  |  Overwrite: {overwrite}')
    print(f'Model: {MODEL}  |  Batch size: {BATCH_SIZE}  |  Server: {base_url}')

    check_server(base_url)

    total_done = 0
    for level in levels:
        total_done += process_level(base_url, level, curriculum, overwrite)

    print(f'\nAll done. {total_done} sentences generated.')


if __name__ == '__main__':
    main()
