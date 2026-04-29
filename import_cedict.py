import sqlite3
import re
import os
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(BASE_DIR, 'vocab.db')
CEDICT_PATH = os.path.join(BASE_DIR, 'cedict_1_0_ts_utf-8_mdbg.txt')

# Traditional Simplified [pinyin] /def1/def2/.../
ENTRY_RE = re.compile(r'^(\S+)\s+(\S+)\s+\[([^\]]+)\]\s+/(.+)/\s*$')

def run():
    conn = sqlite3.connect(DB_PATH)
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
    conn.execute('DELETE FROM dictionary')
    conn.commit()

    count = 0
    batch = []

    with open(CEDICT_PATH, encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('#') or not line.strip():
                continue
            m = ENTRY_RE.match(line)
            if not m:
                continue
            traditional, simplified, pinyin, defs_raw = m.groups()
            # defs_raw is "def1/def2/def3" — the surrounding slashes already consumed
            english = '; '.join(d.strip() for d in defs_raw.split('/') if d.strip())
            batch.append((traditional, simplified, pinyin, english))
            count += 1
            if len(batch) >= 1000:
                conn.executemany(
                    'INSERT INTO dictionary (traditional, simplified, pinyin, english) VALUES (?,?,?,?)',
                    batch
                )
                conn.commit()
                batch.clear()
                print(f'\r  {count:,} entries...', end='', flush=True)

    if batch:
        conn.executemany(
            'INSERT INTO dictionary (traditional, simplified, pinyin, english) VALUES (?,?,?,?)',
            batch
        )
        conn.commit()

    conn.close()
    print(f'\rDone — {count:,} entries imported into dictionary table.')

if __name__ == '__main__':
    run()
