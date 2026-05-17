# Thin development launcher. All logic lives in src/mandarin_trainer/server.py.
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from mandarin_trainer.server import app, init_db, _init_jieba  # noqa: E402

if __name__ == '__main__':
    init_db()
    _init_jieba()
    app.run(host='127.0.0.1', port=5001, debug=True, use_reloader=True)
