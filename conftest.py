"""Put the repo root on sys.path so tests can import the pipeline modules.

pytest prepends the *test file's* directory (tests/), not the rootdir, when
there is no ini file and no packages. `python -m pytest` happens to work
because -m adds the cwd, but CI invokes plain `pytest tests/test_data.py`,
which does not -- so without this, `import payload` fails at collection time
in the workflow and passes locally.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
