"""Session-scoped local HTTP server for frontend tests.

Serves web/ on a free port and tears down at end of session. Tests get the
base URL via the `base_url` fixture.
"""
import socket
import subprocess
import time
from pathlib import Path

import pytest
import urllib.request


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def base_url():
    port = _free_port()
    web_dir = Path("web").resolve()
    assert (web_dir / "index.html").exists(), "run from repo root"

    proc = subprocess.Popen(
        ["python3", "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        cwd=str(web_dir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"

    # Wait for server to accept connections.
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "/", timeout=0.5):
                break
        except Exception:
            time.sleep(0.05)
    else:
        proc.terminate()
        raise RuntimeError("http.server did not start")

    yield url
    proc.terminate()
    proc.wait(timeout=5)
