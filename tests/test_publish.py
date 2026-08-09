"""Guards on what actually gets published to Cloudflare Pages.

Two failure modes this exists to catch:

1. Over-publishing. The old vercel.json had no outputDirectory, so Vercel
   published the whole repository root -- build_dashboard.py, r2_sync.py,
   config.yaml, CLAUDE.md and the CI workflow were all live at
   dod-contract-vehicles.abigailhaddad.com with HTTP 200. The publish
   directory is now web/ and must contain the website and nothing else.

2. File size. Cloudflare Pages rejects any single file over 25 MiB. The
   dashboard payloads (vehicles.json ~80 MB, families.json ~40 MB) are far
   over that, which is fine only because they are served from R2 and
   web/data/ is gitignored. If that ever stops being true, this fails.

The "publish set" is defined as the files git would ship for a Pages git
integration build with output directory web/: tracked files plus untracked
files that are not gitignored.
"""
import subprocess
from pathlib import Path

import pytest

PUBLISH_DIR = Path("web")

# Cloudflare Pages hard per-file limit.
MAX_FILE_BYTES = 25 * 1024 * 1024  # 26_214_400

# Cloudflare Pages limit is 20,000 files per deployment.
MAX_FILE_COUNT = 20_000

# Extensions a static site is allowed to ship.
ALLOWED_SUFFIXES = {
    ".html", ".css", ".js", ".mjs", ".json", ".map",
    ".svg", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".ico",
    ".woff", ".woff2", ".ttf", ".txt", ".xml", ".webmanifest",
}

# Extensionless Pages control files that live in the publish directory.
ALLOWED_NAMES = {"_redirects", "_headers", "_routes.json", "robots.txt"}

# Things that must never be reachable over HTTP.
FORBIDDEN_SUFFIXES = {".py", ".yaml", ".yml", ".md", ".env", ".csv", ".zip", ".sh", ".pyc"}
FORBIDDEN_NAMES = {".env", ".DS_Store", "CLAUDE.md", "vercel.json", ".vercelignore"}


def _git(*args) -> list[str]:
    out = subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True,
    ).stdout
    return [line for line in out.splitlines() if line]


@pytest.fixture(scope="module")
def publish_set() -> list[Path]:
    """Files a Pages git-integration deploy of web/ would contain."""
    assert (PUBLISH_DIR / "index.html").exists(), "run from repo root"
    paths = [Path(p) for p in _git("ls-files", "-co", "--exclude-standard", str(PUBLISH_DIR))]
    assert paths, "publish set is empty -- git ls-files found nothing under web/"
    return paths


# -----------------------------------------------------------------------------
# Size
# -----------------------------------------------------------------------------

def test_no_published_file_exceeds_cloudflare_limit(publish_set):
    oversized = [
        (p, p.stat().st_size) for p in publish_set if p.stat().st_size > MAX_FILE_BYTES
    ]
    assert not oversized, (
        "Cloudflare Pages rejects files over 25 MiB (26,214,400 bytes): "
        + ", ".join(f"{p} = {n:,} bytes" for p, n in oversized)
    )


def test_publish_set_file_count_under_pages_limit(publish_set):
    assert len(publish_set) <= MAX_FILE_COUNT, \
        f"{len(publish_set)} files in publish set exceeds the Pages 20,000-file limit"


def test_bulk_payloads_are_not_in_the_publish_set(publish_set):
    """web/data/*.json is served from R2, not from Pages.

    These files are tens to hundreds of MB. They must stay gitignored so they
    can never enter a deployment.
    """
    data_files = [p for p in publish_set if PUBLISH_DIR / "data" in p.parents]
    assert not data_files, (
        "web/data/ leaked into the publish set -- these payloads are served from R2 "
        f"and blow the 25 MiB limit: {data_files}"
    )


def test_web_data_is_gitignored():
    """The mechanism that keeps the big payloads out of the deployment."""
    # Probe a path INSIDE the directory, not the directory itself. The ignore
    # pattern is "web/data/", which matches directories only, and git decides
    # whether a path is a directory by looking at the working tree. On a fresh
    # CI checkout the directory does not exist -- it is built by the pipeline --
    # so "web/data" matches nothing and this passed locally while failing in CI.
    # A path under it matches the pattern whether or not anything is on disk.
    # check-ignore exits 0 when the path IS ignored, 1 when it is not.
    rc = subprocess.run(
        ["git", "check-ignore", "-q", "web/data/summary.json"], capture_output=True,
    ).returncode
    assert rc == 0, "web/data is no longer gitignored -- its payloads would be deployed"


# -----------------------------------------------------------------------------
# Scope
# -----------------------------------------------------------------------------

def test_publish_set_contains_only_site_assets(publish_set):
    bad = [
        p for p in publish_set
        if p.name not in ALLOWED_NAMES and p.suffix.lower() not in ALLOWED_SUFFIXES
    ]
    assert not bad, f"non-website files in the publish set: {bad}"


def test_no_source_or_config_is_published(publish_set):
    bad = [
        p for p in publish_set
        if p.name in FORBIDDEN_NAMES
        or (p.suffix.lower() in FORBIDDEN_SUFFIXES and p.name not in ALLOWED_NAMES)
    ]
    assert not bad, (
        "pipeline source / config / local state is in the publish set: " + str(bad)
    )


def test_pipeline_source_lives_outside_the_publish_dir():
    """Regression on the original bug: no outputDirectory meant the repo root
    was the publish root, so these were all live."""
    for name in ("build_dashboard.py", "build_families.py", "fetch_awards.py",
                 "enrich_sam.py", "r2_sync.py", "payload.py", "conftest.py",
                 "config.yaml", "CLAUDE.md"):
        assert Path(name).exists(), f"{name} moved -- update this test"
        assert not (PUBLISH_DIR / name).exists(), \
            f"{name} is inside the publish directory and would be served publicly"


def test_vercel_config_pins_output_directory():
    """Until DNS moves, Vercel is still serving this repo. Without
    outputDirectory it publishes the repo root."""
    import json
    cfg = json.loads(Path("vercel.json").read_text())
    assert cfg.get("outputDirectory") == "web", \
        "vercel.json must pin outputDirectory to web/, or Vercel publishes the repo root"


# -----------------------------------------------------------------------------
# Pages requirements
# -----------------------------------------------------------------------------

def test_404_page_is_published(publish_set):
    """Pages has no default 404: without this file it serves index.html with
    HTTP 200 for every unmatched path."""
    assert PUBLISH_DIR / "404.html" in publish_set, "web/404.html is missing from the publish set"


def test_redirects_has_no_absolute_url_sources():
    """Cloudflare Pages matches _redirects on PATH ONLY and silently ignores
    any rule whose source is an absolute URL."""
    redirects = PUBLISH_DIR / "_redirects"
    if not redirects.exists():
        pytest.skip("no _redirects file")
    for lineno, line in enumerate(redirects.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        source = line.split()[0]
        assert source.startswith("/"), (
            f"_redirects line {lineno}: source {source!r} is not a path. "
            "Pages ignores absolute-URL sources silently."
        )
