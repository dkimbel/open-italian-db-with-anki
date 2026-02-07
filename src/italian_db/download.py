"""Download data sources for the Italian Anki deck generator."""

import bz2
import gzip
import hashlib
import io
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from random import Random

import requests

# Data directory paths
DATA_DIR = Path("data")
WIKTEXTRACT_DIR = DATA_DIR / "wiktextract"
TATOEBA_DIR = DATA_DIR / "tatoeba"
OPENSUBTITLES_DIR = DATA_DIR / "opensubtitles"
PROFILO_DIR = DATA_DIR / "profilo"
NVDB_DIR = DATA_DIR / "nvdb"

# GitHub release configuration
GITHUB_REPO = "dkimbel/open-italian-db-with-anki"
RELEASE_TAG = "data-v1"


@dataclass(frozen=True)
class ReleaseAsset:
    """Maps a GitHub release asset to its local file path(s) and decompression strategy."""

    asset_name: str
    # For single files: the destination path relative to DATA_DIR's parent (project root)
    dest_path: Path | None = None
    # For tar.gz archives: the directory to extract into
    extract_dir: Path | None = None


# Assets uploaded to the GitHub release and how to restore them locally.
# Order matters for display only.
RELEASE_ASSETS: list[ReleaseAsset] = [
    # Stanza-tagged JSONL (gzipped single files)
    ReleaseAsset(
        asset_name="tatoeba--ita_sentences_pos.jsonl.gz",
        dest_path=TATOEBA_DIR / "ita_sentences_pos.jsonl",
    ),
    ReleaseAsset(
        asset_name="opensubtitles--it_sentences_pos.jsonl.gz",
        dest_path=OPENSUBTITLES_DIR / "it_sentences_pos.jsonl",
    ),
    # Wiktextract dictionary (gzipped single file)
    ReleaseAsset(
        asset_name="wiktextract--kaikki.org-dictionary-Italian.jsonl.gz",
        dest_path=WIKTEXTRACT_DIR / "kaikki.org-dictionary-Italian.jsonl",
    ),
    # Tatoeba source files (tar.gz archive)
    ReleaseAsset(
        asset_name="tatoeba--sources.tar.gz",
        extract_dir=TATOEBA_DIR,
    ),
    # OpenSubtitles zip (already compressed, kept as-is)
    ReleaseAsset(
        asset_name="opensubtitles--en-it.txt.zip",
        dest_path=OPENSUBTITLES_DIR / "en-it.txt.zip",
    ),
    # OpenSubtitles derived TSVs (tar.gz archive)
    ReleaseAsset(
        asset_name="opensubtitles--derived.tar.gz",
        extract_dir=OPENSUBTITLES_DIR,
    ),
]

# Download URLs
WIKTEXTRACT_URL = "https://kaikki.org/dictionary/Italian/kaikki.org-dictionary-Italian.jsonl"

TATOEBA_BASE_URL = "https://downloads.tatoeba.org/exports"
TATOEBA_FILES = {
    "ita_sentences.tsv": f"{TATOEBA_BASE_URL}/per_language/ita/ita_sentences.tsv.bz2",
    "eng_sentences.tsv": f"{TATOEBA_BASE_URL}/per_language/eng/eng_sentences.tsv.bz2",
    "ita_eng_links.tsv": f"{TATOEBA_BASE_URL}/per_language/ita/ita-eng_links.tsv.bz2",
    "sentences_with_audio.csv": f"{TATOEBA_BASE_URL}/sentences_with_audio.csv",
    # Additional files for quality filtering and tense tags
    "tags.csv": f"{TATOEBA_BASE_URL}/tags.tar.bz2",
    "sentences_in_lists.csv": f"{TATOEBA_BASE_URL}/sentences_in_lists.tar.bz2",
}

# OPUS OpenSubtitles v2024 parallel sentences (en-it Moses format)
OPENSUBTITLES_MOSES_URL = "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2024/moses/en-it.txt.zip"
OPENSUBTITLES_SAMPLE_SIZE = 5_000_000

# Profilo della lingua italiana CEFR word lists
PROFILO_BASE_URL = "https://www.unistrapg.it/profilo_lingua_italiana/site"
PROFILO_URLS = {
    "A1": f"{PROFILO_BASE_URL}/liste_lessicali_a1.html",
    "A2": f"{PROFILO_BASE_URL}/liste_lessicali_a2.html",
    "B1": f"{PROFILO_BASE_URL}/liste_lessicali_b1.html",
    "B2": f"{PROFILO_BASE_URL}/liste_lessicali_b2.html",
}

# NVdB (Nuovo Vocabolario di Base) vocabulary tier list
NVDB_URL = "https://raw.githubusercontent.com/memdevice/nvdb/master/nvdb.html"


def _file_exists_and_nonempty(path: Path) -> bool:
    """Check if file exists and has size > 0."""
    return path.exists() and path.stat().st_size > 0


def _download_with_progress(url: str, desc: str) -> bytes:
    """Download a URL with progress reporting, returning the content as bytes."""
    print(f"Downloading {desc}...")
    print(f"  URL: {url}")

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0
    chunks: list[bytes] = []

    for chunk in response.iter_content(chunk_size=8192):
        chunks.append(chunk)
        downloaded += len(chunk)
        if total_size > 0:
            pct = (downloaded / total_size) * 100
            mb_done = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            print(f"\r  Progress: {mb_done:.1f}/{mb_total:.1f} MB ({pct:.1f}%)", end="")
        else:
            mb_done = downloaded / (1024 * 1024)
            print(f"\r  Downloaded: {mb_done:.1f} MB", end="")

    print()  # newline after progress
    return b"".join(chunks)


def _download_to_file(url: str, dest: Path, desc: str) -> None:
    """Download a URL directly to a file with progress reporting."""
    print(f"Downloading {desc}...")
    print(f"  URL: {url}")
    print(f"  Destination: {dest}")

    dest.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                pct = (downloaded / total_size) * 100
                mb_done = downloaded / (1024 * 1024)
                mb_total = total_size / (1024 * 1024)
                print(f"\r  Progress: {mb_done:.1f}/{mb_total:.1f} MB ({pct:.1f}%)", end="")
            else:
                mb_done = downloaded / (1024 * 1024)
                print(f"\r  Downloaded: {mb_done:.1f} MB", end="")

    print()  # newline after progress
    print(f"  Saved: {dest.stat().st_size / (1024 * 1024):.1f} MB")


def download_wiktextract(force: bool = False) -> dict[str, int]:
    """Download the Wiktextract Italian dictionary.

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    dest = WIKTEXTRACT_DIR / "kaikki.org-dictionary-Italian.jsonl"

    if not force and _file_exists_and_nonempty(dest):
        print(f"Skipping Wiktextract (already exists): {dest}")
        return {"downloaded": 0, "skipped": 1}

    _download_to_file(WIKTEXTRACT_URL, dest, "Wiktextract Italian dictionary")
    return {"downloaded": 1, "skipped": 0}


def download_tatoeba(force: bool = False) -> dict[str, int]:
    """Download Tatoeba sentence files.

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    downloaded = 0
    skipped = 0

    TATOEBA_DIR.mkdir(parents=True, exist_ok=True)

    for dest_name, url in TATOEBA_FILES.items():
        dest = TATOEBA_DIR / dest_name

        if not force and _file_exists_and_nonempty(dest):
            print(f"Skipping Tatoeba file (already exists): {dest}")
            skipped += 1
            continue

        if url.endswith(".tar.bz2"):
            # Download and extract tar.bz2 archive
            content = _download_with_progress(url, f"Tatoeba {dest_name}")
            print("  Extracting tar.bz2 archive...")
            # Extract the file matching dest_name from the archive
            with tarfile.open(fileobj=io.BytesIO(content), mode="r:bz2") as tar:
                # Find the member that matches our target name (without path)
                target_member = None
                for member in tar.getmembers():
                    if member.name.endswith(dest_name):
                        target_member = member
                        break
                if target_member is None:
                    # Fallback: look for any CSV file in the archive
                    for member in tar.getmembers():
                        if member.isfile():
                            target_member = member
                            break
                if target_member is None:
                    raise ValueError(f"Could not find {dest_name} in tar archive")
                # Extract and save
                extracted = tar.extractfile(target_member)
                if extracted is None:
                    raise ValueError(f"Could not extract {target_member.name}")
                dest.write_bytes(extracted.read())
            print(f"  Saved: {dest} ({dest.stat().st_size / (1024 * 1024):.1f} MB)")
        elif url.endswith(".bz2"):
            # Download and decompress plain bz2 file
            content = _download_with_progress(url, f"Tatoeba {dest_name}")
            print("  Decompressing bz2...")
            decompressed = bz2.decompress(content)
            dest.write_bytes(decompressed)
            print(f"  Saved: {dest} ({dest.stat().st_size / (1024 * 1024):.1f} MB)")
        else:
            # Direct download
            _download_to_file(url, dest, f"Tatoeba {dest_name}")

        downloaded += 1

    return {"downloaded": downloaded, "skipped": skipped}


# Regex patterns for cleaning OpenSubtitles lines
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BRACKET_RE = re.compile(r"\[[^\]]*\]")
_PAREN_RE = re.compile(r"\([^)]*\)")
_MULTI_SPACE_RE = re.compile(r"\s+")


def _clean_line(line: str) -> str:
    """Clean an OpenSubtitles line: strip HTML, brackets, parens, normalize whitespace."""
    line = _HTML_TAG_RE.sub("", line)
    line = _BRACKET_RE.sub("", line)
    line = _PAREN_RE.sub("", line)
    line = _MULTI_SPACE_RE.sub(" ", line)
    return line.strip()


def download_opensubtitles(force: bool = False) -> dict[str, int]:
    """Download and preprocess OPUS OpenSubtitles v2024 parallel sentences.

    Downloads en-it.txt.zip (~1.8GB), extracts the Italian and English files,
    then preprocesses:
    - Cleans HTML tags, bracketed/parenthesized annotations
    - Filters by length (3-500 chars for Italian)
    - Deduplicates by Italian text (keeps first occurrence)
    - Samples ~5M pairs (deterministic seed=42)

    Outputs TSV files matching Tatoeba's format for maximum code reuse:
    - it_sentences.tsv: line_number<TAB>ita<TAB>text
    - en_sentences.tsv: line_number<TAB>eng<TAB>text
    - links.tsv: ita_line_number<TAB>eng_line_number

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    OPENSUBTITLES_DIR.mkdir(parents=True, exist_ok=True)

    ita_out = OPENSUBTITLES_DIR / "it_sentences.tsv"
    eng_out = OPENSUBTITLES_DIR / "en_sentences.tsv"
    links_out = OPENSUBTITLES_DIR / "links.tsv"

    if not force and all(_file_exists_and_nonempty(p) for p in [ita_out, eng_out, links_out]):
        print(f"Skipping OpenSubtitles (already exists): {OPENSUBTITLES_DIR}")
        return {"downloaded": 0, "skipped": 1}

    # Download the zip file
    zip_path = OPENSUBTITLES_DIR / "en-it.txt.zip"
    if not force and _file_exists_and_nonempty(zip_path):
        print(f"Using cached zip: {zip_path}")
    else:
        _download_to_file(OPENSUBTITLES_MOSES_URL, zip_path, "OPUS OpenSubtitles v2024 (en-it)")

    # Extract and preprocess
    print("Extracting and preprocessing OpenSubtitles...")

    # Find the Italian and English files in the zip
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        it_name = next((n for n in names if n.endswith(".it")), None)
        en_name = next((n for n in names if n.endswith(".en")), None)
        if it_name is None or en_name is None:
            raise ValueError(f"Expected .it and .en files in zip, found: {names}")

        print(f"  Italian: {it_name}")
        print(f"  English: {en_name}")

        # Split on \n only — .splitlines() also splits on \r, \x0b, \x0c, \x85,
        # \u2028, \u2029 which appear inside subtitle text and break alignment.
        it_lines = zf.read(it_name).decode("utf-8").split("\n")
        en_lines = zf.read(en_name).decode("utf-8").split("\n")

    # Strip trailing empty string from final newline
    if it_lines and it_lines[-1] == "":
        it_lines.pop()
    if en_lines and en_lines[-1] == "":
        en_lines.pop()

    if len(it_lines) != len(en_lines):
        raise ValueError(f"Line count mismatch: {len(it_lines)} Italian vs {len(en_lines)} English")

    print(f"  Raw line pairs: {len(it_lines):,}")

    # Clean, filter, deduplicate
    seen_hashes: set[str] = set()
    valid_indices: list[int] = []

    for i in range(len(it_lines)):
        it_clean = _clean_line(it_lines[i])
        en_clean = _clean_line(en_lines[i])

        # Filter: skip empty or too short/long Italian
        if not it_clean or not en_clean:
            continue
        if len(it_clean) < 3 or len(it_clean) > 500:
            continue

        # Deduplicate by Italian text hash
        it_hash = hashlib.md5(it_clean.encode("utf-8")).hexdigest()  # noqa: S324
        if it_hash in seen_hashes:
            continue
        seen_hashes.add(it_hash)

        # Store cleaned text back for later use
        it_lines[i] = it_clean
        en_lines[i] = en_clean
        valid_indices.append(i)

    print(f"  After cleaning/dedup: {len(valid_indices):,}")

    # Sample if needed
    if len(valid_indices) > OPENSUBTITLES_SAMPLE_SIZE:
        rng = Random(42)  # noqa: S311
        valid_indices = sorted(rng.sample(valid_indices, OPENSUBTITLES_SAMPLE_SIZE))
        print(f"  After sampling: {len(valid_indices):,}")

    # Write output TSVs (1-indexed line numbers as sentence IDs)
    with (
        ita_out.open("w", encoding="utf-8") as f_ita,
        eng_out.open("w", encoding="utf-8") as f_eng,
        links_out.open("w", encoding="utf-8") as f_links,
    ):
        for line_num, idx in enumerate(valid_indices, 1):
            f_ita.write(f"{line_num}\tita\t{it_lines[idx]}\n")
            f_eng.write(f"{line_num}\teng\t{en_lines[idx]}\n")
            f_links.write(f"{line_num}\t{line_num}\n")

    print(f"  Output: {len(valid_indices):,} sentence pairs")
    print(f"  Italian: {ita_out}")
    print(f"  English: {eng_out}")
    print(f"  Links: {links_out}")

    return {"downloaded": 1, "skipped": 0}


def download_profilo(force: bool = False) -> dict[str, int]:
    """Download Profilo della lingua italiana CEFR word list HTML pages.

    Downloads 4 small HTML files (A1, A2, B1, B2) to data/profilo/.
    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    downloaded = 0
    skipped = 0

    PROFILO_DIR.mkdir(parents=True, exist_ok=True)

    for level, url in PROFILO_URLS.items():
        dest = PROFILO_DIR / f"liste_lessicali_{level.lower()}.html"

        if not force and _file_exists_and_nonempty(dest):
            print(f"Skipping Profilo {level} (already exists): {dest}")
            skipped += 1
            continue

        _download_to_file(url, dest, f"Profilo {level} word list")
        downloaded += 1

    return {"downloaded": downloaded, "skipped": skipped}


def download_nvdb(force: bool = False) -> dict[str, int]:
    """Download NVdB (Nuovo Vocabolario di Base) HTML word list.

    Downloads a single HTML file to data/nvdb/nvdb.html.
    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    dest = NVDB_DIR / "nvdb.html"

    if not force and _file_exists_and_nonempty(dest):
        print(f"Skipping NVdB (already exists): {dest}")
        return {"downloaded": 0, "skipped": 1}

    _download_to_file(NVDB_URL, dest, "NVdB vocabulary list")
    return {"downloaded": 1, "skipped": 0}


def _check_gh_cli() -> None:
    """Verify that the GitHub CLI is available."""
    if shutil.which("gh") is None:
        print("Error: 'gh' CLI not found.", file=sys.stderr)
        print("Install it from https://cli.github.com/", file=sys.stderr)
        sys.exit(1)


def _verify_checksums(download_dir: Path, checksums_path: Path, assets: list[ReleaseAsset]) -> None:
    """Verify SHA-256 checksums of downloaded release assets.

    Only checks assets that were actually downloaded (in case some were skipped).
    Raises ValueError if any checksum doesn't match.
    """
    expected: dict[str, str] = {}
    for line in checksums_path.read_text().strip().splitlines():
        sha256, name = line.split()
        expected[name] = sha256

    asset_names = {a.asset_name for a in assets}
    verified = 0
    for name, expected_hash in expected.items():
        if name not in asset_names:
            continue
        path = download_dir / name
        if not path.exists():
            raise ValueError(f"Checksum file lists '{name}' but it was not downloaded")
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        actual_hash = h.hexdigest()
        if actual_hash != expected_hash:
            raise ValueError(
                f"Checksum mismatch for {name}:\n"
                f"  expected: {expected_hash}\n"
                f"  actual:   {actual_hash}"
            )
        verified += 1
    print(f"  All {verified} checksums verified.")


def _decompress_asset(asset: ReleaseAsset, download_dir: Path) -> None:
    """Decompress a downloaded release asset to its final location."""
    src = download_dir / asset.asset_name

    if asset.extract_dir is not None:
        # tar.gz archive → extract into target directory
        asset.extract_dir.mkdir(parents=True, exist_ok=True)
        print(f"  Extracting {asset.asset_name} → {asset.extract_dir}/")
        with tarfile.open(src, "r:gz") as tar:
            tar.extractall(path=asset.extract_dir)  # noqa: S202
    elif asset.dest_path is not None:
        asset.dest_path.parent.mkdir(parents=True, exist_ok=True)
        if asset.asset_name.endswith(".gz"):
            # gzipped single file → decompress
            print(f"  Decompressing {asset.asset_name} → {asset.dest_path}")
            with gzip.open(src, "rb") as f_in, asset.dest_path.open("wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        else:
            # Already in final form (e.g. .zip) → just move
            print(f"  Moving {asset.asset_name} → {asset.dest_path}")
            shutil.move(str(src), str(asset.dest_path))

    size_mb = asset.dest_path.stat().st_size / (1024 * 1024) if asset.dest_path else 0
    if asset.dest_path:
        print(f"    {size_mb:.1f} MB")


def download_release(force: bool = False) -> dict[str, int]:
    """Download data artifacts from the pinned GitHub release.

    Fetches compressed assets from the `data-v1` release, verifies checksums,
    and decompresses them to their expected `data/` paths.

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    _check_gh_cli()

    # Check which assets already exist locally
    assets_to_download: list[ReleaseAsset] = []
    skipped = 0

    for asset in RELEASE_ASSETS:
        if not force:
            if asset.dest_path and _file_exists_and_nonempty(asset.dest_path):
                print(f"Skipping {asset.asset_name} (already exists: {asset.dest_path})")
                skipped += 1
                continue
            if (
                asset.extract_dir
                and asset.extract_dir.exists()
                and any(asset.extract_dir.iterdir())
            ):
                # For archives, skip if the target dir is non-empty
                # (individual files may have been partially extracted, but this is good enough)
                print(f"Skipping {asset.asset_name} (directory exists: {asset.extract_dir})")
                skipped += 1
                continue
        assets_to_download.append(asset)

    if not assets_to_download:
        print("All release assets already present locally.")
        return {"downloaded": 0, "skipped": skipped}

    # Download all needed assets + checksum file to a temp directory
    import tempfile

    with tempfile.TemporaryDirectory(prefix="italian-db-release-") as tmpdir:
        tmp = Path(tmpdir)

        # Build list of asset patterns for gh release download
        asset_names = [a.asset_name for a in assets_to_download] + ["checksums.sha256"]
        pattern_args: list[str] = []
        for name in asset_names:
            pattern_args.extend(["--pattern", name])

        print(f"Downloading {len(assets_to_download)} assets from release {RELEASE_TAG}...")
        cmd = [
            "gh",
            "release",
            "download",
            RELEASE_TAG,
            "--repo",
            GITHUB_REPO,
            "--dir",
            str(tmp),
            *pattern_args,
        ]
        subprocess.run(cmd, check=True)  # noqa: S603

        # Verify checksums
        checksums_path = tmp / "checksums.sha256"
        if checksums_path.exists():
            print("Verifying checksums...")
            _verify_checksums(tmp, checksums_path, assets_to_download)
        else:
            print("  Warning: No checksums.sha256 in release, skipping verification.")

        # Decompress each asset
        print("Decompressing assets...")
        for asset in assets_to_download:
            _decompress_asset(asset, tmp)

    return {"downloaded": len(assets_to_download), "skipped": skipped}


def download_all_upstream(force: bool = False) -> dict[str, dict[str, int]]:
    """Download all data from original upstream sources.

    This fetches fresh data directly from each source's servers.
    Use this to refresh data when upstream sources have been updated.

    Returns a dict mapping source name to stats dict.
    """
    results: dict[str, dict[str, int]] = {}

    print("=" * 60)
    print("Downloading Wiktextract (upstream)")
    print("=" * 60)
    results["wiktextract"] = download_wiktextract(force)
    print()

    print("=" * 60)
    print("Downloading Tatoeba (upstream)")
    print("=" * 60)
    results["tatoeba"] = download_tatoeba(force)
    print()

    print("=" * 60)
    print("Downloading OpenSubtitles (upstream, OPUS v2024)")
    print("=" * 60)
    results["opensubtitles"] = download_opensubtitles(force)
    print()

    print("=" * 60)
    print("Downloading Profilo della lingua italiana (upstream)")
    print("=" * 60)
    results["profilo"] = download_profilo(force)
    print()

    print("=" * 60)
    print("Downloading NVdB (upstream)")
    print("=" * 60)
    results["nvdb"] = download_nvdb(force)
    print()

    # Summary
    print("=" * 60)
    print("Download Summary")
    print("=" * 60)
    total_downloaded = sum(r["downloaded"] for r in results.values())
    total_skipped = sum(r["skipped"] for r in results.values())
    print(f"  Downloaded: {total_downloaded} files")
    print(f"  Skipped:    {total_skipped} files")

    return results


def download_all(force: bool = False) -> dict[str, dict[str, int]]:
    """Download all data: CC-licensed artifacts from GitHub release, others from upstream.

    This is the default download strategy:
    1. Fetch pinned, pre-computed artifacts (Stanza JSONL, Wiktextract, Tatoeba,
       OpenSubtitles) from the GitHub release — fast and reproducible.
    2. Fetch Profilo and NVdB from upstream (no redistribution license).

    Returns a dict mapping source name to stats dict.
    """
    results: dict[str, dict[str, int]] = {}

    print("=" * 60)
    print("Downloading pinned data from GitHub release")
    print("=" * 60)
    results["release"] = download_release(force)
    print()

    print("=" * 60)
    print("Downloading Profilo della lingua italiana (upstream)")
    print("=" * 60)
    results["profilo"] = download_profilo(force)
    print()

    print("=" * 60)
    print("Downloading NVdB (upstream)")
    print("=" * 60)
    results["nvdb"] = download_nvdb(force)
    print()

    # Summary
    print("=" * 60)
    print("Download Summary")
    print("=" * 60)
    total_downloaded = sum(r["downloaded"] for r in results.values())
    total_skipped = sum(r["skipped"] for r in results.values())
    print(f"  Downloaded: {total_downloaded} files")
    print(f"  Skipped:    {total_skipped} files")

    return results


if __name__ == "__main__":
    # Simple CLI for testing
    force = "--force" in sys.argv
    if "--upstream" in sys.argv:
        download_all_upstream(force=force)
    else:
        download_all(force=force)
