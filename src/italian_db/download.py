"""Download data sources for the Italian Anki deck generator."""

import bz2
import io
import sys
import tarfile
from pathlib import Path

import requests

# Data directory paths
DATA_DIR = Path("data")
WIKTEXTRACT_DIR = DATA_DIR / "wiktextract"
TATOEBA_DIR = DATA_DIR / "tatoeba"
PARTUT_DIR = DATA_DIR / "partut"

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

# ParTUT: Universal Dependencies Italian corpus with parallel English translations
# License: CC-BY-NC-SA 4.0 (NonCommercial)
PARTUT_BASE_URL = "https://raw.githubusercontent.com/UniversalDependencies/UD_Italian-ParTUT/master"
PARTUT_ENG_BASE_URL = (
    "https://raw.githubusercontent.com/UniversalDependencies/UD_English-ParTUT/master"
)
PARTUT_ITA_FILES = [
    "it_partut-ud-train.conllu",
    "it_partut-ud-dev.conllu",
    "it_partut-ud-test.conllu",
]
PARTUT_ENG_FILES = [
    "en_partut-ud-train.conllu",
    "en_partut-ud-dev.conllu",
    "en_partut-ud-test.conllu",
]

# OpenSubtitles Frequency (hermitdave/FrequencyWords) - CC-BY-SA 4.0
# Derived from OpenSubtitles2018 corpus (conversational/dialogue text)
OPENSUBTITLES_DIR = DATA_DIR / "opensubtitles"
OPENSUBTITLES_50K_URL = (
    "https://raw.githubusercontent.com/hermitdave/FrequencyWords/master/content/2018/it/it_50k.txt"
)
OPENSUBTITLES_FULL_URL = (
    "https://raw.githubusercontent.com/hermitdave/FrequencyWords/master/content/2018/it/it_full.txt"
)

# PAISA Lemma Frequencies - CC-BY-NC-SA 4.0 (NonCommercial!)
# Web corpus from .it domain (2010), evaluation only due to NC license
PAISA_DIR = DATA_DIR / "paisa"
PAISA_URL = (
    "https://clarin.eurac.edu/repository/xmlui/bitstream/handle/20.500.12124/3/"
    "lemma-frequencies-paisa.txt.gz"
)


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


def download_partut(force: bool = False) -> dict[str, int]:
    """Download ParTUT Italian and English CoNLL-U files.

    ParTUT is a parallel treebank with Italian, English, and French
    translations. We download both Italian (for morphological analysis)
    and English (for translations).

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    downloaded = 0
    skipped = 0

    PARTUT_DIR.mkdir(parents=True, exist_ok=True)

    # Download Italian files
    for filename in PARTUT_ITA_FILES:
        dest = PARTUT_DIR / filename
        url = f"{PARTUT_BASE_URL}/{filename}"

        if not force and _file_exists_and_nonempty(dest):
            print(f"Skipping ParTUT Italian file (already exists): {dest}")
            skipped += 1
            continue

        _download_to_file(url, dest, f"ParTUT Italian {filename}")
        downloaded += 1

    # Download English files
    for filename in PARTUT_ENG_FILES:
        dest = PARTUT_DIR / filename
        url = f"{PARTUT_ENG_BASE_URL}/{filename}"

        if not force and _file_exists_and_nonempty(dest):
            print(f"Skipping ParTUT English file (already exists): {dest}")
            skipped += 1
            continue

        _download_to_file(url, dest, f"ParTUT English {filename}")
        downloaded += 1

    return {"downloaded": downloaded, "skipped": skipped}


def download_opensubtitles(force: bool = False) -> dict[str, int]:
    """Download OpenSubtitles frequency lists from hermitdave/FrequencyWords.

    License: CC-BY-SA 4.0
    Format: Space-separated 'word count' pairs, no header.

    Downloads:
    - it_50k.txt: Top 50K words with frequencies
    - it_full.txt: Complete word list with frequencies

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    downloaded = 0
    skipped = 0

    OPENSUBTITLES_DIR.mkdir(parents=True, exist_ok=True)

    files = {
        "it_50k.txt": OPENSUBTITLES_50K_URL,
        "it_full.txt": OPENSUBTITLES_FULL_URL,
    }

    for filename, url in files.items():
        dest = OPENSUBTITLES_DIR / filename

        if not force and _file_exists_and_nonempty(dest):
            print(f"Skipping OpenSubtitles file (already exists): {dest}")
            skipped += 1
            continue

        _download_to_file(url, dest, f"OpenSubtitles {filename}")
        downloaded += 1

    return {"downloaded": downloaded, "skipped": skipped}


def download_paisa(force: bool = False) -> dict[str, int]:
    """Download PAISA lemma frequencies.

    License: CC-BY-NC-SA 4.0 (NonCommercial - evaluation only!)

    PAISA is a large web corpus of Italian from the .it domain (2010).
    Due to the NC license, this should only be used for evaluation/comparison,
    not as a primary data source.

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    import gzip

    dest = PAISA_DIR / "lemma-frequencies-paisa.txt"

    if not force and _file_exists_and_nonempty(dest):
        print(f"Skipping PAISA (already exists): {dest}")
        return {"downloaded": 0, "skipped": 1}

    # Download the gzipped file
    content = _download_with_progress(PAISA_URL, "PAISA lemma frequencies")

    # Decompress and save
    print("  Decompressing gzip...")
    PAISA_DIR.mkdir(parents=True, exist_ok=True)
    decompressed = gzip.decompress(content)
    dest.write_bytes(decompressed)
    print(f"  Saved: {dest} ({dest.stat().st_size / (1024 * 1024):.1f} MB)")

    return {"downloaded": 1, "skipped": 0}


def download_all(force: bool = False) -> dict[str, dict[str, int]]:
    """Download all data sources.

    Returns a dict mapping source name to stats dict.
    """
    results: dict[str, dict[str, int]] = {}

    print("=" * 60)
    print("Downloading Wiktextract")
    print("=" * 60)
    results["wiktextract"] = download_wiktextract(force)
    print()

    print("=" * 60)
    print("Downloading PAISA (verb frequencies)")
    print("=" * 60)
    print("NOTE: PAISA has a CC-BY-NC-SA license (NonCommercial).")
    results["paisa"] = download_paisa(force)
    print()

    print("=" * 60)
    print("Downloading OpenSubtitles (noun/adjective frequencies)")
    print("=" * 60)
    results["opensubtitles"] = download_opensubtitles(force)
    print()

    print("=" * 60)
    print("Downloading Tatoeba")
    print("=" * 60)
    results["tatoeba"] = download_tatoeba(force)
    print()

    print("=" * 60)
    print("Downloading ParTUT")
    print("=" * 60)
    results["partut"] = download_partut(force)
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
    download_all(force=force)
