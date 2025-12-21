"""Download data sources for the Italian Anki deck generator."""

import bz2
import sys
import tarfile
from io import BytesIO
from pathlib import Path

import requests

# Data directory paths
DATA_DIR = Path("data")
WIKTEXTRACT_DIR = DATA_DIR / "wiktextract"
MORPHIT_DIR = DATA_DIR / "morphit"
ITWAC_DIR = DATA_DIR / "itwac"
TATOEBA_DIR = DATA_DIR / "tatoeba"

# Download URLs
WIKTEXTRACT_URL = "https://kaikki.org/dictionary/Italian/kaikki.org-dictionary-Italian.jsonl"
MORPHIT_URL = "https://docs.sslmit.unibo.it/lib/exe/fetch.php?media=resources:morph-it.tgz"

ITWAC_BASE_URL = "https://raw.githubusercontent.com/franfranz/Word_Frequency_Lists_ITA/main"
ITWAC_FILES = [
    "itwac_verbs_lemmas_notail_2_1_0.csv",
    "itwac_nouns_lemmas_notail_2_0_0.csv",
    "itwac_adj_lemmas_notail_2_1_0.csv",
]

TATOEBA_BASE_URL = "https://downloads.tatoeba.org/exports"
TATOEBA_FILES = {
    "ita_sentences.tsv": f"{TATOEBA_BASE_URL}/per_language/ita/ita_sentences.tsv.bz2",
    "eng_sentences.tsv": f"{TATOEBA_BASE_URL}/per_language/eng/eng_sentences.tsv.bz2",
    "ita_eng_links.tsv": f"{TATOEBA_BASE_URL}/per_language/ita/ita-eng_links.tsv.bz2",
    "sentences_with_audio.csv": f"{TATOEBA_BASE_URL}/sentences_with_audio.csv",
}


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


def download_morphit(force: bool = False) -> dict[str, int]:
    """Download and extract Morph-it! morphological lexicon.

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    dest = MORPHIT_DIR / "morph-it.txt"

    if not force and _file_exists_and_nonempty(dest):
        print(f"Skipping Morph-it! (already exists): {dest}")
        return {"downloaded": 0, "skipped": 1}

    # Download the tgz archive
    content = _download_with_progress(MORPHIT_URL, "Morph-it! archive")

    # Extract morph-it.txt from the archive
    print("  Extracting morph-it.txt from archive...")
    dest.parent.mkdir(parents=True, exist_ok=True)

    with tarfile.open(fileobj=BytesIO(content), mode="r:gz") as tar:
        # Find the morph-it.txt file in the archive
        for member in tar.getmembers():
            if member.name.endswith("morph-it.txt"):
                # Extract the file content
                extracted = tar.extractfile(member)
                if extracted is None:
                    raise ValueError("Failed to extract morph-it.txt from archive")
                dest.write_bytes(extracted.read())
                print(f"  Extracted: {dest} ({dest.stat().st_size / (1024 * 1024):.1f} MB)")
                return {"downloaded": 1, "skipped": 0}

    raise ValueError("morph-it.txt not found in archive")


def download_itwac(force: bool = False) -> dict[str, int]:
    """Download ItWaC frequency list CSV files.

    Returns stats dict with 'downloaded' and 'skipped' counts.
    """
    downloaded = 0
    skipped = 0

    ITWAC_DIR.mkdir(parents=True, exist_ok=True)

    for filename in ITWAC_FILES:
        dest = ITWAC_DIR / filename
        url = f"{ITWAC_BASE_URL}/{filename}"

        if not force and _file_exists_and_nonempty(dest):
            print(f"Skipping ItWaC file (already exists): {dest}")
            skipped += 1
            continue

        _download_to_file(url, dest, f"ItWaC {filename}")
        downloaded += 1

    return {"downloaded": downloaded, "skipped": skipped}


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

        if url.endswith(".bz2"):
            # Download and decompress bz2 file
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
    print("Downloading Morph-it!")
    print("=" * 60)
    results["morphit"] = download_morphit(force)
    print()

    print("=" * 60)
    print("Downloading ItWaC")
    print("=" * 60)
    results["itwac"] = download_itwac(force)
    print()

    print("=" * 60)
    print("Downloading Tatoeba")
    print("=" * 60)
    results["tatoeba"] = download_tatoeba(force)
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
