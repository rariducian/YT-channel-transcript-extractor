"""
fetch_transcripts.py
--------------------
Extract transcripts from YouTube channels defined in channels.json.
Filters by title keywords. Uses youtube-transcript-api (no auth required).

Usage:
    python fetch_transcripts.py                     # all channels
    python fetch_transcripts.py --channel "Sam and Victor"
    python fetch_transcripts.py --dry-run           # list matches, no download

Requirements:
    pip install yt-dlp youtube-transcript-api
"""

import argparse
import json
import re
import sys
from pathlib import Path

import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

CHANNELS_FILE = Path("channels.json")
OUTPUT_ROOT   = Path("transcripts")


# ── Config ─────────────────────────────────────────────────────────────────────

def load_channels() -> list[dict]:
    if not CHANNELS_FILE.exists():
        print(f"ERROR: {CHANNELS_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    with CHANNELS_FILE.open() as f:
        return json.load(f)


# ── Helpers ────────────────────────────────────────────────────────────────────

def safe_filename(title: str, video_id: str) -> str:
    slug = re.sub(r"[^\w\-]", "_", title)[:60]
    return f"{video_id}_{slug}"

def build_regex(keywords: list[str]) -> re.Pattern:
    return re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE)


# ── Step 1: collect matching videos via flat channel index ────────────────────

def collect_videos(channel: dict) -> list[dict]:
    url      = channel["url"]
    keywords = channel.get("keywords", [])
    title_re = build_regex(keywords) if keywords else None

    ydl_opts = {
        "quiet":        True,
        "extract_flat": "in_playlist",
        "playlistend":  channel.get("max_videos", 500),
    }

    print(f"\n[{channel['name']}] Fetching index from {url} …")
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url + "/videos", download=False)

    entries = info.get("entries") or []
    print(f"  {len(entries)} total entries found")

    matched = []
    for e in entries:
        if not e:
            continue
        title    = e.get("title") or ""
        video_id = e.get("id") or ""
        if not video_id:
            continue
        if title_re and not title_re.search(title):
            continue
        matched.append({
            "channel": channel["name"],
            "id":      video_id,
            "title":   title,
            "url":     f"https://www.youtube.com/watch?v={video_id}",
        })

    print(f"  {len(matched)} keyword matches")
    return matched


# ── Step 2: fetch transcript via youtube-transcript-api ───────────────────────

def fetch_transcript(video: dict, out_dir: Path) -> bool:
    video_id = video["id"]
    fname    = safe_filename(video["title"], video_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # prefer manual english, fallback to auto-generated, fallback to any+translate
        try:
            transcript = transcript_list.find_manually_created_transcript(["en"])
        except NoTranscriptFound:
            try:
                transcript = transcript_list.find_generated_transcript(["en", "en-US"])
            except NoTranscriptFound:
                # grab whatever's available and translate
                transcript = next(iter(transcript_list)).translate("en")

        segments = transcript.fetch()
        plain    = " ".join(s.text for s in segments).strip()

    except TranscriptsDisabled:
        print(f"  ⚠  Transcripts disabled: {video['title'][:60]}")
        return False
    except Exception as ex:
        print(f"  ⚠  Failed ({ex}): {video['title'][:60]}")
        return False

    txt_path = out_dir / f"{fname}.txt"
    txt_path.write_text(
        f"Channel: {video['channel']}\n"
        f"Title:   {video['title']}\n"
        f"ID:      {video_id}\n"
        f"URL:     {video['url']}\n"
        f"{'─' * 60}\n\n"
        + plain,
        encoding="utf-8",
    )
    print(f"  ✓ {video['title'][:70]}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="YouTube transcript extractor")
    parser.add_argument("--channel",  help="Run only this channel (by name)")
    parser.add_argument("--dry-run",  action="store_true", help="List matches only, no download")
    args = parser.parse_args()

    channels = load_channels()
    if args.channel:
        channels = [c for c in channels if c["name"].lower() == args.channel.lower()]
        if not channels:
            print(f"ERROR: No channel named '{args.channel}'", file=sys.stderr)
            sys.exit(1)

    all_manifest = []

    for ch in channels:
        videos = collect_videos(ch)
        all_manifest.extend(videos)

        ch_slug = re.sub(r"[^\w]", "_", ch["name"])
        ch_dir  = OUTPUT_ROOT / ch_slug
        ch_dir.mkdir(parents=True, exist_ok=True)

        (ch_dir / "manifest.json").write_text(json.dumps(videos, indent=2), encoding="utf-8")

        if args.dry_run:
            for v in videos:
                print(f"  DRY  {v['title'][:70]}")
            continue

        ok = fail = 0
        for v in videos:
            if fetch_transcript(v, ch_dir):
                ok += 1
            else:
                fail += 1
        print(f"  → {ok} saved, {fail} failed")

    OUTPUT_ROOT.mkdir(exist_ok=True)
    (OUTPUT_ROOT / "all_manifest.json").write_text(json.dumps(all_manifest, indent=2), encoding="utf-8")

    if not args.dry_run:
        print(f"\nAll done. Transcripts in {OUTPUT_ROOT.resolve()}")


if __name__ == "__main__":
    main()
