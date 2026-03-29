"""
fetch_transcripts.py
--------------------
Extract transcripts from YouTube channels defined in channels.json.
Filters by title keywords and upload date lookback window.

Usage:
    python fetch_transcripts.py                     # all channels
    python fetch_transcripts.py --channel "Sam and Victor"
    python fetch_transcripts.py --dry-run           # list matches, no download
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yt_dlp

CHANNELS_FILE = Path("channels.json")
OUTPUT_ROOT   = Path("transcripts")


# ── Config loading ─────────────────────────────────────────────────────────────

def load_channels() -> list[dict]:
    if not CHANNELS_FILE.exists():
        print(f"ERROR: {CHANNELS_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    with CHANNELS_FILE.open() as f:
        return json.load(f)


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)

def safe_filename(title: str, video_id: str) -> str:
    slug = re.sub(r"[^\w\-]", "_", title)[:60]
    return f"{video_id}_{slug}"

def build_regex(keywords: list[str]) -> re.Pattern:
    return re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE)

def vtt_to_plain(vtt_text: str) -> str:
    """Strip VTT markup and deduplicate repeated caption lines."""
    lines, seen = [], None
    for line in vtt_text.splitlines():
        line = line.strip()
        if not line or "-->" in line or line.startswith("WEBVTT") or re.match(r"^\d+$", line):
            continue
        clean = re.sub(r"<[^>]+>", "", line).strip()
        if clean and clean != seen:
            lines.append(clean)
            seen = clean
    return "\n".join(lines)


# ── Step 1: collect matching videos for a channel ─────────────────────────────

def collect_videos(channel: dict) -> list[dict]:
    url          = channel["url"]
    keywords     = channel.get("keywords", [])
    lookback     = channel.get("lookback_days", 365)
    cutoff       = datetime.now(timezone.utc) - timedelta(days=lookback)
    title_re     = build_regex(keywords)

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
        title       = e.get("title") or ""
        upload_date = e.get("upload_date") or ""
        video_id    = e.get("id") or ""

        if not upload_date:
            continue
        if parse_date(upload_date) < cutoff:
            continue
        if keywords and not title_re.search(title):
            continue

        matched.append({
            "channel":     channel["name"],
            "id":          video_id,
            "title":       title,
            "upload_date": upload_date,
            "url":         f"https://www.youtube.com/watch?v={video_id}",
        })

    print(f"  {len(matched)} matched (keywords + date filter)")
    return matched


# ── Step 2: download transcript for one video ─────────────────────────────────

def fetch_transcript(video: dict, out_dir: Path) -> bool:
    fname   = safe_filename(video["title"], video["id"])
    vtt_dir = out_dir / "vtt"
    vtt_dir.mkdir(parents=True, exist_ok=True)

    ydl_opts = {
        "quiet":             True,
        "skip_download":     True,
        "writesubtitles":    True,
        "writeautomaticsub": True,
        "subtitleslangs":    ["en", "en-orig"],
        "subtitlesformat":   "vtt",
        "outtmpl":           str(vtt_dir / fname),
        "noplaylist":        True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video["url"]])

    vtt_files = list(vtt_dir.glob(f"{video['id']}*.vtt"))
    if not vtt_files:
        print(f"  ⚠  No transcript: {video['title'][:60]}")
        return False

    raw   = vtt_files[0].read_text(encoding="utf-8", errors="replace")
    plain = vtt_to_plain(raw)

    txt_path = out_dir / f"{fname}.txt"
    txt_path.write_text(
        f"Channel: {video['channel']}\n"
        f"Title:   {video['title']}\n"
        f"ID:      {video['id']}\n"
        f"Date:    {video['upload_date']}\n"
        f"URL:     {video['url']}\n"
        f"{'─' * 60}\n\n"
        + plain,
        encoding="utf-8",
    )
    print(f"  ✓ {video['upload_date']}  {video['title'][:60]}")
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
            print(f"ERROR: No channel named '{args.channel}' in channels.json", file=sys.stderr)
            sys.exit(1)

    all_videos   = []
    all_manifest = []

    for ch in channels:
        videos = collect_videos(ch)
        all_videos.extend(videos)

        ch_slug = re.sub(r"[^\w]", "_", ch["name"])
        ch_dir  = OUTPUT_ROOT / ch_slug
        ch_dir.mkdir(parents=True, exist_ok=True)

        # per-channel manifest
        manifest_path = ch_dir / "manifest.json"
        manifest_path.write_text(json.dumps(videos, indent=2), encoding="utf-8")

        all_manifest.extend(videos)

        if args.dry_run:
            for v in videos:
                print(f"  DRY  {v['upload_date']}  {v['title'][:70]}")
            continue

        ok = fail = 0
        for v in videos:
            if fetch_transcript(v, ch_dir):
                ok += 1
            else:
                fail += 1
        print(f"  → {ok} saved, {fail} failed")

    # global manifest
    OUTPUT_ROOT.mkdir(exist_ok=True)
    global_manifest = OUTPUT_ROOT / "all_manifest.json"
    global_manifest.write_text(json.dumps(all_manifest, indent=2), encoding="utf-8")

    if not args.dry_run:
        print(f"\nAll done. Transcripts in {OUTPUT_ROOT.resolve()}")


if __name__ == "__main__":
    main()
