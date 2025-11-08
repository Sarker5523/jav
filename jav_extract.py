#!/usr/bin/env python3
"""
ThePornDB JAV Incremental Batch Extractor (Python)
- Uses /jav/ endpoint
- Minimal JSON output (same as PowerShell)
- Handles slug/video_id, retries, deduplication
"""

import json
import time
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests

# ========================= CONFIG =========================
TOKEN = 'Aor5JIDSkxOhmXmNAOYP5Gnr73u7u3UPJZYaevBT2558d467'
INPUT_FILE = 'video.json'
OUTPUT_FILE = 'scene_details.json'
FAILED_FILE = 'failed_entry.json'
PERFORMER_FILE = 'performer.json'
SITE_FILE = 'site.json'
MAX_RETRIES = 3
DELAY_BETWEEN = 0.5  # seconds
# =========================================================

session = requests.Session()
session.headers.update({
    'Authorization': f'Bearer {TOKEN}',
    'Accept': 'application/json',
    'User-Agent': 'curl/8.7.1'  # Required for /jav/ to work
})

all_performers: Dict[int, Dict] = {}
all_sites: Dict[int, Dict] = {}


def normalize_id(s: Any) -> Optional[str]:
    return str(s).strip().lower() if s else None


def remove_empty(obj: Any):
    if isinstance(obj, dict):
        return {k: remove_empty(v) for k, v in obj.items() if v not in ('', [], {})}
    elif isinstance(obj, list):
        return [remove_empty(v) for v in obj if v not in (None, '', [], {})]
    return obj


def capitalize_first(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return s[0].upper() + s[1:]


def split_jav_title(full_title: str) -> tuple[Optional[str], Optional[str]]:
    if not full_title:
        return None, None
    parts = full_title.split(' - ', 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return full_title, None


def load_json(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as e:
        print(f"[!] Failed to parse {path}: {e}")
        return []


def save_json(data: Any, path: Path):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')


def main():
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        print(f"[!] Missing '{INPUT_FILE}'. Create it with slug/scene_id pairs.")
        sys.exit(1)

    # Load input
    videos = load_json(input_path)
    print(f"[+] Loaded {len(videos)} videos from '{INPUT_FILE}'")

    # Normalize keys
    for v in videos:
        raw_key = v.get('slug') or v.get('video_id')
        v['_raw_key'] = raw_key
        v['_norm_key'] = normalize_id(raw_key)

    # Load existing output
    existing = load_json(Path(OUTPUT_FILE))
    existing_map = {}
    for obj in existing:
        key = normalize_id(obj.get('slug') or obj.get('video_id'))
        if key:
            existing_map[key] = obj

    # Determine what to fetch
    to_fetch = []
    for v in videos:
        norm_key = v['_norm_key']
        scene_id = v.get('scene_id')
        key_name = 'slug' if 'slug' in v else 'video_id'

        if not norm_key or not scene_id:
            print(f"[!] Skipping invalid: {v}")
            continue
        if norm_key not in existing_map:
            to_fetch.append({
                'raw_key': v['_raw_key'],
                'norm_key': norm_key,
                'scene_id': scene_id,
                'key_name': key_name
            })

    if not to_fetch:
        print("[+] All scenes up to date.")
        return

    print(f"[+] Fetching {len(to_fetch)} new scenes via /jav/ endpoint...")

    new_results = []
    failed = []
    for idx, item in enumerate(to_fetch, 1):
        raw_key = item['raw_key']
        scene_id = item['scene_id']
        key_name = item['key_name']
        url = f"https://api.theporndb.net/jav/{scene_id}?add_to_collection=true"

        print(f"[{idx}] {raw_key} → {scene_id}", end='')

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 404:
                    raise requests.HTTPError("404 Not Found")
                resp.raise_for_status()
                data = resp.json()['data']

                # === Extract minimal performer/site ===
                for perf in data.get('performers', []):
                    p = perf.get('parent', {})
                    pid = p.get('_id')
                    if not pid or p.get('extras', {}).get('gender') != 'Female':
                        continue
                    if pid not in all_performers:
                        all_performers[pid] = remove_empty({
                            '_id': pid,
                            'name': p.get('name'),
                            'image': p.get('image')
                        })

                site = data.get('site', {})
                site_id = site.get('id')
                if site_id and site_id not in all_sites:
                    all_sites[site_id] = remove_empty({
                        'id': site_id,
                        'name': site.get('name')
                    })

                # === Build minimal scene result ===
                performers_min = [
                    {'parent': {'_id': p['parent']['_id']}}
                    for p in data.get('performers', [])
                    if p.get('parent', {}).get('_id') and p['parent'].get('extras', {}).get('gender') == 'Female'
                ]

                # Split title into code and description
                full_title = data.get('title')
                title_code, title_desc = split_jav_title(full_title)
                description = capitalize_first(title_desc or data.get('description'))

                scene_data = {
                    '_id': data.get('_id'),
                    'title': title_code,
                    'description': description,
                    'date': data.get('date'),
                    'trailer': data.get('trailer'),
                    'background': {'full': data.get('background', {}).get('full')},
                    'performers': performers_min
                }

                site_min = remove_empty({
                    'id': site.get('id')
                })
                if site_min:
                    scene_data['site'] = site_min

                scene_obj = {
                    key_name: raw_key,
                    'data': scene_data
                }

                new_results.append(remove_empty(scene_obj))
                print(" OK")
                success = True
                break

            except Exception as e:
                print(f" [Retry {attempt}/{MAX_RETRIES}]", end='' if attempt < MAX_RETRIES else '\n')
                if attempt < MAX_RETRIES:
                    time.sleep(DELAY_BETWEEN * attempt)
                else:
                    print(f"FAILED: {e}")
                    failed.append({
                        'raw_key': raw_key,
                        'clean_key': item['norm_key'],
                        'scene_id': scene_id,
                        'error': str(e)
                    })

        time.sleep(DELAY_BETWEEN)

    # === Save outputs ===
    if new_results:
        # Merge with existing
        all_results = existing + new_results
        save_json(all_results, Path(OUTPUT_FILE))
        print(f"[+] Updated '{OUTPUT_FILE}'")

    if all_performers:
        performers_list = sorted(all_performers.values(), key=lambda x: x['_id'])
        save_json(performers_list, Path(PERFORMER_FILE))

    if all_sites:
        sites_list = sorted(all_sites.values(), key=lambda x: x['id'])
        save_json(sites_list, Path(SITE_FILE))

    if failed:
        save_json(failed, Path(FAILED_FILE))

    print(f"\n[+] Done: {len(new_results)} success, {len(failed)} failed")


if __name__ == '__main__':
    main()