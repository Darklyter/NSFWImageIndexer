"""A/B compare two caption models through the REAL llmii pipeline.

Runs the same sample of images through the full FileProcessor pipeline
(instructions, samplers, normalizers, TagMatcher from settings.json) in
dry-run mode — nothing is written to image files, sidecars, or any
database. Per-image results are captured from the GUI callback stream and
saved to JSONL for a merged comparison report.

Usage (both models can't share the GPU, so run the sides separately):

  1. Start KoboldCpp with the CURRENT model (V4.5), then:
       .venv\\Scripts\\python tests\\ab_compare_models.py run a --dir D:\\path\\to\\sample --limit 100

  2. Stop KoboldCpp, start LM Studio with the candidate model
     (the one named in settings.json lm_studio_model), then:
       .venv\\Scripts\\python tests\\ab_compare_models.py run b --dir D:\\path\\to\\sample --limit 100

  3. Compare:
       .venv\\Scripts\\python tests\\ab_compare_models.py report

Side 'a' = KoboldCpp (settings.json api_url), side 'b' = LM Studio
(settings.json lm_studio_url / lm_studio_model). Results land in
ab_results/side_a.jsonl and ab_results/side_b.jsonl at the repo root.
The file sample is deterministic (sorted walk), so both sides see the
same first N images.
"""
import argparse
import json
import os
import re
import shutil
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src import llmii  # noqa: E402

RESULTS_DIR = os.path.join(ROOT, 'ab_results')
SETTINGS_PATH = os.path.join(ROOT, 'settings.json')


# ---------------------------------------------------------------------------
# Config construction — mirror the GUI's settings.json -> Config mapping
# ---------------------------------------------------------------------------

_DIRECT_KEYS = [
    'api_url', 'api_password',
    'lm_studio_url', 'lm_studio_model', 'lm_studio_gen_count',
    'lm_studio_temperature', 'lm_studio_top_p', 'lm_studio_top_k',
    'lm_studio_min_p', 'lm_studio_rep_pen',
    'gen_count', 'res_limit',
    'instruction', 'system_instruction', 'caption_instruction',
    'tag_instruction',
    'detailed_caption', 'no_caption',
    'temperature', 'top_p', 'top_k', 'min_p', 'rep_pen',
    'use_json_grammar',
    'depluralize_keywords', 'limit_word_count', 'max_words_per_keyword',
    'split_and_entries', 'ban_prompt_words', 'no_digits_start',
    'min_word_length', 'latin_only',
    'tag_fuzzy_threshold', 'tags_file', 'image_extensions_filter',
]


def build_config(side, sample_dir, backend='kobold'):
    with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
        settings = json.load(f)

    config = llmii.Config()
    for key in _DIRECT_KEYS:
        if key in settings:
            setattr(config, key, settings[key])

    # Blacklist: stored as newline/comma separated text, Config wants a list
    raw_bl = settings.get('tag_blacklist', '') or ''
    if ',' in raw_bl and '\n' not in raw_bl:
        bl = [w.strip() for w in raw_bl.split(',') if w.strip()]
    else:
        bl = [w.strip() for w in raw_bl.splitlines() if w.strip()]
    config.tag_blacklist = bl

    # Comparison harness invariants — identical for both sides:
    config.directory = sample_dir
    config.output_mode = 'json'      # no DB connection at all
    config.dry_run = True            # no sidecars, no renames, no writes
    config.reprocess_all = True      # ignore any embedded/prior status
    config.skip_verify = True        # skip ExifTool validation pass
    config.resume_session = False
    config.update_keywords = False
    config.update_caption = False
    config.reprocess_sparse = False
    config.reprocess_failed = False
    config.reprocess_orphans = False
    config.use_sidecar = False
    config.no_backup = True
    config.rename_invalid = False
    config.fix_extension = False
    config.skip_folders = []
    config.sidecar_dir = ''
    config.normalize_keywords = True

    # The side label only names the result file; the backend is explicit.
    # Default is KoboldCpp for BOTH sides (swap the loaded model between
    # runs) — same backend + samplers makes a fairer model-vs-model test.
    config.lm_studio = (backend == 'lmstudio')
    return config


# ---------------------------------------------------------------------------
# Run one side
# ---------------------------------------------------------------------------

def run_side(side, sample_dir, limit, backend='kobold'):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f'side_{side}.jsonl')

    config = build_config(side, sample_dir, backend)
    backend = ('LM Studio @ ' + config.lm_studio_url + ' / ' + config.lm_studio_model
               ) if config.lm_studio else ('KoboldCpp @ ' + config.api_url)
    print(f"Side {side.upper()}: {backend}")
    print(f"Sample: {sample_dir}  (limit {limit} images)")
    print(f"Results -> {out_path}\n")

    # Safety net: a normally-completing run deletes the resume checkpoint.
    # Park any real checkpoint aside and restore it afterwards.
    cp = os.path.join(ROOT, 'llmii_checkpoint.json')
    cp_backup = cp + '.ab_backup'
    had_checkpoint = os.path.exists(cp)
    if had_checkpoint:
        shutil.copy2(cp, cp_backup)

    state = {'captured': 0, 'failed': 0, 'started': time.time()}
    records = []

    def callback(message):
        if isinstance(message, dict) and message.get('type') == 'image_data':
            rec = {
                'file': message.get('file_path'),
                'caption': message.get('caption') or '',
                'matched': message.get('keywords') or [],
                'raw': message.get('raw_keywords') or [],
                'debug': {
                    k: (v if isinstance(v, str) or v is None else str(v))
                    for k, v in (message.get('debug_map') or {}).items()
                },
            }
            records.append(rec)
            state['captured'] += 1
            n = state['captured']
            print(f"  [{n}/{limit}] {os.path.basename(rec['file'])}: "
                  f"{len(rec['matched'])}/{len(rec['raw'])} matched")
        elif isinstance(message, str) and 'AI Generation Failed' in message:
            state['failed'] += 1

    def check_paused_or_stopped():
        if state['captured'] + state['failed'] >= limit:
            raise llmii.StopProcessing('sample limit reached')
        return False

    try:
        llmii.main(config, callback, check_paused_or_stopped)
    finally:
        if had_checkpoint:
            shutil.move(cp_backup, cp)
        elif os.path.exists(cp):
            os.remove(cp)  # a dry-run must not leave a checkpoint behind

    elapsed = time.time() - state['started']
    meta = {
        '_meta': True,
        'side': side,
        'backend': backend,
        'sample_dir': sample_dir,
        'images': state['captured'],
        'failures': state['failed'],
        'elapsed_s': round(elapsed, 1),
        'sec_per_image': round(elapsed / max(state['captured'], 1), 2),
    }
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(meta, ensure_ascii=False) + '\n')
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')

    print(f"\nSide {side.upper()} done: {state['captured']} images, "
          f"{state['failed']} failures, {meta['sec_per_image']}s/image")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def load_side(side):
    path = os.path.join(RESULTS_DIR, f'side_{side}.jsonl')
    if not os.path.exists(path):
        return None, []
    meta, records = None, []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if obj.get('_meta'):
                meta = obj
            else:
                records.append(obj)
    return meta, records


def side_stats(records):
    if not records:
        return {}
    raw_counts = [len(r['raw']) for r in records]
    matched_counts = [len(r['matched']) for r in records]
    unmatched = {}
    blacklisted = 0
    for r in records:
        for kw, resolved in r['debug'].items():
            if resolved is None:
                unmatched[kw.lower()] = unmatched.get(kw.lower(), 0) + 1
            elif resolved == '__blacklisted__':
                blacklisted += 1
    cap_lens = [len(r['caption'].split()) for r in records]
    return {
        'images': len(records),
        'avg_raw': round(statistics.mean(raw_counts), 1),
        'avg_matched': round(statistics.mean(matched_counts), 1),
        'match_ratio': round(
            sum(matched_counts) / max(sum(raw_counts), 1), 3),
        'zero_matched_images': sum(1 for c in matched_counts if c == 0),
        'distinct_unmatched': len(unmatched),
        'blacklisted_hits': blacklisted,
        'avg_caption_words': round(statistics.mean(cap_lens), 1),
        'top_unmatched': sorted(unmatched.items(),
                                key=lambda x: -x[1])[:10],
    }


def report():
    meta_a, recs_a = load_side('a')
    meta_b, recs_b = load_side('b')
    if not recs_a and not recs_b:
        print("No results found — run sides 'a' and 'b' first.")
        return

    for side, meta, recs in (('A', meta_a, recs_a), ('B', meta_b, recs_b)):
        if not recs:
            print(f"=== Side {side}: (not run yet) ===\n")
            continue
        s = side_stats(recs)
        print(f"=== Side {side}: {meta['backend']} ===")
        print(f"  images: {s['images']}   failures: {meta['failures']}"
              f"   speed: {meta['sec_per_image']}s/image")
        print(f"  avg raw keywords:      {s['avg_raw']}")
        print(f"  avg MATCHED keywords:  {s['avg_matched']}"
              f"   (ratio {s['match_ratio']})")
        print(f"  images with 0 matches: {s['zero_matched_images']}")
        print(f"  distinct unmatched:    {s['distinct_unmatched']}"
              f"   blacklisted hits: {s['blacklisted_hits']}")
        print(f"  avg caption length:    {s['avg_caption_words']} words")
        print(f"  top unmatched: "
              + ', '.join(f"{k}({n})" for k, n in s['top_unmatched']))
        print()

    if recs_a and recs_b:
        by_file_a = {r['file'].lower(): r for r in recs_a}
        by_file_b = {r['file'].lower(): r for r in recs_b}
        common = sorted(set(by_file_a) & set(by_file_b))
        if common:
            wins_a = wins_b = ties = 0
            for f in common:
                ma, mb = len(by_file_a[f]['matched']), len(by_file_b[f]['matched'])
                if ma > mb:
                    wins_a += 1
                elif mb > ma:
                    wins_b += 1
                else:
                    ties += 1
            print(f"=== Head-to-head on {len(common)} common images ===")
            print(f"  A more matched tags: {wins_a}   "
                  f"B more matched tags: {wins_b}   tied: {ties}\n")

            print("=== Sample side-by-side (first 3 common images) ===")
            for f in common[:3]:
                ra, rb = by_file_a[f], by_file_b[f]
                print(f"\n--- {os.path.basename(ra['file'])} ---")
                print(f"  A matched: {', '.join(sorted(ra['matched'])) or '(none)'}")
                print(f"  B matched: {', '.join(sorted(rb['matched'])) or '(none)'}")
                print(f"  A caption: {ra['caption'][:220]}")
                print(f"  B caption: {rb['caption'][:220]}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='cmd', required=True)
    runp = sub.add_parser('run', help='run one side')
    runp.add_argument('side', choices=['a', 'b'],
                      help="result-file label: a = current model, b = candidate")
    runp.add_argument('--dir', required=True, help='sample image directory')
    runp.add_argument('--limit', type=int, default=100)
    runp.add_argument('--backend', choices=['kobold', 'lmstudio'], default='kobold',
                      help='which server to call (default: kobold for both sides)')
    sub.add_parser('report', help='print the comparison report')

    args = p.parse_args()
    if args.cmd == 'run':
        if not os.path.isdir(args.dir):
            print(f"Not a directory: {args.dir}")
            sys.exit(1)
        run_side(args.side, os.path.normpath(args.dir), args.limit, args.backend)
    else:
        report()


if __name__ == '__main__':
    main()
