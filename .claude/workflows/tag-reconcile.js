/*
 * ===========================================================================
 * TAG-RECONCILE RUNBOOK
 * ===========================================================================
 * Triages image_keywords_unmatched (keywords the VLM produced that didn't
 * match the canonical vocabulary) into: aliases on existing tags, new tags,
 * a keyword blacklist, or skip. Writes a reviewable proposal; you apply it
 * with apply_tag_proposals.py after reviewing.
 *
 * WHEN TO RUN: after processing a batch of images, when unmatched has grown.
 * Start at a HIGH threshold and work down (yield/term drops steeply):
 *   ≥10 first, then ≥5, ≥3, ≥2. Singletons (count 1) are one-off noise —
 *   not worth a pass. Anything that recurs climbs back above the threshold.
 *
 * PREREQUISITES: .venv present, DB reachable (creds in settings.json).
 *
 * ---------------------------------------------------------------------------
 * HOW TO RUN — pick ONE:
 *
 * (A) SMALL remainder (<~800 terms): skip this workflow entirely. Just read
 *     tag_unmatched.txt + tag_canonical.txt and classify inline by hand —
 *     faster than a 50-agent fan-out and no reconstruction step. Produce the
 *     same proposal JSON shape and apply with apply_tag_proposals.py.
 *
 * (B) LARGE sweep, RELIABLE method (preferred — avoids the args-binding
 *     flakiness seen when invoking by name):
 *       1. Refresh data + get the count at your threshold T:
 *          .venv/Scripts/python -c "import json,psycopg2; s=json.load(open('settings.json',encoding='utf-8'));
 *            c=psycopg2.connect(host=s['db_host'],port=s['db_port'],user=s['db_user'],password=s['db_password'],
 *            dbname=s['db_name'],options='-c search_path=ai_captioning,public'); k=c.cursor();
 *            k.execute('SELECT tag FROM tags ORDER BY tag'); open('tag_canonical.txt','w',encoding='utf-8').write('\n'.join(r[0] for r in k.fetchall()));
 *            k.execute('SELECT keyword,COUNT(*) n FROM image_keywords_unmatched GROUP BY keyword HAVING COUNT(*)>=T ORDER BY n DESC');
 *            rows=k.fetchall(); open('tag_unmatched.txt','w',encoding='utf-8').write('\n'.join(f'{n}\t{w}' for w,n in rows)); print('COUNT',len(rows))"
 *       2. Make a hardcoded copy of THIS script with literals (no args):
 *          replace `const threshold = (args && args.threshold) || 10` -> `const threshold = T`
 *          replace `let count = (args && args.count) || 0`            -> `let count = <COUNT>`
 *          write it to tag_reconcile_run<T>.js (gitignored).
 *       3. Run it:  Workflow({ scriptPath: "<abs path>/tag_reconcile_run<T>.js" })
 *
 * (C) Quick/named (works most of the time, args occasionally don't bind):
 *       Workflow({ name: "tag-reconcile", args: { threshold: 5, count: <N> } })
 *     or the /tag-reconcile skill. If the run log says the wrong threshold,
 *     fall back to (B).
 *
 * ---------------------------------------------------------------------------
 * AFTER IT RUNS — the final "write:proposal" agent TRUNCATES the big JSON
 * when echoing it verbatim. If tag_proposals_full.json has correct `totals`
 * but short arrays, RECONSTRUCT from the agent transcripts:
 *   dir: <project>/<session>/subagents/workflows/<runId>/*.jsonl
 *   parse each line's message.content[] for tool_use name=="StructuredOutput";
 *   collect inputs with `results` (classify), `verdicts` (verify),
 *   `new_tags` (consolidate); apply verdict corrections; dedup by keyword;
 *   write {aliases,new_tags,blacklist,skipped} to tag_proposals_full.json.
 *
 * REVIEW (the user wants review BEFORE apply): write tag_review_passN.md and
 * surface new tags, name-collisions (auto-folded to aliases), low-confidence
 * mappings, and a blacklist sample. The user is fine with med/low confidence;
 * flag only clear misses (e.g. an object mapped to a tag).
 *
 * APPLY:  .venv/Scripts/python apply_tag_proposals.py [proposal.json] [--dry-run]
 *   creates new tags (folding collisions into aliases on the existing tag),
 *   adds all aliases, merges blacklist into settings.json keyword_blacklist +
 *   deletes those unmatched rows, then promote_aliased_unmatched() retro-tags
 *   every now-aliased image. Writes to PRODUCTION; reversible.
 *
 * keyword_blacklist (engine feature): exact model-output terms dropped in
 * _resolve BEFORE unmatched logging. The user does NOT tag furniture / objects
 * / settings / props / decor / garment-details — blacklist those.
 *
 * Working files (tag_canonical.txt, tag_unmatched.txt, tag_proposals*.json,
 * tag_review*.md, tag_reconcile_run*.js) are gitignored; regenerated on demand.
 * ===========================================================================
 */

export const meta = {
  name: 'tag-reconcile',
  description: 'Classify unmatched LLM keywords against the canonical tag vocabulary — map to existing tags, propose new tags, or blacklist objects/props — and write a reviewable proposal file',
  whenToUse: 'After processing images, to triage image_keywords_unmatched: turn high-frequency unmatched keywords into aliases on existing tags, new canonical tags, or a keyword blacklist. Re-run any time the unmatched table grows.',
  phases: [
    { title: 'Load', detail: 'refresh canonical + unmatched data, count terms' },
    { title: 'Classify', detail: 'one agent per line-range batch of unmatched keywords' },
    { title: 'Verify', detail: 'adversarially re-check each MAP decision against the vocabulary' },
    { title: 'Consolidate', detail: 'unify new-tag proposals across batches' },
  ],
}

const ROOT = 'c:\\temp\\ImageIndexer-main'
const CANON = `${ROOT}\\tag_canonical.txt`
const UNMATCHED = `${ROOT}\\tag_unmatched.txt`
const OUT = `${ROOT}\\tag_proposals_full.json`

// args: { threshold?: number (default 10), batchSize?: number (default 70),
//         count?: number (skip the refresh/Load agent if the data files are
//         already fresh and you pass the line count of tag_unmatched.txt) }
const threshold = (args && args.threshold) || 10
const batchSize = (args && args.batchSize) || 70

const INTENT = `This is an adult-image tagging pipeline with a curated, StashDB-style
vocabulary. The user TAGS: anatomy (breasts, areolas, nipples, vulva/pussy/labia,
ass/butt, body parts), body type/build, skin tone, ethnicity, hair (color/length/
style), nudity level, sex acts and positions, fetish/BDSM, piercings, tattoos (by
body location), clothing and lingerie ACTUALLY WORN (including partial/removed),
worn accessories (glasses, gloves, jewelry, collars), facial expressions, poses,
and costume/cosplay elements worn on the body.
The user does NOT tag and wants BLACKLISTED: furniture, rooms, settings, backgrounds,
scenery, props/objects not worn on the body (guns, food, instruments, electronics,
toys-that-aren't-sex-toys, baskets, books), decor/materials, lighting/image-quality
descriptors, and studio watermarks/logos (map watermark/logo detections to the
'Watermark' tag if it exists, else blacklist).`

const CLASSIFY_SCHEMA = {
  type: 'object',
  properties: {
    results: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          keyword: { type: 'string' },
          images: { type: 'number', description: 'the integer before the tab on that line' },
          decision: { type: 'string', enum: ['map', 'new', 'blacklist', 'skip'] },
          tag: { type: 'string', description: 'for map: the EXACT canonical tag string' },
          new_tag: { type: 'string', description: 'for new: proposed new canonical tag name' },
          confidence: { type: 'string', enum: ['high', 'med', 'low'] },
          note: { type: 'string' },
        },
        required: ['keyword', 'images', 'decision', 'confidence', 'note'],
      },
    },
  },
  required: ['results'],
}

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    verdicts: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          keyword: { type: 'string' },
          tag: { type: 'string' },
          correct: { type: 'boolean' },
          corrected_tag: { type: 'string' },
          reason: { type: 'string' },
        },
        required: ['keyword', 'tag', 'correct', 'reason'],
      },
    },
  },
  required: ['verdicts'],
}

const CONSOLIDATE_SCHEMA = {
  type: 'object',
  properties: {
    new_tags: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          tag: { type: 'string' },
          aliases: { type: 'array', items: { type: 'string' } },
          rationale: { type: 'string' },
        },
        required: ['tag', 'aliases', 'rationale'],
      },
    },
  },
  required: ['new_tags'],
}

phase('Load')

// Refresh the data files and get the line count. Returning ONLY a count keeps
// this robust (no bulk structured-output). Skipped if args.count is provided
// and the files are already fresh.
let count = (args && args.count) || 0
if (!count) {
  const loaded = await agent(
    `Refresh the tag-reconciliation data, then report the line count. Run exactly:

cd ${ROOT} && ./.venv/Scripts/python -c "
import json, psycopg2
s = json.load(open('settings.json', encoding='utf-8'))
conn = psycopg2.connect(host=s['db_host'], port=s['db_port'], user=s['db_user'], password=s['db_password'], dbname=s['db_name'], connect_timeout=10, options='-c search_path=ai_captioning,public')
cur = conn.cursor()
cur.execute('SELECT tag FROM tags ORDER BY tag')
open('tag_canonical.txt','w',encoding='utf-8').write('\\n'.join(r[0] for r in cur.fetchall()))
cur.execute('SELECT keyword, COUNT(*) c FROM image_keywords_unmatched GROUP BY keyword HAVING COUNT(*) >= ${threshold} ORDER BY c DESC')
rows = cur.fetchall()
open('tag_unmatched.txt','w',encoding='utf-8').write('\\n'.join(f'{c}\\t{k}' for k,c in rows))
print('LINES', len(rows))
conn.close()
"

Return the integer printed after LINES.`,
    { label: 'load:refresh-data', phase: 'Load',
      schema: { type: 'object', properties: { count: { type: 'number' } }, required: ['count'] } },
  )
  count = (loaded && loaded.count) || 0
}

log(`${count} unmatched terms (>= ${threshold} images). Classifying in batches of ${batchSize}.`)

phase('Classify')

// Line ranges (1-based inclusive) over tag_unmatched.txt.
const ranges = []
for (let s = 1; s <= count; s += batchSize) ranges.push([s, Math.min(s + batchSize - 1, count)])

const classified = await pipeline(
  ranges,
  (range, _orig, idx) => agent(
    `${INTENT}

Read the canonical vocabulary first (one tag per line; the ONLY valid map targets, must match verbatim):
  Read ${CANON}
Then read lines ${range[0]}-${range[1]} of this file (each line is: <image-count> TAB <keyword>):
  Read ${UNMATCHED}

For EACH keyword on those lines choose a decision:
- "map": same meaning as an existing canonical tag → tag = that EXACT vocabulary string. The engine already fuzzy-matches at 85, so only SEMANTIC equivalents remain (e.g. "moderate areola"→"Medium Areolas", "buttocks"→"Ass"). confidence high only when unambiguous.
- "new": a concept the user tags (per intent) with no existing tag → propose new_tag (Title Case, vocabulary style); group synonyms under one name.
- "blacklist": object/furniture/setting/background/prop/food/decor/image-quality/watermark term the user does NOT want (watermarks→"Watermark" if it exists, else blacklist).
- "skip": too vague/generic/low-value.

Return a result per keyword, including its images integer from the line.`,
    { label: `classify:${idx}`, phase: 'Classify', schema: CLASSIFY_SCHEMA },
  ),
  (cls, _range, idx) => {
    const maps = ((cls && cls.results) || []).filter(r => r.decision === 'map' && r.tag)
    if (!maps.length) return { cls, verdicts: [] }
    return agent(
      `Adversarially verify these keyword→tag mappings for an adult-image vocabulary.
Read ${CANON}. correct=true ONLY if the tag appears verbatim in that file AND the keyword genuinely means the same concept (anatomy size/shade must match: moderate≈medium ok; small→"Big Areolas" NOT ok). If wrong but a better existing tag exists, give corrected_tag.

${maps.map(m => `${m.keyword}  ->  ${m.tag}`).join('\n')}`,
      { label: `verify:${idx}`, phase: 'Verify', schema: VERIFY_SCHEMA },
    ).then(v => ({ cls, verdicts: (v && v.verdicts) || [] })).catch(() => ({ cls, verdicts: [] }))
  },
)

// ---- aggregate ----
const aliases = [], blacklist = [], skipped = [], rawNewTags = []
for (const item of classified.filter(Boolean)) {
  const vmap = Object.fromEntries((item.verdicts || []).map(v => [v.keyword, v]))
  for (const r of ((item.cls && item.cls.results) || [])) {
    const imgs = r.images || 0
    if (r.decision === 'map' && r.tag) {
      const v = vmap[r.keyword]
      let tag = r.tag, conf = r.confidence
      if (v && !v.correct) {
        if (v.corrected_tag) { tag = v.corrected_tag; conf = 'med' }
        else { skipped.push({ keyword: r.keyword, images: imgs, note: 'failed verify: ' + (v.reason || '') }); continue }
      }
      aliases.push({ keyword: r.keyword, tag, confidence: conf, images: imgs, note: r.note })
    } else if (r.decision === 'new' && r.new_tag) {
      rawNewTags.push({ keyword: r.keyword, new_tag: r.new_tag, images: imgs })
    } else if (r.decision === 'blacklist') {
      blacklist.push({ keyword: r.keyword, images: imgs, note: r.note })
    } else {
      skipped.push({ keyword: r.keyword, images: imgs, note: r.note })
    }
  }
}

phase('Consolidate')

let newTags = []
if (rawNewTags.length) {
  const imagesOf = Object.fromEntries(rawNewTags.map(n => [n.keyword, n.images]))
  const consolidated = await agent(
    `These are proposed NEW canonical tags from many batches. Unify synonyms/duplicates into one
Title-Case canonical name each (e.g. "Raised Legs"+"Legs Raised" → one) and list member keywords.
Keep the StashDB-like style. Drop any that are really objects/settings.

${rawNewTags.map(n => `${n.keyword} -> ${n.new_tag}`).join('\n')}`,
    { label: 'consolidate:new-tags', phase: 'Consolidate', schema: CONSOLIDATE_SCHEMA },
  ).catch(() => null)
  if (consolidated && consolidated.new_tags) {
    newTags = consolidated.new_tags.map(nt => ({
      ...nt, images: (nt.aliases || []).reduce((s, a) => s + (imagesOf[a] || 0), 0),
    }))
  }
}

aliases.sort((a, b) => b.images - a.images)
blacklist.sort((a, b) => b.images - a.images)
newTags.sort((a, b) => b.images - a.images)

const proposal = {
  generated_threshold: threshold,
  totals: {
    terms: count, aliases: aliases.length,
    alias_image_rows: aliases.reduce((s, a) => s + a.images, 0),
    new_tags: newTags.length, blacklist: blacklist.length,
    blacklist_image_rows: blacklist.reduce((s, a) => s + a.images, 0),
    skipped: skipped.length,
  },
  aliases, new_tags: newTags, blacklist, skipped,
}

await agent(
  `Write this exact JSON to ${OUT} (overwrite). Reply only "written".\n\n${JSON.stringify(proposal)}`,
  { label: 'write:proposal', phase: 'Consolidate' },
)

log(`Done: ${aliases.length} aliases (${proposal.totals.alias_image_rows.toLocaleString()} rows), ` +
    `${newTags.length} new tags, ${blacklist.length} blacklist (${proposal.totals.blacklist_image_rows.toLocaleString()} rows), ` +
    `${skipped.length} skipped. -> tag_proposals_full.json`)

return proposal.totals
