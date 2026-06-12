export const meta = {
  name: 'tag-reconcile',
  description: 'Classify unmatched LLM keywords against the canonical tag vocabulary — map to existing tags, propose new tags, or blacklist objects/props — and write a reviewable proposal file',
  whenToUse: 'After processing images, to triage image_keywords_unmatched: turn high-frequency unmatched keywords into aliases on existing tags, new canonical tags, or a keyword blacklist. Re-run any time the unmatched table grows.',
  phases: [
    { title: 'Load', detail: 'refresh canonical + unmatched data from the DB' },
    { title: 'Classify', detail: 'one agent per batch of unmatched keywords' },
    { title: 'Verify', detail: 'adversarially re-check each MAP decision against the vocabulary' },
    { title: 'Consolidate', detail: 'unify new-tag proposals across batches' },
  ],
}

const ROOT = 'c:\\temp\\ImageIndexer-main'
const CANON = `${ROOT}\\tag_canonical.txt`
const UNMATCHED = `${ROOT}\\tag_unmatched.txt`
const OUT = `${ROOT}\\tag_proposals_full.json`

// args: { threshold?: number (min image count, default 10), batchSize?: number (default 70) }
const threshold = (args && args.threshold) || 10
const batchSize = (args && args.batchSize) || 70

// The user's tagging taste — what belongs in the vocabulary vs. what's noise.
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
          decision: { type: 'string', enum: ['map', 'new', 'blacklist', 'skip'] },
          tag: { type: 'string', description: 'for decision=map: the EXACT canonical tag string it maps to' },
          new_tag: { type: 'string', description: 'for decision=new: proposed new canonical tag name' },
          confidence: { type: 'string', enum: ['high', 'med', 'low'] },
          note: { type: 'string' },
        },
        required: ['keyword', 'decision', 'confidence', 'note'],
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
          correct: { type: 'boolean', description: 'true if the keyword genuinely means the same as the tag and the tag exists verbatim in the vocabulary' },
          corrected_tag: { type: 'string', description: 'if not correct but a better existing tag exists, the better tag; else empty' },
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
          tag: { type: 'string', description: 'unified canonical name for the new tag' },
          aliases: { type: 'array', items: { type: 'string' }, description: 'unmatched keywords that should map under it' },
          rationale: { type: 'string' },
        },
        required: ['tag', 'aliases', 'rationale'],
      },
    },
  },
  required: ['new_tags'],
}

phase('Load')

const loaded = await agent(
  `Refresh the tag-reconciliation data from the PostgreSQL database, then return the unmatched terms.

Run this exactly (it reads DB creds from settings.json and writes two files):

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
print('TERMS', len(rows))
conn.close()
"

Then read ${UNMATCHED} and return EVERY line as a result item {keyword, images} (images = the integer before the tab). Return all of them, do not truncate.`,
  {
    label: 'load:refresh-data',
    phase: 'Load',
    schema: {
      type: 'object',
      properties: {
        terms: {
          type: 'array',
          items: {
            type: 'object',
            properties: { keyword: { type: 'string' }, images: { type: 'number' } },
            required: ['keyword', 'images'],
          },
        },
      },
      required: ['terms'],
    },
  },
)

const terms = (loaded && loaded.terms) || []
log(`Loaded ${terms.length} unmatched terms (>= ${threshold} images). Batching by ${batchSize}.`)

phase('Classify')

const batches = []
for (let i = 0; i < terms.length; i += batchSize) batches.push(terms.slice(i, i + batchSize))

// Classify each batch, then adversarially verify its MAP decisions.
const classified = await pipeline(
  batches,
  (batch, _orig, idx) => agent(
    `${INTENT}

You are classifying a batch of UNMATCHED keywords (model output that did not match any canonical tag).
First, read the full canonical vocabulary file with the Read tool: ${CANON}
(one tag per line — these are the ONLY valid targets for decision=map; the tag string must appear verbatim).

For EACH keyword below, choose a decision:
- "map": it means the same as an existing canonical tag → set tag to that EXACT vocabulary string. Note: the engine already fuzzy-matches at 85, so only SEMANTIC equivalents remain (e.g. "moderate areola" → "Medium Areolas", "buttocks" → "Ass"). Confidence high only when the meaning is unambiguous.
- "new": a concept the user clearly tags (per the intent above) but no existing tag fits → propose new_tag (Title Case, matching the vocabulary's style). Group obvious synonyms under one new_tag name.
- "blacklist": an object/furniture/setting/background/prop/food/decor/image-quality/watermark term the user does NOT want (per the intent). Watermarks/logos map to "Watermark" if that tag exists, else blacklist.
- "skip": too vague, generic, or low-value to act on ("intimate pose", "nice view").

Keywords (one per line):
${batch.map(t => t.keyword).join('\n')}

Return a result for every keyword.`,
    { label: `classify:${idx}`, phase: 'Classify', schema: CLASSIFY_SCHEMA },
  ),
  (cls, batch, idx) => {
    const maps = ((cls && cls.results) || []).filter(r => r.decision === 'map' && r.tag)
    if (!maps.length) return { cls, verdicts: [] }
    return agent(
      `Adversarially verify these proposed keyword→tag mappings for an adult-image tag vocabulary.
Read the canonical vocabulary: ${CANON}
For each mapping decide correct=true ONLY if (a) the tag string appears verbatim in that file AND (b) the keyword genuinely means the same concept (not merely related). Anatomy size/shade words must match (moderate≈medium, prominent≈big is acceptable; "small" mapped to "Big Areolas" is NOT). If wrong but a better existing tag exists, give corrected_tag.

Mappings:
${maps.map(m => `${m.keyword}  ->  ${m.tag}`).join('\n')}`,
      { label: `verify:${idx}`, phase: 'Verify', schema: VERIFY_SCHEMA },
    ).then(v => ({ cls, verdicts: (v && v.verdicts) || [] })).catch(() => ({ cls, verdicts: [] }))
  },
)

// ---- aggregate in JS ----
const aliases = []         // {keyword, tag, confidence, images, note}
const blacklist = []       // {keyword, images, note}
const skipped = []         // {keyword, images}
const rawNewTags = []      // {keyword, new_tag, images}
const imagesOf = Object.fromEntries(terms.map(t => [t.keyword, t.images]))

for (const item of classified.filter(Boolean)) {
  const cls = item.cls || {}
  const vmap = Object.fromEntries((item.verdicts || []).map(v => [v.keyword, v]))
  for (const r of (cls.results || [])) {
    const imgs = imagesOf[r.keyword] || 0
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
  const consolidated = await agent(
    `These are proposed NEW canonical tags from many batches. Unify synonyms/duplicates into a single
canonical Title-Case name each (e.g. "Raised Legs" and "Legs Raised" → one tag) and list the member
keywords under each. Keep the user's vocabulary style (StashDB-like). Drop any that are really objects/
settings (those should have been blacklisted).

Proposed (keyword -> proposed new_tag):
${rawNewTags.map(n => `${n.keyword} -> ${n.new_tag}`).join('\n')}`,
    { label: 'consolidate:new-tags', phase: 'Consolidate', schema: CONSOLIDATE_SCHEMA },
  ).catch(() => null)
  if (consolidated && consolidated.new_tags) {
    newTags = consolidated.new_tags.map(nt => ({
      ...nt,
      images: (nt.aliases || []).reduce((s, a) => s + (imagesOf[a] || 0), 0),
    }))
  }
}

aliases.sort((a, b) => b.images - a.images)
blacklist.sort((a, b) => b.images - a.images)
newTags.sort((a, b) => b.images - a.images)

const proposal = {
  generated_threshold: threshold,
  totals: {
    terms: terms.length,
    aliases: aliases.length,
    alias_image_rows: aliases.reduce((s, a) => s + a.images, 0),
    new_tags: newTags.length,
    blacklist: blacklist.length,
    blacklist_image_rows: blacklist.reduce((s, a) => s + a.images, 0),
    skipped: skipped.length,
  },
  aliases, new_tags: newTags, blacklist, skipped,
}

// Persist the full proposal for review/apply.
await agent(
  `Write this exact JSON to ${OUT} (overwrite). Return only "written".\n\n${JSON.stringify(proposal)}`,
  { label: 'write:proposal', phase: 'Consolidate' },
)

log(`Done: ${aliases.length} aliases (${proposal.totals.alias_image_rows.toLocaleString()} rows), ` +
    `${newTags.length} new tags, ${blacklist.length} blacklist (${proposal.totals.blacklist_image_rows.toLocaleString()} rows), ` +
    `${skipped.length} skipped. Proposal -> tag_proposals_full.json`)

return proposal.totals
