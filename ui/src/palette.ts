/**
 * The search behind the command palette: the query as a substring, or its
 * letters in order where each jump lands on the start of a word
 * (`wc` finds `web-clients`, not `whereabouts-pjxfg`).
 */

export interface Candidate {
  /** What is matched and shown. */
  label: string
  /** Matched too, ranked lower: a namespace, a kind. */
  hint?: string
}

/**
 * How well `query` matches `text`, higher better; null if it does not.
 * A prefix beats a word start beats a substring beats letters spread out.
 */
export function matchScore(query: string, text: string): number | null {
  const q = query.trim().toLowerCase()
  const t = text.toLowerCase()
  if (q === '') return 0
  if (t.startsWith(q)) return 1000 - t.length
  const at = t.indexOf(q)
  if (at >= 0) return (/[\s/\-_.:]/.test(t[at - 1]) ? 800 : 600) - at - t.length / 100
  // Letters in order; after a skip, only at a word's start.
  let score = 300
  let from = 0
  for (const c of q.replace(/\s+/g, '')) {
    // Next to the letter before, or further on at the start of a word.
    let i = t.indexOf(c, from)
    while (i > from && !wordStart(t, i)) i = t.indexOf(c, i + 1)
    if (i < 0) return null
    score -= i - from
    from = i + 1
  }
  return score - t.length / 100
}

function wordStart(t: string, i: number): boolean {
  return i === 0 || /[\s/\-_.:]/.test(t[i - 1])
}

/** The candidates matching `query`, best first; the label counts more than the hint. */
export function rank<T extends Candidate>(items: T[], query: string, limit = 40): T[] {
  if (query.trim() === '') return items.slice(0, limit)
  const scored: { item: T; score: number }[] = []
  for (const item of items) {
    const a = matchScore(query, item.label)
    const b = item.hint ? matchScore(query, `${item.hint} ${item.label}`) : null
    const score = Math.max(a ?? -Infinity, b === null ? -Infinity : b - 200)
    if (score > -Infinity) scored.push({ item, score })
  }
  return scored
    .sort((x, y) => y.score - x.score)
    .slice(0, limit)
    .map((s) => s.item)
}
