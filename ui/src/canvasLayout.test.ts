import { describe, expect, it } from 'vitest'
import { GROUP_GAP, HEADER_H, hitTest, tileLayout } from './canvasLayout'

/** `n` hosts split into groups of `per` (like 46 hosts per rack). */
function groups(n: number, per: number) {
  const out: { key: string; hosts: { id: string }[] }[] = []
  for (let i = 0; i < n; i++) {
    if (i % per === 0) out.push({ key: `rack-${out.length + 1}`, hosts: [] })
    out[out.length - 1].hosts.push({ id: `h${i}` })
  }
  return out
}

describe('tileLayout', () => {
  it('500 hosts fit with wrapping', () => {
    const width = 800
    const { rects, headers, height } = tileLayout(groups(500, 46), width)
    expect(rects).toHaveLength(500)
    expect(new Set(rects.map((r) => r.hostId)).size).toBe(500)
    expect(headers.map((h) => [h.key, h.count])).toEqual(groups(500, 46).map((g) => [g.key, g.hosts.length]))
    // Every tile is 18 px, inside the width and the height, and none overlap.
    for (const r of rects) {
      expect([r.w, r.h]).toEqual([18, 18])
      expect(r.x).toBeGreaterThanOrEqual(0)
      expect(r.x + r.w).toBeLessThanOrEqual(width)
      expect(r.y + r.h).toBeLessThanOrEqual(height)
    }
    const cells = new Set(rects.map((r) => `${r.x},${r.y}`))
    expect(cells.size).toBe(500)
    // 38 columns of 18 px + 3 px gap fit in 800 px, so a rack of 46 wraps onto a second row.
    const rack1 = rects.slice(0, 46)
    expect(new Set(rack1.map((r) => r.x)).size).toBe(38)
    expect(new Set(rack1.map((r) => r.y))).toEqual(new Set([HEADER_H, HEADER_H + 21]))
    expect(rack1[38]).toMatchObject({ x: 0, y: HEADER_H + 21 })
    // The next group starts below the last row of the previous one, under its own header.
    expect(headers[1].y).toBe(HEADER_H + 21 + 18 + GROUP_GAP)
    expect(rects[46]).toMatchObject({ x: 0, y: headers[1].y + HEADER_H })
    expect(height).toBe(Math.max(...rects.map((r) => r.y + r.h)))
  })

  it('takes the tile size and gap, and keeps one column when the width is too small for one', () => {
    const { rects } = tileLayout(groups(3, 3), 10, 30, 5)
    expect(rects.map((r) => [r.x, r.y, r.w])).toEqual([
      [0, HEADER_H, 30],
      [0, HEADER_H + 35, 30],
      [0, HEADER_H + 70, 30],
    ])
  })

  it('lays out nothing for no hosts', () => {
    expect(tileLayout([], 800)).toEqual({ rects: [], headers: [], height: 0 })
  })
})

describe('hitTest', () => {
  it('hitTest returns host under point', () => {
    const { rects } = tileLayout(groups(500, 46), 800)
    const r = rects[123]
    expect(hitTest(rects, r.x + 9, r.y + 9)).toBe(r.hostId)
    expect(hitTest(rects, r.x, r.y)).toBe(r.hostId)
    expect(hitTest(rects, r.x + r.w - 0.5, r.y + r.h - 0.5)).toBe(r.hostId)
    expect(hitTest(rects, r.x + r.w + 1, r.y + 9), 'the gap between tiles').toBeNull()
    expect(hitTest(rects, 5, 5), 'a group header').toBeNull()
    expect(hitTest(rects, 5000, 5000)).toBeNull()
  })
})
