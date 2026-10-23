/** Where the canvas fleet grid draws each host: pure geometry, so it can be tested without a canvas. */

/** Height of a group's heading row, above its tiles. */
export const HEADER_H = 20
/** Space between the last row of one group and the next group's heading. */
export const GROUP_GAP = 10

export interface TileRect {
  hostId: string
  x: number
  y: number
  w: number
  h: number
}

export interface GroupHeader {
  key: string
  count: number
  x: number
  y: number
}

export interface TileLayout {
  rects: TileRect[]
  headers: GroupHeader[]
  height: number
}

/**
 * Lays groups out top to bottom, each a heading row then its hosts' square
 * tiles left to right, wrapping at `width` (always at least one column).
 * A host in several groups gets a tile in each.
 */
export function tileLayout(groups: { key: string; hosts: { id: string }[] }[], width: number, tile = 18, gap = 3): TileLayout {
  const step = tile + gap
  const cols = Math.max(1, Math.floor((width + gap) / step))
  const rects: TileRect[] = []
  const headers: GroupHeader[] = []
  let y = 0
  for (const g of groups) {
    if (headers.length > 0) y += GROUP_GAP
    headers.push({ key: g.key, count: g.hosts.length, x: 0, y })
    const top = y + HEADER_H
    g.hosts.forEach((h, i) => {
      rects.push({ hostId: h.id, x: (i % cols) * step, y: top + Math.floor(i / cols) * step, w: tile, h: tile })
    })
    const rows = Math.ceil(g.hosts.length / cols)
    y = top + (rows > 0 ? rows * step - gap : 0)
  }
  return { rects, headers, height: y }
}

/** The host whose tile contains the point, or null (gaps and headings hold none). */
export function hitTest(rects: TileRect[], x: number, y: number): string | null {
  for (const r of rects) {
    if (x >= r.x && x < r.x + r.w && y >= r.y && y < r.y + r.h) return r.hostId
  }
  return null
}
