#!/usr/bin/env python3
"""Generate an SVG architecture diagram for the single-node shard scaling experiment.

Run:
    python3 diagram.py            # writes ./architecture.svg in the same dir

Compact "headings-only" variant — sub-bullets removed, three explicit validator
columns (node 0, node 1, ··· , node N), no monitor callout, no bottom legend.
"""

from pathlib import Path

# -------------------- configuration --------------------

# Visible columns: three with content + one "..." gap.
# Column labels are driven by COL_LABELS (last entry is the gap).
COL_LABELS = ["node 0", "node 1", "···", "node N"]
DOTS_INDEX = 2                       # which column is the gap

LEFT_MARGIN = 200                    # tier-label gutter
RIGHT_MARGIN = 60
COL_W = 280
COL_GAP = 44

# -------------------- palette --------------------

NAVY = "#1e3a8a"

ORANGE     = "#f97316"; ORANGE_BG = "#fff7ed"; ORANGE_T = "#9a3412"
RED        = "#dc2626"; RED_BG    = "#fef2f2"; RED_T    = "#991b1b"
GREEN      = "#16a34a"; GREEN_BG  = "#f0fdf4"; GREEN_T  = "#166534"
PURPLE     = "#7c3aed"; PURPLE_BG = "#faf5ff"; PURPLE_T = "#5b21b6"
TEAL       = "#0e7490"; TEAL_BG   = "#ecfeff"; TEAL_T   = "#155e75"
GRAY       = "#6b7280"; GRAY_BG   = "#f3f4f6"
DOTS       = "#94a3b8"

INK = "#111827"
SUBTLE = "#374151"
MUTED = "#6b7280"

# -------------------- vertical layout --------------------

COL_TOP   = 30
HEADER_H  = 42
GAP_BOX   = 28        # vertical spacing between every consecutive box pair
BOX_H     = 54

H_TX  = COL_TOP + HEADER_H + GAP_BOX
H_CP  = H_TX  + BOX_H + GAP_BOX
H_RT  = H_CP  + BOX_H + GAP_BOX
H_NW  = H_RT  + BOX_H + GAP_BOX
H_PIN = H_NW  + BOX_H + GAP_BOX
COL_BOTTOM = H_PIN + BOX_H

P2P_Y = COL_BOTTOM + 28
P2P_H = 60

ROCKS_Y = P2P_Y + P2P_H + 24
ROCKS_H = 54

HDD_Y = ROCKS_Y + ROCKS_H + 10
HDD_H = 30

NUM_COLS = len(COL_LABELS)
CANVAS_W = LEFT_MARGIN + NUM_COLS * COL_W + (NUM_COLS - 1) * COL_GAP + RIGHT_MARGIN
CANVAS_H = HDD_Y + HDD_H + 40


def column_x(i: int) -> int:
    return LEFT_MARGIN + i * (COL_W + COL_GAP)


# -------------------- svg helpers --------------------

def t(x, y, s, *, fill=INK, size=12, weight=400, anchor="start", family="inherit"):
    f = f' font-family="{family}"' if family != "inherit" else ""
    return (
        f'<text x="{x}" y="{y}" fill="{fill}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}"{f}>{s}</text>'
    )


def rect(x, y, w, h, *, fill="none", stroke="none", sw=1, rx=4, dasharray=None):
    da = f' stroke-dasharray="{dasharray}"' if dasharray else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}" rx="{rx}"{da}/>'
    )


def header_box(x, y, label, fill=NAVY):
    return (
        rect(x, y, COL_W, HEADER_H, fill=fill, rx=6)
        + t(x + COL_W / 2, y + 28, label, fill="white", size=18, weight=700, anchor="middle")
    )


def arrow_down(x, y1, y2, color=MUTED):
    return (
        f'<line x1="{x}" y1="{y1}" x2="{x}" y2="{y2 - 6}" stroke="{color}" '
        f'stroke-width="1.6" marker-end="url(#arrow)"/>'
    )


# -------------------- column renderers --------------------

# (y_top, bg, stroke, text_color, heading)
SECTIONS = [
    ("H_TX",  ORANGE_BG, ORANGE, ORANGE_T, "tx_generator"),
    ("H_CP",  RED_BG,    RED,    RED_T,    "chunk producer  (1 CP seat)"),
    ("H_RT",  GREEN_BG,  GREEN,  GREEN_T,  "runtime / apply chunks"),
    ("H_NW",  PURPLE_BG, PURPLE, PURPLE_T, "network endpoints"),
    ("H_PIN", GRAY_BG,   GRAY,   SUBTLE,   "numactl pinning"),
]
SECTION_Y = {"H_TX": H_TX, "H_CP": H_CP, "H_RT": H_RT, "H_NW": H_NW, "H_PIN": H_PIN}


def validator_column(i: int, label: str) -> str:
    x = column_x(i)
    parts = [header_box(x, COL_TOP, label)]
    for key, bg, stroke, text_color, heading in SECTIONS:
        y = SECTION_Y[key]
        parts.append(rect(x, y, COL_W, BOX_H, fill=bg, stroke=stroke, sw=2))
        parts.append(t(x + COL_W / 2, y + BOX_H / 2 + 6, heading,
                       fill=text_color, weight=700, size=15, anchor="middle"))

    # internal data-flow arrows (tx → cp → rt → nw → pin)
    cx = x + COL_W / 2
    parts.append(arrow_down(cx, H_TX + BOX_H, H_CP))
    parts.append(arrow_down(cx, H_CP + BOX_H, H_RT))
    parts.append(arrow_down(cx, H_RT + BOX_H, H_NW))
    parts.append(arrow_down(cx, H_NW + BOX_H, H_PIN))
    return "\n".join(parts)


def dots_column(i: int, label: str) -> str:
    x = column_x(i)
    parts = [header_box(x, COL_TOP, label, fill=DOTS)]
    for key, _bg, stroke, _text_color, _heading in SECTIONS:
        y = SECTION_Y[key]
        parts.append(rect(x, y, COL_W, BOX_H, fill="white",
                          stroke=stroke, sw=1.5, rx=4, dasharray="6,5"))
        parts.append(t(x + COL_W / 2, y + BOX_H / 2 + 8, "· · ·",
                       fill=stroke, weight=700, size=22, anchor="middle"))

    # internal arrows for the gap column too (muted, to match the dashed look)
    cx = x + COL_W / 2
    parts.append(arrow_down(cx, H_TX + BOX_H, H_CP, color=DOTS))
    parts.append(arrow_down(cx, H_CP + BOX_H, H_RT, color=DOTS))
    parts.append(arrow_down(cx, H_RT + BOX_H, H_NW, color=DOTS))
    parts.append(arrow_down(cx, H_NW + BOX_H, H_PIN, color=DOTS))
    return "\n".join(parts)


# -------------------- shared bars --------------------

def p2p_bar() -> str:
    x0 = column_x(0)
    x1 = column_x(NUM_COLS - 1) + COL_W
    parts = [
        rect(x0, P2P_Y, x1 - x0, P2P_H, fill="#eef2ff", stroke=NAVY, sw=2, rx=10),
        t(x0 + (x1 - x0) / 2, P2P_Y + P2P_H / 2 + 7,
          "P2P mesh — full · boot_nodes lists every peer",
          fill=NAVY, weight=700, size=18, anchor="middle"),
    ]
    # arrows from the bottom of each column (below `pinning`) into the P2P bar
    for i in range(NUM_COLS):
        cx = column_x(i) + COL_W / 2
        color = NAVY if i != DOTS_INDEX else DOTS
        parts.append(arrow_down(cx, COL_BOTTOM, P2P_Y, color=color))
    return "\n".join(parts)


def rocks_strip() -> str:
    parts = []
    for i in range(NUM_COLS):
        x = column_x(i)
        if i == DOTS_INDEX:
            parts.append(rect(x, ROCKS_Y, COL_W, ROCKS_H, fill="white",
                              stroke=TEAL, sw=1.5, rx=12, dasharray="6,5"))
            parts.append(t(x + COL_W / 2, ROCKS_Y + ROCKS_H / 2 + 8, "· · ·",
                           fill=TEAL, weight=700, size=22, anchor="middle"))
        else:
            parts.append(rect(x, ROCKS_Y, COL_W, ROCKS_H, fill=TEAL_BG,
                              stroke=TEAL, sw=2, rx=12))
            parts.append(t(x + COL_W / 2, ROCKS_Y + ROCKS_H / 2 + 7, "RocksDB",
                           fill=TEAL_T, weight=700, size=16, anchor="middle"))
        cx = x + COL_W / 2
        color = TEAL if i != DOTS_INDEX else DOTS
        parts.append(arrow_down(cx, P2P_Y + P2P_H, ROCKS_Y, color=color))

    # shared HDD strip
    x0 = column_x(0)
    x1 = column_x(NUM_COLS - 1) + COL_W
    parts.append(rect(x0, HDD_Y, x1 - x0, HDD_H, fill="#1f2937", rx=4))
    parts.append(t(x0 + (x1 - x0) / 2, HDD_Y + HDD_H / 2 + 6,
                   "Single HDD  ·  /dev/sda2",
                   fill="white", weight=700, size=14, anchor="middle",
                   family="ui-monospace, Menlo, monospace"))
    return "\n".join(parts)


# -------------------- side / tier labels --------------------

def left_tier_labels() -> str:
    spec = [
        (H_TX  + BOX_H / 2, "WORKLOAD",  ORANGE),
        (H_CP  + BOX_H / 2, "CONSENSUS", RED),
        (H_RT  + BOX_H / 2, "RUNTIME",   GREEN),
        (H_NW  + BOX_H / 2, "NETWORK",   PURPLE),
        (H_PIN + BOX_H / 2, "PINNING",   GRAY),
        (P2P_Y + P2P_H / 2, "P2P MESH", NAVY),
        (ROCKS_Y + ROCKS_H / 2, "STORAGE", TEAL),
    ]
    parts = []
    for y, label, color in spec:
        parts.append(t(38, y + 5, label, fill=color, weight=700, size=13))
        parts.append(rect(20, y - 11, 6, 22, fill=color, rx=2))
    return "\n".join(parts)


# -------------------- compose --------------------

def build_svg() -> str:
    body = [left_tier_labels()]
    for i, label in enumerate(COL_LABELS):
        if i == DOTS_INDEX:
            body.append(dots_column(i, label))
        else:
            body.append(validator_column(i, label))
    body.append(p2p_bar())
    body.append(rocks_strip())

    arrow_def = (
        '<defs>'
        '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor"/>'
        '</marker>'
        '</defs>'
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'font-family="-apple-system, \'Segoe UI\', Helvetica, Arial, sans-serif" '
        f'font-size="13" color="{MUTED}">'
        f'<rect width="{CANVAS_W}" height="{CANVAS_H}" fill="white"/>'
        f'{arrow_def}'
        + "\n".join(body)
        + "</svg>"
    )


def main():
    out = Path(__file__).parent / "architecture.svg"
    out.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {out}  ({out.stat().st_size / 1024:.1f} KB · {CANVAS_W}×{CANVAS_H})")


if __name__ == "__main__":
    main()
