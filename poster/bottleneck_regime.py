#!/usr/bin/env python3
"""Generate the Fig.5 "Bottleneck Regime" SVG for the poster.

Run:
    python3 bottleneck_regime.py    # writes ./bottleneck_regime.svg

Three vertically stacked boxes summarising the dominant bottleneck at each
shard count, drawn in the same visual idiom as architecture.svg
(pastel fill + colored 2px stroke, gray arrow markers, sans-serif heads).

Color choices intentionally mirror architecture.svg's palette so the two
figures read as part of the same legend on the poster:
    - Cache pressure   -> teal/cyan  (storage palette)
    - Witness Gossip   -> deep blue  (P2P palette)
    - Coherence Collapse -> red     (consensus palette)
"""

from pathlib import Path

# -------------------- palette (matches diagram.py) --------------------

TEAL   = "#0e7490"; TEAL_BG = "#ecfeff"; TEAL_T = "#155e75"
NAVY   = "#1e3a8a"; NAVY_BG = "#eef2ff"
RED    = "#dc2626"; RED_BG  = "#fef2f2"; RED_T  = "#991b1b"
GRAY   = "#6b7280"
INK    = "#1f2937"
WHITE  = "white"

FONT = ("-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif")

# -------------------- layout --------------------

WIDTH    = 420
HEIGHT   = 510

BOX_X    = 60
BOX_W    = 300
BOX_H    = 110
ARROW_GAP = 32        # vertical gap occupied by an arrow between boxes

Y_TOP    = 30
Y_B1     = Y_TOP                        # box 1 top
Y_A1     = Y_B1 + BOX_H                 # arrow 1 top
Y_B2     = Y_A1 + ARROW_GAP             # box 2 top
Y_A2     = Y_B2 + BOX_H                 # arrow 2 top
Y_B3     = Y_A2 + ARROW_GAP             # box 3 top
Y_CAP    = Y_B3 + BOX_H + 46            # caption baseline

CENTER_X = BOX_X + BOX_W // 2

ARROW_PAD = 4   # leave a couple px between box edge and arrow head/tail


# -------------------- helpers --------------------

def header() -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" width="{WIDTH}" height="{HEIGHT}" '
        f'font-family="{FONT}" font-size="13" color="{GRAY}">\n'
        f'  <rect width="{WIDTH}" height="{HEIGHT}" fill="{WHITE}"/>\n'
        f'  <defs>\n'
        f'    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
        f'markerWidth="6" markerHeight="6" orient="auto-start-reverse">\n'
        f'      <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor"/>\n'
        f'    </marker>\n'
        f'  </defs>\n'
    )


def footer() -> str:
    return "</svg>\n"


def box(y: int, fill: str, stroke: str, lines: list[tuple[str, int, int]],
        text_color: str) -> str:
    """Pastel box at (BOX_X, y) with centered, vertically distributed text.

    `lines` is a list of (text, font-size, weight). Text is vertically
    centered as a group inside the box.
    """
    out = [
        f'  <rect x="{BOX_X}" y="{y}" width="{BOX_W}" height="{BOX_H}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2" rx="4"/>\n'
    ]

    line_height = 28 if len(lines) <= 2 else 24
    block_h = line_height * (len(lines) - 1)
    first_baseline = y + (BOX_H - block_h) // 2 + 6  # +6 visual nudge

    for i, (text, size, weight) in enumerate(lines):
        ty = first_baseline + i * line_height
        out.append(
            f'  <text x="{CENTER_X}" y="{ty}" fill="{text_color}" '
            f'font-size="{size}" font-weight="{weight}" '
            f'text-anchor="middle">{text}</text>\n'
        )
    return "".join(out)


def arrow(y_top: int) -> str:
    """Vertical arrow from y_top + ARROW_PAD down to y_top + ARROW_GAP - ARROW_PAD."""
    y1 = y_top + ARROW_PAD
    y2 = y_top + ARROW_GAP - ARROW_PAD
    return (
        f'  <line x1="{CENTER_X}" y1="{y1}" x2="{CENTER_X}" y2="{y2}" '
        f'stroke="{GRAY}" stroke-width="1.6" marker-end="url(#arrow)"/>\n'
    )


def caption(text: str) -> str:
    return (
        f'  <text x="{CENTER_X}" y="{Y_CAP}" fill="{INK}" '
        f'font-size="16" font-weight="700" text-anchor="middle">{text}</text>\n'
    )


# -------------------- compose --------------------

def build_svg() -> str:
    parts = [header()]

    # Box 1: L3 Cache Pressure (N = 1, 2)
    parts.append(box(
        Y_B1, TEAL_BG, TEAL,
        lines=[
            ("N = 1, 2", 18, 700),
            ("L3 Cache Pressure", 15, 700),
        ],
        text_color=TEAL_T,
    ))
    parts.append(arrow(Y_A1))

    # Box 2: Witness Gossip (N = 4, 8)
    parts.append(box(
        Y_B2, NAVY_BG, NAVY,
        lines=[
            ("N = 4, 8", 18, 700),
            ("Witness Gossip", 15, 700),
            ("~600 ms / chunk", 13, 600),
        ],
        text_color=NAVY,
    ))
    parts.append(arrow(Y_A2))

    # Box 3: Coherence Collapse (N >= 16)
    parts.append(box(
        Y_B3, RED_BG, RED,
        lines=[
            ("N &#8805; 16", 18, 700),  # &#8805; = "≥"
            ("Coherence Collapse", 15, 700),
        ],
        text_color=RED_T,
    ))

    parts.append(caption("Fig.5 &#8212; Bottleneck Regime"))  # &#8212; = em-dash
    parts.append(footer())
    return "".join(parts)


def main() -> None:
    out_path = Path(__file__).resolve().parent / "bottleneck_regime.svg"
    out_path.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
