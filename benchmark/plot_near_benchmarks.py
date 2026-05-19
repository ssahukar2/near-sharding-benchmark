#!/usr/bin/env python3
"""Generate publication-quality plots for NEAR sharded benchmark experiments."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

OUT_DIR = Path.home() / "benchmark_plots"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Data (as specified) ---
BASELINE_RPS = np.array(
    [1000, 2000, 3000, 5000, 7500, 9000, 12000, 15000, 20000, 25000, 30000],
 dtype=float,
)
BASELINE_TPS = np.array(
    [4005, 8020, 12059, 18569, 19521, 18930, 19666, 21346, 19140, 21812, 18529],
    dtype=float,
)
BASELINE_WALL = np.array([64, 68, 39, 26, 26, 25, 26, 23, 26, 23, 25], dtype=float)

PINNED_RPS = np.array([1000, 3000, 5000, 7500, 15000, 25000], dtype=float)
PINNED_TPS = np.array([4005, 11747, 14576, 17276, 21068, 21261], dtype=float)

NETEM1_RPS = np.array([3000, 5000, 7500, 15000, 25000], dtype=float)
NETEM1_TPS = np.array([12062, 17155, 17287, 19097, 17740], dtype=float)

NETEM2_RPS = np.array([5000, 7500], dtype=float)
NETEM2_TPS = np.array([17471, 16536], dtype=float)

CEILING_TPS = 59200
PEAK_IDX = int(np.argmax(BASELINE_TPS))
PEAK_RPS = BASELINE_RPS[PEAK_IDX]
PEAK_TPS = BASELINE_TPS[PEAK_IDX]

# Style
FIGSIZE = (10, 6)
DPI = 150
TITLE_FS = 14
LABEL_FS = 12
GRID_ALPHA = 0.3


def style_axes(ax: plt.Axes, xlabel: str, ylabel: str, title: str) -> None:
    ax.set_xlabel(xlabel, fontsize=LABEL_FS)
    ax.set_ylabel(ylabel, fontsize=LABEL_FS)
    ax.set_title(title, fontsize=TITLE_FS)
    ax.grid(True, alpha=GRID_ALPHA)


def lookup_tps(rps_arr: np.ndarray, tps_arr: np.ndarray, rps: float) -> float | None:
    idx = np.where(rps_arr == rps)[0]
    if len(idx) == 0:
        return None
    return float(tps_arr[idx[0]])


def plot1_baseline() -> plt.Figure:
    fig, ax = plt.subplots(figsize=FIGSIZE, layout="constrained")
    ax.set_xscale("log")

    # Shaded regions (log x-axis; data coordinates)
    ax.axvspan(1000, 3000, alpha=0.2, color="green", zorder=0)
    ax.axvspan(5000, 40000, alpha=0.2, color="orange", zorder=0)

    ax.plot(
        BASELINE_RPS,
        BASELINE_TPS,
        "-",
        color="0.2",
        linewidth=1.5,
        zorder=3,
    )
    ax.scatter(BASELINE_RPS, BASELINE_TPS, s=55, c="steelblue", edgecolors="navy", zorder=4)

    ax.axhline(
        CEILING_TPS,
        color="crimson",
        linestyle="--",
        linewidth=1.5,
        zorder=2,
    )

    ax.annotate(
        f"Peak: {PEAK_RPS:,.0f} RPS/shard\n{PEAK_TPS:,.0f} TPS",
        xy=(PEAK_RPS, PEAK_TPS),
        xytext=(PEAK_RPS * 0.55, PEAK_TPS + 2500),
        fontsize=11,
        arrowprops=dict(arrowstyle="->", color="0.3"),
    )

    style_axes(
        ax,
        "RPS per shard (log scale)",
        "approx_tps",
        "4-Shard NEAR Network Throughput vs Injection Rate",
    )
    legend_el = [
        Patch(facecolor="green", alpha=0.2, edgecolor="none", label="Linear Scaling (1000–3000)"),
        Patch(facecolor="orange", alpha=0.2, edgecolor="none", label="Saturation Plateau (5000+)"),
        Line2D([0], [0], color="crimson", linestyle="--", linewidth=1.5, label=f"Theoretical ceiling ({CEILING_TPS:,} TPS)"),
        Line2D([0], [0], color="0.2", linewidth=1.5, label="Baseline sweep"),
    ]
    ax.legend(handles=legend_el, loc="lower right", fontsize=9)
    return fig


def plot2_grouped_bars() -> plt.Figure:
    fig, ax = plt.subplots(figsize=FIGSIZE, layout="constrained")
    groups = [3000, 5000, 7500]
    x = np.arange(len(groups), dtype=float)
    width = 0.2
    colors = ["#1f77b4", "#d62728", "#ff7f0e", "#9467bd"]
    labels = ["Unpinned", "CPU pinned", "netem 1ms", "netem 2ms"]

    def series_for_group(i: int) -> list[float | None]:
        rps = groups[i]
        u = lookup_tps(BASELINE_RPS, BASELINE_TPS, rps)
        p = lookup_tps(PINNED_RPS, PINNED_TPS, rps)
        n1 = lookup_tps(NETEM1_RPS, NETEM1_TPS, rps)
        n2 = lookup_tps(NETEM2_RPS, NETEM2_TPS, rps) if rps in (5000, 7500) else None
        return [u, p, n1, n2]

    for b in range(4):
        heights = []
        for i in range(len(groups)):
            row = series_for_group(i)
            heights.append(row[b])
        offsets = (b - 1.5) * width
        for i, h in enumerate(heights):
            if h is None:
                continue
            ax.bar(
                x[i] + offsets,
                h,
                width,
                color=colors[b],
                edgecolor="0.2",
                linewidth=0.5,
                label=labels[b] if i == 0 else "",
            )

    # % change labels above pinned / netem vs unpinned
    for i, rps in enumerate(groups):
        u = series_for_group(i)[0]
        if u is None:
            continue
        for b, name in [(1, "pinned"), (2, "netem 1ms"), (3, "netem 2ms")]:
            h = series_for_group(i)[b]
            if h is None:
                continue
            pct = (h / u - 1.0) * 100.0
            xpos = x[i] + (b - 1.5) * width
            ax.text(
                xpos,
                h + max(BASELINE_TPS) * 0.012,
                f"{pct:+.1f}%",
                ha="center",
                va="bottom",
                fontsize=9,
                rotation=0,
            )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{int(g):,}" for g in groups])
    style_axes(
        ax,
        "RPS per shard",
        "approx_tps",
        "TPS Comparison: Unpinned vs CPU Pinned vs Network Delay",
    )
    ax.legend(loc="upper left", fontsize=10)
    return fig


def plot3_efficiency() -> plt.Figure:
    fig, ax = plt.subplots(figsize=FIGSIZE, layout="constrained")
    eff = (BASELINE_TPS / (BASELINE_RPS * 4.0)) * 100.0
    ax.plot(
        BASELINE_RPS,
        eff,
        "-o",
        color="darkgreen",
        markersize=6,
        linewidth=1.5,
        label="Unpinned efficiency",
    )
    ax.axhline(
        100.0,
        color="black",
        linestyle="-",
        linewidth=1.2,
        label="100% efficiency",
        zorder=2,
    )
    ax.fill_between(
        BASELINE_RPS,
        0,
        np.minimum(eff, 100.0),
        alpha=0.25,
        color="red",
        label="saturation",
    )
    ax.fill_between(
        BASELINE_RPS,
        100.0,
        eff,
        where=(eff > 100.0),
        alpha=0.25,
        color="lightgreen",
        interpolate=True,
    )
    style_axes(
        ax,
        "RPS per shard",
        "Efficiency %",
        "Network Efficiency vs Injection Rate (Unpinned)",
    )
    ax.legend(loc="upper right", fontsize=10)
    ax.set_ylim(0, max(110.0, float(np.nanmax(eff)) * 1.08))
    return fig


def plot4_wall_scatter() -> plt.Figure:
    fig, ax = plt.subplots(figsize=FIGSIZE, layout="constrained")
    norm = plt.Normalize(BASELINE_RPS.min(), BASELINE_RPS.max())
    cmap = plt.cm.viridis
    sc = ax.scatter(
        BASELINE_WALL,
        BASELINE_TPS,
        c=BASELINE_RPS,
        cmap=cmap,
        s=120,
        edgecolors="white",
        linewidths=1.2,
        zorder=3,
    )
    cbar = fig.colorbar(sc, ax=ax)
    cbar.set_label("RPS per shard", fontsize=LABEL_FS)
    for wx, ty, r in zip(BASELINE_WALL, BASELINE_TPS, BASELINE_RPS):
        ax.annotate(
            f"{int(r):,}",
            (wx, ty),
            textcoords="offset points",
            xytext=(6, 6),
            fontsize=10,
        )
    style_axes(
        ax,
        "wall_time (seconds)",
        "approx_tps",
        "Wall Time vs Throughput — Plateau Visualization",
    )
    return fig


def main() -> None:
    figures = [
        ("plot1_baseline_sweep.png", plot1_baseline),
        ("plot2_four_way_comparison.png", plot2_grouped_bars),
        ("plot3_efficiency_unpinned.png", plot3_efficiency),
        ("plot4_walltime_vs_tps.png", plot4_wall_scatter),
    ]

    saved: list[Path] = []
    for name, fn in figures:
        fig = fn()
        path = OUT_DIR / name
        fig.savefig(path, dpi=DPI, bbox_inches="tight")
        plt.close(fig)
        saved.append(path)

    fig, axes = plt.subplots(2, 2, figsize=(20, 12), layout="constrained")
    ax00, ax01, ax10, ax11 = axes.ravel()

    # Inline minimal copies for combined figure (match individual plots)
    def add_plot1(ax: plt.Axes) -> None:
        ax.set_xscale("log")
        ax.axvspan(1000, 3000, alpha=0.2, color="green")
        ax.axvspan(5000, 40000, alpha=0.2, color="orange")
        ax.plot(BASELINE_RPS, BASELINE_TPS, "-", color="0.2", linewidth=1.5, zorder=3)
        ax.scatter(BASELINE_RPS, BASELINE_TPS, s=40, c="steelblue", edgecolors="navy", zorder=4)
        ax.axhline(CEILING_TPS, color="crimson", linestyle="--", linewidth=1.2)
        ax.annotate(
            f"Peak: {PEAK_RPS:,.0f}\n{PEAK_TPS:,.0f} TPS",
            xy=(PEAK_RPS, PEAK_TPS),
            xytext=(PEAK_RPS * 0.55, PEAK_TPS + 2500),
            fontsize=10,
            arrowprops=dict(arrowstyle="->", color="0.3"),
        )
        ax.set_xlabel("RPS per shard (log)", fontsize=LABEL_FS)
        ax.set_ylabel("approx_tps", fontsize=LABEL_FS)
        ax.set_title("4-Shard NEAR Network Throughput vs Injection Rate", fontsize=TITLE_FS)
        ax.grid(True, alpha=GRID_ALPHA)
        leg = [
            Patch(facecolor="green", alpha=0.2, edgecolor="none", label="Linear Scaling (1000–3000)"),
            Patch(facecolor="orange", alpha=0.2, edgecolor="none", label="Saturation Plateau (5000+)"),
            Line2D([0], [0], color="crimson", linestyle="--", linewidth=1.2, label="Ceiling"),
            Line2D([0], [0], color="0.2", linewidth=1.5, label="Baseline"),
        ]
        ax.legend(handles=leg, fontsize=8, loc="lower right")

    def add_plot2(ax: plt.Axes) -> None:
        groups = [3000, 5000, 7500]
        x = np.arange(len(groups), dtype=float)
        width = 0.2
        colors = ["#1f77b4", "#d62728", "#ff7f0e", "#9467bd"]
        labels = ["Unpinned", "CPU pinned", "netem 1ms", "netem 2ms"]

        def series_for_group(i: int) -> list[float | None]:
            rps = groups[i]
            return [
                lookup_tps(BASELINE_RPS, BASELINE_TPS, rps),
                lookup_tps(PINNED_RPS, PINNED_TPS, rps),
                lookup_tps(NETEM1_RPS, NETEM1_TPS, rps),
                lookup_tps(NETEM2_RPS, NETEM2_TPS, rps) if rps in (5000, 7500) else None,
            ]

        for b in range(4):
            for i in range(len(groups)):
                h = series_for_group(i)[b]
                if h is None:
                    continue
                ax.bar(
                    x[i] + (b - 1.5) * width,
                    h,
                    width,
                    color=colors[b],
                    edgecolor="0.2",
                    linewidth=0.4,
                    label=labels[b] if i == 0 else "",
                )
        for i, rps in enumerate(groups):
            u = series_for_group(i)[0]
            if u is None:
                continue
            for b in (1, 2, 3):
                h = series_for_group(i)[b]
                if h is None:
                    continue
                pct = (h / u - 1.0) * 100.0
                xpos = x[i] + (b - 1.5) * width
                ax.text(
                    xpos,
                    h + max(BASELINE_TPS) * 0.01,
                    f"{pct:+.1f}%",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )
        ax.set_xticks(x)
        ax.set_xticklabels([f"{int(g):,}" for g in groups])
        ax.set_xlabel("RPS per shard", fontsize=LABEL_FS)
        ax.set_ylabel("approx_tps", fontsize=LABEL_FS)
        ax.set_title(
            "TPS Comparison: Unpinned vs CPU Pinned vs Network Delay",
            fontsize=TITLE_FS,
        )
        ax.grid(True, alpha=GRID_ALPHA, axis="y")
        ax.legend(loc="upper left", fontsize=8)

    def add_plot3(ax: plt.Axes) -> None:
        eff = (BASELINE_TPS / (BASELINE_RPS * 4.0)) * 100.0
        ax.plot(
            BASELINE_RPS,
            eff,
            "-o",
            color="darkgreen",
            markersize=4,
            linewidth=1.2,
            label="Efficiency",
        )
        ax.axhline(100.0, color="black", linestyle="-", linewidth=1.0, label="100% efficiency")
        ax.fill_between(
            BASELINE_RPS,
            0,
            np.minimum(eff, 100.0),
            alpha=0.25,
            color="red",
            label="saturation",
        )
        ax.fill_between(
            BASELINE_RPS,
            100.0,
            eff,
            where=(eff > 100.0),
            alpha=0.25,
            color="lightgreen",
            interpolate=True,
        )
        ax.set_xlabel("RPS per shard", fontsize=LABEL_FS)
        ax.set_ylabel("Efficiency %", fontsize=LABEL_FS)
        ax.set_title(
            "Network Efficiency vs Injection Rate (Unpinned)",
            fontsize=TITLE_FS,
        )
        ax.grid(True, alpha=GRID_ALPHA)
        ax.set_ylim(0, max(110.0, float(np.nanmax(eff)) * 1.08))
        ax.legend(loc="upper right", fontsize=8)

    def add_plot4(ax: plt.Axes) -> None:
        sc = ax.scatter(
            BASELINE_WALL,
            BASELINE_TPS,
            c=BASELINE_RPS,
            cmap=plt.cm.viridis,
            s=80,
            edgecolors="white",
            linewidths=0.8,
        )
        fig.colorbar(sc, ax=ax, label="RPS per shard")
        for wx, ty, r in zip(BASELINE_WALL, BASELINE_TPS, BASELINE_RPS):
            ax.annotate(
                f"{int(r):,}",
                (wx, ty),
                textcoords="offset points",
                xytext=(4, 4),
                fontsize=8,
            )
        ax.set_xlabel("wall_time (seconds)", fontsize=LABEL_FS)
        ax.set_ylabel("approx_tps", fontsize=LABEL_FS)
        ax.set_title(
            "Wall Time vs Throughput — Plateau Visualization",
            fontsize=TITLE_FS,
        )
        ax.grid(True, alpha=GRID_ALPHA)

    add_plot1(ax00)
    add_plot2(ax01)
    add_plot3(ax10)
    add_plot4(ax11)
    combined_path = OUT_DIR / "all_plots.png"
    fig.savefig(combined_path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    saved.append(combined_path)

    print("Saved:")
    for p in saved:
        print(f"  {p}")


if __name__ == "__main__":
    main()
