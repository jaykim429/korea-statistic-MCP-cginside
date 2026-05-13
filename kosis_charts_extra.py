"""Small SVG chart helpers used by kosis_mcp_server.

The MCP server keeps the tool logic in kosis_mcp_server.py and imports these
helpers for the extra chart tools. They intentionally have no third-party
dependencies so the server can import cleanly in a minimal environment.
"""

from __future__ import annotations

from html import escape
from math import ceil, floor
from typing import Optional


def _svg(w: int, h: int, body: list[str]) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" font-family="Arial, sans-serif">'
        + "".join(body)
        + "</svg>"
    )


def _scale(v: float, lo: float, hi: float, a: float, b: float) -> float:
    if hi == lo:
        return (a + b) / 2
    return a + (v - lo) * (b - a) / (hi - lo)


def _title(parts: list[str], title: str, source: str, w: int) -> None:
    parts.append(f'<text x="24" y="30" font-size="18" font-weight="700">{escape(title)}</text>')
    if source:
        parts.append(f'<text x="{w - 24}" y="30" text-anchor="end" font-size="11" fill="#64748b">{escape(source)}</text>')


def _wrap_text(text: str, max_chars: int = 34) -> list[str]:
    text = str(text)
    if len(text) <= max_chars:
        return [text]
    lines: list[str] = []
    remaining = text
    while len(remaining) > max_chars and len(lines) < 2:
        cut = remaining.rfind(" ", 0, max_chars + 1)
        if cut <= 0:
            cut = max_chars
        lines.append(remaining[:cut].strip())
        remaining = remaining[cut:].strip()
    if remaining:
        lines.append(remaining)
    return lines[:3]


def chart_heatmap_svg(
    matrix: list[list[Optional[float]]],
    row_labels: list[str],
    col_labels: list[str],
    title: str,
    source: str = "",
    unit: str = "",
) -> str:
    w, h = 760, max(360, 90 + 26 * len(row_labels))
    left, top, cell_w, cell_h = 110, 70, 42, 22
    vals = [v for row in matrix for v in row if v is not None]
    if not vals:
        return _svg(w, h, [f'<text x="{w/2}" y="{h/2}" text-anchor="middle">No data</text>'])
    lo, hi = min(vals), max(vals)
    parts: list[str] = []
    _title(parts, title, source, w)
    for j, label in enumerate(col_labels):
        x = left + j * cell_w + cell_w / 2
        parts.append(f'<text x="{x:.1f}" y="58" text-anchor="middle" font-size="10" fill="#475569">{escape(str(label))}</text>')
    for i, label in enumerate(row_labels):
        y = top + i * cell_h
        parts.append(f'<text x="{left - 8}" y="{y + 15}" text-anchor="end" font-size="11" fill="#334155">{escape(label)}</text>')
        for j, value in enumerate(matrix[i]):
            x = left + j * cell_w
            if value is None:
                fill = "#f1f5f9"
            else:
                t = _scale(float(value), lo, hi, 0, 1)
                red = int(239 - 155 * t)
                green = int(246 - 96 * t)
                blue = int(255 - 55 * t)
                fill = f"#{red:02x}{green:02x}{blue:02x}"
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" rx="2" fill="{fill}"/>')
    parts.append(f'<text x="24" y="{h - 22}" font-size="11" fill="#64748b">min {lo:.2f}, max {hi:.2f} {escape(unit)}</text>')
    return _svg(w, h, parts)


def chart_distribution_svg(
    values: list[float],
    title: str,
    bins: int = 10,
    unit: str = "",
    source: str = "",
    annotation_labels: Optional[list[tuple[str, float]]] = None,
) -> str:
    w, h = 720, 380
    parts: list[str] = []
    _title(parts, title, source, w)
    if not values:
        parts.append(f'<text x="{w/2}" y="{h/2}" text-anchor="middle">No data</text>')
        return _svg(w, h, parts)
    lo, hi = min(values), max(values)
    bins = max(1, bins)
    step = (hi - lo) / bins if hi != lo else 1
    counts = [0] * bins
    for v in values:
        idx = min(bins - 1, int((v - lo) / step))
        counts[idx] += 1
    max_count = max(counts)
    base_y, plot_h, left, plot_w = 320, 230, 54, 600
    bar_w = plot_w / bins
    for i, count in enumerate(counts):
        bh = _scale(count, 0, max_count, 0, plot_h)
        x = left + i * bar_w
        parts.append(f'<rect x="{x + 2:.1f}" y="{base_y - bh:.1f}" width="{bar_w - 4:.1f}" height="{bh:.1f}" fill="#2563eb"/>')
    median = sorted(values)[len(values) // 2]
    mean = sum(values) / len(values)
    for label, val, color in (("mean", mean, "#dc2626"), ("median", median, "#059669")):
        x = _scale(val, lo, hi, left, left + plot_w)
        parts.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="82" y2="{base_y}" stroke="{color}" stroke-width="2"/>')
        parts.append(f'<text x="{x + 4:.1f}" y="78" font-size="11" fill="{color}">{label}</text>')
    if annotation_labels:
        for name, val in annotation_labels:
            x = _scale(val, lo, hi, left, left + plot_w)
            parts.append(f'<circle cx="{x:.1f}" cy="{base_y + 16}" r="4" fill="#f59e0b"/>')
            parts.append(f'<text x="{x:.1f}" y="{base_y + 32}" text-anchor="middle" font-size="10">{escape(name)}</text>')
    parts.append(f'<text x="{left}" y="352" font-size="11" fill="#64748b">{lo:.2f} - {hi:.2f} {escape(unit)}</text>')
    return _svg(w, h, parts)


def chart_dual_axis_svg(
    series_a: list[tuple[str, float]],
    series_b: list[tuple[str, float]],
    label_a: str,
    label_b: str,
    title: str,
    unit_a: str = "",
    unit_b: str = "",
    source: str = "",
) -> str:
    w, h = 760, 420
    parts: list[str] = []
    _title(parts, title, source, w)
    if not series_a or not series_b:
        parts.append(f'<text x="{w/2}" y="{h/2}" text-anchor="middle">No data</text>')
        return _svg(w, h, parts)
    left, right, top, bottom = 64, 696, 70, 340
    xs = sorted(set([t for t, _ in series_a] + [t for t, _ in series_b]))
    xa = {t: _scale(i, 0, max(1, len(xs) - 1), left, right) for i, t in enumerate(xs)}
    a_vals, b_vals = [v for _, v in series_a], [v for _, v in series_b]
    def path(series: list[tuple[str, float]], vals: list[float]) -> str:
        lo, hi = min(vals), max(vals)
        pts = [f'{xa[t]:.1f},{_scale(v, lo, hi, bottom, top):.1f}' for t, v in series if t in xa]
        return "M " + " L ".join(pts)
    parts.append(f'<path d="{path(series_a, a_vals)}" fill="none" stroke="#2563eb" stroke-width="3"/>')
    parts.append(f'<path d="{path(series_b, b_vals)}" fill="none" stroke="#dc2626" stroke-width="3" stroke-dasharray="6 4"/>')
    parts.append(f'<text x="{left}" y="58" font-size="12" fill="#2563eb">{escape(label_a)} ({escape(unit_a)})</text>')
    parts.append(f'<text x="{right}" y="58" text-anchor="end" font-size="12" fill="#dc2626">{escape(label_b)} ({escape(unit_b)})</text>')
    parts.append(f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#cbd5e1"/>')
    return _svg(w, h, parts)


def chart_dashboard_svg(
    title: str,
    timeseries: list[tuple[str, float]],
    items: list[tuple[str, float]],
    summary: dict,
    forecast: Optional[list[tuple[str, float, float, float]]] = None,
    unit: str = "",
    source: str = "",
) -> str:
    w, h = 900, 560
    parts: list[str] = []
    _title(parts, title, source, w)
    parts.append('<rect x="24" y="56" width="410" height="220" fill="#f8fafc" stroke="#cbd5e1"/>')
    if timeseries:
        vals = [v for _, v in timeseries]
        lo, hi = min(vals), max(vals)
        pts = []
        for i, (_, v) in enumerate(timeseries):
            x = _scale(i, 0, max(1, len(timeseries) - 1), 48, 410)
            y = _scale(v, lo, hi, 246, 84)
            pts.append(f"{x:.1f},{y:.1f}")
        parts.append(f'<path d="M {" L ".join(pts)}" fill="none" stroke="#2563eb" stroke-width="3"/>')
    parts.append('<rect x="466" y="56" width="410" height="220" fill="#f8fafc" stroke="#cbd5e1"/>')
    if items:
        top_items = sorted(items, key=lambda x: x[1], reverse=True)[:6]
        max_v = max(v for _, v in top_items)
        for i, (name, v) in enumerate(top_items):
            y = 86 + i * 28
            bw = _scale(v, 0, max_v, 0, 260)
            parts.append(f'<text x="486" y="{y + 13}" font-size="11">{escape(name)}</text>')
            parts.append(f'<rect x="560" y="{y}" width="{bw:.1f}" height="18" fill="#0f766e"/>')
    parts.append('<rect x="24" y="308" width="410" height="220" fill="#f8fafc" stroke="#cbd5e1"/>')
    y = 338
    for key, value in list(summary.items())[:6]:
        lines = _wrap_text(f"{key}: {value}", 48)
        for idx, line in enumerate(lines):
            if idx == 0 and ":" in line:
                label, rest = line.split(":", 1)
                parts.append(
                    f'<text x="48" y="{y}" font-size="12"><tspan font-weight="700">'
                    f'{escape(label)}</tspan>:{escape(rest)}</text>'
                )
            else:
                parts.append(f'<text x="48" y="{y}" font-size="12">{escape(line)}</text>')
            y += 16
        y += 7
    parts.append('<rect x="466" y="308" width="410" height="220" fill="#f8fafc" stroke="#cbd5e1"/>')
    if forecast:
        parts.append(f'<text x="490" y="338" font-size="13" font-weight="700">Forecast ({escape(unit)})</text>')
        for i, row in enumerate(forecast[:5]):
            period, value, low, high = row
            parts.append(f'<text x="490" y="{366 + i * 24}" font-size="12">{escape(str(period))}: {value:.2f} [{low:.2f}, {high:.2f}]</text>')
    return _svg(w, h, parts)
