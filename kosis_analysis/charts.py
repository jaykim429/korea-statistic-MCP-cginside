from __future__ import annotations

from typing import Any, Optional

from mcp.types import TextContent
from scipy import stats as scipy_stats


def _format_number(v: Any) -> str:
    try:
        n = float(v)
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.3f}".rstrip("0").rstrip(".")
    except (ValueError, TypeError):
        return str(v)


def _svg_header(w: int = 640, h: int = 380) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" font-family="sans-serif">'
    )


def _svg_to_image(svg: str) -> TextContent:
    """Wrap an SVG payload in a MCP-compatible content block.

    The MCP spec accepts ImageContent only for raster mime types
    (image/png, image/jpeg, image/gif, image/webp). Claude Desktop and
    Claude Code clients reject image/svg+xml outright, which used to
    break every chart tool with a content-format error.

    We now emit the SVG inside a fenced ```svg block as TextContent.
    Web embeds that render markdown+SVG (claude.ai, browser MCP
    clients) still show the chart; CLI clients see the markup as code,
    which is far better than a hard failure."""
    return TextContent(type="text", text=f"```svg\n{svg}\n```")


def _chart_line_svg(
    series: list[tuple[str, float]],
    title: str, ylabel: str = "",
    source: str = "", note: str = "",
) -> str:
    W, H = 640, 380
    PL, PR, PT, PB = 60, 30, 50, 60

    if not series:
        return f'{_svg_header(W, H)}<text x="{W//2}" y="{H//2}" text-anchor="middle">데이터 없음</text></svg>'

    labels = [s[0] for s in series]
    values = [s[1] for s in series]
    vmin, vmax = min(values), max(values)
    if vmin == vmax:
        vmin, vmax = vmin - 1, vmax + 1
    span = vmax - vmin
    plot_w = W - PL - PR
    plot_h = H - PT - PB

    def x(i):
        if len(values) == 1:
            return PL + plot_w / 2
        return PL + i * plot_w / (len(values) - 1)

    def y(v):
        return PT + plot_h - (v - vmin) / span * plot_h

    points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values))

    y_ticks = []
    for i in range(5):
        v = vmin + span * i / 4
        py = y(v)
        y_ticks.append(
            f'<line x1="{PL}" y1="{py:.1f}" x2="{W-PR}" y2="{py:.1f}" stroke="#eee" stroke-width="0.5"/>'
            f'<text x="{PL-8}" y="{py+4:.1f}" text-anchor="end" font-size="10" fill="#666">{_format_number(v)}</text>'
        )

    step = max(1, len(labels) // 8)
    x_labels = []
    for i, lab in enumerate(labels):
        if i % step == 0 or i == len(labels) - 1:
            x_labels.append(
                f'<text x="{x(i):.1f}" y="{H-PB+18}" text-anchor="middle" font-size="10" fill="#666">{lab}</text>'
            )

    parts = [
        _svg_header(W, H),
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{W//2}" y="24" text-anchor="middle" font-size="14" font-weight="600">{title}</text>',
        *y_ticks,
        f'<polyline fill="none" stroke="#2563eb" stroke-width="2" points="{points}"/>',
    ]
    for i, v in enumerate(values):
        parts.append(f'<circle cx="{x(i):.1f}" cy="{y(v):.1f}" r="3" fill="#2563eb"/>')
    parts.extend(x_labels)
    if ylabel:
        parts.append(
            f'<text x="14" y="{H//2}" text-anchor="middle" font-size="10" fill="#666" '
            f'transform="rotate(-90,14,{H//2})">{ylabel}</text>'
        )
    if source:
        parts.append(f'<text x="{PL}" y="{H-8}" font-size="9" fill="#888">출처: {source}</text>')
    if note:
        parts.append(f'<text x="{W-PR}" y="{H-8}" text-anchor="end" font-size="9" fill="#888">{note}</text>')
    parts.append('</svg>')
    return "".join(parts)


def _chart_bar_svg(items: list[tuple[str, float]], title: str, source: str = "") -> str:
    W, H = 640, 380
    PL, PR, PT, PB = 60, 30, 50, 80

    if not items:
        return f'{_svg_header(W, H)}<text x="{W//2}" y="{H//2}" text-anchor="middle">데이터 없음</text></svg>'

    values = [s[1] for s in items]
    vmin = min(0, min(values))
    vmax = max(values)
    if vmin == vmax:
        vmax = vmin + 1
    span = vmax - vmin

    plot_w = W - PL - PR
    plot_h = H - PT - PB
    n = len(items)
    bar_w = plot_w / n * 0.7
    gap = plot_w / n * 0.3

    def y(v):
        return PT + plot_h - (v - vmin) / span * plot_h

    def x_pos(i):
        return PL + i * plot_w / n + gap / 2

    y_ticks = []
    for i in range(5):
        v = vmin + span * i / 4
        py = y(v)
        y_ticks.append(
            f'<line x1="{PL}" y1="{py:.1f}" x2="{W-PR}" y2="{py:.1f}" stroke="#eee" stroke-width="0.5"/>'
            f'<text x="{PL-8}" y="{py+4:.1f}" text-anchor="end" font-size="10" fill="#666">{_format_number(v)}</text>'
        )

    bars = []
    for i, (lab, v) in enumerate(items):
        bx = x_pos(i)
        by = y(max(v, 0))
        bh = abs(y(v) - y(0))
        bars.append(
            f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="#2563eb"/>'
            f'<text x="{bx+bar_w/2:.1f}" y="{H-PB+18}" text-anchor="middle" font-size="10" fill="#666" '
            f'transform="rotate(-30,{bx+bar_w/2:.1f},{H-PB+18})">{lab}</text>'
        )

    parts = [
        _svg_header(W, H),
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{W//2}" y="24" text-anchor="middle" font-size="14" font-weight="600">{title}</text>',
        *y_ticks, *bars,
    ]
    if source:
        parts.append(f'<text x="{PL}" y="{H-8}" font-size="9" fill="#888">출처: {source}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _chart_scatter_svg(
    points: list[tuple[float, float]],
    title: str, xlabel: str = "", ylabel: str = "",
    source: str = "", r_value: Optional[float] = None,
) -> str:
    W, H = 640, 380
    PL, PR, PT, PB = 60, 30, 50, 60

    if not points:
        return f'{_svg_header(W, H)}<text x="{W//2}" y="{H//2}" text-anchor="middle">데이터 없음</text></svg>'

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    if xmin == xmax:
        xmin, xmax = xmin - 1, xmax + 1
    if ymin == ymax:
        ymin, ymax = ymin - 1, ymax + 1
    xspan, yspan = xmax - xmin, ymax - ymin
    plot_w = W - PL - PR
    plot_h = H - PT - PB

    def xp(v):
        return PL + (v - xmin) / xspan * plot_w

    def yp(v):
        return PT + plot_h - (v - ymin) / yspan * plot_h

    parts = [
        _svg_header(W, H),
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{W//2}" y="24" text-anchor="middle" font-size="14" font-weight="600">{title}</text>',
        f'<line x1="{PL}" y1="{H-PB}" x2="{W-PR}" y2="{H-PB}" stroke="#888" stroke-width="0.5"/>',
        f'<line x1="{PL}" y1="{PT}" x2="{PL}" y2="{H-PB}" stroke="#888" stroke-width="0.5"/>',
    ]
    for x, y in points:
        parts.append(f'<circle cx="{xp(x):.1f}" cy="{yp(y):.1f}" r="4" fill="#2563eb" opacity="0.65"/>')

    if len(points) >= 2:
        slope, intercept, *_ = scipy_stats.linregress(xs, ys)
        x1, x2 = xmin, xmax
        y1, y2 = slope * x1 + intercept, slope * x2 + intercept
        parts.append(
            f'<line x1="{xp(x1):.1f}" y1="{yp(y1):.1f}" x2="{xp(x2):.1f}" y2="{yp(y2):.1f}" '
            f'stroke="#dc2626" stroke-width="1.5" stroke-dasharray="5,3"/>'
        )

    if xlabel:
        parts.append(f'<text x="{W//2}" y="{H-30}" text-anchor="middle" font-size="11" fill="#444">{xlabel}</text>')
    if ylabel:
        parts.append(
            f'<text x="18" y="{H//2}" text-anchor="middle" font-size="11" fill="#444" '
            f'transform="rotate(-90,18,{H//2})">{ylabel}</text>'
        )
    if r_value is not None:
        parts.append(
            f'<text x="{W-PR-10}" y="{PT+18}" text-anchor="end" font-size="11" fill="#dc2626" font-weight="600">'
            f'r = {r_value:.3f}</text>'
        )
    if source:
        parts.append(f'<text x="{PL}" y="{H-8}" font-size="9" fill="#888">출처: {source}</text>')
    parts.append("</svg>")
    return "".join(parts)
