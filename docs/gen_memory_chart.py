#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 readme.md「内存占用对比」小节使用的 SVG 柱状图。

不依赖任何第三方库：直接拼接 SVG 文本，产物是纯文本矢量图，
方便 diff、随仓库演进，并且能被 GitHub 原生渲染。

用法：
    python3 docs/gen_memory_chart.py
"""

import os

# 数据来源：无内存池 / jemalloc / 内置内存池 三种配置在测试开始、峰值、结束时的内存占用（MB）
PHYSICAL = [
    ("无内存池", (4.90, 37.03, 36.52)),
    ("jemalloc", (6.70, 34.69, 33.77)),
    ("内存池", (5.01, 37.55, 37.05)),
]
VIRTUAL = [
    ("无内存池", (110.49, 161.27, 130.57)),
    ("jemalloc", (67.88, 96.38, 95.87)),
    ("内存池", (110.50, 161.28, 131.43)),
]

# 三个阶段各自的颜色，图例顺序与此一致
STAGES = [("开始", "#4472C4"), ("峰值", "#ED7D31"), ("结束", "#A5A5A5")]

FONT = "Helvetica, Arial, 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif"

# 画布与布局常量
CANVAS_W, CANVAS_H = 1200, 600
PLOT_TOP, PLOT_BOTTOM = 96, 460
BAR_W, BAR_GAP = 30, 10

# 两个面板：(标题, 绘图区左边界, 绘图区右边界, y 轴最大值, y 轴刻度)
PANELS = [
    ("物理内存（MB）", 90, 560, 40, [0, 10, 20, 30, 40], PHYSICAL),
    ("虚拟内存（MB）", 680, 1150, 180, [0, 30, 60, 90, 120, 150, 180], VIRTUAL),
]

LEGEND_Y = 540


def esc(text):
    """转义 XML 特殊字符。"""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )


def text(x, y, s, size=14, fill="#333333", anchor="middle", rotate=None):
    """生成一个 <text> 元素，rotate 为绕 (x, y) 的旋转角度。"""
    transform = ' transform="translate({:.2f}, {:.2f}) rotate({})"'.format(x, y, rotate) if rotate else ""
    pos = ' x="0" y="0"' if rotate else ' x="{:.2f}" y="{:.2f}"'.format(x, y)
    return (
        '<text{} font-family="{}" font-size="{}" fill="{}" text-anchor="{}">{}</text>'.format(
            pos + transform, FONT, size, fill, anchor, esc(s)
        )
    )


def render_panel(title, x_left, x_right, y_max, ticks, data):
    """生成一个分组柱状面板的 SVG 片段。"""
    out = []
    plot_h = PLOT_BOTTOM - PLOT_TOP
    panel_w = x_right - x_left

    def y_of(value):
        return PLOT_BOTTOM - value / float(y_max) * plot_h

    # 标题
    out.append(text((x_left + x_right) / 2.0, 36, title, size=17, fill="#222222"))

    # y 轴网格线与刻度
    for tick in ticks:
        y = y_of(tick)
        if tick != 0:
            out.append(
                '<line x1="{:.2f}" y1="{:.2f}" x2="{:.2f}" y2="{:.2f}" stroke="#EAEAEA" stroke-width="1"/>'.format(
                    x_left, y, x_right, y
                )
            )
        out.append(text(x_left - 12, y + 5, str(tick), size=13, fill="#555555", anchor="end"))

    # 坐标轴
    out.append(
        '<line x1="{:.2f}" y1="{:.2f}" x2="{:.2f}" y2="{:.2f}" stroke="#9A9A9A" stroke-width="1.2"/>'.format(
            x_left, PLOT_TOP, x_left, PLOT_BOTTOM
        )
    )
    out.append(
        '<line x1="{:.2f}" y1="{:.2f}" x2="{:.2f}" y2="{:.2f}" stroke="#9A9A9A" stroke-width="1.2"/>'.format(
            x_left, PLOT_BOTTOM, x_right, PLOT_BOTTOM
        )
    )

    # 每类配置一组，组内按「开始 / 峰值 / 结束」排列三根柱子
    group_w = panel_w / float(len(data))
    for gi, (cfg, values) in enumerate(data):
        center = x_left + (gi + 0.5) * group_w
        bars_w = len(STAGES) * BAR_W + (len(STAGES) - 1) * BAR_GAP
        bar_x = center - bars_w / 2.0
        for si, (_stage, color) in enumerate(STAGES):
            value = values[si]
            x = bar_x + si * (BAR_W + BAR_GAP)
            y = y_of(value)
            out.append(
                '<rect x="{:.2f}" y="{:.2f}" width="{}" height="{:.2f}" fill="{}"/>'.format(
                    x, y, BAR_W, PLOT_BOTTOM - y, color
                )
            )
            # 数值标签：贴在柱顶、自下而上竖排，避免三根柱子互相挤压
            out.append(
                text(x + BAR_W / 2.0 + 4, y - 6, "{:.2f}".format(value), size=12.5,
                     fill="#444444", anchor="start", rotate=-90)
            )
        out.append(text(center, PLOT_BOTTOM + 28, cfg, size=15, fill="#333333"))

    return out


def render_legend():
    """生成底部图例：色块 + 阶段名。"""
    out = []
    item_w = 16 + 6 + 34
    gap = 36
    total = len(STAGES) * item_w + (len(STAGES) - 1) * gap
    x = CANVAS_W / 2.0 - total / 2.0
    for stage, color in STAGES:
        out.append(
            '<rect x="{:.2f}" y="{:.2f}" width="16" height="16" fill="{}"/>'.format(x, LEGEND_Y - 13, color)
        )
        out.append(text(x + 22, LEGEND_Y, stage, size=15, fill="#333333", anchor="start"))
        x += item_w + gap
    return out


def build_svg():
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" viewBox="0 0 {} {}" '
        'role="img" aria-label="物理内存与虚拟内存占用对比">'.format(
            CANVAS_W, CANVAS_H, CANVAS_W, CANVAS_H
        ),
        "<title>无内存池 / jemalloc / 内置内存池 在开始、峰值、结束时的物理内存与虚拟内存占用对比（MB）</title>",
        '<rect x="0" y="0" width="{}" height="{}" fill="#FFFFFF"/>'.format(CANVAS_W, CANVAS_H),
    ]
    for title, x_left, x_right, y_max, ticks, data in PANELS:
        parts.extend(render_panel(title, x_left, x_right, y_max, ticks, data))
    parts.extend(render_legend())
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(here, "images", "memory_bar_chart.svg")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    svg = build_svg()
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print("written: {} ({} bytes)".format(out_path, len(svg.encode("utf-8"))))


if __name__ == "__main__":
    main()
