import os
import io
import json
import difflib
from flask import Flask, request, jsonify, render_template, Response, stream_with_context

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["MAX_CONTENT_LENGTH"] = 512 * 1024 * 1024

CHUNK_SIZE = 5000
OVERLAP_SIZE = 500


def _read_lines_stream(file_stream):
    """从文件流中逐行读取，返回生成器。"""
    wrapper = io.TextIOWrapper(file_stream, encoding="utf-8", errors="replace")
    for line in wrapper:
        yield line.rstrip("\n\r")


def _fill_buffer(buf, line_iter, target_size):
    added = 0
    try:
        while len(buf) < target_size:
            buf.append(next(line_iter))
            added += 1
    except StopIteration:
        return True, added
    return False, added


def _find_first_anchor(matching_blocks, min_anchor_size):
    for mb in matching_blocks:
        if mb.size >= min_anchor_size:
            return mb.a, mb.b, mb.size
    return None


def _diff_range(buf_a, buf_b, end_a, end_b, off_a, off_b):
    sm = difflib.SequenceMatcher(None, buf_a[:end_a], buf_b[:end_b])
    lines = []
    added = 0
    deleted = 0
    modified = 0

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for line in buf_a[i1:i2]:
                lines.append({"type": "equal", "content": line})
        elif tag == "replace":
            a_lines = buf_a[i1:i2]
            b_lines = buf_b[j1:j2]
            max_len = max(len(a_lines), len(b_lines))
            for k in range(max_len):
                old = a_lines[k] if k < len(a_lines) else None
                new = b_lines[k] if k < len(b_lines) else None
                if old is not None and new is not None:
                    modified += 1
                    lines.append(
                        {"type": "modified", "old_content": old, "new_content": new}
                    )
                elif old is not None:
                    deleted += 1
                    lines.append({"type": "deleted", "content": old})
                elif new is not None:
                    added += 1
                    lines.append({"type": "added", "content": new})
        elif tag == "delete":
            for line in buf_a[i1:i2]:
                deleted += 1
                lines.append({"type": "deleted", "content": line})
        elif tag == "insert":
            for line in buf_b[j1:j2]:
                added += 1
                lines.append({"type": "added", "content": line})

    return lines, added, deleted, modified


def stream_compare(file_a_stream, file_b_stream, filename_a, filename_b,
                   chunk_size=CHUNK_SIZE, overlap_size=OVERLAP_SIZE):
    lines_a = _read_lines_stream(file_a_stream)
    lines_b = _read_lines_stream(file_b_stream)

    buf_a = []
    buf_b = []
    off_a = 0
    off_b = 0

    eof_a = False
    eof_b = False

    target_size = chunk_size
    max_buf_size = chunk_size * 4
    min_anchor = max(5, chunk_size // 100)

    total_added = 0
    total_deleted = 0
    total_modified = 0
    chunk_idx = 0

    yield json.dumps({
        "type": "start",
        "file_a": filename_a,
        "file_b": filename_b,
        "chunk_size": chunk_size,
        "min_anchor": min_anchor,
    }, ensure_ascii=False) + "\n"

    while True:
        if not eof_a:
            eof_a, _ = _fill_buffer(buf_a, lines_a, target_size)
        if not eof_b:
            eof_b, _ = _fill_buffer(buf_b, lines_b, target_size)

        if eof_a and eof_b and not buf_a and not buf_b:
            break

        sm = difflib.SequenceMatcher(None, buf_a, buf_b)
        matching_blocks = sm.get_matching_blocks()
        anchor = _find_first_anchor(matching_blocks, min_anchor)

        emit_until_a = len(buf_a)
        emit_until_b = len(buf_b)
        advance_a = len(buf_a)
        advance_b = len(buf_b)

        if anchor is not None:
            a_i, b_j, size = anchor
            emit_until_a = a_i + size
            emit_until_b = b_j + size
            advance_a = a_i + size
            advance_b = b_j + size
        else:
            if not eof_a or not eof_b:
                if len(buf_a) < max_buf_size or len(buf_b) < max_buf_size:
                    target_size = min(target_size * 2, max_buf_size)
                    continue
            advance_a = len(buf_a)
            advance_b = len(buf_b)
            emit_until_a = len(buf_a)
            emit_until_b = len(buf_b)

        emit_lines, add_cnt, del_cnt, mod_cnt = _diff_range(
            buf_a, buf_b, emit_until_a, emit_until_b, off_a, off_b
        )

        if emit_lines:
            total_added += add_cnt
            total_deleted += del_cnt
            total_modified += mod_cnt

            yield json.dumps({
                "type": "chunk",
                "chunk_index": chunk_idx,
                "lines": emit_lines,
                "chunk_summary": {
                    "added": add_cnt,
                    "deleted": del_cnt,
                    "modified": mod_cnt,
                },
            }, ensure_ascii=False) + "\n"

            chunk_idx += 1

        if advance_a > 0:
            buf_a = buf_a[advance_a:]
            off_a += advance_a
        if advance_b > 0:
            buf_b = buf_b[advance_b:]
            off_b += advance_b

        if target_size > chunk_size:
            target_size = chunk_size

    yield json.dumps({
        "type": "end",
        "summary": {
            "added": total_added,
            "deleted": total_deleted,
            "modified": total_modified,
        },
        "total_chunks": chunk_idx,
    }, ensure_ascii=False) + "\n"


def compare_texts(text_a: str, text_b: str, filename_a: str, filename_b: str):
    lines_a = text_a.splitlines(keepends=True)
    lines_b = text_b.splitlines(keepends=True)

    sm = difflib.SequenceMatcher(None, lines_a, lines_b)

    added = []
    deleted = []
    modified = []
    unified_lines = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for line in lines_a[i1:i2]:
                unified_lines.append({"type": "equal", "content": line.rstrip("\n\r")})
        elif tag == "replace":
            a_lines = lines_a[i1:i2]
            b_lines = lines_b[j1:j2]
            max_len = max(len(a_lines), len(b_lines))
            for k in range(max_len):
                old_line = a_lines[k].rstrip("\n\r") if k < len(a_lines) else None
                new_line = b_lines[k].rstrip("\n\r") if k < len(b_lines) else None
                if old_line is not None and new_line is not None:
                    modified.append(
                        {
                            "old_line": i1 + k + 1,
                            "old_content": old_line,
                            "new_line": j1 + k + 1,
                            "new_content": new_line,
                        }
                    )
                    unified_lines.append(
                        {"type": "modified", "old_content": old_line, "new_content": new_line}
                    )
                elif old_line is not None:
                    deleted.append(
                        {"line": i1 + k + 1, "content": old_line}
                    )
                    unified_lines.append({"type": "deleted", "content": old_line})
                elif new_line is not None:
                    added.append(
                        {"line": j1 + k + 1, "content": new_line}
                    )
                    unified_lines.append({"type": "added", "content": new_line})
        elif tag == "delete":
            for k, line in enumerate(lines_a[i1:i2]):
                deleted.append({"line": i1 + k + 1, "content": line.rstrip("\n\r")})
                unified_lines.append({"type": "deleted", "content": line.rstrip("\n\r")})
        elif tag == "insert":
            for k, line in enumerate(lines_b[j1:j2]):
                added.append({"line": j1 + k + 1, "content": line.rstrip("\n\r")})
                unified_lines.append({"type": "added", "content": line.rstrip("\n\r")})

    unified_diff = list(
        difflib.unified_diff(lines_a, lines_b, fromfile=filename_a, tofile=filename_b, lineterm="")
    )

    return {
        "summary": {
            "added": len(added),
            "deleted": len(deleted),
            "modified": len(modified),
        },
        "added": added,
        "deleted": deleted,
        "modified": modified,
        "unified_lines": unified_lines,
        "unified_diff": unified_diff,
    }


HTML_REPORT_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  background: #fff;
  color: #24292e;
  line-height: 1.5;
  padding: 20px;
}
.report-header {
  background: #f6f8fa;
  border: 1px solid #e1e4e8;
  border-radius: 6px;
  padding: 16px;
  margin-bottom: 20px;
}
.report-header h1 {
  font-size: 18px;
  font-weight: 600;
  margin-bottom: 12px;
  color: #24292e;
}
.report-meta {
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
  font-size: 13px;
  color: #586069;
}
.meta-item span { font-weight: 600; color: #24292e; }
.summary {
  display: flex;
  gap: 16px;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid #e1e4e8;
}
.summary-card {
  padding: 8px 16px;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
}
.summary-card.added { background: #e6ffec; color: #1a7f37; }
.summary-card.deleted { background: #ffebe9; color: #cf222e; }
.summary-card.modified { background: #fff8c5; color: #9a6700; }
.file-header {
  background: #f6f8fa;
  border: 1px solid #e1e4e8;
  border-radius: 6px 6px 0 0;
  padding: 10px 16px;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 13px;
  font-weight: 600;
  margin-top: 20px;
}
.file-header .old { color: #cf222e; }
.file-header .new { color: #1a7f37; }
.file-header .arrow { color: #586069; margin: 0 8px; }
.hunk-header {
  background: #f1f8ff;
  border-left: 1px solid #e1e4e8;
  border-right: 1px solid #e1e4e8;
  padding: 6px 12px;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 12px;
  color: #032f62;
  font-weight: 600;
}
.diff-table {
  width: 100%;
  border-collapse: collapse;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 12px;
  line-height: 1.4;
  border: 1px solid #e1e4e8;
  border-top: none;
}
.diff-table tr { display: table-row; }
.diff-table td {
  padding: 0 8px;
  white-space: pre;
  vertical-align: top;
}
.diff-table td.empty {
  width: 1%;
  background: #fafbfc;
  border-right: 1px solid #e1e4e8;
  color: #d1d5da;
  user-select: none;
  text-align: right;
  min-width: 40px;
}
.diff-table td.line-num {
  width: 1%;
  color: #959da5;
  text-align: right;
  user-select: none;
  border-right: 1px solid #e1e4e8;
  min-width: 40px;
  background: #fafbfc;
}
.diff-table td.content { width: 48%; }
.diff-table tr.equal td.line-num { background: #fafbfc; }
.diff-table tr.equal td.content { background: #fff; }
.diff-table tr.added td.line-num { background: #e6ffec; }
.diff-table tr.added td.content { background: #e6ffec; color: #1a7f37; }
.diff-table tr.deleted td.line-num { background: #ffebe9; }
.diff-table tr.deleted td.content { background: #ffebe9; color: #cf222e; }
.diff-table tr.modified-old td.line-num { background: #ffebe9; }
.diff-table tr.modified-old td.content { background: #ffebe9; color: #cf222e; text-decoration: line-through; }
.diff-table tr.modified-new td.line-num { background: #e6ffec; }
.diff-table tr.modified-new td.content { background: #e6ffec; color: #1a7f37; }
.diff-gutter { width: 1%; background: #fafbfc; user-select: none; text-align: center; min-width: 20px; }
.diff-gutter.added { background: #e6ffec; color: #1a7f37; font-weight: 600; }
.diff-gutter.deleted { background: #ffebe9; color: #cf222e; font-weight: 600; }
.diff-gutter.modified { background: #fff8c5; color: #9a6700; font-weight: 600; }
.footer { margin-top: 30px; text-align: center; color: #959da5; font-size: 12px; }
"""


def generate_html_report(lines_a, lines_b, filename_a, filename_b, context_lines=3):
    sm = difflib.SequenceMatcher(None, lines_a, lines_b)

    from datetime import datetime

    total_added = 0
    total_deleted = 0
    total_modified = 0

    hunks = []
    current_hunk = []
    last_i = -1
    last_j = -1
    hunk_start_i = None
    hunk_start_j = None

    def flush_hunk():
        if not current_hunk:
            return
        if hunk_start_i is None or hunk_start_j is None:
            return
        hunk_i_len = (last_i - hunk_start_i + 1) if last_i >= hunk_start_i else 0
        hunk_j_len = (last_j - hunk_start_j + 1) if last_j >= hunk_start_j else 0
        hunks.append({
            "start_i": hunk_start_i,
            "start_j": hunk_start_j,
            "len_i": hunk_i_len,
            "len_j": hunk_j_len,
            "rows": current_hunk,
        })

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for idx in range(i2 - i1):
                i = i1 + idx
                j = j1 + idx
                if current_hunk or (hunk_start_i is None):
                    pass
                if len(current_hunk) == 0 or (current_hunk and current_hunk[-1]["type"] != "equal"):
                    flush_hunk()
                    hunk_start_i = max(0, i - context_lines)
                    hunk_start_j = max(0, j - context_lines)
                    current_hunk = []
                    for ctx_idx in range(context_lines):
                        ctx_i = i - context_lines + ctx_idx
                        ctx_j = j - context_lines + ctx_idx
                        if ctx_i >= 0 and ctx_j >= 0 and ctx_i < i and ctx_j < j:
                            current_hunk.append({
                                "type": "equal",
                                "line_a": ctx_i + 1,
                                "line_b": ctx_j + 1,
                                "content": lines_a[ctx_i],
                            })
                current_hunk.append({
                    "type": "equal",
                    "line_a": i + 1,
                    "line_b": j + 1,
                    "content": lines_a[i],
                })
                last_i = i
                last_j = j
                if len([r for r in current_hunk if r["type"] != "equal"]) > 0:
                    ctx_count = 0
                    for r in reversed(current_hunk):
                        if r["type"] == "equal":
                            ctx_count += 1
                        else:
                            break
                    if ctx_count > context_lines * 2:
                        flush_hunk()
                        hunk_start_i = None
                        hunk_start_j = None
                        current_hunk = []
        elif tag == "replace":
            a_lines = lines_a[i1:i2]
            b_lines = lines_b[j1:j2]
            max_len = max(len(a_lines), len(b_lines))
            if hunk_start_i is None or hunk_start_j is None:
                hunk_start_i = max(0, i1 - context_lines)
                hunk_start_j = max(0, j1 - context_lines)
                current_hunk = []
                for ctx_idx in range(context_lines):
                    ctx_i = i1 - context_lines + ctx_idx
                    ctx_j = j1 - context_lines + ctx_idx
                    if ctx_i >= 0 and ctx_j >= 0 and ctx_i < i1 and ctx_j < j1:
                        current_hunk.append({
                            "type": "equal",
                            "line_a": ctx_i + 1,
                            "line_b": ctx_j + 1,
                            "content": lines_a[ctx_i],
                        })
            for k in range(max_len):
                if k < len(a_lines) and k < len(b_lines):
                    current_hunk.append({
                        "type": "modified-old",
                        "line_a": i1 + k + 1,
                        "line_b": None,
                        "content": a_lines[k],
                    })
                    current_hunk.append({
                        "type": "modified-new",
                        "line_a": None,
                        "line_b": j1 + k + 1,
                        "content": b_lines[k],
                    })
                    total_modified += 1
                elif k < len(a_lines):
                    current_hunk.append({
                        "type": "deleted",
                        "line_a": i1 + k + 1,
                        "line_b": None,
                        "content": a_lines[k],
                    })
                    total_deleted += 1
                else:
                    current_hunk.append({
                        "type": "added",
                        "line_a": None,
                        "line_b": j1 + k + 1,
                        "content": b_lines[k],
                    })
                    total_added += 1
            last_i = i2 - 1
            last_j = j2 - 1
        elif tag == "delete":
            if hunk_start_i is None or hunk_start_j is None:
                hunk_start_i = max(0, i1 - context_lines)
                hunk_start_j = max(0, j1 - context_lines)
                current_hunk = []
                for ctx_idx in range(context_lines):
                    ctx_i = i1 - context_lines + ctx_idx
                    ctx_j = j1 - context_lines + ctx_idx
                    if ctx_i >= 0 and ctx_j >= 0 and ctx_i < i1 and ctx_j < j1:
                        current_hunk.append({
                            "type": "equal",
                            "line_a": ctx_i + 1,
                            "line_b": ctx_j + 1,
                            "content": lines_a[ctx_i],
                        })
            for k, line in enumerate(lines_a[i1:i2]):
                current_hunk.append({
                    "type": "deleted",
                    "line_a": i1 + k + 1,
                    "line_b": None,
                    "content": line,
                })
                total_deleted += 1
            last_i = i2 - 1
            last_j = j1 - 1
        elif tag == "insert":
            if hunk_start_i is None or hunk_start_j is None:
                hunk_start_i = max(0, i1 - context_lines)
                hunk_start_j = max(0, j1 - context_lines)
                current_hunk = []
                for ctx_idx in range(context_lines):
                    ctx_i = i1 - context_lines + ctx_idx
                    ctx_j = j1 - context_lines + ctx_idx
                    if ctx_i >= 0 and ctx_j >= 0 and ctx_i < i1 and ctx_j < j1:
                        current_hunk.append({
                            "type": "equal",
                            "line_a": ctx_i + 1,
                            "line_b": ctx_j + 1,
                            "content": lines_a[ctx_i],
                        })
            for k, line in enumerate(lines_b[j1:j2]):
                current_hunk.append({
                    "type": "added",
                    "line_a": None,
                    "line_b": j1 + k + 1,
                    "content": line,
                })
                total_added += 1
            last_i = i1 - 1
            last_j = j2 - 1

    flush_hunk()

    def esc(s):
        return (
            s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\t", "    ")
        )

    html_parts = [
        "<!DOCTYPE html>",
        '<html lang="zh-CN">',
        "<head>",
        '<meta charset="UTF-8">',
        "<title>文本对比报告</title>",
        f"<style>{HTML_REPORT_CSS}</style>",
        "</head>",
        "<body>",
        '<div class="report-header">',
        "<h1>📄 文本对比报告</h1>",
        '<div class="report-meta">',
        f'<div class="meta-item">生成时间: <span>{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</span></div>',
        f'<div class="meta-item">原始文件: <span>{esc(filename_a)}</span></div>',
        f'<div class="meta-item">对比文件: <span>{esc(filename_b)}</span></div>',
        "</div>",
        '<div class="summary">',
        f'<div class="summary-card added">+ {total_added} 新增</div>',
        f'<div class="summary-card deleted">- {total_deleted} 删除</div>',
        f'<div class="summary-card modified">~ {total_modified} 修改</div>',
        "</div>",
        "</div>",
    ]

    if hunks:
        html_parts.append(
            f'<div class="file-header"><span class="old">--- {esc(filename_a)}</span><span class="arrow">→</span><span class="new">+++ {esc(filename_b)}</span></div>'
        )

        for hunk in hunks:
            hunk_header = f"@@ -{hunk['start_i']+1},{hunk['len_i']} +{hunk['start_j']+1},{hunk['len_j']} @@"
            html_parts.append(f'<div class="hunk-header">{esc(hunk_header)}</div>')
            html_parts.append('<table class="diff-table"><colgroup><col class="line-num"><col class="diff-gutter"><col class="content"><col class="line-num"><col class="diff-gutter"><col class="content"></colgroup><tbody>')

            for row in hunk["rows"]:
                rtype = row["type"]
                line_a = row["line_a"]
                line_b = row["line_b"]
                content = esc(row["content"])

                if rtype == "equal":
                    html_parts.append(
                        f'<tr class="equal">'
                        f'<td class="line-num">{line_a}</td>'
                        f'<td class="diff-gutter"> </td>'
                        f'<td class="content">{content}</td>'
                        f'<td class="line-num">{line_b}</td>'
                        f'<td class="diff-gutter"> </td>'
                        f'<td class="content">{content}</td>'
                        f"</tr>"
                    )
                elif rtype == "deleted":
                    html_parts.append(
                        f'<tr class="deleted">'
                        f'<td class="line-num">{line_a}</td>'
                        f'<td class="diff-gutter deleted">-</td>'
                        f'<td class="content">{content}</td>'
                        f'<td class="empty"></td>'
                        f'<td class="diff-gutter deleted"> </td>'
                        f'<td class="content"></td>'
                        f"</tr>"
                    )
                elif rtype == "added":
                    html_parts.append(
                        f'<tr class="added">'
                        f'<td class="empty"></td>'
                        f'<td class="diff-gutter added"> </td>'
                        f'<td class="content"></td>'
                        f'<td class="line-num">{line_b}</td>'
                        f'<td class="diff-gutter added">+</td>'
                        f'<td class="content">{content}</td>'
                        f"</tr>"
                    )
                elif rtype == "modified-old":
                    html_parts.append(
                        f'<tr class="modified-old">'
                        f'<td class="line-num">{line_a}</td>'
                        f'<td class="diff-gutter modified">-</td>'
                        f'<td class="content">{content}</td>'
                        f'<td class="empty"></td>'
                        f'<td class="diff-gutter modified"> </td>'
                        f'<td class="content"></td>'
                        f"</tr>"
                    )
                elif rtype == "modified-new":
                    html_parts.append(
                        f'<tr class="modified-new">'
                        f'<td class="empty"></td>'
                        f'<td class="diff-gutter modified"> </td>'
                        f'<td class="content"></td>'
                        f'<td class="line-num">{line_b}</td>'
                        f'<td class="diff-gutter modified">+</td>'
                        f'<td class="content">{content}</td>'
                        f"</tr>"
                    )

            html_parts.append("</tbody></table>")

    html_parts.append('<div class="footer">Generated by Text Diff Service</div>')
    html_parts.append("</body></html>")

    return "\n".join(html_parts), {
        "added": total_added,
        "deleted": total_deleted,
        "modified": total_modified,
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/compare", methods=["POST"])
def api_compare():
    if "file_a" not in request.files or "file_b" not in request.files:
        return jsonify({"error": "请上传两个文件 (file_a, file_b)"}), 400

    file_a = request.files["file_a"]
    file_b = request.files["file_b"]

    if file_a.filename == "" or file_b.filename == "":
        return jsonify({"error": "文件名不能为空"}), 400

    try:
        text_a = file_a.read().decode("utf-8", errors="replace")
        text_b = file_b.read().decode("utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": f"读取文件失败: {str(e)}"}), 400

    result = compare_texts(text_a, text_b, file_a.filename, file_b.filename)
    return jsonify(result)


@app.route("/api/compare/stream", methods=["POST"])
def api_compare_stream():
    if "file_a" not in request.files or "file_b" not in request.files:
        return jsonify({"error": "请上传两个文件 (file_a, file_b)"}), 400

    file_a = request.files["file_a"]
    file_b = request.files["file_b"]

    if file_a.filename == "" or file_b.filename == "":
        return jsonify({"error": "文件名不能为空"}), 400

    chunk_size = int(request.form.get("chunk_size", CHUNK_SIZE))
    overlap_size = int(request.form.get("overlap_size", OVERLAP_SIZE))
    overlap_size = min(overlap_size, chunk_size // 2)

    try:
        stream_a = file_a.stream
        stream_b = file_b.stream
    except Exception as e:
        return jsonify({"error": f"读取文件失败: {str(e)}"}), 400

    def generate():
        try:
            yield from stream_compare(
                stream_a, stream_b,
                file_a.filename, file_b.filename,
                chunk_size=chunk_size, overlap_size=overlap_size,
            )
        except Exception as e:
            yield json.dumps({"type": "error", "error": str(e)}, ensure_ascii=False) + "\n"

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson; charset=utf-8",
        headers={"X-Accel-Buffering": "no"},
    )


@app.route("/api/compare/html", methods=["POST"])
def api_compare_html():
    if "file_a" not in request.files or "file_b" not in request.files:
        return jsonify({"error": "请上传两个文件 (file_a, file_b)"}), 400

    file_a = request.files["file_a"]
    file_b = request.files["file_b"]

    if file_a.filename == "" or file_b.filename == "":
        return jsonify({"error": "文件名不能为空"}), 400

    try:
        text_a = file_a.read().decode("utf-8", errors="replace")
        text_b = file_b.read().decode("utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": f"读取文件失败: {str(e)}"}), 400

    lines_a = [line.rstrip("\n\r") for line in text_a.splitlines() if line or True]
    lines_b = [line.rstrip("\n\r") for line in text_b.splitlines() if line or True]

    html_content, stats = generate_html_report(
        lines_a, lines_b, file_a.filename, file_b.filename
    )

    return Response(
        html_content,
        mimetype="text/html; charset=utf-8",
        headers={
            "Content-Disposition": f"inline; filename=\"diff_report.html\""
        },
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
