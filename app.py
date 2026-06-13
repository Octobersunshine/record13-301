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


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
