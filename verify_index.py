#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""独立复算倒排索引, 与 MapReduce 输出逐词条比对, 并输出报告所需统计。"""
import re, sys, collections, os

DATA = "/tmp/exp2/data"
STOP = "/tmp/exp2/stopwords/stopwords.txt"
MR = "/tmp/exp2/out/index.txt"
REPORT = "/tmp/exp2/out/stats.txt"

stop = set()
with open(STOP, encoding="utf-8") as f:
    for line in f:
        w = line.strip().lower()
        if w:
            stop.add(w)

delim = re.compile(r"[^a-zA-Z]+")
idx = collections.defaultdict(dict)  # word -> {doc: count}
total_words = 0
docs = sorted(f for f in os.listdir(DATA) if f.endswith(".txt"))
for doc in docs:
    with open(os.path.join(DATA, doc), encoding="utf-8", errors="replace") as f:
        text = f.read().lower()
    for tok in delim.split(text):
        if len(tok) < 2 or tok in stop:
            continue
        idx[tok][doc] = idx[tok].get(doc, 0) + 1
        total_words += 1

expected = {}
for w, d in idx.items():
    parts = ["<%s,%d>;" % (doc, d[doc]) for doc in sorted(d)]
    expected[w] = "".join(parts) + "<total,%d>." % sum(d.values())

# 解析 MapReduce 结果
actual = {}
with open(MR, encoding="utf-8") as f:
    for line in f:
        line = line.rstrip("\n")
        if not line:
            continue
        k, v = line.split("\t", 1)
        actual[k] = v

out = []
out.append("文档数: %d" % len(docs))
out.append("被索引的单词总出现次数: %d" % total_words)
out.append("索引词条数(Python 复算): %d" % len(expected))
out.append("索引词条数(MapReduce): %d" % len(actual))
missing = set(expected) - set(actual)
extra = set(actual) - set(expected)
diff = [w for w in expected if w in actual and expected[w] != actual[w]]
out.append("缺失词条: %d" % len(missing))
out.append("多余词条: %d" % len(extra))
out.append("数值不一致词条: %d" % len(diff))
if missing:
    out.append("缺失示例: %s" % sorted(missing)[:5])
if extra:
    out.append("多余示例: %s" % sorted(extra)[:5])
if diff:
    out.append("不一致示例: %s" % [(w, expected[w], actual[w]) for w in diff[:3]])
out.append("")
out.append("=== Top 20 高频词(按 total 排序) ===")
top = sorted(idx.items(), key=lambda kv: -sum(kv[1].values()))[:20]
for w, d in top:
    out.append("%-14s 文档数=%2d  total=%6d  分布=%s" % (
        w, len(d), sum(d.values()),
        ";".join("%s,%d" % (k, d[k]) for k in sorted(d))))
out.append("")
out.append("=== 指定单词的倒排索引 ===")
for w in ["data", "hadoop", "index", "mapreduce", "search", "file", "system"]:
    out.append("%s\t%s" % (w, actual.get(w, "NOT FOUND")))
out.append("")
out.append("=== 停词过滤校验(应为 NOT FOUND) ===")
for w in ["the", "and", "of", "to", "a", "is", "it", "that"]:
    out.append("%s\t%s" % (w, actual.get(w, "NOT FOUND")))
out.append("")
out.append("=== 词条长度<2 校验(应为 NOT FOUND) ===")
for w in ["b", "c", "x"]:
    out.append("%s\t%s" % (w, actual.get(w, "NOT FOUND")))

with open(REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(out) + "\n")
print("\n".join(out))