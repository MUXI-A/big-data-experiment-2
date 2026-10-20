#!/bin/bash
# 实验二：文档倒排索引 —— 一键运行脚本
# 用法: bash run_experiment.sh
set -e

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

BASE=/user/hadoop/exp2
WORK=$(cd "$(dirname "$0")" && pwd)

echo "===== 1. 编译打包 ====="
rm -rf "$WORK/classes" "$WORK/invertedindex.jar"
mkdir -p "$WORK/classes"
javac -cp $(hadoop classpath) -d "$WORK/classes" "$WORK/src/InvertedIndex.java"
jar cf "$WORK/invertedindex.jar" -C "$WORK/classes" .
echo "打包完成: $WORK/invertedindex.jar"

echo "===== 2. 上传语料与停词表到 HDFS ====="
hdfs dfs -rm -r -f "$BASE" >/dev/null 2>&1 || true
hdfs dfs -mkdir -p "$BASE/input" "$BASE/stopwords"
hdfs dfs -put "$WORK"/data/document*.txt "$BASE/input/"
hdfs dfs -put "$WORK/stopwords/stopwords.txt" "$BASE/stopwords/"

echo "===== 3. 提交 MapReduce 作业（两阶段） ====="
hadoop jar "$WORK/invertedindex.jar" InvertedIndex \
    "$BASE/input" "$BASE/tmp" "$BASE/output" "$BASE/stopwords/stopwords.txt"

echo "===== 4. 输出倒排索引结果 ====="
OUT="$WORK/sample_output/result_full.txt"
mkdir -p "$WORK/sample_output"
hdfs dfs -cat "$BASE/output/part-r-00000" > "$OUT"
echo "索引词条总数: $(wc -l < "$OUT")"
echo "--- 前 8 条 ---"
head -8 "$OUT"
echo "--- 指定单词 ---"
for w in hadoop mapreduce index data search; do
    awk -F'\t' -v w="$w" '$1==w' "$OUT"
done
echo "===== 完成 ====="