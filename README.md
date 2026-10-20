# 实验二 文档倒排索引算法实现

山东大学 大数据管理与分析 课程实验二代码

学生：罗亚淞（202400390010，二班）

## 实验目标

使用 MapReduce 编程框架，对一组英文文档构建**倒排索引**（Inverted Index）：
对每个单词，统计它在各文档中出现的次数，并按实验要求输出形如

```
word	<document01.txt,6>;<document02.txt,3>;<document05.txt,2>;<total,11>.
```

的结果。

## 目录结构

```
├── src/
│   └── InvertedIndex.java      # 两阶段 MapReduce 倒排索引程序
├── stopwords/
│   └── stopwords.txt           # 停词表（182 个英文停词）
├── data/                       # 英文文档语料（15 篇）
│   ├── document01.txt ~ document12.txt   # Project Gutenberg 公共领域英文原著
│   ├── document13.txt                     # Apache Hadoop 官方文档 — MapReduce Tutorial
│   ├── document14.txt                     # Apache Hadoop 官方文档 — HDFS Architecture
│   └── document15.txt                     # Python 官方文档 — re 正则表达式模块
├── screenshots/                # 实验步骤截图（语料/停词表/编译打包/作业运行/索引结果）
├── sample_output/
│   └── index_sample.txt        # 倒排索引结果片段（前 20 条 + 指定单词）
├── verify_index.py             # 独立复算校验脚本（Python 实现，与 MapReduce 结果逐词条比对）
└── run_experiment.sh           # 一键运行脚本（编译 → 上传 HDFS → 运行 → 输出结果）
```

## 算法设计

程序 `InvertedIndex` 采用**两阶段（Two-Stage）MapReduce** 实现：

| 阶段 | Mapper | Reducer / Combiner | 输出 |
|------|--------|--------------------|------|
| 阶段一 | `IndexMapper`：读入文档 → 小写化 → 用非字母字符正则 `[^a-zA-Z]+` 分词 → 过滤停词与长度<2 的词 → 输出 `<word@docName, 1>` | `WordCountReducer`：同时用作 Combiner 与 Reducer，累加同一 `<word,docName>` 的词频 | `<word@docName, count>` |
| 阶段二 | `InvertedMapper`：用 `lastIndexOf("@")` 拆分 `<word@doc, count>` → 输出 `<word, "doc,count">` | `InvertedReducer`：同一单词的各文档词频按文档名排序，累加 `total` | `<word, <doc,count>;...;<total,N>.>` |

关键点：

- **分词**：英文以空格和标点分隔，直接用正则 `[^a-zA-Z]+` 切分，然后统一转小写。
- **停词过滤**：停词表从 HDFS 读取（通过 `Configuration` 传入 `stopwords.path`），在 Mapper 的 `setup()` 中一次性载入，过滤 `a / an / the` 等对检索意义小的词，同时过滤长度小于 2 的单词。
- **文档标识**：通过 `FileSplit.getPath().getName()` 取得当前文档文件名，作为倒排索引中的文档 ID。
- **Combiner 优化**：阶段一启用 Combiner（复用 Reducer 逻辑），Map 输出 462040 条记录经 Combine 压缩为 75293 条，显著减少 Shuffle 网络传输。
- **阶段二输入格式**：使用 `KeyValueTextInputFormat`，以 `@` 拼接的中间结果作为 key、词频作为 value。

## 运行环境

- Linux（Ubuntu 22.04）
- OpenJDK 1.8
- Hadoop 3.3.6（伪分布式）

## 运行方式

```bash
# 1. 编译打包
javac -cp $(hadoop classpath) -d classes src/InvertedIndex.java
jar cf invertedindex.jar -C classes .

# 2. 上传语料与停词表到 HDFS
hdfs dfs -mkdir -p /user/hadoop/exp2/input
hdfs dfs -put data/document*.txt /user/hadoop/exp2/input/
hdfs dfs -put stopwords/stopwords.txt /user/hadoop/exp2/stopwords/

# 3. 提交 MapReduce 作业（阶段一临时目录 tmp，阶段二最终目录 output）
hadoop jar invertedindex.jar InvertedIndex \
    /user/hadoop/exp2/input \
    /user/hadoop/exp2/tmp \
    /user/hadoop/exp2/output \
    /user/hadoop/exp2/stopwords/stopwords.txt

# 4. 查看倒排索引结果
hdfs dfs -cat /user/hadoop/exp2/output/part-r-00000
```

## 实验结果

- 文档数：**15** 篇；被索引的单词总出现次数：**462040**；索引词条数：**25536**
- 两阶段 MapReduce 作业均成功完成；**Python 独立复算与 MapReduce 输出逐词条比对：0 缺失、0 多余、0 数值不一致**（`verify_index.py`）。
- 指定单词的倒排索引结果（节选）：

```
data	    <document03.txt,10>;<document07.txt,2>;<document13.txt,16>;<document14.txt,75>;<document15.txt,2>;<total,105>.
hadoop	    <document13.txt,87>;<document14.txt,32>;<total,119>.
index	    <document03.txt,2>;<document07.txt,3>;<document15.txt,8>;<total,13>.
mapreduce	<document13.txt,112>;<document14.txt,3>;<total,115>.
search	    <document01.txt,2>;<document02.txt,1>;<document03.txt,11>;<document04.txt,7>;<document05.txt,6>;<document07.txt,27>;<document08.txt,4>;<document09.txt,6>;<document13.txt,2>;<document14.txt,1>;<document15.txt,64>;<total,131>.
```