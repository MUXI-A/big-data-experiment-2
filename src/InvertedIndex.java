import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 实验二: 文档倒排索引(Inverted Index)算法实现
 *
 * 采用两阶段 MapReduce:
 *   阶段一: 对每篇文档分词, 过滤停词, 统计 <单词, 文档> 的词频
 *           Mapper  : (word@docName, 1)
 *           Reducer : (word@docName, sum)
 *   阶段二: 按单词聚合各文档的词频, 汇总 total
 *           Mapper  : (word, docName,count)
 *           Reducer : (word, <doc1,count1>;<doc2,count2>;...;<total,N>.)
 *
 * 用法: hadoop jar invertedindex.jar InvertedIndex <input> <tmpOut> <finalOut> [stopwordsPath]
 */
public class InvertedIndex extends Configured implements Tool {

    /** 阶段一中间结果的 key 分隔符: word@docName */
    private static final String SEP = "@";

    /** 英文分词: 以非字母字符(空格、标点、数字等)作为分隔符 */
    private static final Pattern DELIMITER = Pattern.compile("[^a-zA-Z]+");

    /**
     * 阶段一 Mapper: 读入一篇文档, 逐行分词, 过滤停词与单字符词,
     * 输出 <单词@文档名, 1>
     */
    public static class IndexMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Text outKey = new Text();
        private final IntWritable one = new IntWritable(1);
        private final Set<String> stopwords = new HashSet<String>();
        private String docName = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 载入停词表(HDFS 上的文件)
            String stopwordPath = context.getConfiguration().get("stopwords.path", "");
            if (stopwordPath.length() > 0) {
                FileSystem fs = FileSystem.get(URI.create(stopwordPath), context.getConfiguration());
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(new Path(stopwordPath)), "UTF-8"));
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim().toLowerCase();
                    if (line.length() > 0) {
                        stopwords.add(line);
                    }
                }
                reader.close();
            }
            // 当前处理的文档名, 作为倒排索引中的文档标识
            FileSplit split = (FileSplit) context.getInputSplit();
            docName = split.getPath().getName();
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = DELIMITER.split(value.toString().toLowerCase());
            for (String token : tokens) {
                // 停词与长度小于 2 的词不建立索引
                if (token.length() < 2 || stopwords.contains(token)) {
                    continue;
                }
                outKey.set(token + SEP + docName);
                context.write(outKey, one);
            }
        }
    }

    /** 阶段一 Reducer/Combiner: 累加同一 <单词,文档> 的词频 */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /** 阶段二 Mapper: 把 <单词@文档, 词频> 拆成 <单词, 文档,词频> */
    public static class InvertedMapper extends Mapper<Text, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String compound = key.toString();
            int index = compound.lastIndexOf(SEP);
            if (index <= 0) {
                return;
            }
            outKey.set(compound.substring(0, index));
            outValue.set(compound.substring(index + 1) + "," + value.toString());
            context.write(outKey, outValue);
        }
    }

    /** 阶段二 Reducer: 汇成 <doc,count>;<doc,count>;...;<total,N>. */
    public static class InvertedReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> items = new ArrayList<String>();
            long total = 0L;
            for (Text value : values) {
                String item = value.toString();
                items.add(item);
                total += Long.parseLong(item.substring(item.lastIndexOf(',') + 1));
            }
            // 文档按名称排序, 便于阅读与核对
            Collections.sort(items);

            StringBuilder builder = new StringBuilder();
            for (String item : items) {
                int comma = item.lastIndexOf(',');
                builder.append("<").append(item.substring(0, comma)).append(",")
                        .append(item.substring(comma + 1)).append(">;");
            }
            builder.append("<total,").append(total).append(">.");
            outValue.set(builder.toString());
            context.write(key, outValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: InvertedIndex <input> <tmpOut> <finalOut> [stopwordsPath]");
            return 2;
        }
        String inputPath = args[0];
        String tempPath = args[1];
        String outputPath = args[2];
        String stopwordPath = args.length > 3 ? args[3] : "";

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(tempPath), true);
        fs.delete(new Path(outputPath), true);
        conf.set("stopwords.path", stopwordPath);

        // ---------- 阶段一: 统计每篇文档中每个单词的词频 ----------
        Job stageOne = Job.getInstance(conf, "inverted-index-stage-one");
        stageOne.setJarByClass(InvertedIndex.class);
        stageOne.setMapperClass(IndexMapper.class);
        stageOne.setCombinerClass(WordCountReducer.class);
        stageOne.setReducerClass(WordCountReducer.class);
        stageOne.setOutputKeyClass(Text.class);
        stageOne.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(stageOne, new Path(inputPath));
        FileOutputFormat.setOutputPath(stageOne, new Path(tempPath));
        if (!stageOne.waitForCompletion(true)) {
            return 1;
        }

        // ---------- 阶段二: 按单词汇总各文档词频, 输出倒排索引 ----------
        Job stageTwo = Job.getInstance(conf, "inverted-index-stage-two");
        stageTwo.setJarByClass(InvertedIndex.class);
        stageTwo.setMapperClass(InvertedMapper.class);
        stageTwo.setReducerClass(InvertedReducer.class);
        stageTwo.setInputFormatClass(KeyValueTextInputFormat.class);
        stageTwo.setOutputFormatClass(TextOutputFormat.class);
        stageTwo.setOutputKeyClass(Text.class);
        stageTwo.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(stageTwo, new Path(tempPath));
        FileOutputFormat.setOutputPath(stageTwo, new Path(outputPath));
        return stageTwo.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
        System.exit(exitCode);
    }
}